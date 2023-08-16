# This file is part of rubintv_production.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import time
import logging
import numpy as np

import lsst.summit.utils.butlerUtils as butlerUtils
from astro_metadata_translator import ObservationInfo
from lsst.obs.lsst.translators.lsst import FILTER_DELIMITER
from lsst.utils.iteration import ensure_iterable

from lsst.utils import getPackageDir
from lsst.pipe.tasks.characterizeImage import CharacterizeImageTask, CharacterizeImageConfig
from lsst.pipe.tasks.calibrate import CalibrateTask, CalibrateConfig
from lsst.meas.algorithms import ReferenceObjectLoader
import lsst.daf.butler as dafButler
from lsst.obs.base import DefineVisitsConfig, DefineVisitsTask
from lsst.pipe.base import Instrument
from lsst.pipe.tasks.postprocess import ConsolidateVisitSummaryTask, MakeCcdVisitTableTask

from lsst.summit.utils.bestEffort import BestEffortIsr
from lsst.summit.utils.imageExaminer import ImageExaminer
from lsst.summit.utils.spectrumExaminer import SpectrumExaminer
from lsst.summit.utils.utils import getCurrentDayObs_int
from lsst.summit.utils.tmaUtils import (TMAEventMaker,
                                        plotEvent,
                                        getCommandsDuringEvent,
                                        getAzimuthElevationDataForEvent,
                                        )
from lsst.summit.utils.efdUtils import clipDataToEvent

from lsst.atmospec.utils import isDispersedDataId, isDispersedExp
from lsst.summit.utils import NightReport

from lsst.rubintv.production.mountTorques import (calculateMountErrors, MOUNT_IMAGE_WARNING_LEVEL,
                                                  MOUNT_IMAGE_BAD_LEVEL)
from lsst.rubintv.production.monitorPlotting import plotExp
from .utils import writeMetadataShard, expRecordToUploadFilename, raiseIf, hasDayRolledOver, catchPrintOutput
from .uploaders import Uploader, Heartbeater
from .baseChannels import BaseButlerChannel
from .exposureLogUtils import getLogsForDayObs, LOG_ITEM_MAPPINGS
from .plotting import latissNightReportPlots
from .metadataServers import TimedMetadataServer


__all__ = (
    'SfmRunner'
)

_LOG = logging.getLogger(__name__)


class SfmRunner(BaseButlerChannel):
    """Class to run single frame measurement for a set of CCDs in the focal
    plane.

    Runs isr via BestEffortIsr, puts the quickLookExp, then moves on to running
    CharacterizeImageTask and CalibrateTask all in-memory, putting the srcCat
    and CcdVisitSummary.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(self, locationConfig, instrument, detectors, *, embargo=False, doRaise=False):
        self.detectors = list(ensure_iterable(detectors))
        self.bestEffort = BestEffortIsr(embargo=embargo)
        # do we actually need writeable butlers? Could a head node do the visit
        # definition instead? Does it actually matter?
        self.butler = XXX  # need these butlers to be writeable here

        super().__init__(locationConfig=locationConfig,
                         instrument=instrument,
                         butler=self.bestEffort.butler,
                         dataProduct='raw',
                         channelName='auxtel_isr_runner',
                         doRaise=doRaise)

        # TODO DM-37272 need to get the collection name from a central place
        # XXX need runs for the new instrument
        self.outputRunName = f"{self.instrument}/runs/quickLook/1"

        # This will all need changing after the great calibration refactor
        config = CharacterizeImageConfig()
        basicConfig = CharacterizeImageConfig()
        obs_lsst = getPackageDir("obs_lsst")
        config.load(os.path.join(obs_lsst, "config", "characterizeImage.py"))
        config.load(os.path.join(obs_lsst, "config", "latiss", "characterizeImage.py"))
        config.measurement = basicConfig.measurement

        config.doApCorr = False
        config.doDeblend = False
        self.charImage = CharacterizeImageTask(config=config)

        config = CalibrateConfig()
        basicConfig = CalibrateConfig()
        config.load(os.path.join(obs_lsst, "config", "calibrate.py"))
        config.load(os.path.join(obs_lsst, "config", "latiss", "calibrate.py"))
        config.measurement = basicConfig.measurement

        # TODO DM-37426 add some more overrides to speed up runtime
        config.doApCorr = False
        config.doDeblend = False

        self.calibrate = CalibrateTask(config=config, icSourceSchema=self.charImage.schema)

    def _getRefObjLoader(self, refcatName, dataId, config):
        """Construct a referenceObjectLoader for a given refcat

        Parameters
        ----------
        refcatName : `str`
            Name of the reference catalog to load.
        dataId : `dict` or `lsst.daf.butler.DataCoordinate`
            DataId to determine bounding box of sources to load.
        config : `lsst.meas.algorithms.LoadReferenceObjectsConfig`
            Configuration for the reference object loader.

        Returns
        -------
        loader : `lsst.meas.algorithms.ReferenceObjectLoader`
            The object loader.
        """
        refs = self.butler.registry.queryDatasets(refcatName, dataId=dataId).expanded()
        # generator not guaranteed to yield in the same order every iteration
        # therefore critical to materialize a list before iterating twice
        refs = list(refs)
        handles = [dafButler.DeferredDatasetHandle(butler=self.butler, ref=ref, parameters=None)
                   for ref in refs]
        dataIds = [ref.dataId for ref in refs]

        loader = ReferenceObjectLoader(
            dataIds,
            handles,
            name=refcatName,
            log=self.log,
            config=config
        )
        return loader

    def doProcessImage(self, expRecord):
        """Determine if we should skip this image.

        Should take responsibility for logging the reason for skipping.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.

        Returns
        -------
        doProcess : `bool`
            True if the image should be processed, False if we should skip it.
        """
        # XXX Do we want to do it like this, or should the head node be in
        # charge? Probably the head node now, given that's how work will be
        # being farmed out, but perhaps this can still act as a veto??
        if expRecord.observation_type != 'science':
            if expRecord.science_program == 'CWFS' and expRecord.exposure_time == 5:
                self.log.info('Processing 5s post-CWFS image as a special case')
                return True
            self.log.info(f"Skipping non-science-type exposure {expRecord.observation_type}")
            return False
        return True

    def callback(self, expRecord):
        """Method called on each new expRecord as it is found in the repo.

        Runs on the quickLookExp and writes shards with various measured
        quantities, as calculated by the CharacterizeImageTask and
        CalibrateTask.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        for detector in self.detectors:
            try:
                # XXX TODO: Deal with caching in bestEffortIsr for multi-CCD
                # mode cache several calibs? Or invalidate and get each time?
                # Depends on: the number of detectors, the memory required,
                # calibrate's memory usage, pod sizing, how long calib loading
                # *actually* takes
                dataId = butlerUtils.updateDataId(expRecord.dataId, detector=detector)
                # XXX check that setting detector in the dataId in the way that
                # util does actually works to then *not* pass it to bestEffort
                exp = self.bestEffort.getExposure(dataId)  # noqa: F841 - automatically puts
                self.log.info(f'Put quickLookExp for {dataId} {detector=}')

                if not self.doProcessImage(expRecord):
                    return

                tStart = time.time()
                self.log.info(f'Running Image Characterization for {dataId}')

                if not exp:
                    raise RuntimeError(f'Failed to get {self.dataProduct} for {dataId}')

                # TODO DM-37427 dispersed images do not have a filter and fail
                if isDispersedExp(exp):
                    self.log.info(f'Skipping dispersed image: {dataId}')
                    return

                visitDataId = self.getVisitDataId(expRecord)
                if not visitDataId:
                    self.defineVisit(expRecord)
                    visitDataId = self.getVisitDataId(expRecord)

                loader = self._getRefObjLoader(self.calibrate.config.connections.astromRefCat, visitDataId,
                                               config=self.calibrate.config.astromRefObjLoader)
                self.calibrate.astrometry.setRefObjLoader(loader)
                loader = self._getRefObjLoader(self.calibrate.config.connections.photoRefCat, visitDataId,
                                               config=self.calibrate.config.photoRefObjLoader)
                self.calibrate.photoCal.match.setRefObjLoader(loader)

                charRes = self.charImage.run(exp)
                tCharacterize = time.time()
                self.log.info(f"Ran characterizeImageTask in {tCharacterize-tStart:.2f} seconds")

                nSources = len(charRes.sourceCat)
                dayObs = butlerUtils.getDayObs(expRecord)
                seqNum = butlerUtils.getSeqNum(expRecord)
                outputDict = {"50-sigma source count": nSources}
                # flag as measured to color the cells in the table
                labels = {"_" + k: "measured" for k in outputDict.keys()}
                outputDict.update(labels)

                mdDict = {seqNum: outputDict}
                writeMetadataShard(self.locationConfig.auxTelMetadataShardPath, dayObs, mdDict)

                calibrateRes = self.calibrate.run(charRes.exposure,
                                                background=charRes.background,
                                                icSourceCat=charRes.sourceCat)
                tCalibrate = time.time()
                self.log.info(f"Ran calibrateTask in {tCalibrate-tCharacterize:.2f} seconds")

                summaryStats = calibrateRes.outputExposure.getInfo().getSummaryStats()
                pixToArcseconds = calibrateRes.outputExposure.getWcs().getPixelScale().asArcseconds()
                SIGMA2FWHM = np.sqrt(8 * np.log(2))
                e1 = (summaryStats.psfIxx - summaryStats.psfIyy) / (summaryStats.psfIxx + summaryStats.psfIyy)
                e2 = 2*summaryStats.psfIxy / (summaryStats.psfIxx + summaryStats.psfIyy)

                outputDict = {
                    '5-sigma source count': len(calibrateRes.outputCat),
                    'PSF FWHM': summaryStats.psfSigma * SIGMA2FWHM * pixToArcseconds,
                    'PSF e1': e1,
                    'PSF e2': e2,
                    'Sky mean': summaryStats.skyBg,
                    'Sky RMS': summaryStats.skyNoise,
                    'Variance plane mean': summaryStats.meanVar,
                    'PSF star count': summaryStats.nPsfStar,
                    'Astrometric bias': summaryStats.astromOffsetMean,
                    'Astrometric scatter': summaryStats.astromOffsetStd,
                    'Zeropoint': summaryStats.zeroPoint
                }

                # flag all these as measured items to color the cell
                labels = {"_" + k: "measured" for k in outputDict.keys()}
                outputDict.update(labels)

                mdDict = {seqNum: outputDict}
                writeMetadataShard(self.locationConfig.auxTelMetadataShardPath, dayObs, mdDict)
                self.log.info(f'Wrote metadata shard. Putting calexp for {dataId}')
                self.clobber(calibrateRes.outputExposure, "calexp", visitDataId)
                tFinal = time.time()
                self.log.info(f"Ran characterizeImage and calibrate in {tFinal-tStart:.2f} seconds")

                tVisitInfoStart = time.time()
                self.putVisitSummary(visitDataId)
                self.log.info(f"Put the visit info summary in {time.time()-tVisitInfoStart:.2f} seconds")

                del exp
                del charRes
                del calibrateRes

            except Exception as e:
                raiseIf(self.doRaise, e, self.log)

    def defineVisit(self, expRecord):
        """Define a visit in the registry, given an expRecord.

        Note that this takes about 9ms regardless of whether it exists, so it
        is no quicker to check than just run the define call.

        NB: butler must be writeable for this to work.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record to define the visit for.
        """
        instr = Instrument.from_string(self.butler.registry.defaults.dataId['instrument'],
                                       self.butler.registry)
        config = DefineVisitsConfig()
        instr.applyConfigOverrides(DefineVisitsTask._DefaultName, config)

        task = DefineVisitsTask(config=config, butler=self.butler)

        task.run([{'exposure': expRecord.id}], collections=self.butler.collections)

    def getVisitDataId(self, expRecord):
        """Lookup visitId for an expRecord or dataId containing an exposureId
        or other uniquely identifying keys such as dayObs and seqNum.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record for which to get the visit id.

        Returns
        -------
        visitDataId : `lsst.daf.butler.DataCoordinate`
            Data Id containing a visitId.
        """
        expIdDict = {'exposure': expRecord.id}
        visitDataIds = self.butler.registry.queryDataIds(["visit", "detector"], dataId=expIdDict)
        visitDataIds = list(set(visitDataIds))
        if len(visitDataIds) == 1:
            visitDataId = visitDataIds[0]
            return visitDataId
        else:
            self.log.warning(f"Failed to find visitId for {expIdDict}, got {visitDataIds}. Do you need to run"
                             " define-visits?")
            return None

    def clobber(self, object, datasetType, visitDataId):
        """Put object in the butler.

        If there is one already there, remove it beforehand.

        Parameters
        ----------
        object : `object`
            Any object to put in the butler.
        datasetType : `str`
            Dataset type name to put it as.
        visitDataId : `lsst.daf.butler.DataCoordinate`
            The data coordinate record of the exposure to put. Must contain the
            visit id.
        """
        self.butler.registry.registerRun(self.outputRunName)
        if butlerUtils.datasetExists(self.butler, datasetType, visitDataId):
            self.log.warning(f'Overwriting existing {datasetType} for {visitDataId}')
            dRef = self.butler.registry.findDataset(datasetType, visitDataId)
            self.butler.pruneDatasets([dRef], disassociate=True, unstore=True, purge=True)
        self.butler.put(object, datasetType, dataId=visitDataId, run=self.outputRunName)
        self.log.info(f'Put {datasetType} for {visitDataId}')

    def putVisitSummary(self, visitId):
        """Create and butler.put the visitSummary for this visit.

        Note that this only works like this while we have a single detector.

        Note: the whole method takes ~0.25s so it is probably not worth
        cluttering the class with the ConsolidateVisitSummaryTask at this
        point, though it could be done.

        Parameters
        ----------
        visitId : `lsst.daf.butler.DataCoordinate`
            The visit id to create and put the visitSummary for.
        """
        dRefs = list(self.butler.registry.queryDatasets('calexp',
                                                        dataId=visitId,
                                                        collections=self.outputRunName).expanded())
        if len(dRefs) != 1:
            raise RuntimeError(f'Found {len(dRefs)} calexps for {visitId} and it should have exactly 1')

        ddRef = self.butler.getDirectDeferred(dRefs[0])
        visit = ddRef.dataId.byName()['visit']  # this is a raw int
        consolidateTask = ConsolidateVisitSummaryTask()  # if this ctor is slow move to class
        expCatalog = consolidateTask._combineExposureMetadata(visit, [ddRef])
        self.clobber(expCatalog, 'visitSummary', visitId)
        return
