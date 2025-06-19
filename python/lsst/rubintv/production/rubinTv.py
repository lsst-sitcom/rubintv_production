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

from __future__ import annotations

import json
import logging
import os
import time

import numpy as np
import pandas as pd

import lsst.daf.butler as dafButler
import lsst.summit.utils.butlerUtils as butlerUtils
from lsst.meas.algorithms import ReferenceObjectLoader
from lsst.obs.base import DefineVisitsConfig, DefineVisitsTask
from lsst.pipe.base import Instrument
from lsst.pipe.tasks.calibrate import CalibrateConfig, CalibrateTask
from lsst.pipe.tasks.characterizeImage import CharacterizeImageConfig, CharacterizeImageTask
from lsst.pipe.tasks.postprocess import ConsolidateVisitSummaryTask, MakeCcdVisitTableTask
from lsst.utils import getPackageDir

try:
    from lsst_efd_client import EfdClient  # noqa: F401 just check we have it, but don't use it

    HAS_EFD_CLIENT = True
except ImportError:
    HAS_EFD_CLIENT = False

from lsst.atmospec.utils import isDispersedExp
from lsst.summit.utils import NightReport
from lsst.summit.utils.utils import getCurrentDayObs_int

from .baseChannels import BaseButlerChannel
from .plotting import latissNightReportPlots
from .utils import NumpyEncoder, catchPrintOutput, hasDayRolledOver, raiseIf, writeMetadataShard

__all__ = [
    "CalibrateCcdRunner",
    "NightReportChannel",
]

_LOG = logging.getLogger(__name__)


class CalibrateCcdRunner(BaseButlerChannel):
    """Class for running CharacterizeImageTask and CalibrateTasks on images.

    Runs these tasks and writes shards with various measured quantities for
    upload to the table.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(self, locationConfig, instrument, *, embargo=False, doRaise=False):
        super().__init__(
            locationConfig=locationConfig,
            instrument=instrument,
            # writeable true is required to define visits
            butler=butlerUtils.makeDefaultLatissButler(
                extraCollections=["refcats/DM-42295"], embargo=embargo, writeable=True
            ),
            detectors=0,
            watcherType="file",
            dataProduct="quickLookExp",
            channelName="auxtel_calibrateCcd",
            doRaise=doRaise,
        )
        self.detector = 0
        # TODO DM-37272 need to get the collection name from a central place
        self.outputRunName = "LATISS/runs/quickLook/1"

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
        config.load(os.path.join(obs_lsst, "config", "calibrate.py"))
        config.load(os.path.join(obs_lsst, "config", "latiss", "calibrate.py"))

        # restrict to basic set of plugins
        config.measurement.plugins.names = [
            "base_CircularApertureFlux",
            "base_PsfFlux",
            "base_NaiveCentroid",
            "base_CompensatedGaussianFlux",
            "base_LocalBackground",
            "base_SdssCentroid",
            "base_SdssShape",
            "base_Variance",
            "base_Jacobian",
            "base_PixelFlags",
            "base_GaussianFlux",
            "base_SkyCoord",
            "base_FPPosition",
            "base_ClassificationSizeExtendedness",
        ]
        config.measurement.slots.shape = "base_SdssShape"
        config.measurement.slots.psfShape = "base_SdssShape_psf"
        # TODO DM-37426 add some more overrides to speed up runtime
        config.doApCorr = False
        config.doDeblend = False
        config.astrometry.sourceSelector["science"].doRequirePrimary = False
        config.astrometry.sourceSelector["science"].doIsolated = False

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
        handles = [
            dafButler.DeferredDatasetHandle(butler=self.butler, ref=ref, parameters=None) for ref in refs
        ]
        dataIds = [ref.dataId for ref in refs]

        loader = ReferenceObjectLoader(dataIds, handles, name=refcatName, log=self.log, config=config)
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
        if expRecord.observation_type != "science":
            if expRecord.science_program == "CWFS" and expRecord.exposure_time == 5:
                self.log.info("Processing 5s post-CWFS image as a special case")
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
        try:
            if not self.doProcessImage(expRecord):
                return

            dataId = butlerUtils.updateDataId(expRecord.dataId, detector=self.detector)
            tStart = time.time()

            self.log.info(f"Running Image Characterization for {dataId}")
            exp = self._waitForDataProduct(dataId)

            if not exp:
                raise RuntimeError(f"Failed to get {self.dataProduct} for {dataId}")

            # TODO DM-37427 dispersed images do not have a filter and fail
            if isDispersedExp(exp):
                self.log.info(f"Skipping dispersed image: {dataId}")
                return

            visitDataId = self.getVisitDataId(expRecord)
            if not visitDataId:
                self.defineVisit(expRecord)
                visitDataId = self.getVisitDataId(expRecord)

            loader = self._getRefObjLoader(
                self.calibrate.config.connections.astromRefCat,
                visitDataId,
                config=self.calibrate.config.astromRefObjLoader,
            )
            self.calibrate.astrometry.setRefObjLoader(loader)
            loader = self._getRefObjLoader(
                self.calibrate.config.connections.photoRefCat,
                visitDataId,
                config=self.calibrate.config.photoRefObjLoader,
            )
            self.calibrate.photoCal.match.setRefObjLoader(loader)

            charRes = self.charImage.run(exp)
            tCharacterize = time.time()
            self.log.info(f"Ran characterizeImageTask in {tCharacterize - tStart:.2f} seconds")

            nSources = len(charRes.sourceCat)
            dayObs = butlerUtils.getDayObs(expRecord)
            seqNum = butlerUtils.getSeqNum(expRecord)
            outputDict = {"50-sigma source count": nSources}
            # flag as measured to color the cells in the table
            labels = {"_" + k: "measured" for k in outputDict.keys()}
            outputDict.update(labels)

            mdDict = {seqNum: outputDict}
            writeMetadataShard(self.locationConfig.auxTelMetadataShardPath, dayObs, mdDict)

            calibrateRes = self.calibrate.run(
                charRes.exposure, background=charRes.background, icSourceCat=charRes.sourceCat
            )
            tCalibrate = time.time()
            self.log.info(f"Ran calibrateTask in {tCalibrate - tCharacterize:.2f} seconds")

            summaryStats = calibrateRes.outputExposure.getInfo().getSummaryStats()
            pixToArcseconds = calibrateRes.outputExposure.getWcs().getPixelScale().asArcseconds()
            SIGMA2FWHM = np.sqrt(8 * np.log(2))
            e1 = (summaryStats.psfIxx - summaryStats.psfIyy) / (summaryStats.psfIxx + summaryStats.psfIyy)
            e2 = 2 * summaryStats.psfIxy / (summaryStats.psfIxx + summaryStats.psfIyy)

            outputDict = {
                "5-sigma source count": len(calibrateRes.outputCat),
                "PSF FWHM": summaryStats.psfSigma * SIGMA2FWHM * pixToArcseconds,
                "PSF e1": e1,
                "PSF e2": e2,
                "Sky mean": summaryStats.skyBg,
                "Sky RMS": summaryStats.skyNoise,
                "Variance plane mean": summaryStats.meanVar,
                "PSF star count": summaryStats.nPsfStar,
                "Astrometric bias": summaryStats.astromOffsetMean,
                "Astrometric scatter": summaryStats.astromOffsetStd,
                "Zeropoint": summaryStats.zeroPoint,
            }

            # flag all these as measured items to color the cell
            labels = {"_" + k: "measured" for k in outputDict.keys()}
            outputDict.update(labels)

            mdDict = {seqNum: outputDict}
            writeMetadataShard(self.locationConfig.auxTelMetadataShardPath, dayObs, mdDict)
            self.log.info(f"Wrote metadata shard. Putting preliminary_visit_image for {dataId}")
            self.clobber(calibrateRes.outputExposure, "preliminary_visit_image", visitDataId)
            tFinal = time.time()
            self.log.info(f"Ran characterizeImage and calibrate in {tFinal - tStart:.2f} seconds")

            tVisitInfoStart = time.time()
            self.putVisitSummary(visitDataId)
            self.log.info(f"Put the visit info summary in {time.time() - tVisitInfoStart:.2f} seconds")

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
        instr = Instrument.from_string(
            self.butler.registry.defaults.dataId["instrument"], self.butler.registry
        )
        config = DefineVisitsConfig()
        instr.applyConfigOverrides(DefineVisitsTask._DefaultName, config)

        task = DefineVisitsTask(config=config, butler=self.butler)

        task.run([{"exposure": expRecord.id}], collections=self.butler.collections)

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
        expIdDict = {"exposure": expRecord.id}
        visitDataIds = self.butler.registry.queryDataIds(["visit", "detector"], dataId=expIdDict)
        visitDataIds = list(set(visitDataIds))
        if len(visitDataIds) == 1:
            visitDataId = visitDataIds[0]
            return visitDataId
        else:
            self.log.warning(
                f"Failed to find visitId for {expIdDict}, got {visitDataIds}. Do you need to run"
                " define-visits?"
            )
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
            self.log.warning(f"Overwriting existing {datasetType} for {visitDataId}")
            dRef = self.butler.registry.findDataset(datasetType, visitDataId)
            self.butler.pruneDatasets([dRef], disassociate=True, unstore=True, purge=True)
        self.butler.put(object, datasetType, dataId=visitDataId, run=self.outputRunName)
        self.log.info(f"Put {datasetType} for {visitDataId}")

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
        dRefs = list(
            self.butler.registry.queryDatasets(
                "preliminary_visit_image", dataId=visitId, collections=self.outputRunName
            ).expanded()
        )
        if len(dRefs) != 1:
            raise RuntimeError(
                f"Found {len(dRefs)} preliminary_visit_image for {visitId} and it should have exactly 1"
            )

        ddRef = self.butler.getDeferred(dRefs[0])
        visit = ddRef.dataId.byName()["visit"]  # this is a raw int
        consolidateTask = ConsolidateVisitSummaryTask()  # if this ctor is slow move to class
        expCatalog = consolidateTask._combineExposureMetadata(visit, [ddRef])
        self.clobber(expCatalog, "preliminary_visit_summary", visitId)
        return


class NightReportChannel(BaseButlerChannel):
    """Class for running the AuxTel Night Report channel on RubinTV.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    dayObs : `int`, optional
        The dayObs. If not provided, will be calculated from the current time.
        This should be supplied manually if running catchup or similar, but
        when running live it will be set automatically so that the current day
        is processed.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(self, locationConfig, instrument, *, dayObs=None, embargo=False, doRaise=False):
        super().__init__(
            locationConfig=locationConfig,
            instrument=instrument,
            butler=butlerUtils.makeDefaultLatissButler(embargo=embargo),
            detectors=0,
            watcherType="file",
            dataProduct="quickLookExp",
            channelName="auxtel_night_reports",
            doRaise=doRaise,
        )

        # we update when the quickLookExp lands, but we scrape for everything,
        # updating the CcdVisitSummaryTable in the hope that the
        # CalibrateCcdRunner is producing. Because that takes longer to run,
        # this means the summary table is often a visit behind, but the only
        # alternative is to block on waiting for preliminary_visit_images,
        # which, if images fail/aren't attempted to be produced, would result
        # in no update at all.
        #
        # This solution is fine as long as there is an end-of-night
        # finalization step to catch everything in the end, and this is
        # easily achieved as we need to reinstantiate a report as each day
        # rolls over anyway.

        self.dayObs = dayObs if dayObs else getCurrentDayObs_int()

        # always attempt to resume on init
        saveFile = self.getSaveFile()
        if os.path.isfile(saveFile):
            self.log.info(f"Resuming from {saveFile}")
            self.report = NightReport(self.butler, self.dayObs, saveFile)
            self.report.rebuild()
        else:  # otherwise start a new report from scratch
            self.report = NightReport(self.butler, self.dayObs)

    def finalizeDay(self):
        """Perform the end of day actions and roll the day over.

        Creates a final version of the plots at the end of the day, starts a
        new NightReport object, and rolls ``self.dayObs`` over.
        """
        self.log.info(f"Creating final plots for {self.dayObs}")
        self.createPlotsAndUpload()
        # TODO: add final plotting of plots which live in the night reporter
        # class here somehow, perhaps by moving them to their own plot classes.

        self.dayObs = getCurrentDayObs_int()
        self.saveFile = self.getSaveFile()
        self.log.info(f"Starting new report for dayObs {self.dayObs}")
        self.report = NightReport(self.butler, self.dayObs)
        return

    def getSaveFile(self):
        return os.path.join(self.locationConfig.nightReportPath, f"report_{self.dayObs}.pickle")

    def getMetadataTableContents(self):
        """Get the measured data for the current night.

        Returns
        -------
        mdTable : `pandas.DataFrame`
            The contents of the metdata table from the front end.
        """
        # TODO: need to find a better way of getting this path ideally,
        # but perhaps is OK?
        sidecarFilename = os.path.join(self.locationConfig.auxTelMetadataPath, f"dayObs_{self.dayObs}.json")

        try:
            mdTable = pd.read_json(sidecarFilename).T
            mdTable = mdTable.sort_index()
        except Exception as e:
            self.log.warning(f"Failed to load metadata table from {sidecarFilename}: {e}")
            return None

        if mdTable.empty:
            return None

        return mdTable

    def createCcdVisitTable(self, dayObs):
        """Make the consolidated visit summary table for the given dayObs.

        Parameters
        ----------
        dayObs : `int`
            The dayObs.

        Returns
        -------
        visitSummaryTableOutputCatalog : `pandas.DataFrame` or `None`
            The visit summary table for the dayObs.
        """
        visitSummaries = self.butler.registry.queryDatasets(
            "visitSummary",
            where="visit.day_obs=dayObs",
            bind={"dayObs": dayObs},
            collections=["LATISS/runs/quickLook/1"],
        ).expanded()
        visitSummaries = list(visitSummaries)
        if len(visitSummaries) == 0:
            self.log.warning(f"Found no visitSummaries for dayObs {dayObs}")
            return None
        self.log.info(f"Found {len(visitSummaries)} visitSummaries for dayObs {dayObs}")
        ddRefs = [self.butler.getDeferred(vs) for vs in visitSummaries]
        task = MakeCcdVisitTableTask()
        table = task.run(ddRefs)
        return table.outputCatalog

    def createPlotsAndUpload(self):
        """Create and upload all plots defined in nightReportPlots.

        All plots defined in __all__ in nightReportPlots are discovered,
        created and uploaded. If any fail, the exception is logged and the next
        plot is created and uploaded.
        """
        md = self.getMetadataTableContents()
        report = self.report
        ccdVisitTable = self.createCcdVisitTable(self.dayObs)
        self.log.info(
            f"Creating plots for dayObs {self.dayObs} with: "
            f"{len(report.data)} items in the night report, "
            f"{0 if md is None else len(md)} items in the metadata table, and "
            f"{0 if ccdVisitTable is None else len(ccdVisitTable)} items in the ccdVisitTable."
        )

        for plotName in latissNightReportPlots.PLOT_FACTORIES:
            try:
                self.log.info(f"Creating plot {plotName}")
                plotFactory = getattr(latissNightReportPlots, plotName)
                plot = plotFactory(
                    dayObs=self.dayObs,
                    locationConfig=self.locationConfig,
                    s3Uploader=self.s3Uploader,
                )
                plot.createAndUpload(report, md, ccdVisitTable)
            except Exception:
                self.log.exception(f"Failed to create plot {plotName}")
                continue

    def callback(self, expRecord, doCheckDay=True):
        """Method called on each new expRecord as it is found in the repo.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record for the latest data.
        doCheckDay : `bool`, optional
            Whether to check if the day has rolled over. This should be left as
            True for normal operation, but set to False when manually running
            on past exposures to save triggering on the fact it is no longer
            that day, e.g. during testing or doing catch-up/backfilling.
        """
        dataId = expRecord.dataId
        md = {}
        try:
            if doCheckDay and hasDayRolledOver(self.dayObs):
                self.log.info(f"Day has rolled over, finalizing report for dayObs {self.dayObs}")
                self.finalizeDay()

            else:
                self.report.rebuild()
                self.report.save(self.getSaveFile())  # save on each call, it's quick and allows resuming

                # make plots here, uploading one by one
                # make all the automagic plots from nightReportPlots.py
                self.createPlotsAndUpload()

                # plots which come from the night report object itself:
                # the per-object airmass plot
                airMassPlotFile = os.path.join(self.locationConfig.nightReportPath, "airmass.png")
                self.report.plotPerObjectAirMass(saveFig=airMassPlotFile)
                self.s3Uploader.uploadNightReportData(
                    instrument="auxtel",
                    dayObs=self.dayObs,
                    filename=airMassPlotFile,
                    plotGroup="Coverage",
                    uploadAs="airmass.png",
                )

                # the alt/az coverage polar plot
                altAzCoveragePlotFile = os.path.join(self.locationConfig.nightReportPath, "alt-az.png")
                self.report.makeAltAzCoveragePlot(saveFig=altAzCoveragePlotFile)
                self.s3Uploader.uploadNightReportData(
                    instrument="auxtel",
                    dayObs=self.dayObs,
                    filename=altAzCoveragePlotFile,
                    plotGroup="Coverage",
                    uploadsAs="alt-az.png",
                )

                # Add text items here
                shutterTimes = catchPrintOutput(self.report.printShutterTimes)
                md["text_010"] = shutterTimes

                obsGaps = catchPrintOutput(self.report.printObsGaps)
                md["text_020"] = obsGaps

                # Upload the text here
                # Note this file must be called md.json because this filename
                # is used for the upload, and that's what the frontend expects
                jsonFilename = os.path.join(self.locationConfig.nightReportPath, "md.json")
                with open(jsonFilename, "w") as f:
                    json.dump(md, f, cls=NumpyEncoder)
                self.s3Uploader.uploadNightReportData(
                    instrument="auxtel",
                    dayObs=self.dayObs,
                    filename=jsonFilename,
                    isMetadataFile=True,
                )

                self.log.info(f"Finished updating plots and table for {dataId}")

        except Exception as e:
            msg = f"Skipped updating the night report for {dataId}:"
            raiseIf(self.doRaise, e, self.log, msg=msg)
