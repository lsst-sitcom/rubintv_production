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
import tempfile
import numpy as np
import matplotlib.pyplot as plt

import lsst.summit.utils.butlerUtils as butlerUtils
from astro_metadata_translator import ObservationInfo
from lsst.obs.lsst.translators.lsst import FILTER_DELIMITER

from lsst.utils import getPackageDir
from lsst.pipe.tasks.characterizeImage import CharacterizeImageTask, CharacterizeImageConfig
from lsst.pipe.tasks.calibrate import CalibrateTask, CalibrateConfig
from lsst.meas.algorithms import ReferenceObjectLoader
import lsst.daf.butler as dafButler
from lsst.obs.base import DefineVisitsConfig, DefineVisitsTask
from lsst.pipe.base import Instrument

try:
    from lsst_efd_client import EfdClient
    HAS_EFD_CLIENT = True
except ImportError:
    HAS_EFD_CLIENT = False

from lsst.summit.utils.bestEffort import BestEffortIsr
from lsst.summit.utils.imageExaminer import ImageExaminer
from lsst.summit.utils.spectrumExaminer import SpectrumExaminer

from lsst.atmospec.utils import isDispersedDataId, isDispersedExp

from lsst.rubintv.production.mountTorques import (calculateMountErrors, MOUNT_IMAGE_WARNING_LEVEL,
                                                  MOUNT_IMAGE_BAD_LEVEL)
from lsst.rubintv.production.monitorPlotting import plotExp
from .utils import writeMetadataShard, expRecordToUploadFilename, raiseIf
from .uploaders import Uploader, Heartbeater
from .baseChannels import BaseButlerChannel

__all__ = [
    'IsrRunner',
    'ImExaminerChannel',
    'SpecExaminerChannel',
    'MonitorChannel',
    'MountTorqueChannel',
    'MetadataCreator',
    'Uploader',
    'Heartbeater',
    'CalibrateCcdRunner',
]


_LOG = logging.getLogger(__name__)

SIDECAR_KEYS_TO_REMOVE = ['instrument',
                          'obs_id',
                          'seq_start',
                          'seq_end',
                          'group_name',
                          'has_simulated',
                          ]

# The values here are used in HTML so do not include periods in them, eg "Dec."
MD_NAMES_MAP = {"id": 'Exposure id',
                "exposure_time": 'Exposure time',
                "dark_time": 'Darktime',
                "observation_type": 'Image type',
                "observation_reason": 'Observation reason',
                "day_obs": 'dayObs',
                "seq_num": 'seqNum',
                "group_id": 'Group id',
                "target_name": 'Target',
                "science_program": 'Science program',
                "tracking_ra": 'RA',
                "tracking_dec": 'Dec',
                "sky_angle": 'Sky angle',
                "azimuth": 'Azimuth',
                "zenith_angle": 'Zenith angle',
                "time_begin_tai": 'TAI',
                "filter": 'Filter',
                "disperser": 'Disperser',
                "airmass": 'Airmass',
                "focus_z": 'Focus-Z',
                "seeing": 'DIMM Seeing',
                "altitude": 'Altitude',
                }


class IsrRunner(BaseButlerChannel):
    """Class to run isr for each image that lands in the repo.

    Note: this is currently AuxTel-only.

    Runs isr via BestEffortIsr, and puts the result in the quickLook
    collection.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """
    def __init__(self, locationConfig, *, embargo=False, doRaise=False):
        self.bestEffort = BestEffortIsr(embargo=embargo)
        super().__init__(locationConfig=locationConfig,
                         butler=self.bestEffort.butler,
                         dataProduct='raw',
                         channelName='auxtel_isr_runner',
                         doRaise=doRaise)

    def callback(self, expRecord):
        """Method called on each new expRecord as it is found in the repo.

        Produce a quickLookExp of the latest image, and butler.put() it to the
        repo so that downstream processes can find and use it.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        dataId = expRecord.dataId
        quickLookExp = self.bestEffort.getExposure(dataId, detector=0)  # noqa: F841 - automatically puts
        del quickLookExp
        self.log.info(f'Put quickLookExp for {dataId}, awaiting next image...')


class ImExaminerChannel(BaseButlerChannel):
    """Class for running the ImExam channel on RubinTV.

    Note: this is currently AuxTel-only.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """
    def __init__(self, locationConfig, *, embargo=False, doRaise=False):
        super().__init__(locationConfig=locationConfig,
                         butler=butlerUtils.makeDefaultLatissButler(embargo=embargo),
                         dataProduct='quickLookExp',
                         channelName='summit_imexam',
                         doRaise=doRaise)
        self.detector = 0

    def _imExamine(self, exp, outputFilename):
        """Run the imExam analysis on the exposure.

        Parameters
        ----------
        exp : `lsst.afw.image.Exposure`
            The exposure.
        outputFilename : `str`
            The filename to save the plot to.
        """
        if os.path.exists(outputFilename):  # unnecessary now we're using tmpfile
            self.log.warning(f"Skipping {outputFilename}")
            return
        imexam = ImageExaminer(exp, savePlots=outputFilename, doTweakCentroid=True)
        imexam.plot()

    def callback(self, expRecord):
        """Method called on each new expRecord as it is found in the repo.

        Plot the quick imExam analysis of the latest image, writing the plot
        to a temp file, and upload it to Google cloud storage via the uploader.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        try:
            dataId = butlerUtils.updateDataId(expRecord.dataId, detector=self.detector)
            self.log.info(f'Running imexam on {dataId}')
            tempFilename = tempfile.mktemp(suffix='.png')
            uploadFilename = expRecordToUploadFilename(self.channelName, expRecord)
            exp = self._waitForDataProduct(dataId)

            if not exp:
                raise RuntimeError(f'Failed to get {self.dataProduct} for {dataId}')
            self._imExamine(exp, tempFilename)

            self.log.info("Uploading imExam to storage bucket")
            self.uploader.googleUpload(self.channelName, tempFilename, uploadFilename)
            self.log.info('Upload complete')

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)


class SpecExaminerChannel(BaseButlerChannel):
    """Class for running the SpecExam channel on RubinTV.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(self, locationConfig, *, embargo=False, doRaise=False):
        super().__init__(locationConfig=locationConfig,
                         butler=butlerUtils.makeDefaultLatissButler(embargo=embargo),
                         dataProduct='quickLookExp',
                         channelName='summit_specexam',
                         doRaise=doRaise)
        self.detector = 0

    def _specExamine(self, exp, outputFilename):
        """Run the specExam analysis on the exposure.

        Parameters
        ----------
        exp : `lsst.afw.image.Exposure`
            The exposure.
        outputFilename : `str`
            The filename to save the plot to.
        """
        if os.path.exists(outputFilename):  # unnecessary now we're using tmpfile?
            self.log.warning(f"Skipping {outputFilename}")
            return
        summary = SpectrumExaminer(exp, savePlotAs=outputFilename)
        summary.run()

    def callback(self, expRecord):
        """Method called on each new expRecord as it is found in the repo.

        Plot the quick spectral reduction of the latest image, writing the plot
        to a temp file, and upload it to Google cloud storage via the uploader.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        try:
            dataId = butlerUtils.updateDataId(expRecord.dataId, detector=self.detector)
            oldStyleDataId = {'day_obs': expRecord.day_obs, 'seq_num': expRecord.seq_num}
            if not isDispersedDataId(oldStyleDataId, self.butler):
                self.log.info(f'Skipping non dispersed image {dataId}')
                return

            self.log.info(f'Running specExam on {dataId}')
            tempFilename = tempfile.mktemp(suffix='.png')
            uploadFilename = expRecordToUploadFilename(self.channelName, expRecord)
            exp = self._waitForDataProduct(dataId)
            if not exp:
                raise RuntimeError(f'Failed to get {self.dataProduct} for {dataId}')
            self._specExamine(exp, tempFilename)

            self.log.info("Uploading specExam to storage bucket")
            self.uploader.googleUpload(self.channelName, tempFilename, uploadFilename)
            self.log.info('Upload complete')

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)


class MonitorChannel(BaseButlerChannel):
    """Class for running the monitor channel on RubinTV.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """
    def __init__(self, locationConfig, *, embargo=False, doRaise=False):
        super().__init__(locationConfig=locationConfig,
                         butler=butlerUtils.makeDefaultLatissButler(embargo=embargo),
                         dataProduct='quickLookExp',
                         channelName='auxtel_monitor',
                         doRaise=doRaise)
        self.fig = plt.figure(figsize=(12, 12))
        self.detector = 0

    def _plotImage(self, exp, outputFilename):
        """Plot the image.

        Parameters
        ----------
        exp : `lsst.afw.image.Exposure`
            The exposure.
        outputFilename : `str`
            The filename to save the plot to.
        """
        if os.path.exists(outputFilename):  # unnecessary now we're using tmpfile
            self.log.warning(f"Skipping {outputFilename}")
            return
        plotExp(exp, self.fig, outputFilename)

    def callback(self, expRecord):
        """Method called on each new expRecord as it is found in the repo.

        Plot the image for display on the monitor, writing the plot
        to a temp file, and upload it to Google cloud storage via the uploader.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        try:
            dataId = butlerUtils.updateDataId(expRecord.dataId, detector=self.detector)
            self.log.info(f'Generating monitor image for {dataId}')
            tempFilename = tempfile.mktemp(suffix='.png')
            uploadFilename = expRecordToUploadFilename(self.channelName, expRecord)
            exp = self._waitForDataProduct(dataId)
            if not exp:
                raise RuntimeError(f'Failed to get {self.dataProduct} for {dataId}')
            self._plotImage(exp, tempFilename)

            self.log.info("Uploading monitor image to storage bucket")
            self.uploader.googleUpload(self.channelName, tempFilename, uploadFilename)
            self.log.info('Upload complete')

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)


class MountTorqueChannel(BaseButlerChannel):
    """Class for running the mount torque channel on RubinTV.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """
    def __init__(self, locationConfig, *, embargo=False, doRaise=False):
        if not HAS_EFD_CLIENT:
            from lsst.summit.utils.utils import EFD_CLIENT_MISSING_MSG
            raise RuntimeError(EFD_CLIENT_MISSING_MSG)

        super().__init__(locationConfig=locationConfig,
                         butler=butlerUtils.makeDefaultLatissButler(embargo=embargo),
                         dataProduct='raw',
                         channelName='auxtel_mount_torques',
                         doRaise=doRaise)
        self.client = EfdClient('summit_efd')
        self.fig = plt.figure(figsize=(16, 16))
        self.detector = 0

    def writeMountErrorShard(self, errors, expRecord):
        """Write a metadata shard for the mount error, including the flag
        for coloring the cell based on the threshold values.

        Parameters
        ----------
        errors : `tuple`
            The mount errors, in the form (az_rms, el_rms, rot_rms).
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        dayObs = butlerUtils.getDayObs(expRecord)
        seqNum = butlerUtils.getSeqNum(expRecord)
        az_rms, el_rms, _ = errors  # we don't need the rot errors here so assign to _
        imageError = (az_rms ** 2 + el_rms ** 2) ** .5
        key = 'Mount motion image degradation'
        flagKey = '_' + key  # color coding of cells always done by prepending with an underscore
        contents = {key: imageError}

        if imageError > MOUNT_IMAGE_BAD_LEVEL:
            contents.update({flagKey: 'bad'})
        elif imageError > MOUNT_IMAGE_WARNING_LEVEL:
            contents.update({flagKey: 'warning'})

        md = {seqNum: contents}
        writeMetadataShard(self.locationConfig.auxTelMetadataShardPath, dayObs, md)
        return

    def callback(self, expRecord):
        """Method called on each new expRecord as it is found in the repo.

        Plot the mount torques, pulling data from the EFD, writing the plot
        to a temp file, and upload it to Google cloud storage via the uploader.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        try:
            dataId = butlerUtils.updateDataId(expRecord.dataId, detector=self.detector)
            tempFilename = tempfile.mktemp(suffix='.png')
            uploadFilename = expRecordToUploadFilename(self.channelName, expRecord)

            # calculateMountErrors() calculates the errors, but also performs
            # the plotting.
            errors = calculateMountErrors(dataId, self.butler, self.client, self.fig, tempFilename, self.log)

            if os.path.exists(tempFilename):  # skips many image types and short exps
                self.log.info("Uploading mount torque plot to storage bucket")
                self.uploader.googleUpload(self.channelName, tempFilename, uploadFilename)
                self.log.info('Upload complete')

            # write the mount error shard, including the cell coloring flag
            if errors:  # if the mount torque fails or skips it returns False
                self.writeMountErrorShard(errors, expRecord)

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)


class MetadataCreator(BaseButlerChannel):
    """Class for creating metadata shards for RubinTV. Note the shards are
    merged and uploaded by a TimedMetadataServer, not this class.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(self, locationConfig, *, embargo=False, doRaise=False):
        super().__init__(locationConfig=locationConfig,
                         butler=butlerUtils.makeDefaultLatissButler(embargo=embargo),
                         dataProduct='raw',
                         channelName='auxtel_metadata_creator',
                         doRaise=doRaise)
        self.detector = 0  # can be removed once we have the requisite summit DBs

        # We inherit an uploader, so be explicit about the fact we don't use it
        self.uploader = None

    def expRecordToMetadataDict(self, expRecord, keysToRemove):
        """Create a dictionary of metadata for an expRecord.

        Given an expRecord, create a dictionary containing all the metadata
        that should be displayed in the table on RubinTV. The table creation is
        dynamic, so any entries which should not appear as columns in the table
        should be removed via keysToRemove.

        Note that for now, while there is data in the headers which cannot be
        gleaned from the expRecord, we are getting the raw metadata from the
        butler. Once there are summit databases with all the info we need, or
        a schema migration is done meaning everything is in the expRecord, this
        can be stopped.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        keysToRemove : `list` [`str`]
            Keys to remove from the exposure record.

        Returns
        -------
        metadata : `dict` [`dict`]
            A dict, with a single key, corresponding to the expRecord's seqNum,
            containing a dict of the exposure's metadata.
        """
        seqNum = butlerUtils.getSeqNum(expRecord)
        d = expRecord.toDict()

        time_begin_tai = expRecord.timespan.begin.to_datetime().strftime("%H:%M:%S")
        d['time_begin_tai'] = time_begin_tai
        d.pop('timespan')

        filt, disperser = d['physical_filter'].split(FILTER_DELIMITER)
        d.pop('physical_filter')
        d['filter'] = filt
        d['disperser'] = disperser

        rawmd = self.butler.get('raw.metadata', expRecord.dataId, detector=self.detector)
        obsInfo = ObservationInfo(rawmd)
        d['airmass'] = obsInfo.boresight_airmass
        d['focus_z'] = obsInfo.focus_z.value

        d['altitude'] = None  # altaz_begin is None when not on sky so need check it's not None first
        if obsInfo.altaz_begin is not None:
            d['altitude'] = obsInfo.altaz_begin.alt.value

        if 'SEEING' in rawmd:  # SEEING not yet in the obsInfo so take direct from header
            d['seeing'] = rawmd['SEEING']

        for key in keysToRemove:
            if key in d:
                d.pop(key)

        properNames = {MD_NAMES_MAP[attrName]: d[attrName] for attrName in d}

        return {seqNum: properNames}

    def writeShardForExpRecord(self, expRecord):
        """Write a standard shard for this expRecord.

        Calls expRecordToMetadataDict to get the normal set of metadata
        components and then writes it to a shard file in the shards directory
        ready for upload.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        md = self.expRecordToMetadataDict(expRecord, SIDECAR_KEYS_TO_REMOVE)
        dayObs = butlerUtils.getDayObs(expRecord)
        writeMetadataShard(self.locationConfig.auxTelMetadataShardPath, dayObs, md)
        return

    def callback(self, expRecord):
        """Method called on each new expRecord as it is found in the repo.

        Add the metadata to the sidecar for the expRecord and upload.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        try:
            self.log.info(f'Writing metadata shard for {expRecord.dataId}')
            self.writeShardForExpRecord(expRecord)
            # Note: we do not upload anythere here, as the TimedMetadataServer
            # does the collation and upload, and runs as a separate process.

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)


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
    def __init__(self, locationConfig, *, embargo=False, doRaise=False):
        super().__init__(locationConfig=locationConfig,
                         # writeable true is required to define visits
                         butler=butlerUtils.makeDefaultLatissButler(embargo=embargo, writeable=True),
                         dataProduct='quickLookExp',
                         channelName='auxtel_calibrateCcd',
                         doRaise=doRaise)
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
            dataId = butlerUtils.updateDataId(expRecord.dataId, detector=self.detector)
            tStart = time.time()

            self.log.info(f'Running Image Characterization for {dataId}')
            exp = self._waitForDataProduct(dataId)

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
            self.clobber(calibrateRes.outputExposure, "calexp", dataId)
            tFinal = time.time()
            self.log.info(f"Ran characterizeImage and calibrate in {tFinal-tStart:.2f} seconds")

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

    def clobber(self, object, datasetType, dataId):
        """Put object in the butler.

        If there is one already there, remove it beforehand.

        Parameters
        ----------
        object : `object`
            Any object to put in the butler.
        datasetType : `str`
            Dataset type name to put it as.
        dataId : `lsst.daf.butler.DataCoordinate`
            DataId to put the object at.
        """
        if not (visitDataId := self.getVisitDataId(dataId)):
            self.log.warning(f'Skipped butler.put of {datasetType} for {dataId} due to lack of visitId.'
                             " Do you need to run define-visits?")
            return

        self.butler.registry.registerRun(self.outputRunName)
        if butlerUtils.datasetExists(self.butler, datasetType, visitDataId):
            self.log.warning(f'Overwriting existing {datasetType} for {dataId}')
            dRef = self.butler.registry.findDataset(datasetType, visitDataId)
            self.butler.pruneDatasets([dRef], disassociate=True, unstore=True, purge=True)
        self.butler.put(object, datasetType, dataId=visitDataId, run=self.outputRunName)
        self.log.info(f'Put {datasetType} for {dataId} with visitId: {visitDataId}')
