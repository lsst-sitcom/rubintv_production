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

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt
import numpy as np

import lsst.daf.butler as dafButler
from lsst.afw.geom import ellipses
from lsst.atmospec.utils import isDispersedDataId
from lsst.pipe.tasks.peekExposure import PeekExposureTask, PeekExposureTaskConfig
from lsst.summit.utils import ConsDbClient
from lsst.summit.utils.auxtel.mount import hasTimebaseErrors
from lsst.summit.utils.efdUtils import getEfdData, makeEfdClient
from lsst.summit.utils.imageExaminer import ImageExaminer
from lsst.summit.utils.simonyi.mountAnalysis import (
    MOUNT_IMAGE_BAD_LEVEL,
    MOUNT_IMAGE_WARNING_LEVEL,
    N_REPLACED_BAD_LEVEL,
    N_REPLACED_WARNING_LEVEL,
    calculateMountErrors,
    plotMountErrors,
)
from lsst.summit.utils.spectrumExaminer import SpectrumExaminer
from lsst.summit.utils.utils import calcEclipticCoords

from .baseChannels import BaseButlerChannel
from .consdbUtils import ConsDBPopulator
from .exposureLogUtils import LOG_ITEM_MAPPINGS, getLogsForDayObs
from .monitorPlotting import plotExp
from .mountTorques import MOUNT_IMAGE_BAD_LEVEL as MOUNT_IMAGE_BAD_LEVEL_AUXTEL
from .mountTorques import MOUNT_IMAGE_WARNING_LEVEL as MOUNT_IMAGE_WARNING_LEVEL_AUXTEL
from .mountTorques import calculateMountErrors as _calculateMountErrors_oldVersion
from .redisUtils import RedisHelper
from .uploaders import MultiUploader
from .utils import (
    getFilterColorName,
    getRubinTvInstrumentName,
    isCalibration,
    managedTempFile,
    raiseIf,
    writeMetadataShard,
)

if TYPE_CHECKING:
    from numpy.typing import NDArray

    from lsst.afw.image import Exposure
    from lsst.daf.butler import Butler, DataCoordinate, DimensionRecord

    from .payloads import Payload
    from .podDefinition import PodDetails
    from .utils import LocationConfig

__all__ = [
    "OneOffProcessor",
]


class OneOffProcessor(BaseButlerChannel):
    """A processor which runs arbitrary code on an arbitrary data product for
    a single CCD in the focal plane.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    butler : `lsst.daf.butler.Butler`
        The butler to use.
    instrument : `str`
        The instrument name.
    podDetails : `lsst.rubintv.production.podDefinition.PodDetails`
        The details of the pod this worker is running on.
    detectorNumber : `int`
        The detector number that this worker should process.
    shardsDirectory : `str`
        The directory to write the metadata shards to.
    processingStage : `str`
        The data product that this runner needs in order to run, e.g. if it
        should run once ISR has completed for the specified detector, use
        "postISRCCD", and if it should run after step1 is complete use "calexp"
        (or "pvi" once we switch).
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(
        self,
        locationConfig: LocationConfig,
        butler: Butler,
        instrument: str,
        podDetails: PodDetails,
        detectorNumber: int,
        shardsDirectory: str,
        processingStage: str,
        *,
        doRaise=False,
    ):
        super().__init__(
            locationConfig=locationConfig,
            instrument=instrument,
            butler=butler,
            watcherType="redis",
            # TODO: DM-43764 this shouldn't be necessary on the
            # base class after this ticket, I think.
            detectors=None,
            dataProduct=processingStage,
            # TODO: DM-43764 should also be able to fix needing
            # channelName when tidying up the base class. Needed
            # in some contexts but not all. Maybe default it to
            # ''?
            channelName="",
            podDetails=podDetails,
            doRaise=doRaise,
            addUploader=False,  # pipeline running pods don't upload directly
        )
        self.instrument = instrument
        self.butler = butler
        self.podDetails = podDetails
        self.detector = detectorNumber
        self.shardsDirectory = shardsDirectory
        self.processingStage = processingStage
        if self.processingStage == "expRecord":
            # remove this conditional once we have the squid proxy
            self.uploader = MultiUploader()

        self.mountFigure = plt.figure(figsize=(10, 8))

        peekConfig = PeekExposureTaskConfig()
        self.peekTask = PeekExposureTask(config=peekConfig)

        self.log.info(f"Pipeline running configured to consume from {self.podDetails.queueName}")

        self.redisHelper = RedisHelper(butler, self.locationConfig)
        self.efdClient = makeEfdClient()
        self.consdbClient = ConsDbClient("http://consdb-pq.consdb:8080/consdb")
        self.consDBPopulator = ConsDBPopulator(self.consdbClient, self.redisHelper)

    def writeFocusZ(self, exp: Exposure, dayObs: int, seqNum: int) -> None:
        vi = exp.info.getVisitInfo()
        focus = vi.focusZ
        if focus is not None and not np.isnan(focus):
            focus = float(focus)
            md = {seqNum: {"Focus Z": f"{focus:.3f}"}}
        else:
            md = {seqNum: {"Focus Z": "MISSING VALUE!"}}

        # TODO XXX see if airmass is in here, and if not, get it from elsewhere
        # it has been removed from AuxTel in the rewrite and I think we want it
        # for all instruments anyway
        # Same for DIMM seeing via the "seeing" header and report that too.

        writeMetadataShard(self.shardsDirectory, dayObs, md)

    def writePhysicalRotation(self, expRecord: DimensionRecord) -> None:
        data = getEfdData(self.efdClient, "lsst.sal.MTRotator.rotation", expRecord=expRecord)
        if data.empty:
            self.log.warning(f"Failed to get physical rotation data for {expRecord.id} - EFD data was empty")
            return

        outputDict = {"Rotator physical position": f"{np.mean(data['actualPosition']):.3f}"}
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        rowData = {seqNum: outputDict}

        writeMetadataShard(self.shardsDirectory, dayObs, rowData)

    def writeObservationAnnotation(self, exp: Exposure, dayObs: int, seqNum: int) -> None:
        headerMetadata = exp.metadata.toDict()
        note = headerMetadata.get("OBSANNOT")
        if note is not None:
            md = {seqNum: {"Observation annotation": f"{note}"}}
            writeMetadataShard(self.shardsDirectory, dayObs, md)

    def calcPsfAndWrite(self, exp: Exposure, dayObs: int, seqNum: int) -> None:
        try:
            result = self.peekTask.run(exp)

            shape = result.psfEquatorialShape
            fwhm = np.nan
            if shape is not None:
                SIGMA2FWHM = np.sqrt(8 * np.log(2))
                ellipse = ellipses.SeparableDistortionDeterminantRadius(shape)
                fwhm = SIGMA2FWHM * ellipse.getDeterminantRadius()

            md = {seqNum: {"PSF FWHM (central CCD, robust measure)": f"{fwhm:.3f}"}}
            writeMetadataShard(self.shardsDirectory, dayObs, md)
            self.log.info(f"Wrote measured PSF for {dayObs}-{seqNum} det={exp.detector.getId()}: {fwhm:.3f}")
        except Exception as e:
            self.log.error(f"Failed to calculate PSF for {dayObs}-{seqNum} det={exp.detector.getId()}: {e}")
            raiseIf(self.doRaise, e, self.log)
            return

    def calcTimeSincePrevious(self, expRecord: DimensionRecord) -> None:
        if expRecord.seq_num == 1:  # nothing to do for first image of the day
            return

        # this is kinda gross, but it's robust enough for this thing that's
        # only a convenience for RubinTV. Will occasionally raise when we skip
        # a seqNum, but that's very rare (less than once per day and usually
        # at the start of the night when this doesn't matter)
        try:
            (previousImage,) = self.butler.registry.queryDimensionRecords(
                "exposure", exposure=expRecord.id - 1
            )
            timeSincePrevious = expRecord.timespan.begin - previousImage.timespan.end

            md = {expRecord.seq_num: {"Time since previous exposure": f"{timeSincePrevious.sec:.2f}"}}
            writeMetadataShard(self.shardsDirectory, expRecord.day_obs, md)
        except Exception as e:
            self.log.error(f"Failed to calculate time since previous exposure for {expRecord.id}: {e}")
            raiseIf(self.doRaise, e, self.log)
            return

    def runPostISRCCD(self, dataId: DataCoordinate) -> None:
        self.log.info(f"Waiting for postISRCCD for {dataId}")
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
        assert expRecord.instrument == self.instrument, "Logic error in work distribution!"

        # redis signal is sent on the dispatch of the raw, so 40s is plenty but
        # not too much
        postISR = self._waitForDataProduct(dataId, gettingButler=self.butler, timeout=40)
        if postISR is None:
            self.log.warning(f"Failed to get postISRCCD for {dataId}")
            return

        if isinstance(self, OneOffProcessorAuxTel):
            # automatically run the LATISS processing
            # XXX does this actually work? Is this an OK pattern?
            # Also consider moving this to run ~first for speed of the monitor
            self.runAuxTelProcessing(postISR, expRecord)

        self.log.info(f"Writing focus Z for {dataId}")
        self.writeFocusZ(postISR, expRecord.day_obs, expRecord.seq_num)

        self.log.info(f"Pulling OBSANNOT from image header for {dataId}")
        self.writeObservationAnnotation(postISR, expRecord.day_obs, expRecord.seq_num)

        self.log.info(f"Getting physical rotation data from EFD for {dataId}")
        self.writePhysicalRotation(expRecord)

        if not isCalibration(expRecord) and not isinstance(self, OneOffProcessorAuxTel):
            self.log.info(f"Calculating PSF for {dataId}")
            self.calcPsfAndWrite(postISR, expRecord.day_obs, expRecord.seq_num)

        # TODO: add this back in once the log-fetcher is fixed
        # self.log.info(f"Fetching all exposure log messages for day_obs
        # #{expRecord.day_obs}")
        # self.writeLogMessageShards(expRecord.day_obs)

        self.log.info(f"Finished one-off processing {dataId}")

    def publishPointingOffsets(
        self,
        calexp: Exposure,
        dataId: DataCoordinate,
        expRecord: DimensionRecord,
    ) -> None:
        raw = self.butler.get("raw", dataId)

        offsets = {
            "delta Ra (arcsec)": "nan",
            "delta Dec (arcsec)": "nan",
            "delta Rot (arcsec)": "nan",
        }

        calexpWcs = calexp.wcs
        if calexpWcs is None:
            self.log.warning(f"Astrometic failed for {dataId} - no pointing offsets calculated")
            md = {expRecord.seq_num: offsets}
            writeMetadataShard(self.shardsDirectory, expRecord.day_obs, md)
            return

        rawWcs = raw.wcs
        rawSkyCenter = raw.wcs.getSkyOrigin()
        calExpSkyCenter = calexpWcs.pixelToSky(rawWcs.getPixelOrigin())
        deltaRa = rawSkyCenter.getRa().asArcseconds() - calExpSkyCenter.getRa().asArcseconds()
        deltaDec = rawSkyCenter.getDec().asArcseconds() - calExpSkyCenter.getDec().asArcseconds()

        deltaRot = rawWcs.getRelativeRotationToWcs(calexpWcs)
        deltaRotDeg = deltaRot.asDegrees() % 360
        offset = min(deltaRotDeg, 360 - deltaRotDeg)
        deltaRotArcSec = offset * 3600

        offsets = {
            "delta Ra (arcsec)": f"{deltaRa:.1f}",
            "delta Dec (arcsec)": f"{deltaDec:.1f}",
            "delta Rot (arcsec)": f"{deltaRotArcSec:.1f}",
        }

        md = {expRecord.seq_num: offsets}
        writeMetadataShard(self.shardsDirectory, expRecord.day_obs, md)

    def publishEclipticCoords(
        self,
        calexp: Exposure,
        expRecord: DimensionRecord,
    ) -> None:

        calexpWcs = calexp.wcs
        if calexpWcs is None:
            self.log.warning(f"Failed to calculate ecliptic coords from calexp for {expRecord.id}")
            return

        raAngle, decAngle = calexpWcs.getSkyOrigin()
        raDeg = raAngle.asDegrees()
        decDeg = decAngle.asDegrees()
        lambda_, beta = calcEclipticCoords(raDeg, decDeg)

        eclipticData = {
            "lambda (deg)": f"{lambda_:.2f}",
            "beta (deg)": f"{beta:.2f}",
        }

        md = {expRecord.seq_num: eclipticData}
        writeMetadataShard(self.shardsDirectory, expRecord.day_obs, md)

    def runCalexp(self, dataId: DataCoordinate) -> None:
        self.log.info(f"Waiting for calexp for {dataId}")
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
        (visitRecord,) = self.butler.registry.queryDimensionRecords("visit", dataId=dataId)
        assert expRecord.instrument == self.instrument, "Logic error in work distribution!"
        assert visitRecord.instrument == self.instrument, "Logic error in work distribution!"

        visitDataId = dafButler.DataCoordinate.standardize(visitRecord.dataId, detector=self.detector)

        # is triggered once all CCDs have finished step1 so should be instant
        calexp = self._waitForDataProduct(visitDataId, gettingButler=self.butler, timeout=3)
        if calexp is None:
            self.log.warning(f"Failed to get postISRCCD for {dataId}")
            return
        self.log.info("Calculating pointing offsets...")
        self.publishPointingOffsets(calexp, dataId, expRecord)
        self.log.info("Finished calculating pointing offsets")

        self.log.info("Calculating ecliptic coords...")
        self.publishEclipticCoords(calexp, expRecord)
        self.log.info("Finished publishing ecliptic coords")
        return

    def runMountAnalysis(self, expRecord: DimensionRecord) -> None:
        errors, data = calculateMountErrors(expRecord, self.efdClient)
        if errors is None or data is None:
            self.log.warning(f"Failed to calculate mount errors for {expRecord.id}")
            return

        assert errors is not None
        assert data is not None

        outputDict = {}

        value = errors.imageImpactRms
        key = "Mount motion image degradation"
        outputDict[key] = f"{value:.3f}"
        outputDict = self._setFlag(value, key, MOUNT_IMAGE_WARNING_LEVEL, MOUNT_IMAGE_BAD_LEVEL, outputDict)

        value = errors.azRms
        key = "Mount azimuth RMS"
        outputDict[key] = f"{value:.3f}"

        value = errors.elRms
        key = "Mount elevation RMS"
        outputDict[key] = f"{value:.3f}"

        value = errors.rotRms
        key = "Mount rotator RMS"
        outputDict[key] = f"{value:.3f}"

        value = errors.nReplacedAz
        key = "Mount azimuth points replaced"
        outputDict[key] = f"{value}"
        outputDict = self._setFlag(value, key, N_REPLACED_WARNING_LEVEL, N_REPLACED_BAD_LEVEL, outputDict)

        value = errors.nReplacedEl
        key = "Mount elevation points replaced"
        outputDict[key] = f"{value}"
        outputDict = self._setFlag(value, key, N_REPLACED_WARNING_LEVEL, N_REPLACED_BAD_LEVEL, outputDict)

        dayObs: int = expRecord.day_obs
        seqNum: int = expRecord.seq_num
        rowData = {seqNum: outputDict}
        self.log.info(f"Writing mount analysis shard for {dayObs}-{seqNum}")
        writeMetadataShard(self.shardsDirectory, dayObs, rowData)

        # TODO: DM-45437 Use a context manager here and everywhere
        self.log.info(f"Creating mount plot for {dayObs}-{seqNum}")

        ciOutputName = (
            Path(self.locationConfig.plotPath)
            / self.instrument
            / str(dayObs)
            / f"{self.instrument}_mountTorque_dayObs_{dayObs}_seqNum_{seqNum:06}.png"
        )
        with managedTempFile(suffix=".png", ciOutputName=ciOutputName.as_posix()) as tempFile:
            plotMountErrors(data, errors, self.mountFigure, saveFilename=tempFile)
            self.uploader.uploadPerSeqNumPlot(
                instrument=getRubinTvInstrumentName(expRecord.instrument),
                plotName="mount",
                dayObs=expRecord.day_obs,
                seqNum=expRecord.seq_num,
                filename=tempFile,
            )
        self.mountFigure.clear()
        self.mountFigure.gca().clear()

    def writeLogMessageShards(self, dayObs: int) -> None:
        """Write a shard containing all the expLog annotations on the dayObs.

        The expRecord is used to identify the dayObs and nothing else.

        This method is called for each new image, but each time polls the
        exposureLog for all the logs for the dayObs. This is because it will
        take time for observers to make annotations, and so this needs
        constantly updating throughout the night.

        Parameters
        ----------
        dayObs : `int`
            The dayObs to get the log messages for.
        """
        logs = getLogsForDayObs(self.instrument, dayObs)

        if not logs:
            self.log.info(f"No exposure log entries found yet for day_obs={dayObs} for {self.instrument}")
            return

        itemsToInclude = ["message_text", "level", "urls", "exposure_flag"]

        md: dict[int, dict[str, Any]] = {seqNum: {} for seqNum in logs.keys()}

        for seqNum, log in logs.items():
            wasAnnotated = False
            for item in itemsToInclude:
                if item in log:
                    itemValue = log[item]
                    newName = LOG_ITEM_MAPPINGS[item]
                    if isinstance(itemValue, str):  # string values often have trailing '\r\n'
                        itemValue = itemValue.rstrip()
                    md[seqNum].update({newName: itemValue})
                    wasAnnotated = True

            if wasAnnotated:
                md[seqNum].update({"Has annotations?": "🚩"})

        writeMetadataShard(self.locationConfig.auxTelMetadataShardPath, dayObs, md)

    @staticmethod
    def _setFlag(
        value: float, key: str, warningLevel: float, badLevel: float, outputDict: dict[str, Any]
    ) -> dict[str, Any]:
        if value >= warningLevel:
            flag = f"_{key}"
            outputDict[flag] = "warning"
        elif value >= badLevel:
            flag = f"_{key}"
            outputDict[flag] = "bad"
        return outputDict

    def setFilterCellColor(self, expRecord: DimensionRecord) -> None:
        filterName = expRecord.physical_filter
        filterColor = getFilterColorName(filterName)
        if filterColor:
            md = {expRecord.seq_num: {"_Filter": filterColor}}
            writeMetadataShard(self.shardsDirectory, expRecord.day_obs, md)

    def runExpRecord(self, expRecord: DimensionRecord) -> None:
        self.calcTimeSincePrevious(expRecord)
        self.setFilterCellColor(expRecord)
        if expRecord.instrument != "LATISS":
            # TODO: DM-49609 unify this code
            self.runMountAnalysis(expRecord)

    def callback(self, payload: Payload) -> None:
        dataId: DataCoordinate = payload.dataIds[0]
        if len(payload.dataIds) > 1:
            raise ValueError(f"Expected only one dataId, got {len(payload.dataIds)}")

        match self.processingStage:
            case "expRecord":
                (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
                self.runExpRecord(expRecord)
            case "postISRCCD":
                dataId = dafButler.DataCoordinate.standardize(dataId, detector=self.detector)
                self.runPostISRCCD(dataId)
            case "calexp":
                dataId = dafButler.DataCoordinate.standardize(dataId, detector=self.detector)
                self.runCalexp(dataId)
            case _:
                raise ValueError(f"Unknown processing stage {self.processingStage}")


class OneOffProcessorAuxTel(OneOffProcessor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mountFigure = plt.figure(figsize=(16, 16))
        self.monitorFigure = plt.figure(figsize=(12, 12))
        self.s3Uploader = MultiUploader()

    def runAuxTelProcessing(self, exp: Exposure, expRecord: DimensionRecord) -> None:
        # TODO: consider threading and adding a CPU to the pod if this is slow
        self.makeMonitorImage(exp, expRecord)
        self.runImexam(exp, expRecord)
        self.runSpecExam(exp, expRecord)
        self.doMountAnalysis(expRecord)

    def makeMonitorImage(self, exp: Exposure, expRecord: DimensionRecord) -> None:
        self.log.info(f"Making monitor image for {expRecord.dataId}")
        try:
            with tempfile.NamedTemporaryFile(suffix=".png") as tempFile:
                plotExp(exp, self.monitorFigure, tempFile.name, doSmooth=False, scalingOption="CCS")
                self.log.info("Uploading imExam to storage bucket")
                assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
                self.s3Uploader.uploadPerSeqNumPlot(
                    instrument="auxtel",
                    plotName="monitor",
                    dayObs=expRecord.day_obs,
                    seqNum=expRecord.seq_num,
                    filename=tempFile.name,
                )
                self.log.info("Upload complete")

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def runImexam(self, exp: Exposure, expRecord: DimensionRecord) -> None:
        if expRecord.observation_type in ["bias", "dark", "flat"]:
            self.log.info(f"Skipping running imExam on calib image: {expRecord.observation_type}")
        self.log.info(f"Running imexam on {expRecord.dataId}")

        try:
            with tempfile.NamedTemporaryFile(suffix=".png") as tempFile:
                imExam = ImageExaminer(exp, savePlots=tempFile.name, doTweakCentroid=True)
                imExam.plot()
                self.log.info("Uploading imExam to storage bucket")
                assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
                self.s3Uploader.uploadPerSeqNumPlot(
                    instrument="auxtel",
                    plotName="imexam",
                    dayObs=expRecord.day_obs,
                    seqNum=expRecord.seq_num,
                    filename=tempFile.name,
                )
                self.log.info("Upload complete")
                del imExam

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def runSpecExam(self, exp: Exposure, expRecord: DimensionRecord) -> None:

        # XXX do we still need to construct this?
        # XXX also need to check Josh's abandoned ticket - did it touch this
        # and/or fix this issue?
        oldStyleDataId = {"day_obs": expRecord.day_obs, "seq_num": expRecord.seq_num}
        if not isDispersedDataId(oldStyleDataId, self.butler):
            self.log.info(f"Skipping running specExam on non dispersed image {expRecord.dataId}")
            return

        self.log.info(f"Running specExam on {expRecord.dataId}")
        try:
            with tempfile.NamedTemporaryFile(suffix=".png") as tempFile:
                summary = SpectrumExaminer(exp, savePlotAs=tempFile.name)
                summary.run()
                self.log.info("Uploading specExam to storage bucket")
                assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
                self.s3Uploader.uploadPerSeqNumPlot(
                    instrument="auxtel",
                    plotName="specexam",
                    dayObs=expRecord.day_obs,
                    seqNum=expRecord.seq_num,
                    filename=tempFile.name,
                )
                self.log.info("Upload complete")
        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def doMountAnalysis(self, expRecord: DimensionRecord) -> None:
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        try:
            with tempfile.NamedTemporaryFile(suffix=".png") as tempFile:
                # calculateMountErrors() calculates the errors, but also
                # performs the plotting. It skips many image types and short
                # exps and returns False in these cases, otherwise it returns
                # errors and will have made the plot
                errors = _calculateMountErrors_oldVersion(
                    expRecord, self.butler, self.efdClient, self.mountFigure, tempFile.name, self.log
                )
                if errors is False:
                    self.log.info(f"Skipped making mount torque plot for {dayObs}-{seqNum}")
                    return

                self.log.info("Uploading mount torque plot to storage bucket")
                assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
                self.s3Uploader.uploadPerSeqNumPlot(
                    instrument="auxtel",
                    plotName="mount",
                    dayObs=dayObs,
                    seqNum=seqNum,
                    filename=tempFile.name,
                )
                self.log.info("Upload complete")

            # write the mount error shard, including the cell coloring flag
            assert errors is not True and errors is not False  # it's either False or the right type
            self.writeMountErrorShard(errors, expRecord)

            # check for timebase errors and write a metadata shard if found
            self.checkTimebaseErrors(expRecord)

            self.log.info("Sending mount jitter to ConsDB")
            self.consDBPopulator.populateMountErrors(expRecord, errors, "latiss")

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def writeMountErrorShard(self, errors: dict[str, NDArray], expRecord: DimensionRecord) -> None:
        """Write a metadata shard for the mount error, including the flag
        for coloring the cell based on the threshold values.

        Parameters
        ----------
        errors : `dict`
            The mount errors, as a dict, containing keys:
            ``az_rms`` - The RMS azimuth error.
            ``el_rms`` - The RMS elevation error.
            ``rot_rms`` - The RMS rotator error.
            ``image_az_rms`` - The RMS azimuth error for the image.
            ``image_el_rms`` - The RMS elevation error for the image.
            ``image_rot_rms`` - The RMS rotator error for the image.
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        # TODO: DM-49609 unify this code to work for Simonyi as well
        # also the call to this function to the exposure record processor
        # from the postISR processor.
        assert expRecord.instrument == "LATISS", "This method is only for AuxTel at present"
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        # the mount error itself, *not* the image component. No quality flags
        # on this part.
        az_rms = errors["az_rms"]
        el_rms = errors["el_rms"]
        mountError = (az_rms**2 + el_rms**2) ** 0.5
        contents: dict[str, Any] = {"Mount jitter RMS": mountError}
        if np.isnan(mountError):
            contents = {"Mount jitter RMS": "nan"}

        # the contribution to the image error from the mount. This is the part
        # that matters and gets a quality flag. Note that the rotator error
        # contibution is zero at the field centre and increases radially, and
        # is usually very small, so we don't add that here as its contrinution
        # is not really well definited and including it would be misleading.
        image_az_rms = errors["image_az_rms"]
        image_el_rms = errors["image_el_rms"]
        imageError = (image_az_rms**2 + image_el_rms**2) ** 0.5

        key = "Mount motion image degradation"
        flagKey = "_" + key  # color coding of cells always done by prepending with an underscore
        contents.update({key: imageError})
        if np.isnan(imageError):
            contents.update({key: "nan"})

        if imageError > MOUNT_IMAGE_BAD_LEVEL_AUXTEL:
            contents.update({flagKey: "bad"})
        elif imageError > MOUNT_IMAGE_WARNING_LEVEL_AUXTEL:
            contents.update({flagKey: "warning"})

        md = {seqNum: contents}
        writeMetadataShard(self.locationConfig.auxTelMetadataShardPath, dayObs, md)
        return

    def checkTimebaseErrors(self, expRecord: DimensionRecord) -> None:
        """Write a metadata shard if an exposure has cRIO timebase errors.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        hasError = hasTimebaseErrors(expRecord, self.efdClient)
        if hasError:
            md = {expRecord.seq_num: {"Mount timebase errors": "⚠️"}}
            writeMetadataShard(self.locationConfig.auxTelMetadataShardPath, expRecord.day_obs, md)
