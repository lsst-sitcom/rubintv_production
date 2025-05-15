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

from typing import TYPE_CHECKING, Any

import numpy as np

import lsst.daf.butler as dafButler
from lsst.afw.geom import ellipses
from lsst.atmospec.utils import isDispersedDataId
from lsst.pipe.tasks.peekExposure import PeekExposureTask, PeekExposureTaskConfig
from lsst.summit.extras.slewTimingSimonyi import plotExposureTiming
from lsst.summit.utils import ConsDbClient
from lsst.summit.utils.auxtel.mount import hasTimebaseErrors
from lsst.summit.utils.efdUtils import getEfdData, makeEfdClient
from lsst.summit.utils.imageExaminer import ImageExaminer
from lsst.summit.utils.plotting import plot
from lsst.summit.utils.simonyi.mountAnalysis import (
    MOUNT_IMAGE_BAD_LEVEL,
    MOUNT_IMAGE_WARNING_LEVEL,
    N_REPLACED_BAD_LEVEL,
    N_REPLACED_WARNING_LEVEL,
    calculateMountErrors,
    plotMountErrors,
)
from lsst.summit.utils.spectrumExaminer import SpectrumExaminer
from lsst.summit.utils.utils import calcEclipticCoords, getCameraFromInstrumentName
from lsst.utils.plotting.figures import make_figure

from .baseChannels import BaseButlerChannel
from .consdbUtils import ConsDBPopulator
from .exposureLogUtils import LOG_ITEM_MAPPINGS, getLogsForDayObs
from .monitorPlotting import plotExp
from .mountTorques import MOUNT_IMAGE_BAD_LEVEL as MOUNT_IMAGE_BAD_LEVEL_AUXTEL
from .mountTorques import MOUNT_IMAGE_WARNING_LEVEL as MOUNT_IMAGE_WARNING_LEVEL_AUXTEL
from .mountTorques import calculateMountErrors as _calculateMountErrors_oldVersion
from .redisUtils import RedisHelper
from .utils import (
    getFilterColorName,
    getRubinTvInstrumentName,
    isCalibration,
    makePlotFile,
    makeWitnessDetectorTitle,
    raiseIf,
    runningCI,
    writeMetadataShard,
)

if TYPE_CHECKING:
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
        "post_isr_image", and if it should run after step1a is complete use
        "preliminary_visit_image".
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
    ) -> None:
        super().__init__(
            locationConfig=locationConfig,
            butler=butler,
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
            addUploader=True,
        )
        self.instrument = instrument
        self.butler = butler
        self.podDetails = podDetails
        self.detector = detectorNumber
        self.shardsDirectory = shardsDirectory
        self.processingStage = processingStage

        peekConfig = PeekExposureTaskConfig()
        self.peekTask = PeekExposureTask(config=peekConfig)

        self.log.info(f"Pipeline running configured to consume from {self.podDetails.queueName}")

        self.redisHelper = RedisHelper(butler, self.locationConfig)
        self.efdClient = makeEfdClient()
        self.consdbClient = ConsDbClient(self.locationConfig.consDBURL)
        self.consDBPopulator = ConsDBPopulator(self.consdbClient, self.redisHelper)
        self.camera = getCameraFromInstrumentName(self.instrument)

    def writeHeaderOrVisitInfoBasedQuantities(self, exp: Exposure, dayObs: int, seqNum: int) -> None:
        vi = exp.info.getVisitInfo()
        header = exp.metadata.toDict()

        md: dict[int, dict[str, str]] = {}

        focus = vi.focusZ
        if focus is not None and not np.isnan(focus):
            focus = float(focus)
            md = {seqNum: {"Focus Z": f"{focus:.3f}"}}
        else:
            md = {seqNum: {"Focus Z": "MISSING VALUE!"}}

        airmass = vi.boresightAirmass
        if airmass is not None and not np.isnan(airmass):
            airmass = float(airmass)
            md[seqNum].update({"Airmass": f"{airmass:.3f}"})

        controller = header.get("CONTRLLR", None)
        if controller is not None and controller != "":
            md[seqNum].update({"Controller": f"{controller}"})

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

    def runPostIsrImage(self, dataId: DataCoordinate) -> None:
        self.log.info(f"Waiting for post_isr_image for {dataId}")
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
        assert expRecord.instrument == self.instrument, "Logic error in work distribution!"

        # redis signal is sent on the dispatch of the raw, so 40s is plenty but
        # not too much
        postIsr = self._waitForDataProduct(dataId, gettingButler=self.butler, timeout=40)
        if postIsr is None:
            self.log.warning(f"Failed to get post_isr_image for {dataId}")
            return

        if isinstance(self, OneOffProcessorAuxTel):
            self.runAuxTelProcessing(postIsr, expRecord)

        self.log.info(f"Writing focus Z for {dataId}")
        self.writeHeaderOrVisitInfoBasedQuantities(postIsr, expRecord.day_obs, expRecord.seq_num)

        self.log.info(f"Pulling OBSANNOT from image header for {dataId}")
        self.writeObservationAnnotation(postIsr, expRecord.day_obs, expRecord.seq_num)

        self.log.info(f"Getting physical rotation data from EFD for {dataId}")
        self.writePhysicalRotation(expRecord)

        if not isCalibration(expRecord) and not isinstance(self, OneOffProcessorAuxTel):
            self.log.info(f"Calculating PSF for {dataId}")
            self.calcPsfAndWrite(postIsr, expRecord.day_obs, expRecord.seq_num)

        if isCalibration(expRecord):  # make witness images with post-isr for all calibs and no on-sky images
            self.log.info("Making witness detector image...")
            self.makeWitnessImage(postIsr, expRecord, stretch="ccs")
            self.log.info("Finished making witness detector image")

        if self.locationConfig.location == "summit":
            self.log.info(f"Fetching all exposure log messages for day_obs {expRecord.day_obs}")
            self.writeLogMessageShards(expRecord.day_obs)

        self.log.info(f"Finished one-off processing {dataId}")

    def publishPointingOffsets(
        self,
        visitImage: Exposure,
        dataId: DataCoordinate,
        expRecord: DimensionRecord,
    ) -> None:
        raw = self.butler.get("raw", dataId)

        offsets = {
            "delta Ra (arcsec)": "nan",
            "delta Dec (arcsec)": "nan",
            "delta Rot (arcsec)": "nan",
        }

        visitImageWcs = visitImage.wcs
        if visitImageWcs is None:
            self.log.warning(f"Astrometic failed for {dataId} - no pointing offsets calculated")
            md = {expRecord.seq_num: offsets}
            writeMetadataShard(self.shardsDirectory, expRecord.day_obs, md)
            return

        rawWcs = raw.wcs
        rawSkyCenter = raw.wcs.getSkyOrigin()
        visitImageSkyCenter = visitImageWcs.pixelToSky(rawWcs.getPixelOrigin())
        deltaRa = rawSkyCenter.getRa().asArcseconds() - visitImageSkyCenter.getRa().asArcseconds()
        deltaDec = rawSkyCenter.getDec().asArcseconds() - visitImageSkyCenter.getDec().asArcseconds()

        deltaRot = rawWcs.getRelativeRotationToWcs(visitImageWcs)
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
        expRecord: DimensionRecord,
    ) -> None:

        raDeg = expRecord.tracking_ra
        decDeg = expRecord.tracking_dec
        lambda_, beta = calcEclipticCoords(raDeg, decDeg)

        eclipticData = {
            "Ecliptic Longitude (deg)": f"{lambda_:.2f}",
            "Ecliptic Latitude (deg)": f"{beta:.2f}",
        }

        md = {expRecord.seq_num: eclipticData}
        writeMetadataShard(self.shardsDirectory, expRecord.day_obs, md)

    def makeWitnessImage(self, visitImage: Exposure, expRecord: DimensionRecord, stretch: str) -> None:
        detNum = visitImage.detector.getId()
        detName = visitImage.detector.getName()
        title = makeWitnessDetectorTitle(expRecord, detNum, self.camera)

        fig = make_figure(figsize=(12, 12))
        fig = plot(visitImage, figure=fig, stretch=stretch, title=title)

        plotName = "witness_detector"
        plotFile = makePlotFile(
            self.locationConfig, self.instrument, expRecord.day_obs, expRecord.seq_num, plotName, "jpg"
        )
        fig.tight_layout()
        fig.savefig(plotFile)
        assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
        self.s3Uploader.uploadPerSeqNumPlot(
            instrument=getRubinTvInstrumentName(expRecord.instrument),
            plotName=plotName,
            dayObs=expRecord.day_obs,
            seqNum=expRecord.seq_num,
            filename=plotFile,
        )

        md = {expRecord.seq_num: {"Witness detector": f"{detName} ({detNum})"}}
        writeMetadataShard(self.shardsDirectory, expRecord.day_obs, md)

    def runVisitImage(self, dataId: DataCoordinate) -> None:
        # for safety, as this is now dynamically set in the previous function
        # and is inside the dataId already
        self.detector = -999  # this will always error like None would, but keeps it an int for mypy

        self.log.info(f"Waiting for preliminary_visit_image for {dataId}")
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
        (visitRecord,) = self.butler.registry.queryDimensionRecords("visit", dataId=dataId)
        assert expRecord.instrument == self.instrument, "Logic error in work distribution!"
        assert visitRecord.instrument == self.instrument, "Logic error in work distribution!"

        visitDataId = dafButler.DataCoordinate.standardize(visitRecord.dataId, detector=dataId["detector"])

        # is triggered once all CCDs have finished step1a so should be instant
        visitImage = self._waitForDataProduct(visitDataId, gettingButler=self.butler, timeout=3)
        if visitImage is None:
            self.log.warning(f"Failed to get post_isr_image for {dataId}")
            return
        self.log.info("Calculating pointing offsets...")
        self.publishPointingOffsets(visitImage, dataId, expRecord)
        self.log.info("Finished calculating pointing offsets")

        if not isCalibration(expRecord):  # make witness images with visitImage for all on-sky and no calibs
            self.log.info("Making witness detector image...")
            self.makeWitnessImage(visitImage, expRecord, stretch="midtone")
            self.log.info("Finished making witness detector image")
        return

    def _doMountAnalysisSimonyi(self, expRecord: DimensionRecord) -> None:
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

        self.log.info(f"Creating mount plot for {dayObs}-{seqNum}")

        plotName = "mount"
        plotFile = makePlotFile(
            self.locationConfig, self.instrument, expRecord.day_obs, expRecord.seq_num, plotName, "png"
        )
        fig = make_figure(figsize=(10, 8))
        plotMountErrors(data, errors, fig, saveFilename=plotFile)
        assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
        self.s3Uploader.uploadPerSeqNumPlot(
            instrument=getRubinTvInstrumentName(expRecord.instrument),
            plotName=plotName,
            dayObs=expRecord.day_obs,
            seqNum=expRecord.seq_num,
            filename=plotFile,
        )
        del fig

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

        writeMetadataShard(self.shardsDirectory, dayObs, md)

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

    def getPreviousExpRecord(self, expRecord: DimensionRecord) -> DimensionRecord | None:
        """Get the previous (contiguous) exposure record for the given record.

        Returns the previous contiguous exposure within the dayObs, or None if
        it's not found, or if images aren't contiguous, or it's the first image
        of the day.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.

        Returns
        -------
        previous: `lsst.daf.butler.DimensionRecord` or `None`
            The previous exposure record, or ``None`` if not found.
        """
        try:
            (previousExpRecord,) = self.butler.registry.queryDimensionRecords(
                "exposure", dataId={"exposure": expRecord.id - 1}  # true for contiguous and with dayObs
            )
            return previousExpRecord
        except ValueError:
            if expRecord.seq_num > 1:
                self.log.warning(f"Failed to find previous expRecord for {expRecord.id}")
            return None

    def runExpRecord(self, expRecord: DimensionRecord) -> None:
        self.calcTimeSincePrevious(expRecord)
        self.setFilterCellColor(expRecord)

        self.log.info("Calculating ecliptic coords...")
        self.publishEclipticCoords(expRecord)
        self.log.info("Finished publishing ecliptic coords")

        if expRecord.instrument == "LATISS":
            self._doMountAnalysisAuxTel(expRecord)
        else:
            try:  # this often fails due to missing mount data, catch so other plots can still work
                if expRecord.zenith_angle is not None:  # XXX hopefully we can remove this soon
                    self._doMountAnalysisSimonyi(expRecord)
                else:
                    self.log.warning(f"Skipping mount analysis for {expRecord.id} - no zenith angle")
            except KeyError as e:
                self.log.warning(f"KeyError during plotting mount torques for LSSTCam: {e}")
            except Exception as e:  # but all others are a raiseIf
                raiseIf(self.doRaise, e, self.log)

            previous = self.getPreviousExpRecord(expRecord)
            if previous is not None:
                self.makeExposureTimingPlot(previous, expRecord)

    def makeExposureTimingPlot(self, previousExpRecord: DimensionRecord, expRecord: DimensionRecord) -> None:
        self.log.info(f"Creating exposure timing plot for {expRecord.id}")
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        try:
            fig = plotExposureTiming(
                self.efdClient, [previousExpRecord, expRecord], prePadding=0, postPadding=0
            )
            if fig is None:
                self.log.warning(f"Failed to create exposure timing plot for {expRecord.id}")
                return
        except KeyError as e:  # this often fails due to missing mount data, so this is just a warn
            self.log.warning(f"KeyError during plotting mount torques for LSSTCam: {e}")
            return
        except Exception as e:  # but all others are a raiseIf
            raiseIf(self.doRaise, e, self.log)
            return

        plotName = "event_timeline"
        plotFile = makePlotFile(self.locationConfig, self.instrument, dayObs, seqNum, plotName, "png")
        fig.savefig(plotFile)
        assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
        self.s3Uploader.uploadPerSeqNumPlot(
            instrument=getRubinTvInstrumentName(expRecord.instrument),
            plotName=plotName,
            dayObs=dayObs,
            seqNum=seqNum,
            filename=plotFile,
        )
        self.log.info("Event timeline upload complete")

    def _doMountAnalysisAuxTel(self, expRecord: DimensionRecord) -> None:
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        try:
            plotName = "mount"
            plotFile = makePlotFile(self.locationConfig, self.instrument, dayObs, seqNum, plotName, "png")
            # calculateMountErrors() calculates the errors, but also, "jpg"
            # performs the plotting. It skips many image types and short
            # exps and returns False in these cases, otherwise it returns
            # errors and will have made the plot
            fig = make_figure(figsize=(16, 16))
            errors = _calculateMountErrors_oldVersion(
                expRecord, self.butler, self.efdClient, fig, plotFile, self.log
            )
            if errors is False:
                self.log.info(f"Skipped making mount torque plot for {dayObs}-{seqNum}")
                return

            self.log.info("Uploading mount torque plot to storage bucket")
            assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
            self.s3Uploader.uploadPerSeqNumPlot(
                instrument="auxtel",
                plotName=plotName,
                dayObs=dayObs,
                seqNum=seqNum,
                filename=plotFile,
            )
            self.log.info("Upload complete")
            del fig

            # write the mount error shard, including the cell coloring flag
            assert errors is not True and errors is not False  # it's either False or the right type
            self.writeMountErrorShardAuxTel(errors, expRecord)

            # check for timebase errors and write a metadata shard if found
            self.checkTimebaseErrors(expRecord)

            self.log.info("Sending mount jitter to ConsDB")
            if not runningCI():
                self.consDBPopulator.populateMountErrors(expRecord, errors, "latiss")

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def writeMountErrorShardAuxTel(self, errors: dict[str, float], expRecord: DimensionRecord) -> None:
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
        # from the postIsr processor.
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
        # is not really well defined and including it would be misleading.
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

    def callback(self, payload: Payload) -> None:
        dataId: DataCoordinate = payload.dataIds[0]
        if len(payload.dataIds) > 1:
            raise ValueError(f"Expected only one dataId, got {len(payload.dataIds)}")

        match self.processingStage:
            case "expRecord":
                (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
                self.runExpRecord(expRecord)
            case "post_isr_image":
                dataId = dafButler.DataCoordinate.standardize(dataId, detector=self.detector)
                self.runPostIsrImage(dataId)
            case "preliminary_visit_image":
                detector = self.redisHelper.getWitnessDetectorNumber(self.instrument, self.camera)
                dataId = dafButler.DataCoordinate.standardize(dataId, detector=detector)
                self.runVisitImage(dataId)
            case _:
                raise ValueError(f"Unknown processing stage {self.processingStage}")


class OneOffProcessorAuxTel(OneOffProcessor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def runAuxTelProcessing(self, exp: Exposure, expRecord: DimensionRecord) -> None:
        # TODO: consider threading and adding a CPU to the pod if this is slow
        self.makeMonitorImage(exp, expRecord)
        self.runImexam(exp, expRecord)
        self.runSpecExam(exp, expRecord)

    def makeMonitorImage(self, exp: Exposure, expRecord: DimensionRecord) -> None:
        self.log.info(f"Making monitor image for {expRecord.dataId}")
        try:
            plotName = "monitor"
            plotFile = makePlotFile(
                self.locationConfig, self.instrument, expRecord.day_obs, expRecord.seq_num, plotName, "jpg"
            )
            fig = make_figure(figsize=(12, 12))
            plotExp(exp, fig, plotFile, doSmooth=False, scalingOption="CCS")
            self.log.info("Uploading imExam to storage bucket")
            assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
            self.s3Uploader.uploadPerSeqNumPlot(
                instrument="auxtel",
                plotName=plotName,
                dayObs=expRecord.day_obs,
                seqNum=expRecord.seq_num,
                filename=plotFile,
            )
            self.log.info("Upload complete")
            del fig

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def runImexam(self, exp: Exposure, expRecord: DimensionRecord) -> None:
        if expRecord.observation_type in ["bias", "dark", "flat"]:
            self.log.info(f"Skipping running imExam on calib image: {expRecord.observation_type}")
        self.log.info(f"Running imexam on {expRecord.dataId}")

        try:
            plotName = "imexam"
            plotFile = makePlotFile(
                self.locationConfig, self.instrument, expRecord.day_obs, expRecord.seq_num, plotName, "png"
            )
            imExam = ImageExaminer(exp, savePlots=plotFile, doTweakCentroid=True)
            imExam.plot()
            self.log.info("Uploading imExam to storage bucket")
            assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
            self.s3Uploader.uploadPerSeqNumPlot(
                instrument="auxtel",
                plotName=plotName,
                dayObs=expRecord.day_obs,
                seqNum=expRecord.seq_num,
                filename=plotFile,
            )
            self.log.info("Upload complete")
            del imExam

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def runSpecExam(self, exp: Exposure, expRecord: DimensionRecord) -> None:

        # TODO: DM-41764 see if we can remove the need to construct a dataId
        # when this ticket is done.
        oldStyleDataId = {"day_obs": expRecord.day_obs, "seq_num": expRecord.seq_num}
        if not isDispersedDataId(oldStyleDataId, self.butler):
            self.log.info(f"Skipping running specExam on non dispersed image {expRecord.dataId}")
            return

        self.log.info(f"Running specExam on {expRecord.dataId}")
        try:
            plotName = "specexam"
            plotFile = makePlotFile(
                self.locationConfig, self.instrument, expRecord.day_obs, expRecord.seq_num, plotName, "png"
            )
            summary = SpectrumExaminer(exp, savePlotAs=plotFile)
            summary.run()
            self.log.info("Uploading specExam to storage bucket")
            assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
            self.s3Uploader.uploadPerSeqNumPlot(
                instrument="auxtel",
                plotName=plotName,
                dayObs=expRecord.day_obs,
                seqNum=expRecord.seq_num,
                filename=plotFile,
            )
            self.log.info("Upload complete")
        except Exception as e:
            raiseIf(self.doRaise, e, self.log)
