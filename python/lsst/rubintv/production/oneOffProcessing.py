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
from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt
import numpy as np

import lsst.daf.butler as dafButler
from lsst.afw.geom import ellipses
from lsst.pipe.tasks.peekExposure import PeekExposureTask, PeekExposureTaskConfig
from lsst.summit.utils.efdUtils import getEfdData, makeEfdClient
from lsst.summit.utils.simonyi.mountAnalysis import (
    MOUNT_IMAGE_BAD_LEVEL,
    MOUNT_IMAGE_WARNING_LEVEL,
    N_REPLACED_BAD_LEVEL,
    N_REPLACED_WARNING_LEVEL,
    calculateMountErrors,
    plotMountErrors,
)
from lsst.summit.utils.utils import calcEclipticCoords

from .baseChannels import BaseButlerChannel
from .redisUtils import RedisHelper
from .uploaders import MultiUploader
from .utils import getFilterColorName, getRubinTvInstrumentName, isCalibration, raiseIf, writeMetadataShard

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

    def writeFocusZ(self, exp: Exposure, dayObs: int, seqNum: int) -> None:
        vi = exp.info.getVisitInfo()
        focus = vi.focusZ
        if focus is not None and not np.isnan(focus):
            focus = float(focus)
            md = {seqNum: {"Focus Z": f"{focus:.3f}"}}
        else:
            md = {seqNum: {"Focus Z": "MISSING VALUE!"}}
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

        # redis signal is sent on the dispatch of the raw, so 40s is plenty but
        # not too much
        postISR = self._waitForDataProduct(dataId, gettingButler=self.butler, timeout=40)
        if postISR is None:
            self.log.warning(f"Failed to get postISRCCD for {dataId}")
            return

        self.log.info(f"Writing focus Z for {dataId}")
        self.writeFocusZ(postISR, expRecord.day_obs, expRecord.seq_num)

        self.log.info(f"Pulling OBSANNOT from image header for {dataId}")
        self.writeObservationAnnotation(postISR, expRecord.day_obs, expRecord.seq_num)

        self.log.info(f"Getting physical rotation data from EFD for {dataId}")
        self.writePhysicalRotation(expRecord)

        if not isCalibration(expRecord):
            self.log.info(f"Calculating PSF for {dataId}")
            self.calcPsfAndWrite(postISR, expRecord.day_obs, expRecord.seq_num)

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

        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        rowData = {seqNum: outputDict}
        self.log.info(f"Writing mount analysis shard for {dayObs}-{seqNum}")
        writeMetadataShard(self.shardsDirectory, dayObs, rowData)

        # TODO: DM-45437 Use a context manager here and everywhere
        self.log.info(f"Creating mount plot for {dayObs}-{seqNum}")
        tempFilename = tempfile.mktemp(suffix=".png")
        plotMountErrors(data, errors, self.mountFigure, saveFilename=tempFilename)
        self.uploader.uploadPerSeqNumPlot(
            instrument=getRubinTvInstrumentName(expRecord.instrument),
            plotName="mount",
            dayObs=expRecord.day_obs,
            seqNum=expRecord.seq_num,
            filename=tempFilename,
        )
        self.mountFigure.clear()
        self.mountFigure.gca().clear()

    def setFilterCellColor(self, expRecord: DimensionRecord) -> None:
        filterName = expRecord.physical_filter
        filterColor = getFilterColorName(filterName)
        if filterColor:
            md = {expRecord.seq_num: {"_Filter": filterColor}}
            writeMetadataShard(self.shardsDirectory, expRecord.day_obs, md)

    def runExpRecord(self, expRecord: DimensionRecord) -> None:
        self.calcTimeSincePrevious(expRecord)
        self.setFilterCellColor(expRecord)
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
