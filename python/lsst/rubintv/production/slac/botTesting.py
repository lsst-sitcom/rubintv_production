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

import glob
import logging
import os
from dataclasses import dataclass
from functools import partial
from time import sleep
from typing import TYPE_CHECKING, Any, Callable

import matplotlib.pyplot as plt

import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.daf.butler as dafButler
from lsst.eo.pipe.plotting import focal_plane_plotting
from lsst.ip.isr import IsrTask
from lsst.summit.utils.butlerUtils import getExpRecord
from lsst.summit.utils.utils import getExpRecordAge
from lsst.utils.iteration import ensure_iterable

from ..uploaders import MultiUploader, Uploader
from ..utils import (
    getGlobPatternForShardedData,
    getNumExpectedItems,
    getShardedData,
    writeDataShard,
    writeMetadataShard,
)
from ..watchers import FileWatcher, writeDataIdFile
from .mosaicing import getBinnedImageExpIds, getBinnedImageFiles, plotFocalPlaneMosaic, writeBinnedImage
from .utils import (
    fullAmpDictToPerCcdDicts,
    gainsToPtcDataset,
    getAmplifierRegions,
    getCamera,
    getGains,
    waitForDataProduct,
)

if TYPE_CHECKING:
    from lsst.afw.image import Exposure
    from lsst.daf.butler import Butler, DimensionRecord
    from lsst.pipe.base import Task

    from ..utils import LocationConfig

_LOG = logging.getLogger(__name__)

# The header keys which pertain to the REB condition. These have to be listed
# and pulled from the metadata dict because DM merges all the headers into
# one dict, so we can't just use the full header from the relevant HDU as that
# would mean going back to the raw FITS file, which we don't have/can't do.
REB_HEADERS = [
    "EXTNAME",
    "TEMP1",
    "TEMP2",
    "TEMP3",
    "TEMP4",
    "TEMP5",
    "TEMP6",
    "TEMP7",
    "TEMP8",
    "TEMP9",
    "TEMP10",
    "ATEMPU",
    "ATEMPL",
    "CCDTEMP",
    "RTDTEMP",
    "DIGPS_V",
    "DIGPS_I",
    "ANAPS_V",
    "ANAPS_I",
    "CLKHPS_V",
    "CLKHPS_I",
    "CLKLPS_V",
    "CLKLPS_I",
    "ODPS_V",
    "ODPS_I",
    "HTRPS_V",
    "HTRPS_W",
    "PCKU_V",
    "PCKL_V",
    "SCKU_V",
    "SCKL_V",
    "RGU_V",
    "RGL_V",
    "ODV",
    "OGV",
    "RDV",
    "GDV",
    "GDP",
    "RDP",
    "OGP",
    "ODP",
    "CSGATEP",
    "SCK_LOWP",
    "SCK_HIP",
    "PCK_LOWP",
    "PCK_HIP",
    "RG_LOWP",
    "RG_HIP",
    "AP0_RC",
    "AP1_RC",
    "AP0_GAIN",
    "AP1_GAIN",
    "AP0_CLMP",
    "AP1_CLMP",
    "AP0_AF1",
    "AP1_AF1",
    "AP0_TM",
    "AP1_TM",
    "HVBIAS",
    "IDLEFLSH",
    "POWER",
    "DIGVB",
    "DIGIB",
    "DIGVA",
    "DIGIA",
    "DIGVS",
    "ANAVB",
    "ANAIB",
    "ANAVA",
    "ANAIA",
    "ANAIS",
    "ODVB",
    "ODIB",
    "ODVA",
    "ODVA2",
    "ODIA",
    "ODVS",
    "CKHVB",
    "CKHIB",
    "CKHVA",
    "CKHIA",
    "CKHVS",
    "CKLVB",
    "CKLIB",
    "CKLVA",
    "CKLV2",
    "CKLIA",
    "CKLVS",
    "HTRVB",
    "HTRIB",
    "HTRVA",
    "HTRIA",
    "HTRVAS",
    "BSSVBS",
    "BSSIBS",
    "DATASUM",
]

# The mapping of header keys to the human-readable names in the RubinTV table
# columns. Each of these is necessarily the same for all CCDs in the
# raft/camera, so these are pulled from a single detector and put in the table.
PER_IMAGE_HEADERS = {
    "OBSID": "Observation Id",
    "SEQFILE": "Sequencer file",
    "FILTER": "Filter",
    "FILTER1": "Secondary filter",
    "TEMPLED1": "CCOB daughter board front temp",
    "TEMPLED2": "CCOB daughter board back temp",
    "TEMPBRD": "CCOB board temp",
    "CCOBLED": "Selected CCOB LED",
    "CCOBCURR": "CCOB LED current",
    "CCOBADC": "CCOB Photodiode value",
    "CCOBFLST": "CCOB flash time (commanded)",
    "PROJTIME": "CCOB flash time (measured)",
    "CCOBFLUX": "CCOB target flux",
}

# The magic detector which writes the per-image metadata shard
TS8_METADATA_DETECTOR = 18
LSSTCOMCAM_METADATA_DETECTOR = 0
# The magic detectors which write the REB headers for TS8, selected to land on
# the three different REBs
TS8_REB_HEADER_DETECTORS = [18, 21, 24]
LSSTCOMCAM_REB_HEADER_DETECTORS = [0, 3, 6]
# The magic detectors which write the REB headers for LSSTCam, selected to land
# on all the different REBs
LSSTCAM_REB_HEADER_DETECTORS = [13, 94]  # 1x e2v, 1x ITL. Need a better way to do this for partial reads.

TS8_X_RANGE = (-63.2, 63.1)
TS8_Y_RANGE = (-62.5, 62.6)
LSSTCAM_X_RANGE = (-325, 325)
LSSTCAM_Y_RANGE = (-325, 325)


def isOneRaft(instrument: str) -> bool:
    """A convenience function for checking if we are processing a single raft.

    Parameters
    ----------
    instrument : `str`
        The instrument.
    """
    return instrument in ["LSST-TS8", "LSSTComCam", "LSSTComCamSim"]


class RawProcesser:
    """Class for processing raw cleanroom data for RubinTV.

    Currently, this class loads the raws, assembles them, and writes out a
    binned image for use in the focal plane mosaic and data shard for the
    per-amp noises.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration.
    instrument : `str`
        The instrument.
    detectors : `int` or `list` [`int`]
        The detector, or detectors, to process.
    doRaise : `bool`
        If True, raise exceptions instead of logging them.
    """

    def __init__(
        self,
        butler: Butler,
        locationConfig: LocationConfig,
        instrument: str,
        detectors: list[int],
        doRaise=False,
    ) -> None:
        if instrument not in ["LSST-TS8", "LSSTCam", "LSSTComCam", "LSSTComCamSim"]:
            raise ValueError(f"Instrument {instrument} not supported, must be LSST-TS8 or LSSTCam")
        self.locationConfig = locationConfig
        self.butler = butler
        self.instrument = instrument
        match instrument:
            case "LSST-TS8":
                metadataShardPath = locationConfig.ts8MetadataShardPath
            case "LSSTComCam":
                metadataShardPath = locationConfig.comCamMetadataShardPath
            case "LSSTComCamSim":
                metadataShardPath = locationConfig.comCamSimMetadataShardPath
            case "LSSTCam":
                metadataShardPath = locationConfig.botMetadataShardPath
            case _:
                raise ValueError(f"Instrument {instrument} not supported.")
        self.metadataShardPath = metadataShardPath

        self.detectors = list(ensure_iterable(detectors))
        name = f'rawProcesser_{instrument}_{",".join([str(d) for d in self.detectors])}'
        self.log = _LOG.getChild(name)
        self.watcher = FileWatcher(
            locationConfig=locationConfig, instrument=self.instrument, dataProduct="raw", doRaise=doRaise
        )

        self.isrTask = self.makeIsrTask()

    def makeIsrTask(self) -> Task:
        """Make an isrTask with the appropriate configuration.

        Returns
        -------
        isrTask : `lsst.ip.isr.IsrTask`
            The isrTask.
        """
        isrConfig = IsrTask.ConfigClass()
        isrConfig.doLinearize = False
        isrConfig.doBias = False
        isrConfig.doFlat = False
        isrConfig.doDark = False
        isrConfig.doFringe = False
        isrConfig.doDefect = False
        isrConfig.doWrite = False
        isrConfig.doSaturation = False
        isrConfig.doVariance = False  # we can have negative values which this would mark as BAD
        isrConfig.doNanInterpolation = False
        isrConfig.doNanMasking = False
        isrConfig.doSaturation = False
        isrConfig.doSaturationInterpolation = False
        isrConfig.doWidenSaturationTrails = False

        isrConfig.overscan.fitType = "MEDIAN_PER_ROW"
        isrConfig.overscan.doParallelOverscan = True  # NB: doParallelOverscan *requires* MEDIAN_PER_ROW too
        isrConfig.doApplyGains = True
        isrConfig.usePtcGains = True

        return IsrTask(config=isrConfig)

    def runIsr(self, raw: Exposure):
        """Run the isrTask to get a post-ISR exposure.

        Parameters
        ----------
        raw : `lsst.afw.image.Exposure`
            The raw exposure.

        Returns
        -------
        postIsr : `lsst.afw.image.Exposure`
            The post-ISR exposure.
        """
        ptcDataset = None
        if self.isrTask.config.doApplyGains:  # type: ignore
            gains = getGains(self.instrument)

            match self.instrument:
                case "LSST-TS8":
                    # The TS8 dict keys are just like S01 part as there is no
                    # raft
                    detNameForGains = raw.detector.getName().split("_")[1]
                case "LSSTComCam":
                    # the dict is keyed by the detector's short name like TS8
                    detNameForGains = raw.detector.getName().split("_")[1]
                case "LSSTComCamSim":  # treat as same as ComCam for now
                    detNameForGains = raw.detector.getName().split("_")[1]
                case "LSSTCam":
                    # the dict is keyed by the detector's full name e.g R01_S21
                    detNameForGains = raw.detector.getName()
                case _:
                    raise ValueError(f"Unknown instrument {self.instrument}")

            ptcDataset = gainsToPtcDataset(gains[detNameForGains])

        postIsr = self.isrTask.run(raw, ptc=ptcDataset).exposure  # type: ignore
        return postIsr

    def writeExpRecordMetadataShard(self, expRecord: DimensionRecord) -> None:
        """Write the exposure record metedata to a shard.

        Only fires once, based on the value of TS8_METADATA_DETECTOR or
        LSSTCOMCAM_METADATA_DETECTOR, depending on the instrument.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        metadataDetector = (
            TS8_METADATA_DETECTOR if self.instrument == "LSST-TS8" else LSSTCOMCAM_METADATA_DETECTOR
        )
        if metadataDetector not in self.detectors:
            return

        md = {}
        md["Exposure time"] = expRecord.exposure_time
        md["Dark time"] = expRecord.dark_time
        md["Image type"] = expRecord.observation_type
        md["Test type"] = expRecord.observation_reason
        md["Date"] = expRecord.timespan.begin.isot
        md["Run number"] = expRecord.science_program

        seqNum = expRecord.seq_num
        dayObs = expRecord.day_obs
        shardData = {seqNum: md}

        writeMetadataShard(self.metadataShardPath, dayObs, shardData)

    def calculateNoise(self, overscanData, nSkipParallel, nSkipSerial):
        """Calculate the noise, based on the overscans in a raw image.

        Parameters
        ----------
        overscanData : `numpy.ndarray`
            The overscan data.

        Returns
        -------
        noise : `float`
            The sigma-clipped standard deviation of the overscan.
        overscanMean : `float`
            The sigma-clipped mean of the overscan.
        nSkipParallel : `int`
            The number of parallel overscan rows to skip.
        nSkipSerial : `int`
            The number of serial overscan pixels to skip in each row.
        """
        data = overscanData[nSkipSerial:, nSkipParallel:]

        sctrl = afwMath.StatisticsControl()
        sctrl.setNumSigmaClip(3)
        sctrl.setNumIter(2)
        statTypes = afwMath.MEANCLIP | afwMath.STDEVCLIP
        tempImage = afwImage.MaskedImageF(afwImage.ImageF(data))
        stats = afwMath.makeStatistics(tempImage, statTypes, sctrl)
        std, _ = stats.getResult(afwMath.STDEVCLIP)
        mean, _ = stats.getResult(afwMath.MEANCLIP)
        return std, mean

    def writeImageMetadataShard(self, expRecord: DimensionRecord, exposureMetadata: dict[Any, Any]) -> None:
        """Write the image metadata to a shard.

        Note that all these header values are constant across all detectors,
        so it is perfectly safe to pull them from one and display once.

        Only fires once, based on the value of TS8_METADATA_DETECTOR or
        LSSTCOMCAM_METADATA_DETECTOR, depending on the instrument.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        exposureMetadata : `dict`
            The exposure metadata as a dict.
        """
        metadataDetector = (
            TS8_METADATA_DETECTOR if self.instrument == "LSST-TS8" else LSSTCOMCAM_METADATA_DETECTOR
        )
        if metadataDetector not in self.detectors:
            return

        md = {}
        for headerKey, displayValue in PER_IMAGE_HEADERS.items():
            value = exposureMetadata.get(headerKey)
            if value:
                md[displayValue] = value

        seqNum = expRecord.seq_num
        dayObs = expRecord.day_obs
        shardData = {seqNum: md}

        writeMetadataShard(self.metadataShardPath, dayObs, shardData)

    def writeRebHeaderShard(self, expRecord: DimensionRecord, raw: Exposure) -> None:
        """Write the REB condition metadata to a shard.

        Note that all these header values are constant across all detectors, so
        it is perfectly safe to pull them from one and display once.

        Only fires once per REB, based on the value of
        TS8_REB_HEADER_DETECTORS, LSSTCOMCAM_REB_HEADER_DETECTORS, or
        LSSTCAM_REB_HEADER_DETECTORS, depending on the instrument.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        raw : `lsst.afw.image.Exposure`
            The image containing the detector and metadata.
        """
        detector = raw.detector
        exposureMetadata = raw.getMetadata().toDict()

        if self.instrument == "LSST-TS8":
            detectorList = TS8_REB_HEADER_DETECTORS
            rebNumber = detector.getName().split("_")[1][1]  # This is the X part of the S part of R12_SXY
            itemName = f"REB{rebNumber} Header"
        elif self.instrument in ["LSSTComCam", "LSSTComCamSim"]:
            detectorList = LSSTCOMCAM_REB_HEADER_DETECTORS
            rebNumber = detector.getName().split("_")[1][1]  # This is the X part of the S part of R12_SXY
            itemName = f"REB{rebNumber} Header"
        elif self.instrument == "LSSTCam":
            detectorList = LSSTCAM_REB_HEADER_DETECTORS
            rebNumber = detector.getName()  # use the full name for now
            itemName = f"REB{rebNumber} Header"
        else:
            raise ValueError(f"Unknown instrument {self.instrument}")

        if not any(detNum in detectorList for detNum in self.detectors):
            return

        md = {}
        for headerKey in REB_HEADERS:
            value = exposureMetadata.get(headerKey)
            if value:
                md[headerKey] = value

        md["DISPLAY_VALUE"] = "📖"

        seqNum = expRecord.seq_num
        dayObs = expRecord.day_obs
        shardData = {seqNum: {itemName: md}}  # uploading a dict item here!

        writeMetadataShard(self.metadataShardPath, dayObs, shardData)

    def callback(self, expRecord: DimensionRecord) -> None:
        """Method called on each new expRecord as it is found in the repo.

        Current behaviour is to:
            Load the raw
            Assemble the image
            Calculate the std dev of each amp
            Write these std devs out as sharded data
            Write a binned image out according to locationConfig.binning

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """

        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        try:  # metadata writing should never bring the processing down
            # this only fires for a single detector, based on METADATA_DETECTOR
            self.writeExpRecordMetadataShard(expRecord)
        except Exception:
            self.log.exception(f"Failed to write metadata shard for {expRecord.dataId}")

        if expRecord.observation_type == "scan":
            self.log.info(f"Skipping scan-mode image {expRecord.dataId}")
            return

        for detNum in self.detectors:
            dataId = dafButler.DataCoordinate.standardize(expRecord.dataId, detector=detNum)
            self.log.info(f"Processing raw for {dataId}")
            ampNoises = {}
            raw = waitForDataProduct(
                butler=self.butler,
                expRecord=expRecord,
                dataset="raw",
                detector=detNum,
                timeout=5,
                logger=self.log,
            )
            if not raw:
                # Note to future: given that if we timeout here, everything
                # downstream of this will fail, perhaps we should consider
                # writing some kind of failure shard to signal that other
                # things shouldn't bother waiting, rather than letting all
                # downstream things timeout in these situations. I guess it
                # depends on how often this actually ends up happening.
                # Perhaps, give we should be striving for always processing
                # everything, especially so early on (i.e. in isr on lab data)
                # we actually shouldn't do this.
                continue  # waitForDataProduct itself warns if it times out

            self.writeImageMetadataShard(expRecord, raw.getMetadata().toDict())
            self.writeRebHeaderShard(expRecord, raw)

            detector = raw.detector
            imaging, serialOverscan, parallelOverscan = getAmplifierRegions(raw)
            for amp in detector:
                ampName = amp.getName()
                overscan = serialOverscan[ampName]
                noise, _ = self.calculateNoise(overscan, 5, 200)
                entryName = "_".join([detector.getName(), amp.getName()])
                ampNoises[entryName] = float(noise)  # numpy float32 is not json serializable

            # write the data
            writeDataShard(
                path=self.locationConfig.calculatedDataPath,
                instrument=self.instrument,
                dayObs=dayObs,
                seqNum=seqNum,
                dataSetName="rawNoises",
                dataDict=ampNoises,
            )
            self.log.info(f"Wrote rawNoises data shard for detector {detNum}")
            # then signal we're done for downstream
            writeDataIdFile(self.locationConfig.dataIdScanPath, "rawNoises", expRecord, self.log)

            postIsr = self.runIsr(raw)

            writeBinnedImage(
                exp=postIsr,
                instrument=self.instrument,
                outputPath=self.locationConfig.calculatedDataPath,
                binSize=self.locationConfig.binning,
            )
            writeDataIdFile(self.locationConfig.dataIdScanPath, "binnedImage", expRecord, self.log)
            self.log.info(f"Wrote binned image for detector {detNum}")

            del raw
            del postIsr

    def run(self):
        """Run continuously, calling the callback method with the latest
        expRecord.
        """
        self.watcher.run(self.callback)


class Plotter:
    """Channel for producing the plots for the cleanroom on RubinTV.

    This will make plots for whatever it can find, and if the input data forms
    a complete set across the focal plane (taking into account partial
    readouts), deletes the input data, both to tidy up after itself, and to
    signal that this was completely processed and nothing is left to do.

    The Replotter class, which inherits from this one, will replot anything
    that it finds to be complete later on, motivating this to leave any
    incomplete data, so that other processes can make complete plots once their
    input processing has finished.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration.
    instrument : `str`
        The instrument.
    doRaise : `bool`
        If True, raise exceptions instead of logging them.
    """

    def __init__(
        self, butler: Butler, locationConfig: LocationConfig, instrument: str, doRaise: bool = False
    ) -> None:
        self.locationConfig = locationConfig
        self.butler = butler
        self.camera = getCamera(self.butler, instrument)
        self.instrument = instrument
        self.s3Uploader = MultiUploader()
        self.log = _LOG.getChild(f"plotter_{self.instrument}")
        # currently watching for binnedImage as this is made last
        self.watcher = FileWatcher(
            locationConfig=locationConfig,
            instrument=self.instrument,
            dataProduct="binnedImage",
            doRaise=doRaise,
        )
        self.fig = plt.figure(figsize=(12, 12))
        self.doRaise = doRaise
        self.STALE_AGE_SECONDS = 45  # in seconds

    def plotNoises(self, expRecord: DimensionRecord, timeout: float) -> str | None:
        """Create a focal plane heatmap of the per-amplifier noises as a png.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        timeout : `int`
            The timeout for waiting for the data to be complete.

        Returns
        -------
        filename : `str`
            The filename the plot was saved to.
        """
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        nExpected = getNumExpectedItems(expRecord)
        noises, _ = getShardedData(
            path=self.locationConfig.calculatedDataPath,
            instrument=self.instrument,
            dayObs=dayObs,
            seqNum=seqNum,
            dataSetName="rawNoises",
            nExpected=nExpected,
            timeout=timeout,
            logger=self.log,
            deleteIfComplete=True,
        )

        if not noises:
            self.log.warning(f"No noise data found for {expRecord.dataId}")
            return None

        perCcdNoises = fullAmpDictToPerCcdDicts(noises)
        self.fig.clear()
        ax = self.fig.gca()
        ax.clear()

        xRange = TS8_X_RANGE if isOneRaft(self.instrument) else LSSTCAM_X_RANGE
        yRange = TS8_Y_RANGE if isOneRaft(self.instrument) else LSSTCAM_Y_RANGE

        instPrefix = self.getInstrumentChannelName(self.instrument)

        plotName = f"{instPrefix}-noise-map_dayObs_{dayObs}_seqNum_{seqNum}.png"
        saveFile = os.path.join(self.locationConfig.plotPath, plotName)
        focal_plane_plotting.plot_focal_plane(
            ax, perCcdNoises, x_range=xRange, y_range=yRange, z_range=(0, 15), camera=self.camera
        )

        self.fig.savefig(saveFile)
        self.log.info(f"Wrote rawNoises plot for {expRecord.dataId} to {saveFile}")

        return saveFile

    def plotFocalPlane(self, expRecord: DimensionRecord, timeout: float) -> str:
        """Create a binned mosaic of the full focal plane as a png.

        The binning factor is controlled via the locationConfig.binning
        property.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        timeout : `int`
            The timeout for waiting for the data to be complete.

        Returns
        -------
        filename : `str`
            The filename the plot was saved to.
        """
        expId = expRecord.id
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        plotName = f"ts8FocalPlane_dayObs_{dayObs}_seqNum_{seqNum}.png"
        saveFile = os.path.join(self.locationConfig.plotPath, plotName)

        nExpected = getNumExpectedItems(expRecord)

        self.fig.clear()
        plotFocalPlaneMosaic(
            butler=self.butler,
            figureOrDisplay=self.fig,
            expId=expId,
            camera=self.camera,
            binSize=self.locationConfig.binning,
            dataPath=self.locationConfig.calculatedDataPath,
            savePlotAs=saveFile,
            nExpected=nExpected,
            stretch="CCS",
            timeout=timeout,
            logger=self.log,
        )
        self.log.info(f"Wrote focal plane plot for {expRecord.dataId} to {saveFile}")
        return saveFile

    @staticmethod
    def getInstrumentChannelName(instrument: str) -> str:
        """Get the instrument channel name for the current instrument.

        This is the plot prefix to use for upload.

        Parameters
        ----------
        instrument : `str`
            The instrument name, e.g. 'LSSTCam'.

        Returns
        -------
        channel : `str`
            The channel prefix name.
        """
        match instrument:
            case "LSST-TS8":
                return "ts8"
            case "LSSTComCam":
                return "comcam"
            case "LSSTComCamSim":  # treat as same as ComCam for now
                return "comcam_sim"
            case "LSSTCam":
                return "slac_lsstcam"
            case _:
                raise ValueError(f"Unknown instrument {instrument}")

    def callback(
        self,
        expRecord: DimensionRecord,
        doPlotMosaic: bool = False,
        doPlotNoises: bool = False,
        timeout: float = 5,
    ) -> None:
        """Method called on each new expRecord as it is found in the repo.

        Note: the callback is used elsewhere to reprocess old data, so the
        default doX kwargs are all set to False, but are overrided to True in
        this class' run() method, such that replotting code sets what it *does*
        want to True, rather than having to know to set everything it *doesn't*
        want to False. This might feel a little counterintuitive here, but it
        makes the replotting code much more natural.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        doPlotMosaic : `bool`
            If True, plot and upload the focal plane mosaic.
        doPlotNoises : `bool`
            If True, plot and upload the per-amplifier noise map.
        timeout : `float`
            How to wait for data products to land before giving up and plotting
            what we have.
        """
        self.log.info(f"Making plots for {expRecord.dataId}")
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        instPrefix = self.getInstrumentChannelName(self.instrument)

        if doPlotNoises:
            noiseMapFile = self.plotNoises(expRecord, timeout=timeout)
            if noiseMapFile:  # only upload on plot success
                self.s3Uploader.uploadPerSeqNumPlot(
                    instrument=instPrefix,
                    plotName="noise_map",
                    dayObs=dayObs,
                    seqNum=seqNum,
                    filename=noiseMapFile,
                )

        if doPlotMosaic:
            focalPlaneFile = self.plotFocalPlane(expRecord, timeout=timeout)
            if focalPlaneFile:  # only upload on plot success
                self.s3Uploader.uploadPerSeqNumPlot(
                    instrument=instPrefix,
                    plotName="focal_plane_mosaic",
                    dayObs=dayObs,
                    seqNum=seqNum,
                    filename=focalPlaneFile,
                )

    def run(self):
        """Run continuously, calling the callback method with the latest
        expRecord.
        """
        self.watcher.run(self.callback, doPlotMosaic=True, doPlotNoises=True)


class Replotter(Plotter):
    # TODO: Consider removing this whole class. If it's not in use by the time
    # we're on sky then that's probably the right time. I think the redis
    # system will mean we don't need this, but let's wait until we're sure, and
    # then delete.

    @dataclass
    class ReplotterWorkload(Uploader):  # TODO: DM-44166 why does this inherit from Uploader?!
        finderFunction: Callable
        workerFunction: Callable
        name: str

    def getLeftoverMosaicDict(self):
        """Get exposure records for which there are leftover mosaic files.

        Returns a dict, keyed by exposure record, containing a list of the
        files for that record.

        Returns
        -------
        shards : `dict` [`lsst.daf.butler.DimensionRecord`, `list` [`str`]]
            A dictionary, keyed by exposure record, of the binned image files.
        """
        # must grab this before scanning for data, to protect against new
        # images arriving during the scrape. This watcher watches for
        # binnedImages, which are always an earlier or equal expRecord to the
        # raw one, and so is the right one to choose
        mostRecentExp = self.watcher.getMostRecentExpRecord()

        records = {}
        dataPath = self.locationConfig.calculatedDataPath
        expIds = getBinnedImageExpIds(dataPath, self.instrument)
        if not expIds:
            return {}

        allFiles = getBinnedImageFiles(dataPath, self.instrument)
        self.log.debug(f"Found {len(expIds)} expIds with binned images, and {len(allFiles)} associated files")
        for expId in expIds:
            record = getExpRecord(self.butler, self.instrument, expId=expId)
            # a list comp is *much* faster than re-globbing if there are a lot
            # of files.
            binnedImages = [f for f in allFiles if f.find(f"{expId}") != -1]
            records[record] = binnedImages

        # Check to see if there is anything more recent than the most recent
        # expRecord which existed when we started looking for data, and if so,
        # remove them, so that we don't collide with the running processes.
        # Note this is <= not < to deliberately include that record itself,
        # because the complete ones are now only processed if also stale.
        records = {r: files for r, files in records.items() if r.timespan.end <= mostRecentExp.timespan.end}
        return records

    def run(self):
        """Run continuously, looking for complete file sets and plotting
        them.
        """
        # workloads are a dataclass, holding a callable called finderFunction,
        # which returns returns a dict of {expRecord: [corresponding files]}
        # which are the exposure records which have data to be processed, and
        # the corresponding files to process, a callable called workerFunction,
        # which does the actual processing of each exposure record, and a
        # string called name, which is used for logging.
        workload1 = self.ReplotterWorkload(
            finderFunction=self.getLeftoverMosaicDict,
            workerFunction=partial(self.callback, doPlotMosaic=True, timeout=0),
            name="mosaic",
        )
        workload2 = self.ReplotterWorkload(
            finderFunction=partial(self.getDataShardFilesDict, "rawNoises"),
            workerFunction=partial(self.callback, doPlotNoises=True, timeout=0),
            name="noisePlot",
        )

        while True:
            for workload in [workload1, workload2]:
                leftovers = workload.finderFunction()
                if not leftovers:
                    continue

                records = list(leftovers.keys())
                records = sorted(records, key=lambda r: r.timespan.end, reverse=True)  # newest first
                for recordNum, expRecord in enumerate(records):
                    files = leftovers[expRecord]
                    self.log.info(f"Processing leftover {workload.name} {recordNum + 1} of {len(leftovers)}")
                    isComplete = getNumExpectedItems(expRecord) == len(files)
                    isStale = getExpRecordAge(expRecord) > self.STALE_AGE_SECONDS
                    if not isStale:
                        # note that unless it's stale we don't process, ever,
                        # because this could collide with the normally running
                        # processes.
                        self.log.info(
                            f"Not processing {workload.name} for {expRecord.dataId},"
                            " waiting for it to go stale"
                        )
                        continue
                    if isComplete:
                        # no need to delete here because it's complete and so
                        # will self-delete automatically
                        self.log.info(f"Remaking full {workload.name} for {expRecord.dataId}")
                        workload.workerFunction(expRecord)
                    else:
                        self.log.info(f"Remaking partial, stale {workload.name} for {expRecord.dataId}")
                        workload.workerFunction(expRecord)
                        self.log.info(
                            f"Removing {len(files)} stale {workload.name} files" f" for {expRecord.dataId}"
                        )
                        for f in files:
                            os.remove(f)

            sleep(10)  # this need not be very aggressive

    def getDayObsSeqNumTuplesFromFiles(self, files):
        """Get the unique dayObs and seqNum tuples from a list of files.

        For a list of many files, get the set of (dayObs, seqNum) tuples for
        which there is data, as a list.

        Parameters
        ----------
        files : `list` [`str`]
            The list of files.

        Returns
        -------
        tuples : `list` [`tuple` [`int`, `int`]]
            The list of tuples, sorted by dayObs and seqNum.
        """
        dayObsSeqNumTuples = set([self.dayObsSeqNumFromFilename(file) for file in files])
        return sorted(list(dayObsSeqNumTuples), key=lambda x: (x[0], x[1]))

    @staticmethod
    def dayObsSeqNumFromFilename(filename):
        """Get the dayObs and seqNum from a data shard file.

        Parameters
        ----------
        filename : `str`
            The filename.

        Returns
        -------
        dayObs : `int`
            The dayObs.
        seqNum : `int`
            The seqNum.
        """
        # files are like
        # dataShard-rawNoises-LSSTCam-dayObs_20230601_seqNum_000059_...json
        basename = os.path.basename(filename)  # in case of underscores and dayObs duplication in a full path
        dayObs = int(basename.split("dayObs_")[1].split("_")[0])
        seqNum = int(basename.split("seqNum_")[1].split("_")[0])
        return dayObs, seqNum

    def getDataShardFilesDict(self, dataSetName):
        """Get a dictionary of data shard files, keyed by exposure record.

        Parameters
        ----------
        dataSetName : `str`
            The data type, e.g. 'rawNoises'.

        Returns
        -------
        shards : `dict` [`lsst.daf.butler.DimensionRecord`, `list` [`str`]]
            A dictionary, keyed by dayObs and seqNum, of lists of data shard
            files.
        """
        # must grab this before scanning for data, to protect against new
        # images arriving during the scrape. This watcher watches for
        # binnedImages, which are always an earlier or equal expRecord to the
        # raw one, and so is the right one to choose
        mostRecentExp = self.watcher.getMostRecentExpRecord()

        shards = {}
        # get all the files for this data type
        pattern = getGlobPatternForShardedData(
            path=self.locationConfig.calculatedDataPath,
            dataSetName=dataSetName,
            instrument=self.instrument,
            dayObs="*",
            seqNum="*",
        )
        files = glob.glob(pattern)

        # get the ``set`` of (dayObs, seqNum)s this list of files covers, i.e.
        # which images we have some data for. ``getExpRecord`` is slow, so
        # ensure we only do this for each image, rather than for each file.
        dayObsSeqNums = self.getDayObsSeqNumTuplesFromFiles(files)
        # _recordMapping is temporary object so that we can look up the main
        # dict key, which as an expRecord, by its dayObs and seqNum
        _recordMapping = {}
        for dayObs, seqNum in dayObsSeqNums:
            record = getExpRecord(self.butler, self.instrument, dayObs=dayObs, seqNum=seqNum)
            shards[record] = []  # creating the template return object
            _recordMapping[(dayObs, seqNum)] = record

        for file in files:
            dayObs, seqNum = self.dayObsSeqNumFromFilename(file)
            expRecord = _recordMapping[(dayObs, seqNum)]
            shards[expRecord].append(file)

        # Check to see if there is anything more recent than the most recent
        # expRecord which existed when we started looking for data, and if so,
        # remove the entries so that we don't collide with the running
        # processes. Note this is <= not < to deliberately include that record
        # itself, because the complete ones are now only processed if also
        # stale.
        shards = {
            record: fileList
            for record, fileList in shards.items()
            if record.timespan.end <= mostRecentExp.timespan.end
        }
        return shards
