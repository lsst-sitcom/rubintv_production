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
import logging
import matplotlib.pyplot as plt
import numpy as np
from lsst.eo.pipe.plotting import focal_plane_plotting

from lsst.ip.isr import IsrTask

import lsst.daf.butler as dafButler
from lsst.utils.iteration import ensure_iterable
import lsst.afw.math as afwMath
import lsst.afw.image as afwImage

from .utils import waitForDataProduct, getAmplifierRegions
from ..utils import writeDataShard, getShardedData, writeMetadataShard
from ..uploaders import Uploader
from ..watchers import FileWatcher, writeDataIdFile
from .mosaicing import writeBinnedImage, plotFocalPlaneMosaic
from .utils import fullAmpDictToPerCcdDicts, getCamera

_LOG = logging.getLogger(__name__)

METADATA_DETECTOR = 18  # the magic detector which writes the metadata shard
TS8_X_RANGE = (-63.2, 63.1)
TS8_Y_RANGE = (-62.5, 62.6)
LSSTCAM_X_RANGE = (-325, 325)
LSSTCAM_Y_RANGE = (-325, 325)


def getNumExpectedItems(instrument, expRecord):
    """A placeholder function for getting the number of expected items.

    For a given instrument, get the number of detectors which were read out or
    for which we otherwise expect to have data for.

    This method will be updated once we have a way of knowing, from the camera,
    how many detectors were actually read out (the plan is the CCS writes a
    JSON file with this info).

    Parameters
    ----------
    instrument : `str`
        The instrument.
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record. This is currently unused, but will be used once
        we are doing this properly.
    """
    if instrument == "LATISS":
        return 1
    elif instrument == "LSSTCam":
        return 201
    elif instrument == "LSST-TS8":
        return 9
    else:
        raise ValueError(f"Unknown instrument {instrument}")


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
    def __init__(self, butler, locationConfig, instrument, detectors, doRaise=False):
        if instrument not in ['LSST-TS8', 'LSSTCam']:
            raise ValueError(f'Instrument {instrument} not supported, must be LSST-TS8 or LSSTCam')
        self.locationConfig = locationConfig
        self.butler = butler
        self.instrument = instrument
        self.detectors = list(ensure_iterable(detectors))
        name = f'rawProcesser_{instrument}_{",".join([str(d) for d in self.detectors])}'
        self.log = _LOG.getChild(name)
        self.watcher = FileWatcher(locationConfig=locationConfig,
                                   dataProduct='raw',
                                   doRaise=doRaise)

        isrConfig = IsrTask.ConfigClass()
        isrConfig.doLinearize = False
        isrConfig.doBias = False
        isrConfig.doFlat = False
        isrConfig.doDark = False
        isrConfig.doFringe = False
        isrConfig.doDefect = False
        isrConfig.doWrite = False
        isrConfig.doSaturation = False
        isrConfig.doNanInterpolation = False
        isrConfig.doNanMasking = False
        isrConfig.doSaturation = False
        isrConfig.doSaturationInterpolation = False
        isrConfig.doWidenSaturationTrails = False
        isrConfig.overscan.fitType = 'MEDIAN_PER_ROW'
        isrConfig.overscan.doParallelOverscan = True  # NB: doParallelOverscan *requires* MEDIAN_PER_ROW too
        self.isrTask = IsrTask(config=isrConfig)

    def writeExpRecordMetadataShard(self, expRecord):
        """Write the exposure record metedata to a shard.

        Only fires once, based on the value METADATA_DETECTOR.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        if METADATA_DETECTOR not in self.detectors:
            return

        md = {}
        md['Exposure time'] = expRecord.exposure_time
        md['Dark time'] = expRecord.dark_time
        md['Image type'] = expRecord.observation_type
        md['Test type'] = expRecord.observation_reason
        md['Date'] = expRecord.timespan.begin.isot

        seqNum = expRecord.seq_num
        dayObs = expRecord.day_obs
        shardData = {seqNum: md}

        writeMetadataShard(self.locationConfig.ts8MetadataShardPath, dayObs, shardData)

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

    def callback(self, expRecord):
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
            self.log.exception(f'Failed to write metadata shard for {expRecord.dataId}')

        for detNum in self.detectors:
            dataId = dafButler.DataCoordinate.standardize(expRecord.dataId, detector=detNum)
            self.log.info(f'Processing raw for {dataId}')
            ampNoises = {}
            raw = waitForDataProduct(butler=self.butler,
                                     expRecord=expRecord,
                                     dataset='raw',
                                     detector=detNum,
                                     timeout=5,
                                     logger=self.log)
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

            detector = raw.detector
            imaging, serialOverscan, parallelOverscan = getAmplifierRegions(raw)
            for amp in detector:
                ampName = amp.getName()
                overscan = serialOverscan[ampName]
                noise, _ = self.calculateNoise(overscan, 5, 200)
                entryName = "_".join([detector.getName(), amp.getName()])
                ampNoises[entryName] = float(noise)  # numpy float32 is not json serializable

            # write the data
            writeDataShard(self.locationConfig.calculatedDataPath, dayObs, seqNum, 'rawNoises', ampNoises)
            self.log.info(f'Wrote metadata shard for detector {detNum}')
            # then signal we're done for downstream
            writeDataIdFile(self.locationConfig.dataIdScanPath, 'rawNoises', expRecord, self.log)

            postIsr = self.isrTask.run(raw).exposure
            writeBinnedImage(postIsr, self.locationConfig.binnedImagePath, self.locationConfig.binning)
            writeDataIdFile(self.locationConfig.dataIdScanPath, 'binnedImage', expRecord, self.log)
            self.log.info(f'Wrote binned image for detector {detNum}')

    def run(self):
        """Run continuously, calling the callback method with the latest
        expRecord.
        """
        self.watcher.run(self.callback)


class Plotter:
    """Channel for producing the plots for the cleanroom on RubinTv.

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
    def __init__(self, butler, locationConfig, instrument, doRaise=False):
        self.locationConfig = locationConfig
        self.butler = butler
        self.camera = getCamera(self.butler, instrument)
        self.instrument = instrument
        self.uploader = Uploader(self.locationConfig.bucketName)
        self.log = _LOG.getChild(f"plotter_{self.instrument}")
        # currently watching for binnedImage as this is made last
        self.watcher = FileWatcher(locationConfig=locationConfig,
                                   dataProduct='binnedImage',
                                   doRaise=doRaise)
        self.fig = plt.figure(figsize=(12, 12))
        self.doRaise = doRaise

    def plotNoises(self, expRecord):
        """Create a focal plane heatmap of the per-amplifier noises as a png.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.

        Returns
        -------
        filename : `str`
            The filename the plot was saved to.
        """
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        nExpected = getNumExpectedItems(self.instrument, expRecord)  # expRecord currently unused in function
        noises, _ = getShardedData(self.locationConfig.calculatedDataPath, dayObs, seqNum, 'rawNoises',
                                   nExpected=nExpected,
                                   logger=self.log,
                                   deleteAfterReading=False)

        if not noises:
            self.log.warning(f'No noise data found for {expRecord.dataId}')
            return None

        perCcdNoises = fullAmpDictToPerCcdDicts(noises)
        plt.figure(figsize=(10, 10))
        ax = plt.subplot(111)

        xRange = TS8_X_RANGE if self.instrument == 'LSST-TS8' else LSSTCAM_X_RANGE
        yRange = TS8_Y_RANGE if self.instrument == 'LSST-TS8' else LSSTCAM_Y_RANGE

        plotName = f'noise-map_dayObs_{dayObs}_seqNum_{seqNum}.png'
        saveFile = os.path.join(self.locationConfig.plotPath, plotName)
        focal_plane_plotting.plot_focal_plane(ax, perCcdNoises,
                                              x_range=xRange,
                                              y_range=yRange,
                                              camera=self.camera)

        plt.savefig(saveFile)
        self.log.info(f'Wrote rawNoises plot for {expRecord.dataId} to {saveFile}')

        return saveFile

    def plotFocalPlane(self, expRecord):
        """Create a binned mosaic of the full focal plane as a png.

        The binning factor is controlled via the locationConfig.binning
        property.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.

        Returns
        -------
        filename : `str`
            The filename the plot was saved to.
        """
        expId = expRecord.id
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        plotName = f'ts8FocalPlane_dayObs_{dayObs}_seqNum_{seqNum}.png'
        saveFile = os.path.join(self.locationConfig.plotPath, plotName)

        plotFocalPlaneMosaic(self.butler, expId, self.camera, self.locationConfig.binning,
                             self.locationConfig.binnedImagePath, saveFile, timeout=5)
        self.log.info(f'Wrote focal plane plot for {expRecord.dataId} to {saveFile}')
        return saveFile

    def callback(self, expRecord):
        """Method called on each new expRecord as it is found in the repo.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        self.log.info(f'Making plots for {expRecord.dataId}')
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        # TODO: Need some kind of wait mechanism for each of these
        noiseMapFile = self.plotNoises(expRecord)
        channel = 'ts8_noise_map'
        self.uploader.uploadPerSeqNumPlot(channel, dayObs, seqNum, noiseMapFile)

        focalPlaneFile = self.plotFocalPlane(expRecord)
        channel = 'ts8_focal_plane_mosiac'
        self.uploader.uploadPerSeqNumPlot(channel, dayObs, seqNum, focalPlaneFile)

    def run(self):
        """Run continuously, calling the callback method with the latest
        expRecord.
        """
        self.watcher.run(self.callback)
