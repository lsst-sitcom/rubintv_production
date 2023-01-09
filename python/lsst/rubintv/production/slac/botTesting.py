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

from lsst.ip.isr import AssembleCcdTask

from ..utils import writeDataShard, LocationConfig, getShardedData
from ..uploaders import Uploader
from ..watchers import FileWatcher, writeDataIdFile
from .mosaicing import writeBinnedImage, plotFocalPlaneMosaic
from .utils import fullAmpDictToPerCcdDicts, getCamera

_LOG = logging.getLogger(__name__)


def getNumExpectedItems(instrument):
    """Very much just a placeholder once we have a way of knowing how many
    detectors were read out for a given image.
    """
    if instrument == "LATISS":
        return 1
    elif instrument == "LSSTCam":
        return 201
    elif instrument == "LSST-TS8":
        return 9
    else:
        raise ValueError(f"Unknown instrument {instrument}")


class RawProcesser():
    """
    """
    def __init__(self, butler, location, instrument, detector, doRaise=False):
        if instrument not in ['LSST-TS8', 'LSSTCam']:
            raise ValueError(f'Instrument {instrument} not supported, must be LSST-TS8 or LSSTCam')
        self.locationConfig = LocationConfig(location)
        self.butler = butler
        self.instrument = instrument
        self.detector = detector
        name = f'rawProcesser_{instrument}_{detector:03}'
        self.log = _LOG.getChild(name)
        self.watcher = FileWatcher(locationConfig=self.locationConfig,
                                   dataProduct='raw',
                                   doRaise=doRaise)

        config = AssembleCcdTask.ConfigClass()
        config.doTrim = True
        self.assembleTask = AssembleCcdTask(config=config)

    def callback(self, expRecord):
        """Method called on each new dataId as it is found in the repo.

        Produce a quickLookExp of the latest image, and butler.put() it to the
        repo so that downstream processes can find and use it.
        """
        # run isr, calculate any numbers you want here with the raw
        # put those into a json or pickle
        # maybe create a thumbnail, or actually just the binned raw image
        # and the scaling of it, so that it can be assembled into a focal
        # plane thumbnail

        # write out to the scratch space with the necessary info in the
        # filename for downstream processing to pick it up.
        self.log.info(f'Processing raw for detector {self.detector} - {expRecord.dataId}')

        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        for detNum in range(18, 27):  # TODO: Change this to a single detector once we have multiple pods!
            self.log.info(f'Actually processing raw for detector {detNum}...')
            ampNoises = {}
            raw = self.butler.get('raw', expRecord.dataId, detector=detNum)
            detector = raw.detector
            for amp in detector:
                noise = np.std(raw[amp.getBBox()].image.array)
                entryName = "_".join([detector.getName(), amp.getName()])
                ampNoises[entryName] = float(noise)  # numpy float32 is not json serializable
            # write the data
            writeDataShard(self.locationConfig.calculatedDataPath, dayObs, seqNum, 'rawNoises', ampNoises)
            self.log.info(f'Wrote metadata shard for detector {detNum}')
            # then signal we're done for downstream
            writeDataIdFile(self.locationConfig.dataIdScanPath, 'rawNoises', expRecord, self.log)

            assembled = self.assembleTask.assembleCcd(raw)
            writeBinnedImage(assembled, self.locationConfig.binnedImagePath, self.locationConfig.binning)
            writeDataIdFile(self.locationConfig.dataIdScanPath, 'binnedImage', expRecord, self.log)
            self.log.info(f'Wrote binned image for detector {detNum}')

    def run(self):
        """Run continuously, calling the callback method on the latest dataId.
        """
        self.watcher.run(self.callback)


class Plotter():
    """Channel for producing the plots for the cleanroom on RubinTv.
    """
    def __init__(self, butler, location, instrument, doRaise=False):
        self.locationConfig = LocationConfig(location)
        self.butler = butler
        self.camera = getCamera(self.butler, instrument)
        self.instrument = instrument
        self.uploader = Uploader(self.locationConfig.bucketName)
        self.log = _LOG.getChild(f"plotter_{self.instrument}")
        # currently watching for binnedImage as this is made last
        self.watcher = FileWatcher(locationConfig=self.locationConfig,
                                   dataProduct='binnedImage',
                                   doRaise=doRaise)
        self.fig = plt.figure(figsize=(12, 12))
        self.doRaise = doRaise

    def plotNoises(self, expRecord):
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        nExpected = getNumExpectedItems(self.instrument)
        noises = getShardedData(self.locationConfig.calculatedDataPath, dayObs, seqNum, 'rawNoises',
                                nExpected=nExpected,
                                logger=self.log,
                                deleteAfterReading=False)

        if not noises:
            self.log.warning(f'No noise data found for {expRecord.dataId}')
            return None

        perCcdNoises = fullAmpDictToPerCcdDicts(noises)
        plt.figure(figsize=(10, 10))
        ax = plt.subplot(111)

        plotName = f'noise-map_dayObs_{dayObs}_seqNum_{seqNum}.png'
        saveFile = os.path.join(self.locationConfig.plotPath, plotName)
        focal_plane_plotting.plot_focal_plane(ax, perCcdNoises, camera=self.camera)

        plt.savefig(saveFile)
        self.log.info(f'Wrote rawNoises plot for {expRecord.dataId} to {saveFile}')

        return saveFile

    def plotFocalPlane(self, expRecord):
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
        """
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
        """Run continuously, calling the callback method on the latest dataId.
        """
        self.watcher.run(self.callback)
