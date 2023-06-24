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

import json
import os
import logging
from glob import glob
from time import sleep

from .uploaders import Heartbeater, Uploader

from .utils import (isFileWorldWritable,
                    raiseIf,
                    sanitizeNans,
                    )
from .watchers import FileWatcher, writeDataIdFile

_LOG = logging.getLogger(__name__)


class TimedMetadataServer:
    """Class for serving metadata to RubinTV.

    Metadata shards are written to a /shards directory, which are collated on a
    timer and uploaded if new shards were found. This happens on a timer,
    defined by ``self.cadence``.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration.
    metadataDirectory : `str`
        The name of the directory for which the metadata is being served. Note
        that this directory and the ``shardsDirectory`` are passed in because
        although the ``LocationConfig`` holds all the location based path info
        (and the name of the bucket to upload to), many directories containg
        shards exist, and each one goes to a different page on the web app, so
        this class must be told which set of files to be collating and
        uploading to which channel.
    shardsDirectory : `str`
        The directory to find the shards in, usually of the form
        ``metadataDirectory`` + ``'/shards'``.
    channelName : `str`
        The name of the channel to serve the metadata files to, also used for
        heartbeats.
    doRaise : `bool`
        If True, raise exceptions instead of logging them.
    """
    # The time between searches of the metadata shard directory to merge the
    # shards and upload.
    cadence = 1.5
    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 120

    def __init__(self, *,
                 locationConfig,
                 metadataDirectory,
                 shardsDirectory,
                 channelName,
                 doRaise=False):
        self.locationConfig = locationConfig
        self.metadataDirectory = metadataDirectory
        self.shardsDirectory = shardsDirectory
        self.channelName = channelName
        self.doRaise = doRaise
        self.log = _LOG.getChild(self.channelName)
        self.uploader = Uploader(self.locationConfig.bucketName)
        self.heartbeater = Heartbeater(self.channelName,
                                       self.locationConfig.bucketName,
                                       self.HEARTBEAT_UPLOAD_PERIOD,
                                       self.HEARTBEAT_FLATLINE_PERIOD)

        if not os.path.isdir(self.metadataDirectory):
            # created by the LocationConfig init so this should be impossible
            raise RuntimeError(f"Failed to find/create {self.metadataDirectory}")

    def mergeShardsAndUpload(self):
        """Merge all the shards in the shard directory into their respective
        files and upload the updated files.

        For each file found in the shard directory, merge its contents into the
        main json file for the corresponding dayObs, and for each file updated,
        upload it.
        """
        filesTouched = set()
        shardFiles = sorted(glob(os.path.join(self.shardsDirectory, "metadata-*")))
        if shardFiles:
            self.log.debug(f'Found {len(shardFiles)} shardFiles')
            sleep(0.1)  # just in case a shard is in the process of being written

        updating = set()

        for shardFile in shardFiles:
            # filenames look like
            # metadata-dayObs_20221027_049a5f12-5b96-11ed-80f0-348002f0628.json
            filename = os.path.basename(shardFile)
            dayObs = int(filename.split("_", 2)[1])
            mainFile = self.getSidecarFilename(dayObs)
            filesTouched.add(mainFile)

            data = {}
            # json.load() doesn't like empty files so check size is non-zero
            if os.path.isfile(mainFile) and os.path.getsize(mainFile) > 0:
                with open(mainFile) as f:
                    data = json.load(f)

            with open(shardFile) as f:
                shard = json.load(f)
            if shard:  # each is a dict of dicts, keyed by seqNum
                for seqNum, seqNumData in shard.items():
                    seqNumData = sanitizeNans(seqNumData)  # remove NaNs
                    if seqNum not in data.keys():
                        data[seqNum] = {}
                    data[seqNum].update(seqNumData)
                    updating.add((dayObs, seqNum))
            os.remove(shardFile)

            with open(mainFile, 'w') as f:
                json.dump(data, f)
            if not isFileWorldWritable(mainFile):
                os.chmod(mainFile, 0o777)  # file may be amended by another process

        if updating:
            for (dayObs, seqNum) in sorted(updating, key=lambda x: (x[0], x[1])):
                self.log.info(f"Updating metadata tables for: {dayObs=}, {seqNum=}")

        if filesTouched:
            self.log.info(f"Uploading {len(filesTouched)} metadata files")
            for file in filesTouched:
                self.uploader.googleUpload(self.channelName, file, isLiveFile=True)
        return

    def getSidecarFilename(self, dayObs):
        """Get the name of the metadata sidecar file for the dayObs.

        Parameters
        ----------
        dayObs : `int`
            The dayObs.

        Returns
        -------
        filename : `str`
            The filename.
        """
        return os.path.join(self.metadataDirectory, f'dayObs_{dayObs}.json')

    def callback(self):
        """Method called on a timer to gather the shards and upload as needed.

        Adds the metadata to the sidecar file for the dataId and uploads it.
        """
        try:
            self.log.debug('Getting metadata from shards')
            self.mergeShardsAndUpload()  # updates all shards everywhere

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def run(self):
        """Run continuously, looking for metadata and uploading.
        """
        while True:
            self.callback()
            if self.heartbeater is not None:
                self.heartbeater.beat()
            sleep(self.cadence)


class MultiSensorMetadataServer:
    """Class for serving per-detector, per-REB/raft and per-CCD metedata in
    multi-CCD cameras, i.e. ComCam, LSSTCam and TS8.

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
        if instrument not in ['LSST-TS8', 'LSSTCam', 'LSSTComCam']:
            raise ValueError(f'Instrument {instrument} not supported, must be LSST-TS8 or LSSTCam')
        self.locationConfig = locationConfig
        self.butler = butler
        self.instrument = instrument
        self.camera = butler.get('camera', instrument=self.instrument)

        match instrument:
            case 'LSST-TS8':
                metadataShardPath = locationConfig.ts8MetadataShardPath
            case 'LSSTComCam':
                metadataShardPath = locationConfig.comCamMetadataShardPath
            case 'LSSTCam':
                metadataShardPath = locationConfig.botMetadataShardPath
            case _:
                raise ValueError(f'Instrument {instrument} not supported.')
        self.metadataShardPath = metadataShardPath

        self.detectors = self.camera.getDetector()
        name = f'MultiSensorMetadataServer_{self.instrument}'
        self.log = _LOG.getChild(name)
        self.watcher = FileWatcher(locationConfig=locationConfig,
                                   instrument=self.instrument,
                                   dataProduct='raw',
                                   doRaise=doRaise)


    def writeExpRecordMetadataShard(self, expRecord):
        """Write the exposure record metedata to a shard.

        Only fires once, based on the value of TS8_METADATA_DETECTOR or
        LSSTCOMCAM_METADATA_DETECTOR, depending on the instrument.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        metadataDetector = (TS8_METADATA_DETECTOR if self.instrument == 'LSST-TS8'
                            else LSSTCOMCAM_METADATA_DETECTOR)
        if metadataDetector not in self.detectors:
            return

        md = {}
        md['Exposure time'] = expRecord.exposure_time
        md['Dark time'] = expRecord.dark_time
        md['Image type'] = expRecord.observation_type
        md['Test type'] = expRecord.observation_reason
        md['Date'] = expRecord.timespan.begin.isot
        md['Run number'] = expRecord.science_program

        seqNum = expRecord.seq_num
        dayObs = expRecord.day_obs
        shardData = {seqNum: md}

        writeMetadataShard(self.metadataShardPath, dayObs, shardData)

    def writeImageMetadataShard(self, expRecord, exposureMetadata):
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
        metadataDetector = (TS8_METADATA_DETECTOR if self.instrument == 'LSST-TS8'
                            else LSSTCOMCAM_METADATA_DETECTOR)
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

    def writeRebHeaderShard(self, expRecord, raw):
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

        if self.instrument == 'LSST-TS8':
            detectorList = TS8_REB_HEADER_DETECTORS
            rebNumber = detector.getName().split('_')[1][1]  # This is the X part of the S part of R12_SXY
            itemName = f'REB{rebNumber} Header'
        elif self.instrument == 'LSSTComCam':
            detectorList = LSSTCOMCAM_REB_HEADER_DETECTORS
            rebNumber = detector.getName().split('_')[1][1]  # This is the X part of the S part of R12_SXY
            itemName = f'REB{rebNumber} Header'
        elif self.instrument == 'LSSTCam':
            detectorList = LSSTCAM_REB_HEADER_DETECTORS
            rebNumber = detector.getName()  # use the full name for now
            itemName = f'REB{rebNumber} Header'
        else:
            raise ValueError(f'Unknown instrument {self.instrument}')

        if not any(detNum in detectorList for detNum in self.detectors):
            return

        md = {}
        for headerKey in REB_HEADERS:
            value = exposureMetadata.get(headerKey)
            if value:
                md[headerKey] = value

        md['DISPLAY_VALUE'] = "📖"

        seqNum = expRecord.seq_num
        dayObs = expRecord.day_obs
        shardData = {seqNum: {itemName: md}}  # uploading a dict item here!

        writeMetadataShard(self.metadataShardPath, dayObs, shardData)

    def callback(self, expRecord):
        """Method called for each new expRecord as it is found in the repo.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """

        today = expRecord.day_obs
        seqNum = expRecord.seq_num

        # things we need to do:
        # 1. write the exposure record metadata to a shard - always do that

        """
        For each day:
        get a list of all the seqNums we have

        """

        try:  # metadata writing should never bring the processing down
            # this only fires for a single detector, based on METADATA_DETECTOR
            self.writeExpRecordMetadataShard(expRecord)
        except Exception:
            self.log.exception(f'Failed to write metadata shard for {expRecord.dataId}')

        if expRecord.observation_type == 'scan':
            self.log.info(f'Skipping scan-mode image {expRecord.dataId}')
            return

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
            writeDataShard(path=self.locationConfig.calculatedDataPath,
                           instrument=self.instrument,
                           dayObs=dayObs,
                           seqNum=seqNum,
                           dataSetName='rawNoises',
                           dataDict=ampNoises)
            self.log.info(f'Wrote rawNoises data shard for detector {detNum}')
            # then signal we're done for downstream
            writeDataIdFile(self.locationConfig.dataIdScanPath, 'rawNoises', expRecord, self.log)

            postIsr = self.runIsr(raw)

            writeBinnedImage(exp=postIsr,
                             instrument=self.instrument,
                             outputPath=self.locationConfig.calculatedDataPath,
                             binSize=self.locationConfig.binning)
            writeDataIdFile(self.locationConfig.dataIdScanPath, 'binnedImage', expRecord, self.log)
            self.log.info(f'Wrote binned image for detector {detNum}')

            del raw
            del postIsr

    def run(self):
        """Run continuously, calling the callback method with the latest
        expRecord.
        """
        self.watcher.run(self.callback)
