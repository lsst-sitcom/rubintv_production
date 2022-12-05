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

import lsst.daf.butler as dafButler
from lsst.utils.iteration import ensure_iterable

from lsst.summit.utils.butlerUtils import sortRecordsByDayObsThenSeqNum

from .utils import isFileWorldWritable, raiseIf, LocationConfig

_LOG = logging.getLogger(__name__)

DATA_ID_TEMPLATE = os.path.join("{path}", "{dataProduct}_{expId}.json")


def writeDataIdFile(dataIdPath, dataProduct, expRecord, log=None):
    """
    """
    expId = expRecord.id
    dayObs = expRecord.day_obs
    seqNum = expRecord.seq_num
    outFile = DATA_ID_TEMPLATE.format(path=dataIdPath, dataProduct=dataProduct, expId=expId)
    with open(outFile, 'w') as f:
        f.write(json.dumps(expRecord.to_simple().json()))
    if log:
        log.info(f"Wrote dataId file for {dataProduct} {dayObs}/{seqNum}, {expId} to {outFile}")


def getGlobPatternForDataProduct(dataIdPath, dataProduct):
    return DATA_ID_TEMPLATE.format(path=dataIdPath, dataProduct=dataProduct, expId='*')


class FileWatcher:
    """A file-system based watcher, looking for outputs from a ButlerWatcher.

    The ButlerWatcher polls the repo looking for dataIds, and writes them out
    to a file, to be found by a File watcher.

    Many of these can be instantiated per-location.

    Uploads a heartbeat to the bucket every ``HEARTBEAT_PERIOD`` seconds if
    heartbeatChannelName is specified.
    """
    cadence = 1  # in seconds

    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 120

    def __init__(self, location, dataProduct, heartbeatChannelName='', doRaise=False):
        from lsst.rubintv.production import Heartbeater

        self.config = LocationConfig(location)
        self.dataProduct = dataProduct
        self.doRaise = doRaise
        self.log = _LOG.getChild("fileWatcher")
        self.doHeartbeat = False
        if heartbeatChannelName:
            self.doHeartbeat = True
            self.heartbeatChannelName = heartbeatChannelName  # currently unused
            self.heartbeater = Heartbeater(heartbeatChannelName,
                                           self.config.bucketName,
                                           self.HEARTBEAT_UPLOAD_PERIOD,
                                           self.HEARTBEAT_FLATLINE_PERIOD)

    def getMostRecentExpRecord(self, previousExpId=None):
        """Get the most recent exposure record from the file system.

        If the most recent exposure is the same as the previous one, ``None``
        is returned.
        """
        pattern = getGlobPatternForDataProduct(self.config.dataIdScanPath, self.dataProduct)
        files = glob(pattern)
        files = sorted(files, reverse=True)
        if not files:
            self.log.warning(f'No files found matching {pattern}')
            return None

        filename = files[0]
        expId = int(filename.split("_")[-1].removesuffix(".json"))
        if expId == previousExpId:
            self.log.debug(f'Found the same exposure again: {expId}')
            return None

        with open(filename, 'r') as f:
            expRecordJson = json.load(f)
            expRecord = dafButler.dimensions.DimensionRecord.from_json(expRecordJson,
                                                                       universe=dafButler.DimensionUniverse())
        return expRecord

    def run(self, callback):
        lastFound = None
        while True:
            try:
                expRecord = self.getMostRecentExpRecord(lastFound)
                if expRecord is None:  # either there is nothing, or it is the same expId
                    if self.doHeartbeat:
                        self.heartbeater.beat()
                    sleep(self.cadence)
                    continue
                else:
                    lastFound = expRecord.id
                    callback(expRecord)
                    if self.doHeartbeat:
                        self.heartbeater.beat()  # call after the callback so as not to delay processing

            except Exception as e:
                raiseIf(self.doRaise, e, self.log)


class ButlerWatcher:
    """A main watcher, which polls the butler for new data.

    When a new expRecord is found it writes it out to a file so that it
    can be found by a FileWatcher, so that we can poll for new data without
    hammering the main repo with 201x ButlerWatchers.

    Only one of these should be instantiated per-location.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    location : `str`
        The location.
    dataProducts : `str` or `list` [`str`]
        The data products to watch for.
    doRaise : `bool`, optional
        Raise exceptions or log them as warnings?
    """
    # look for new images every ``cadence`` seconds
    cadence = 1
    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 120

    def __init__(self, butler, location, dataProducts, doRaise=False):
        self.config = LocationConfig(location)
        self.butler = butler
        self.dataProducts = list(ensure_iterable(dataProducts))  # must call list or we get a generator back
        self.doRaise = doRaise
        self.log = _LOG.getChild("butlerWatcher")

    def _getLatestExpRecords(self):
        """Get the most recent dataId and expId from the butler.

        expId is to have an easily comparable value to check if we've found a
        new image, and the dataId is for use. This is written to a file for the
        FileWatcher to pick up.
        """
        expRecordDict = {}
        where = 'exposure.day_obs>20221201'  # TODO: find a better way of doing this?

        for product in self.dataProducts:
            # NB if you list multiple products for datasets= then it will only
            # give expRecords for which all those products exist, so these must
            # be done as separate queries
            records = set(self.butler.registry.queryDimensionRecords("exposure",
                                                                     datasets=product,
                                                                     where=where))
            recentRecord = sortRecordsByDayObsThenSeqNum(records)[-1]
            expRecordDict[product] = recentRecord
        return expRecordDict

    def run(self):
        lastWrittenIds = {product: 0 for product in self.dataProducts}
        while True:
            try:
                # get the new records for all dataproducts
                newRecords = self._getLatestExpRecords()
                # work out which ones are actually new and only write those out
                found = {product: expRecord for product, expRecord in newRecords.items()
                         if expRecord.id != lastWrittenIds[product]}

                if not found:  # only sleep when there's nothing new at all
                    sleep(self.cadence)
                    # TODO: re-enable?
                    # self.heartbeater.beat()
                    continue
                else:
                    for product, expRecord in found.items():
                        writeDataIdFile(self.config.dataIdScanPath, product, expRecord, log=self.log)
                        lastWrittenIds[product] = expRecord.id
                    # beat after the callback so as not to delay processing
                    # and also so it is only called if things are working
                    # TODO: re-enable?
                    # self.heartbeater.beat()

            except Exception as e:
                raiseIf(self.doRaise, e, self.log)


class TimedMetadataServer:
    """Class for serving metadata to RubinTV.
    """
    # The time between searches of the metadata shard directory to merge the
    # shards and upload.
    cadence = 1.5
    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 120

    def __init__(self, *,
                 location,
                 name=None,
                 doRaise=False):
        from lsst.rubintv.production import Heartbeater, Uploader

        self.config = LocationConfig(location)
        self.name = name
        self.doRaise = doRaise
        self.log = _LOG.getChild("timedMetadataServer")
        self.uploader = Uploader(self.config.bucketName)
        self.heartbeater = (Heartbeater(self.name,
                                        self.config.bucketName,
                                        self.HEARTBEAT_UPLOAD_PERIOD,
                                        self.HEARTBEAT_FLATLINE_PERIOD)
                            if self.name is not None else None)
        try:
            os.makedirs(self.config.metadataShardPath, exist_ok=True)
        except Exception as e:
            raise RuntimeError(f"Failed to find/create {self.config.metadataShardPath}") from e

    def mergeShardsAndUpload(self):
        """Merge all the shards in the shard directory into their respective
        files and upload the updated files.

        For each file found in the shard directory, merge its contents into the
        main json file for the corresponding dayObs, and for each file updated,
        upload it.
        """
        filesTouched = set()
        shardFiles = sorted(glob(os.path.join(self.config.metadataShardPath, "metadata-*")))
        if shardFiles:
            self.log.debug(f'Found {len(shardFiles)} shardFiles')
            sleep(0.1)  # just in case a shard is in the process of being written

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
            if shard:
                for row in shard:
                    if row in data:
                        data[row].update(shard[row])
                    else:
                        data.update({row: shard[row]})
            os.remove(shardFile)

            with open(mainFile, 'w') as f:
                json.dump(data, f)
            if not isFileWorldWritable(mainFile):
                os.chmod(mainFile, 0o777)  # file may be amended by another process

        if filesTouched:
            self.log.info(f"Uploading {len(filesTouched)} metadata files")
            for file in filesTouched:
                self.uploader.googleUpload(self.channel, file, isLiveFile=True)
        return

    def getSidecarFilename(self, dayObs):
        """Get the name of the metadata sidecar file for the dayObs.

        Returns
        -------
        dayObs : `int`
            The dayObs.
        """
        return os.path.join(self.config.metadataPath, f'dayObs_{dayObs}.json')

    def callback(self):
        """Method called on each new dataId as it is found in the repo.

        Add the metadata to the sidecar for the dataId and upload.
        """
        try:
            self.log.info('Getting metadata from shards')
            self.mergeShardsAndUpload()  # updates all shards everywhere

        except Exception as e:
            if self.doRaise:
                raise RuntimeError("Error when collection metadata") from e
            self.log.warning(f"Error when collection metadata because {repr(e)}")
            return None

    def run(self):
        """Run continuously, looking for metadata and uploading.
        """
        while True:
            self.callback()
            if self.heartbeater is not None:
                self.heartbeater.beat()
            sleep(self.cadence)
