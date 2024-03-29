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

import logging
import os
from glob import glob
from time import sleep
import atexit

from lsst.utils.iteration import ensure_iterable

from .uploaders import Heartbeater
from .redisUtils import RedisHelper

from .utils import (raiseIf,
                    writeDataIdFile,
                    getGlobPatternForDataProduct,
                    getGlobPatternForShardedData,
                    safeJsonOpen,
                    ALLOWED_DATASET_TYPES,
                    expRecordFromJson,
                    )

__all__ = (
    'FileWatcher',
    'RedisWatcher',
    'ButlerWatcher'
)

_LOG = logging.getLogger(__name__)


class FileWatcher:
    """A file-system based watcher, looking for outputs from a ButlerWatcher.

    The ButlerWatcher polls the repo looking for dataIds, and writes them out
    to a file, to be found by a File watcher.

    Many of these can be instantiated per-location.

    # TODO: DM-39225 pretty sure this is no longer true:
    It is worth noting that the ``dataProduct`` to watch for need not be an
    official dafButler dataset type. We can use FileWatchers to signal to any
    downstream processing that something is finished and ready for consumption.

    Uploads a heartbeat to the bucket every ``HEARTBEAT_PERIOD`` seconds if
    ``heartbeatChannelName`` is specified.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration to use.
    dataProduct : `str`
        The data product to watch for.
    heartbeatChannelName : `str`, optional
        The name of the channel to use when uploading heartbeats. If one is not
        provided, no heartbeats are sent.
    doRaise : `bool`, optional
        If ``True``, raise exceptions. If ``False``, log them.
    """
    cadence = 1  # in seconds

    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 120

    def __init__(self, *, locationConfig, instrument, dataProduct, heartbeatChannelName='', doRaise=False):
        self.locationConfig = locationConfig
        self.instrument = instrument
        self.dataProduct = dataProduct
        self.doRaise = doRaise
        self.log = _LOG.getChild("fileWatcher")
        self.heartbeatChannelName = heartbeatChannelName
        if heartbeatChannelName:
            self.heartbeater = Heartbeater(heartbeatChannelName,
                                           self.locationConfig.bucketName,
                                           self.HEARTBEAT_UPLOAD_PERIOD,
                                           self.HEARTBEAT_FLATLINE_PERIOD)
        else:
            self.heartbeater = None

    def getMostRecentExpRecord(self, previousExpId=None):
        """Get the most recent exposure record from the file system.

        If the most recent exposure is the same as the previous one, ``None``
        is returned.

        Parameters
        ----------
        previousExpId : `int`, optional
            The previous exposure id.

        Returns
        -------
        expRecord : `lsst.daf.butler.DimensionRecord` or `None`
            The most recent exposure record, or `None` if no new record was
            found.
        """
        pattern = getGlobPatternForDataProduct(dataIdPath=self.locationConfig.dataIdScanPath,
                                               dataProduct=self.dataProduct,
                                               instrument=self.instrument)
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

        expRecordJson = safeJsonOpen(filename)
        # TODO: DM-39225 pretty sure this line breaks the old behavior
        expRecord = expRecordFromJson(expRecordJson, self.locationConfig)
        return expRecord

    def run(self, callback, **kwargs):
        """Run forever, calling ``callback`` on each most recent expRecord.

        Parameters
        ----------
        callback : `callable`
            The callback to run, with the most recent expRecord as the
            argument.
        """
        lastFound = None
        while True:
            try:
                expRecord = self.getMostRecentExpRecord(lastFound)
                if expRecord is None:  # either there is nothing, or it is the same expId
                    if self.heartbeater is not None:
                        self.heartbeater.beat()
                    sleep(self.cadence)
                    continue
                else:
                    lastFound = expRecord.id
                    callback(expRecord, **kwargs)
                    if self.heartbeater is not None:
                        self.heartbeater.beat()  # call after the callback so as not to delay processing

            except Exception as e:
                raiseIf(self.doRaise, e, self.log)


class RedisWatcher:
    """A redis-based watcher, looking for work in a redis queue from the
    HeadProcessController.

    Parameters
    ----------
    detectors : `int` or `list` [`int`]
        The detector, or detectors, to process data for.
    """
    def __init__(self, butler, locationConfig, queueName):
        self.redisHelper = RedisHelper(butler, locationConfig)
        self.queueName = queueName
        self.heartbeater = None  # TODO once we have websockets to the front end
        self.cadence = 0.1  # seconds - this is fine, redis likes a beating
        self.log = _LOG.getChild("redisWatcher")
        self.payload = None

    # @atexit.register
    # def _deleteWorkerFromPool(self):
    #     """This is not robust at all, definitely not to OOM messages, but
    #     gives workers a chance of removing themselves, at least.
    #     """
    #     self.redisHelper.announceExistence(self.queueName, remove=True)
    #     if self.payload is not None:
    #         # XXX send this to failed queue here
    #         # Here we have not come from a task error, but from
    #         # SIGINT or KeyboardInterrupt, so push the failed payload to the
    #         # failed stack
    #         return

    def run(self, callback, **kwargs):
        """Run forever, calling ``callback`` on each most recent expRecord.

        Parameters
        ----------
        callback : `callable`
            The callback to run, with the most recent ``Payload`` as the
            argument.
        """
        while True:
            self.redisHelper.announceFree(self.queueName)
            payload = self.redisHelper.dequeuePayload(self.queueName)
            if payload is not None:
                try:
                    self.payload = payload
                    self.redisHelper.announceBusy(self.queueName)
                    callback(payload)
                    self.payload = None
                except Exception as e:  # deliberately don't catch KeyboardInterrupt, SIGINT etc
                    self.log.error(f"Error processing payload {payload}: {e}")
                finally:
                    self.redisHelper.announceFree(self.queueName)
            else:  # only sleep when no work is found
                sleep(self.cadence)


class ButlerWatcher:
    """A main watcher, which polls the butler for new data.

    When a new expRecord is found, it writes it out to a file so that it
    can be found by a FileWatcher, so that we can poll for new data without
    hammering the main repo with 201x ButlerWatchers.

    Only one of these should be instantiated per-location.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location config.
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

    def __init__(self, locationConfig, instrument, butler, dataProducts, doRaise=False):
        self.locationConfig = locationConfig
        self.instrument = instrument
        self.butler = butler
        self.dataProducts = list(ensure_iterable(dataProducts))  # must call list or we get a generator back
        self.doRaise = doRaise
        self.log = _LOG.getChild("butlerWatcher")
        self.redisHelper = RedisHelper(butler, locationConfig, isHeadNode=True)

    def _getLatestExpRecords(self):
        """Get the most recent expRecords from the butler.

        Get the most recent expRecord for all the dataset types. These are
        written to files for the FileWatchers to pick up.

        Returns
        -------
        expRecords : `dict` [`str`, `lsst.daf.butler.DimensionRecord` or `None`]  # noqa: W505
            A dict of the most recent exposure records, keyed by dataProduct.
        """
        expRecordDict = {}

        for product in self.dataProducts:
            # NB if you list multiple products for datasets= then it will only
            # give expRecords for which all those products exist, so these must
            # be done as separate queries
            records = self.butler.registry.queryDimensionRecords("exposure", datasets=product)

            # we must sort using the timespan because:
            # we can't use exposure.id because it is calculated differently
            # for different instruments, e.g. TS8 is 10x bigger than AuxTel
            # and also C-controller data has expIds like 3YYYMMDDNNNNN so would
            # always be the "most recent".
            records.order_by('-exposure.timespan.end')  # the minus means descending ordering
            records.limit(1)
            records = list(records)
            if len(records) != 1:
                self.log.warning(f"Found {len(records)} records for {product}, expected 1")
                expRecordDict[product] = None
            else:
                expRecordDict[product] = list(records)[0]
        return expRecordDict

    def _deleteExistingData(self, expRecord):
        """Delete existing data for this exposure.

        Given an exposure record, delete all sharded/binned data for this
        image.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        # delete all downstream products associated with this exposureRecord
        for dataset in ALLOWED_DATASET_TYPES:
            pattern = getGlobPatternForShardedData(path=self.locationConfig.calculatedDataPath,
                                                   dataSetName=dataset,
                                                   instrument=expRecord.instrument,
                                                   dayObs=dayObs,
                                                   seqNum=seqNum)
            shardFiles = glob(pattern)
            if len(shardFiles) > 0:
                self.log.info(f'Deleting {len(shardFiles)} pre-existing files for {dataset}')
                for filename in shardFiles:
                    # deliberately not checking for permission errors here,
                    # if they're raised we want to fail at this point.
                    os.remove(filename)

    def run(self):
        lastWrittenIds = {product: None for product in self.dataProducts}

        # check for what we actually already have on disk, given that the
        # service will rarely be starting from literally scratch
        for product in self.dataProducts:
            fileWatcher = FileWatcher(locationConfig=self.locationConfig,
                                      instrument=self.instrument,
                                      dataProduct=product,
                                      doRaise=self.doRaise)
            expRecord = fileWatcher.getMostRecentExpRecord()  # returns None if not found
            lastWrittenIds[product] = expRecord
            del fileWatcher

        while True:
            try:
                # get the new records for all dataproducts
                newRecords = self._getLatestExpRecords()
                # work out which ones are actually new and only write those out
                found = {product: expRecord for product, expRecord in newRecords.items()
                         if expRecord is not None and expRecord.id != lastWrittenIds[product]}

                if not found:  # only sleep when there's nothing new at all
                    sleep(self.cadence)
                    # TODO: re-enable?
                    # self.heartbeater.beat()
                    continue
                else:
                    # all processing starts with triggering on a raw, so we
                    # only perform that deletion at the very start, and
                    # therefore hard-code raw
                    if 'raw' in found:
                        self._deleteExistingData(found['raw'])

                    for product, expRecord in found.items():
                        if product == 'raw':  # only push raws to redis
                            self.redisHelper.pushNewExposureToHeadNode(expRecord)
                        writeDataIdFile(self.locationConfig.dataIdScanPath, product, expRecord, log=self.log)
                        lastWrittenIds[product] = expRecord.id
                    # beat after the callback so as not to delay processing
                    # and also so it is only called if things are working
                    # TODO: re-enable?
                    # self.heartbeater.beat()

            except Exception as e:
                sleep(1)  # in case we are in a tight loop of raising, don't hammer the butler
                raiseIf(self.doRaise, e, self.log)
