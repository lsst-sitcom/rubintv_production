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
import logging
from glob import glob
from time import sleep

import lsst.daf.butler as dafButler
from lsst.utils.iteration import ensure_iterable

from .uploaders import Heartbeater

from .utils import (raiseIf,
                    writeDataIdFile,
                    getGlobPatternForDataProduct)

_LOG = logging.getLogger(__name__)


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

    def __init__(self, *, locationConfig, dataProduct, heartbeatChannelName='', doRaise=False):
        self.locationConfig = locationConfig
        self.dataProduct = dataProduct
        self.doRaise = doRaise
        self.log = _LOG.getChild("fileWatcher")
        self.doHeartbeat = False
        if heartbeatChannelName:
            self.doHeartbeat = True
            self.heartbeatChannelName = heartbeatChannelName  # currently unused
            self.heartbeater = Heartbeater(heartbeatChannelName,
                                           self.locationConfig.bucketName,
                                           self.HEARTBEAT_UPLOAD_PERIOD,
                                           self.HEARTBEAT_FLATLINE_PERIOD)

    def getMostRecentExpRecord(self, previousExpId=None):
        """Get the most recent exposure record from the file system.

        If the most recent exposure is the same as the previous one, ``None``
        is returned.
        """
        pattern = getGlobPatternForDataProduct(self.locationConfig.dataIdScanPath, self.dataProduct)
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

    def __init__(self, locationConfig, butler, dataProducts, doRaise=False):
        self.locationConfig = locationConfig
        self.butler = butler
        self.dataProducts = list(ensure_iterable(dataProducts))  # must call list or we get a generator back
        self.doRaise = doRaise
        self.log = _LOG.getChild("butlerWatcher")

    def _getLatestExpRecords(self):
        """Get the most recent expRecords from the butler.

        Get the most recent expRecord for all the dataset types. These are
        written to files for the FileWatchers to pick up.

        Returns
        -------
        expRecords : `dict` [`str`, `lsst.daf.butler.DimensionRecord`]
            A dict of the most recent exposure records, keyed by dataProduct.
        """
        expRecordDict = {}

        # this where clause is no longer necessary for speed now that we are
        # using order_by and limit, but it now functions to restrict data to
        # being inside the range where exposures could ever realistically be
        # taken, as some simulated data is "taken" in the year 3000.
        where = 'exposure.day_obs>20200101 and exposure.day_obs<21000101'

        for product in self.dataProducts:
            # NB if you list multiple products for datasets= then it will only
            # give expRecords for which all those products exist, so these must
            # be done as separate queries
            records = self.butler.registry.queryDimensionRecords("exposure",
                                                                 datasets=product,
                                                                 where=where)
            records.order_by('-exposure.id')  # the minus means descending ordering
            records.limit(1)
            expRecordDict[product] = list(records)[0]
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
                        writeDataIdFile(self.locationConfig.dataIdScanPath, product, expRecord, log=self.log)
                        lastWrittenIds[product] = expRecord.id
                    # beat after the callback so as not to delay processing
                    # and also so it is only called if things are working
                    # TODO: re-enable?
                    # self.heartbeater.beat()

            except Exception as e:
                raiseIf(self.doRaise, e, self.log)
