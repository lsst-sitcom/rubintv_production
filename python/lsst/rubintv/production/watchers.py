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

__all__ = ("RedisWatcher", "ButlerWatcher")

import logging
from time import sleep
from typing import TYPE_CHECKING

from lsst.daf.butler import Butler

from .redisUtils import RedisHelper
from .utils import LocationConfig, raiseIf

if TYPE_CHECKING:
    from lsst.daf.butler import DimensionRecord

    from .payloads import Payload
    from .podDefinition import PodDetails


_LOG = logging.getLogger(__name__)


class RedisWatcher:
    """A redis-based watcher, looking for work in a redis queue from the
    HeadProcessController.

    Parameters
    ----------
    detectors : `int` or `list` [`int`]
        The detector, or detectors, to process data for.
    """

    def __init__(self, butler: Butler, locationConfig: LocationConfig, podDetails: PodDetails) -> None:
        self.redisHelper = RedisHelper(butler, locationConfig)
        self.podDetails = podDetails
        self.cadence = 0.2  # seconds - there's 400+ workers, don't go too high!
        self.log = _LOG.getChild("redisWatcher")
        self.payload: Payload | None = None  # XXX that is this for?

    def run(self, callback, **kwargs) -> None:
        """Run forever, calling ``callback`` on each most recent Payload.

        Parameters
        ----------
        callback : `callable`
            The callback to run, with the most recent ``Payload`` as the
            argument.
        """
        while True:
            self.redisHelper.announceFree(self.podDetails)
            payload = self.redisHelper.dequeuePayload(self.podDetails)  # blocks for up to DEQUE_TIMEOUT sec
            if payload is not None:
                try:
                    self.payload = payload  # XXX why is this being saved on the class?
                    self.redisHelper.announceBusy(self.podDetails)
                    callback(payload)
                    self.payload = None
                except Exception as e:  # deliberately don't catch KeyboardInterrupt, SIGINT etc
                    self.log.exception(f"Error processing payload {payload}: {e}")
                finally:
                    self.redisHelper.announceFree(self.podDetails)
            else:  # only sleep when no work is found
                sleep(self.cadence)  # probably unnecessary now we use a blocking dequeue but it doesn't hurt


class ButlerWatcher:
    """A main watcher, which polls the butler for new data.

    Only one of these should be instantiated per-location and per-instrument.

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

    def __init__(
        self,
        locationConfig: LocationConfig,
        instrument: str,
        butler: Butler,
        doRaise=False,
    ) -> None:
        self.locationConfig = locationConfig
        self.instrument = instrument
        self.butler = butler
        self.doRaise = doRaise
        self.log = _LOG.getChild("butlerWatcher")
        self.redisHelper = RedisHelper(butler, locationConfig, isHeadNode=True)

    def _getLatestExpRecord(self) -> DimensionRecord:
        """Get the most recent expRecords from the butler.

        Get the most recent expRecord for all the dataset types. These are
        written to redis for watchers to pick up.

        Returns
        -------
        expRecords : `dict` [`str`, `lsst.daf.butler.DimensionRecord` or `None`]  # noqa: W505
            A dict of the most recent exposure records, keyed by dataProduct.
        """
        records = self.butler.registry.queryDimensionRecords("exposure", datasets="raw")

        # we must sort using the timespan because:
        # we can't use exposure.id because it is calculated differently
        # for different instruments, e.g. TS8 is 10x bigger than AuxTel
        # and also C-controller data has expIds like 3YYYMMDDNNNNN so would
        # always be the "most recent".
        records.order_by("-exposure.timespan.end")  # the minus means descending ordering
        records.limit(1)
        recordList = list(records)
        if len(recordList) != 1:
            raise RuntimeError(f"Found {len(recordList)} records for 'raw', expected 1")
        return recordList[0]

    def run(self) -> None:
        lastSeen = None
        while True:
            try:
                latestRecord = self._getLatestExpRecord()

                if lastSeen is None:  # starting up for the first time
                    seenBefore = self.redisHelper.checkButlerWatcherList(self.instrument, latestRecord)
                    if seenBefore:
                        self.log.info(
                            f"Skipping dispatching {latestRecord.instrument}-{latestRecord.id} as"
                            " it was dispatched by a ButlerWatcher in a previous life. You should only"
                            " ever see this on pod startup."
                        )
                        continue

                if latestRecord == lastSeen:
                    sleep(self.cadence)
                    continue

                self.redisHelper.pushNewExposureToHeadNode(latestRecord)
                self.redisHelper.pushToButlerWatcherList(self.instrument, latestRecord)
                lastSeen = latestRecord

            except Exception as e:
                sleep(1)  # in case we are in a tight loop of raising, don't hammer the butler
                raiseIf(self.doRaise, e, self.log)
