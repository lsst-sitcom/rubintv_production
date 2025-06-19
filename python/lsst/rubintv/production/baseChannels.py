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

import logging
import time
from abc import ABC, abstractmethod
from time import sleep
from typing import TYPE_CHECKING, Any

from .uploaders import MultiUploader
from .watchers import RedisWatcher

if TYPE_CHECKING:
    from logging import Logger

    from lsst.daf.butler import Butler, DataCoordinate, LimitedButler

    from .podDefinition import PodDetails
    from .starTracker import StarTrackerWatcher
    from .utils import LocationConfig


__all__ = [
    "BaseChannel",
    "BaseButlerChannel",
]


class BaseChannel(ABC):
    """Base class for all channels.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration to use.
    log : `logging.Logger`
        The logger to use.
    watcher : `lsst.rubintv.production.watchers.RedisWatcher`
        The watcher to use.
    doRaise : `bool`
        If ``True``, raise exceptions. If ``False``, log them.
    addUploader : `bool`, optional
        If ``True``, add an S3 uploader to the channel.
    """

    def __init__(
        self,
        *,
        locationConfig: LocationConfig,
        log: Logger,
        watcher: RedisWatcher | StarTrackerWatcher,
        doRaise: bool,
        addUploader: bool = False,
    ) -> None:
        self.locationConfig = locationConfig
        self.log = log
        self.watcher = watcher
        self.s3Uploader: MultiUploader | None = None
        if addUploader:
            self.s3Uploader = MultiUploader()
        self.doRaise: bool = doRaise

    @abstractmethod
    def callback(self, arg, /):
        """The callback function, called as each new value of arg is found.

        ``arg`` is usually an exposure record, but can be, for example, a
        filename.

        Parameters
        ----------
        arg : `any`
            The argument to run the callback with.
        """
        raise NotImplementedError()

    def run(self) -> None:
        """Run continuously, calling the callback method with the latest
        expRecord.
        """
        self.watcher.run(self.callback)


class BaseButlerChannel(BaseChannel):
    """Base class for all channels that use a Butler.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration to use.
    instrument : `str`
        The instrument to process data for.
    butler : `lsst.daf.butler.Butler`
        The Butler to use.
    dataProduct : `str`
        The dataProduct to watch for.
    detectors : `list` of `int`
        The detectors to process the data for. TODO: This is unused in some
        contexts - fix this, ideally fully removing it.
    channelName : `str`
        The name of the channel, used for uploads and logging.
    watcherType : `str`
        The type of watcher to use - either `"file"` or `"redis"`.
    doRaise : `bool`
        If ``True``, raise exceptions. If ``False``, log them.
    queueName : `str`, optional
        If using a `"redis"` type watcher, which queue should this consume
        from.
    addUploader : `bool`, optional
        If ``True``, add an S3 uploader to the channel.
    """

    def __init__(
        self,
        *,
        locationConfig: LocationConfig,
        butler: Butler,
        dataProduct: str | None,
        detectors: int | list[int] | None,
        channelName: str,
        doRaise: bool,
        # podDetails only needed for redis watcher. Not the neatest but will do
        # for now
        podDetails: PodDetails,
        addUploader: bool = True,
    ) -> None:
        watcher = RedisWatcher(
            butler=butler,
            locationConfig=locationConfig,
            podDetails=podDetails,
        )
        log = logging.getLogger(f"lsst.rubintv.production.{channelName}")
        super().__init__(
            locationConfig=locationConfig, log=log, watcher=watcher, doRaise=doRaise, addUploader=addUploader
        )
        self.butler = butler
        self.dataProduct = dataProduct
        self.channelName = channelName
        self.detectors = detectors
        self.podDetails = podDetails

    @abstractmethod
    def callback(self, expRecord):
        raise NotImplementedError()

    def _waitForDataProduct(
        self, dataId: DataCoordinate, timeout: float = 20, gettingButler: Butler | LimitedButler | None = None
    ) -> Any:
        """Wait for a dataProduct to land inside a repo.

        Wait for a maximum of ``timeout`` seconds for a dataProduct to land,
        and returns the dataProduct if it does, or ``None`` if it doesn't.

        Parameters
        ----------
        dataId : `dict` or `lsst.daf.butler.DataCoordinate`
            The fully-qualified dataId of the product to wait for.
        timeout : `float`
            The timeout, in seconds, to wait before giving up and returning
            ``None``.
        gettingButler : `lsst.daf.butler.LimitedButler`
            The butler to use. If ``None``, uses the butler attribute. Provided
            so that a CachingLimitedButler can be used instead.

        Returns
        -------
        dataProduct : dataProduct or None
            Either the dataProduct being waited for, or ``None`` if timeout was
            exceeded.
        """
        if self.dataProduct is None:
            return

        cadence = 0.25
        start = time.time()
        while time.time() - start < timeout:
            if self.butler.exists(self.dataProduct, dataId):
                if gettingButler is None:
                    return self.butler.get(self.dataProduct, dataId)
                else:
                    ref = self.butler.find_dataset(self.dataProduct, dataId)
                    assert ref is not None, f"Registry error: could not find {self.dataProduct} for {dataId}"
                    return gettingButler.get(ref)
            else:
                sleep(cadence)
        self.log.warning(f"Waited {timeout}s for {self.dataProduct} for {dataId} to no avail")
        return None
