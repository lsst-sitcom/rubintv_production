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

import time
import logging
from time import sleep
from abc import ABC, abstractmethod

import lsst.summit.utils.butlerUtils as butlerUtils

from .watchers import FileWatcher
from .uploaders import Uploader, MultiUploader

__all__ = [
    'BaseChannel',
    'BaseButlerChannel',
]


class BaseChannel(ABC):
    """Base class for all channels.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration to use.
    log : `logging.Logger`
        The logger to use.
    watcher : `lsst.rubintv.production.watchers.FileWatcher`
        The file watcher to use.
    doRaise : `bool`
        If ``True``, raise exceptions. If ``False``, log them.
    """

    def __init__(self, *,
                 locationConfig,
                 log,
                 watcher,
                 doRaise
                 ):
        self.locationConfig = locationConfig
        self.log = log
        self.watcher = watcher
        self.uploader = Uploader(self.locationConfig.bucketName)
        self.s3Uploader = MultiUploader()
        self.doRaise = doRaise

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

    def run(self):
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
    butler : `lsst.daf.butler.Butler`
        The Butler to use.
    dataProduct : `str`
        The dataProduct to watch for.
    channelName : `str`
        The name of the channel, used for uploads and logging.
    doRaise : `bool`
        If ``True``, raise exceptions. If ``False``, log them.
    """

    def __init__(self,
                 *,
                 locationConfig,
                 instrument,
                 butler,
                 dataProduct,
                 channelName,
                 doRaise,
                 ):
        fileWatcher = FileWatcher(locationConfig=locationConfig,
                                  instrument=instrument,
                                  dataProduct=dataProduct,
                                  heartbeatChannelName=channelName,
                                  doRaise=doRaise)
        log = logging.getLogger(f'lsst.rubintv.production.{channelName}')
        super().__init__(locationConfig=locationConfig,
                         log=log,
                         watcher=fileWatcher,
                         doRaise=doRaise)
        self.butler = butler
        self.dataProduct = dataProduct
        self.channelName = channelName

    @abstractmethod
    def callback(self, expRecord):
        raise NotImplementedError()

    def _waitForDataProduct(self, dataId, timeout=20):
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

        Returns
        -------
        dataProduct : dataProduct or None
            Either the dataProduct being waited for, or ``None`` if timeout was
            exceeded.
        """
        cadence = 0.25
        start = time.time()
        while time.time() - start < timeout:
            if butlerUtils.datasetExists(self.butler, self.dataProduct, dataId):
                return self.butler.get(self.dataProduct, dataId)
            else:
                sleep(cadence)
        self.log.warning(f'Waited {timeout}s for {self.dataProduct} for {dataId} to no avail')
        return None
