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
import time
import logging
import tempfile
from glob import glob
from time import sleep
import matplotlib.pyplot as plt

from lsst.pex.exceptions import NotFoundError
from lsst.summit.utils.utils import (dayObsIntToString,
                                     getCurrentDayObs_int,
                                     getCurrentDayObs_datetime,
                                     )
from lsst.summit.utils.utils import dayObsIntToString

from .channels import PREFIXES, CHANNELS
from .utils import writeMetadataShard, isFileWorldWritable
from .rubinTv import Uploader, Heartbeater

_LOG = logging.getLogger(__name__)


def getCurrentRawDataDir(rootDataPath, wide):
    """Get the raw data dir corresponding to the current dayObs.

    Returns
    -------
    path : `str`
        The raw data dir for today.
    """
    datetime = getCurrentDayObs_datetime()
    camNum = 101 if wide else 102  # TODO move this config to the top somehow?
    dirSuffix = f'GenericCamera/{camNum}/{datetime.year}/{datetime.month}/{datetime.day}/'
    return os.path.join(rootDataPath, dirSuffix)


def dayObsSeqNumFromFilename(filename):
    """Get the dayObs and seqNum from a filename.

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
    # filenames are like GC101_O_20221114_000005.fits
    filename = os.path.basename(filename)  # in case we're passed a full path
    _, _, dayObs, seqNumAndSuffix = filename.split('_')
    dayObs = int(dayObs)
    seqNum = int(seqNumAndSuffix.removesuffix('.fits'))
    return dayObs, seqNum


def expIdFromFilename(filename):
    """Get the dayObs and seqNum from a filename.

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
    raise NotImplementedError
    # call dayObsSeqNumFromFilename() above and zero pad the seqNum

    # # filenames are like GC101_O_20221114_000005.fits
    # filename = os.path.basename(filename)  # in case we're passed a full path
    # _, _, dayObs, seqNumAndSuffix = filename.split('_')
    # dayObs = int(dayObs)
    # seqNum = int(seqNumAndSuffix.removesuffix('.fits'))
    # return dayObs, seqNum


class StarTrackerWatcher():
    """Class for continuously watching for new data products landing in a repo.
    Uploads a heartbeat to the bucket every ``HEARTBEAT_PERIOD`` seconds.

    Parameters
    ----------
    dataProduct : `str`
        The data product to watch for.
    channel : `str`
        The channel for which this is a watcher, needed so that the heartbeat
        can be uploaded to the right file in GCS.

    Wathces a repo for the specified data product to land, and runs a callback
    on the dataId for the data product once it has landed in the repo.
    """
    cadence = 1  # in seconds

    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 240

    def __init__(self, *, rootDataPath, wide):
        self.rootDataPath = rootDataPath
        self.wide = wide
        self.uploader = Uploader()
        self.log = _LOG.getChild("watcher")
        # self.heartbeaterRaw = Heartbeater(self.channel,
        #                                   self.HEARTBEAT_UPLOAD_PERIOD,
        #                                   self.HEARTBEAT_FLATLINE_PERIOD)

    def _getLatestImageDataIdAndExpId(self):
        """Get the dataId and expId for the most recent image in the repo.
        """
        currentDir = getCurrentRawDataDir(self.rootDataPath, self.wide)
        files = glob(os.path.join(currentDir, '*.fits'))
        files = sorted(files, reverse=True)  # everything is zero-padded so sorts nicely
        if not files:
            return None, None, None

        latestFile = files[0]

        # filenames are like GC101_O_20221114_000005.fits
        _, _, dayObs, seqNumAndSuffix = latestFile.split('_')
        dayObs = int(dayObs)
        seqNum = int(seqNumAndSuffix.removesuffix('.fits'))
        expId = int(str(dayObs) + seqNumAndSuffix.removesuffix('.fits'))
        # return {'day_obs': dayObs, 'seq_num': seqNum}, expId
        return latestFile, dayObs, seqNum, expId

    def run(self, callback):
        """Wait for the image to land, then run callback(filename).

        Parameters
        ----------
        callback : `callable`
            The method to call, with the latest dataId as the argument.
        """
        lastFound = -1

        while True:
            try:
                filename, seqNum, expId = self._getLatestImageDataIdAndExpId()
                self.log.debug(f"{filename}")

                if lastFound == expId:
                    sleep(self.cadence)
                    self.log.debug('Found nothing, sleeping')
                    # self.heartbeater.beat()
                    continue
                else:
                    lastFound = expId
                    self.log.debug(f'Calling back with {filename}')
                    callback(filename)
                    # self.heartbeater.beat()  # after the callback so as not to delay processing with an upload

            except NotFoundError as e:  # NotFoundError when filters aren't defined
                print(f'Skipped displaying {filename} due to {e}')
