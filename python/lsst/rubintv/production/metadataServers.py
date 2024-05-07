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
import os
from glob import glob
from time import sleep

from .uploaders import Heartbeater, MultiUploader, Uploader
from .utils import isFileWorldWritable, raiseIf, sanitizeNans

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

    def __init__(self, *, locationConfig, metadataDirectory, shardsDirectory, channelName, doRaise=False):
        self.locationConfig = locationConfig
        self.metadataDirectory = metadataDirectory
        self.shardsDirectory = shardsDirectory
        self.channelName = channelName
        self.doRaise = doRaise
        self.log = _LOG.getChild(self.channelName)
        self.uploader = Uploader(self.locationConfig.bucketName)
        self.s3Uploader = MultiUploader()
        self.heartbeater = Heartbeater(
            self.channelName,
            self.locationConfig.bucketName,
            self.HEARTBEAT_UPLOAD_PERIOD,
            self.HEARTBEAT_FLATLINE_PERIOD,
        )

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
            self.log.debug(f"Found {len(shardFiles)} shardFiles")
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

            with open(mainFile, "w") as f:
                json.dump(data, f)
            if not isFileWorldWritable(mainFile):
                os.chmod(mainFile, 0o777)  # file may be amended by another process

        if updating:
            for dayObs, seqNum in sorted(updating, key=lambda x: (x[0], x[1])):
                self.log.info(f"Updating metadata tables for: {dayObs=}, {seqNum=}")

        if filesTouched:
            self.log.info(f"Uploading {len(filesTouched)} metadata files")
            for file in filesTouched:
                self.uploader.googleUpload(self.channelName, file, isLiveFile=True)
                dayObs = self.dayObsFromFilename(file)
                self.s3Uploader.uploadMetdata(self.channelName, dayObs, file)
        return

    def dayObsFromFilename(self, filename):
        """Get the dayObs from a metadata sidecar filename.

        Parameters
        ----------
        filename : `str`
            The filename.

        Returns
        -------
        dayObs : `int`
            The dayObs.
        """
        return int(os.path.basename(filename).split("_")[1].split(".")[0])

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
        return os.path.join(self.metadataDirectory, f"dayObs_{dayObs}.json")

    def callback(self):
        """Method called on a timer to gather the shards and upload as needed.

        Adds the metadata to the sidecar file for the dataId and uploads it.
        """
        try:
            self.log.debug("Getting metadata from shards")
            self.mergeShardsAndUpload()  # updates all shards everywhere

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def run(self):
        """Run continuously, looking for metadata and uploading."""
        while True:
            self.callback()
            if self.heartbeater is not None:
                self.heartbeater.beat()
            sleep(self.cadence)
