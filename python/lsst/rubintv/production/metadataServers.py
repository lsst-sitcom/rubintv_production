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
                    LocationConfig,
                    )

_LOG = logging.getLogger(__name__)


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
