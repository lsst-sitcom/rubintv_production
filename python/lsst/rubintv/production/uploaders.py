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

from lsst.summit.utils.utils import dayObsIntToString

from .channels import CHANNELS

try:
    from google.cloud import storage
    from google.cloud.storage.retry import DEFAULT_RETRY
    HAS_GOOGLE_STORAGE = True
except ImportError:
    HAS_GOOGLE_STORAGE = False

__all__ = [
    'Uploader',
    'Heartbeater',
]

_LOG = logging.getLogger(__name__)


class Uploader:
    """Class for handling uploads to the Google Cloud Storage bucket.
    """
    HEARTBEAT_PREFIX = "heartbeats"

    def __init__(self, bucketName):
        if not HAS_GOOGLE_STORAGE:
            from lsst.summit.utils.utils import GOOGLE_CLOUD_MISSING_MSG
            raise RuntimeError(GOOGLE_CLOUD_MISSING_MSG)
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucketName)
        self.log = _LOG.getChild("googleUploader")

    def uploadHeartbeat(self, channel, flatlinePeriod):
        """Upload a heartbeat for the specified channel to the bucket.

        Parameters
        ----------
        channel : `str`
            The channel name.
        flatlinePeriod : `float`
            The period after which to consider the channel dead.

        Returns
        -------
        success : `bool`
            Did the upload succeed?
        """
        filename = "/".join([self.HEARTBEAT_PREFIX, channel]) + ".json"

        currTime = int(time.time())
        nextExpected = currTime + flatlinePeriod

        heartbeatJsonDict = {
            "channel": channel,
            "currTime": currTime,
            "nextExpected": nextExpected,
            "errors": {}
        }
        heartbeatJson = json.dumps(heartbeatJsonDict)

        blob = self.bucket.blob("/".join([filename]))
        blob.cache_control = 'no-store'  # must set before upload

        # heartbeat retry strategy
        modified_retry = DEFAULT_RETRY.with_deadline(0.6)  # single retry here
        modified_retry = modified_retry.with_delay(initial=0.5, multiplier=1.2, maximum=2)

        try:
            blob.upload_from_string(heartbeatJson, retry=modified_retry)
            self.log.debug(f'Uploaded heartbeat to channel {channel} with datetime {currTime}')
            return True
        except Exception:
            return False

    def uploadPerSeqNumPlot(self,
                            channel,
                            dayObsInt,
                            seqNumInt,
                            filename,
                            isLiveFile=False,
                            isLargeFile=False,
                            ):
        """Upload a per-dayObs/seqNum plot to the bucket.

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        dayObsInt : `int`
            The dayObs of the plot.
        seqNumInt : `int`
            The seqNum of the plot.
        filename : `str`
            The full path and filename of the file to upload.
        isLiveFile : `bool`, optional
            The file is being updated constantly, and so caching should be
            disabled.
        isLargeFile : `bool`, optional
            The file is large, so add a longer timeout to the upload.

        Raises
        ------
        ValueError
            Raised if the specified channel is not in the list of existing
            channels as specified in CHANNELS
        RuntimeError
            Raised if the Google cloud storage is not installed/importable.
        """
        if channel not in CHANNELS:
            raise ValueError(f"Error: {channel} not in {CHANNELS}")

        dayObsStr = dayObsIntToString(dayObsInt)
        # TODO: sort out this prefix nonsense as part of the plot organization
        # fixup in the new year?
        plotPrefix = channel.replace('_', '-')

        uploadAs = f"{channel}/{plotPrefix}_dayObs_{dayObsStr}_seqNum_{seqNumInt}.png"

        blob = self.bucket.blob(uploadAs)
        if isLiveFile:
            blob.cache_control = 'no-store'

        # general retry strategy
        # still quite gentle as the catchup service will fill in gaps
        # and we don't want to hold up new images landing
        timeout = 1000 if isLargeFile else 60  # default is 60s
        deadline = timeout if isLargeFile else 2.0
        modified_retry = DEFAULT_RETRY.with_deadline(deadline)  # in seconds
        modified_retry = modified_retry.with_delay(initial=.5, multiplier=1.2, maximum=2)
        try:
            blob.upload_from_filename(filename, retry=modified_retry, timeout=timeout)
            self.log.info(f'Uploaded {filename} to {uploadAs}')
        except Exception as e:
            self.log.warning(f"Failed to upload {uploadAs} to {channel} because {repr(e)}")
            return None

        return blob

    def uploadNightReportData(self,
                              channel,
                              dayObsInt,
                              filename,
                              plotGroup='',
                              ):
        """Upload night report type plot or json file to a night report channel

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        dayObsInt : `int`
            The dayObs.
        filename : `str`
            The full path and filename of the file to upload.
        plotGroup : `str`, optional

        Raises
        ------
        ValueError
            Raised if the specified channel is not in the list of existing
            channels as specified in CHANNELS
        """
        if channel not in CHANNELS:
            raise ValueError(f"Error: {channel} not in {CHANNELS}")

        # dayObsStr = dayObsIntToString(dayObsInt)
        basename = os.path.basename(filename)  # deals with png vs jpeg
        uploadAs = f"{channel}/{dayObsInt}/{plotGroup if plotGroup else 'default'}/{basename}"

        blob = self.bucket.blob(uploadAs)
        blob.cache_control = 'no-store'

        # retry strategy here is also gentle as these plots are routintely
        # updated, so we'll get a new version soon enough.
        timeout = 60  # default is 60s
        deadline = 2.0
        modified_retry = DEFAULT_RETRY.with_deadline(deadline)  # in seconds
        modified_retry = modified_retry.with_delay(initial=.5, multiplier=1.2, maximum=2)
        try:
            blob.upload_from_filename(filename, retry=modified_retry, timeout=timeout)
            self.log.info(f'Uploaded {filename} to {uploadAs}')
        except Exception as e:
            self.log.warning(f"Failed to upload {uploadAs} to {channel} because {repr(e)}")
            return None

        return blob

    def googleUpload(self, channel, sourceFilename,
                     uploadAsFilename=None,
                     isLiveFile=False,
                     isLargeFile=False,
                     ):
        """Upload a file to the RubinTV Google cloud storage bucket.

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        sourceFilename : `str`
            The full path and filename of the file to upload.
        uploadAsFilename : `str`, optional
            Optionally rename the file to this upon upload.
        isLiveFile : `bool`, optional
            The file is being updated constantly, and so caching should be
            disabled.
        isLargeFile : `bool`, optional
            The file is large, so add a longer timeout to the upload.

        Raises
        ------
        ValueError
            Raised if the specified channel is not in the list of existing
            channels as specified in CHANNELS
        RuntimeError
            Raised if the Google cloud storage is not installed/importable.
        """
        if channel not in CHANNELS:
            raise ValueError(f"Error: {channel} not in {CHANNELS}")

        if not uploadAsFilename:
            uploadAsFilename = os.path.basename(sourceFilename)
        finalName = "/".join([channel, uploadAsFilename])

        blob = self.bucket.blob(finalName)
        if isLiveFile:
            blob.cache_control = 'no-store'

        # general retry strategy
        # still quite gentle as the catchup service will fill in gaps
        # and we don't want to hold up new images landing
        timeout = 1000 if isLargeFile else 60  # default is 60s
        deadline = timeout if isLargeFile else 2.0
        modified_retry = DEFAULT_RETRY.with_deadline(deadline)  # in seconds
        modified_retry = modified_retry.with_delay(initial=.5, multiplier=1.2, maximum=2)
        try:
            blob.upload_from_filename(sourceFilename, retry=modified_retry, timeout=timeout)
            self.log.info(f'Uploaded {sourceFilename} to {finalName}')
        except Exception as e:
            self.log.warning(f"Failed to upload {finalName} to {channel} because {repr(e)}")
            return None

        return blob


class Heartbeater:
    """A class for uploading heartbeats to the GCS bucket.

    Call ``heartbeater.beat()`` as often as you like. Files will only be
    uploaded once ``self.uploadPeriod`` has elapsed, or if ``ensure=True`` when
    calling beat.

    Parameters
    ----------
    handle : `str`
        The name of the channel to which the heartbeat corresponds.
    bucketName : `str`
        The name of the bucket to upload the heartbeats to.
    uploadPeriod : `float`
        The time, in seconds, which must have passed since the last successful
        heartbeat for a new upload to be undertaken.
    flatlinePeriod : `float`
        If a new heartbeat is not received before the flatlinePeriod elapses,
        in seconds, the channel will be considered down.
    """
    def __init__(self, handle, bucketName, uploadPeriod, flatlinePeriod):
        self.handle = handle
        self.uploadPeriod = uploadPeriod
        self.flatlinePeriod = flatlinePeriod

        self.lastUpload = -1
        self.uploader = Uploader(bucketName)

    def beat(self, ensure=False, customFlatlinePeriod=None):
        """Upload the heartbeat if enough time has passed or ensure=True.

        customFlatlinePeriod implies ensure, as long forecasts should always be
        uploaded.

        Parameters
        ----------
        ensure : `str`
            Ensure that the heartbeat is uploaded, even if one has been sent
            within the last ``uploadPeriod``.
        customFlatlinePeriod : `float`
            Upload with a different flatline period. Use before starting long
            running jobs.
        """
        forecast = self.flatlinePeriod if not customFlatlinePeriod else customFlatlinePeriod

        now = time.time()
        elapsed = now - self.lastUpload
        if (elapsed >= self.uploadPeriod) or ensure or customFlatlinePeriod:
            if self.uploader.uploadHeartbeat(self.handle, forecast):  # returns True on successful upload
                self.lastUpload = now  # only reset this if the upload was successful
