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
import time

from abc import abstractmethod, ABC
from boto3.session import Session as S3_session
from botocore.exceptions import ClientError
from enum import Enum
from typing_extensions import Optional, override

from lsst.summit.utils.utils import dayObsIntToString

from .channels import CHANNELS

_LOG = logging.getLogger(__name__)

try:
    from google.cloud import storage
    from google.cloud.storage.retry import DEFAULT_RETRY

    HAS_GOOGLE_STORAGE = True
except ImportError:
    HAS_GOOGLE_STORAGE = False


__all__ = ["Heartbeater", "Uploader", "S3Uploader", "UploadError"]


class ConnectionError(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class UploadError(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class IUploader(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def uploadPerSeqNumPlot(
        self,
        channel: str,
        dayObsInt: int,
        seqNumInt: int,
        filename: str,
        isLiveFile: bool = False,
        isLargeFile: bool = False,
    ) -> str:
        """
        Upload a per-dayObs/seqNum plot to the bucket.

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        observation_day : `int`
            The dayObs of the plot.
        sequence_number : `int`
            The seqNum of the plot.
        filename : `str`
            The full path and filename of the file to upload.
        is_live_file : `bool`, optional
            The file is being updated constantly, and so caching should be
            disabled.
        is_large_file : `bool`, optional
            The file is large, so add a longer timeout to the upload.

        Raises
        ------
        ValueError
            Raised if the specified channel is not in the list of existing
            channels as specified in CHANNELS
        UploadError
            Raised if uploading the file to the Bucket was no possible
        """
        raise NotImplementedError()

    @abstractmethod
    def uploadNightReportData(
        self, channel: str, dayObsInt: int, filename: str, plotGroup: Optional[str]
    ) -> str:
        """
        Upload night report type plot or json file to a night report channel

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        observation_day : `int`
            The dayObs.
        filename : `str`
            The full path and filename of the file to upload.
        plot_group : `str`, optional
            The group to upload the plot to. The 'default' group is used if
            this is not specified. However, people are encouraged to supply
            groups for their plots, so the 'default' value is not put in the
            function signature to indicate this.

        Raises
        ------
        ValueError
            Raised if the specified channel is not in the list of existing
            channels as specified in CHANNELS
        UploadError
            Raised if uploading the file to the Bucket was no possible
        """
        raise NotImplementedError()

    @abstractmethod
    def upload(self, channel: str, source_filename: str) -> None:
        """Upload a file to an storage bucket.

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        source_filename : `str`
            The full path and filename of the file to upload.
        Raises
        ------
        UploadError
            Raised if uploading the file to the Bucket was no possible
        """
        raise NotImplementedError()


class Bucket(Enum):
    SUMMIT = "rubin-rubintv-data-summit"
    TTS = "rubin-rubintv-data-tts"
    USDF = "rubin-rubintv-data-usdf"


class EndPoint(Enum):
    USDF = {"data_point": "https://s3dfrgw.slac.stanford.edu",
            "buckets_available": [Bucket.SUMMIT, Bucket.TTS, Bucket.USDF]}


class S3Uploader(IUploader):

    def __init__(self, end_point: EndPoint = EndPoint.USDF, bucket: Bucket = Bucket.TTS) -> None:
        """
        S3 Uploader initialization. Here the connection with the remote S3
        bucket is stablished.
        Parameters
        ----------
        bucket: Enum Bucket
        Bucket identifier to connect to. Available buckets: SUMMIT, TTS, USDF.

        Raises
        ValueError: If bucket not valid for endpoint selected
        CommunicationError: When connection could not be stablished with S3
        server
        ------
        """
        super().__init__()
        self._log = _LOG.getChild("S3Uploader")
        if bucket not in end_point.value['buckets_available']:
            raise ValueError('Invalid bucket')
        try:
            self._session = S3_session(profile_name=bucket.value)
            self._S3 = self._session.resource(
                "s3", endpoint_url=end_point.value["data_point"]
            )  # type: ServiceResource
            self._bucket_s3 = self._S3.Bucket(bucket.value)  # type: ignore
        except ClientError:
            self._log.exception(f"Failed client connection: {bucket.value}")
            raise ConnectionError(f"Failed client connection: {bucket.value}")

    @override
    def uploadPerSeqNumPlot(
        self,
        channel: str,
        observation_day: int,
        sequence_number: int,
        filename: str,
        is_live_file: bool = False,
        is_large_file: bool = False,
    ) -> str:
        """
        Upload a per-dayObs/seqNum plot to the bucket.

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        observation_day : `int`
            The dayObs of the plot.
        sequence_number : `int`
            The seqNum of the plot.
        filename : `str`
            The full path and filename of the file to upload.
        is_live_file : `bool`, optional
            The file is being updated constantly, and so caching should be
            disabled.
        is_large_file : `bool`, optional
            The file is large, so add a longer timeout to the upload.

        Raises
        ------
        ValueError
            Raised if the specified channel is not in the list of existing
            channels as specified in CHANNELS
        UploadError
            Raised if uploading the file to the Bucket was no possible
        """
        if channel not in CHANNELS:
            raise ValueError(f"Error: {channel} not in {CHANNELS}")

        date_observation_str = dayObsIntToString(observation_day)
        # TODO: sort out this prefix nonsense as part of the plot organization
        # fixup in the new year?
        plot_prefix = channel.replace("_", "-")

        upload_as = f"{channel}/{plot_prefix}_dayObs_{date_observation_str}_seqNum_{sequence_number}.png"

        try:
            self.upload(channel=upload_as, source_filename=filename)
            self._log.info(f"Uploaded {filename} to {upload_as}")
        except Exception as ex:
            self._log.exception(
                f"Failed to upload {filename} as {upload_as} to {channel}"
            )
            raise ex

        return upload_as

    @override
    def uploadNightReportData(
        self,
        channel: str,
        dayObsInt: int,
        filename: str,
        plotGroup: Optional[str] = None,
    ):
        """
        Upload night report type plot or json file to a night report channel

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        observation_day : `int`
            The dayObs.
        filename : `str`
            The full path and filename of the file to upload.
        plot_group : `str`, optional
            The group to upload the plot to. The 'default' group is used if
            this is not specified. However, people are encouraged to supply
            groups for their plots, so the 'default' value is not put in the
            function signature to indicate this.

        Raises
        ------
        ValueError
            Raised if the specified channel is not in the list of existing
            channels as specified in CHANNELS
        UploadError
            Raised if uploading the file to the Bucket was no possible
        """
        if channel not in CHANNELS:
            raise ValueError(f"Error: {channel} not in {CHANNELS}")

        basename = os.path.basename(filename)  # deals with png vs jpeg

        # the plot filenames have the channel name saved into them in the form
        # path/channelName-plotName.png, so remove the channel name and dash
        basename = basename.replace(channel + "-", "")
        upload_as = (
            f"{channel}/{dayObsInt}/{plotGroup if plotGroup else 'default'}/{basename}"
        )
        try:
            self.upload(channel=upload_as, source_filename=filename)
            self._log.info(f"Uploaded {filename} to {upload_as}")
        except Exception as ex:
            self._log.exception(
                f"Failed to upload {filename} as {upload_as} to {channel}"
            )
            raise ex

        return upload_as

    @override
    def upload(self, destiny_filename: str, source_filename: str) -> None:
        """Upload a file to an storage bucket.

        Parameters
        ----------
        destiny_filename : `str`
            The destiny filename including channel location
        source_filename : `str`
            The full path and filename of the file to upload.
        Raises
        ------
        UploadError
            Raised if uploading the file to the Bucket was no possible
        """
        try:
            self._bucket_s3.upload_file(
                Filename=source_filename, Key=destiny_filename
            )
        except ClientError as e:
            logging.error(e)
            raise UploadError(
                f"Failed uploadig file {source_filename} as Key: {destiny_filename}"
            )


class Uploader:
    """Class for handling uploads to the Google Cloud Storage bucket."""

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
            "errors": {},
        }
        heartbeatJson = json.dumps(heartbeatJsonDict)

        blob = self.bucket.blob("/".join([filename]))
        blob.cache_control = "no-store"  # must set before upload

        # heartbeat retry strategy
        modified_retry = DEFAULT_RETRY.with_deadline(0.6)  # single retry here
        modified_retry = modified_retry.with_delay(
            initial=0.5, multiplier=1.2, maximum=2
        )

        try:
            blob.upload_from_string(heartbeatJson, retry=modified_retry)
            self.log.debug(
                f"Uploaded heartbeat to channel {channel} with datetime {currTime}"
            )
            return True
        except Exception:
            return False

    def uploadPerSeqNumPlot(
        self,
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
        plotPrefix = channel.replace("_", "-")

        uploadAs = f"{channel}/{plotPrefix}_dayObs_{dayObsStr}_seqNum_{seqNumInt}.png"

        blob = self.bucket.blob(uploadAs)
        if isLiveFile:
            blob.cache_control = "no-store"

        # general retry strategy
        # still quite gentle as the catchup service will fill in gaps
        # and we don't want to hold up new images landing
        timeout = 1000 if isLargeFile else 60  # default is 60s
        deadline = timeout if isLargeFile else 2.0
        modified_retry = DEFAULT_RETRY.with_deadline(deadline)  # in seconds
        modified_retry = modified_retry.with_delay(
            initial=0.5, multiplier=1.2, maximum=2
        )
        try:
            blob.upload_from_filename(filename, retry=modified_retry, timeout=timeout)
            self.log.info(f"Uploaded {filename} to {uploadAs}")
        except Exception:
            self.log.exception(
                f"Failed to upload {filename} as {uploadAs} to {channel}"
            )
            return None

        return blob

    def uploadNightReportData(
        self,
        channel,
        dayObsInt,
        filename,
        plotGroup="",
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
            The group to upload the plot to. The 'default' group is used if
            this is not specified. However, people are encouraged to supply
            groups for their plots, so the 'default' value is not put in the
            function signature to indicate this.

        Raises
        ------
        ValueError
            Raised if the specified channel is not in the list of existing
            channels as specified in CHANNELS
        """
        if channel not in CHANNELS:
            raise ValueError(f"Error: {channel} not in {CHANNELS}")

        basename = os.path.basename(filename)  # deals with png vs jpeg

        # the plot filenames have the channel name saved into them in the form
        # path/channelName-plotName.png, so remove the channel name and dash
        basename = basename.replace(channel + "-", "")
        uploadAs = (
            f"{channel}/{dayObsInt}/{plotGroup if plotGroup else 'default'}/{basename}"
        )

        blob = self.bucket.blob(uploadAs)
        blob.cache_control = "no-store"

        # retry strategy here is also gentle as these plots are routintely
        # updated, so we'll get a new version soon enough.
        timeout = 60  # default is 60s
        deadline = 2.0
        modified_retry = DEFAULT_RETRY.with_deadline(deadline)  # in seconds
        modified_retry = modified_retry.with_delay(
            initial=0.5, multiplier=1.2, maximum=2
        )
        try:
            blob.upload_from_filename(filename, retry=modified_retry, timeout=timeout)
            self.log.info(f"Uploaded {filename} to {uploadAs}")
        except Exception as e:
            self.log.warning(
                f"Failed to upload {uploadAs} to {channel} because {repr(e)}"
            )
            return None

        return blob

    def googleUpload(
        self,
        channel,
        sourceFilename,
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
            blob.cache_control = "no-store"

        # general retry strategy
        # still quite gentle as the catchup service will fill in gaps
        # and we don't want to hold up new images landing
        timeout = 1000 if isLargeFile else 60  # default is 60s
        deadline = timeout if isLargeFile else 2.0
        modified_retry = DEFAULT_RETRY.with_deadline(deadline)  # in seconds
        modified_retry = modified_retry.with_delay(
            initial=0.5, multiplier=1.2, maximum=2
        )
        try:
            blob.upload_from_filename(
                sourceFilename, retry=modified_retry, timeout=timeout
            )
            self.log.info(f"Uploaded {sourceFilename} to {finalName}")
        except Exception as e:
            self.log.warning(
                f"Failed to upload {finalName} to {channel} because {repr(e)}"
            )
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
        forecast = (
            self.flatlinePeriod if not customFlatlinePeriod else customFlatlinePeriod
        )

        now = time.time()
        elapsed = now - self.lastUpload
        if (elapsed >= self.uploadPeriod) or ensure or customFlatlinePeriod:
            if self.uploader.uploadHeartbeat(
                self.handle, forecast
            ):  # returns True on successful upload
                self.lastUpload = now  # only reset this if the upload was successful
