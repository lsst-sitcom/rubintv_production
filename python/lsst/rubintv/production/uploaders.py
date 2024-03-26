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
from boto3.resources.base import ServiceResource
from botocore.config import Config
from botocore.exceptions import ClientError
from dataclasses import dataclass
from enum import Enum
from typing_extensions import Optional, override

from lsst.summit.utils.utils import dayObsIntToString, getSite

from .channels import CHANNELS, KNOWN_INSTRUMENTS, getCameraAndPlotName

_LOG = logging.getLogger(__name__)

try:
    from google.cloud import storage
    from google.cloud.storage.retry import DEFAULT_RETRY

    HAS_GOOGLE_STORAGE = True
except ImportError:
    HAS_GOOGLE_STORAGE = False


__all__ = [
    "createLocalS3UploaderForSite",
    "createRemoteS3UploaderForSite",
    "Heartbeater",
    "Uploader",
    "S3Uploader",
    "UploadError"
]


def createLocalS3UploaderForSite(httpsProxy=''):
    """Create the S3Uploader with the correct config for the site
    automatically.

    Returns
    -------
    uploader : `S3Uploader`
        The S3Uploader for the site.
    """
    site = getSite()
    match site:
        case "base":
            return S3Uploader.from_information(
                endPoint=EndPoint.BASE,
                bucket=Bucket.BTS,
                httpsProxy=httpsProxy
            )
        case "summit":
            return S3Uploader.from_information(
                endPoint=EndPoint.SUMMIT,
                bucket=Bucket.SUMMIT,
                httpsProxy=httpsProxy
            )
        case site if site in ['usdf', 'rubin-devl']:
            return S3Uploader.from_information(
                endPoint=EndPoint.USDF,
                bucket=Bucket.USDF,
                httpsProxy=httpsProxy
            )
        case "tucson":
            return S3Uploader.from_information(
                endPoint=EndPoint.TUCSON,
                bucket=Bucket.TTS,
                httpsProxy=httpsProxy
            )
        case _:
            raise ValueError(f"Unknown site: {site}")


def createRemoteS3UploaderForSite():
    """Create the S3Uploader with the correct config
       for the site automatically.

    Returns
    -------
    uploader : `S3Uploader`
        The S3Uploader for the site.
    """
    site = getSite()
    match site:
        case "base":
            return S3Uploader.from_information(
                endPoint=EndPoint.USDF,
                bucket=Bucket.BTS,
            )
        case "summit":
            httpsProxy = 'http://ocio-gpu03.slac.stanford.edu:3128'
            return S3Uploader.from_information(
                endPoint=EndPoint.USDF,
                bucket=Bucket.SUMMIT,
                httpsProxy=httpsProxy
            )
        case "usdf":
            _LOG.info('No remote uploader is necessary for USDF')
            return None
        case "tucson":
            return S3Uploader.from_information(
                endPoint=EndPoint.USDF,
                bucket=Bucket.TTS,
            )
        case _:
            raise ValueError(f"Unknown site: {site}")


def checkInstrument(instrument):
    if instrument not in KNOWN_INSTRUMENTS:
        raise ValueError(f"Error: {instrument} not in {KNOWN_INSTRUMENTS}")


class Bucket(Enum):
    USDF = 1
    SUMMIT = 2
    TTS = 3
    BTS = 4


@dataclass(frozen=True)
class BucketInformation:
    profileName: str
    bucketName: str


class EndPoint(Enum):
    USDF = {
        "end_point": "https://s3dfrgw.slac.stanford.edu",
        "buckets_available": {
            Bucket.SUMMIT: BucketInformation(
                "rubin-rubintv-data-summit",
                "rubin-rubintv-data-summit"
            ),
            Bucket.USDF: BucketInformation(
                "rubin-rubintv-data-usdf",
                "rubin-rubintv-data-usdf"
            ),
        },
    }

    SUMMIT = {
        "end_point": "https://s3.rubintv.cp.lsst.org",
        "buckets_available": {
            Bucket.SUMMIT: BucketInformation("summit-data-summit",
                                             "rubintv")
        },
    }

    BASE = {
        "end_point": "https://s3.rubintv.ls.lsst.org",
        "buckets_available": {
            Bucket.BTS: BucketInformation("base-data-base",
                                          "rubintv")
        },
    }

    TUCSON = {
        "end_point": "https://s3.rubintv.tu.lsst.org",
        "buckets_available": {
            Bucket.TTS: BucketInformation("tucson-data-tucson",
                                          "rubintv")
        },
    }


class ConnectionError(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class UploadError(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class IUploader(ABC):
    """Uploader interface
    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    def uploadNightReportData(
        self, channel: str, dayObsInt: int, filename: str, plotGroup: Optional[str]
    ) -> str:
        """Upload night report type plot or json file to a night
           report channel.

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        observation_day : `int`
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
            channels as specified in CHANNELS.
        UploadError
            Raised if uploading the file to the Bucket was not possible.
        """
        raise NotImplementedError()

    @abstractmethod
    def upload(self, destinationFilename: str, sourceFilename: str) -> None:
        """Upload a file to a storage bucket.

        Parameters
        ----------
        destinationFilename : `str`
            The destination filename including channel location.
        sourceFilename : `str`
            The full path and filename of the file to upload.

        Raises
        ------
        UploadError
            Raised if uploading the file to the Bucket was not possible.
        """
        raise NotImplementedError()


class MultiUploader(IUploader):
    def __init__(self):
        # TODO: thread the remote upload
        self.localUploader = createLocalS3UploaderForSite()

        try:
            self.remoteUploader = createRemoteS3UploaderForSite()
        except Exception:
            self.remoteUploader = None

        self.log = _LOG.getChild("MultiUploader")
        self.log.info(f"Created MultiUploader with local: {self.localUploader}"
                      f" and remote: {self.remoteUploader}")

    @property
    def hasRemote(self):
        return self.remoteUploader is not None

    def uploadPerSeqNumPlot(self, *args, **kwargs):
        self.localUploader.uploadPerSeqNumPlot(*args, **kwargs)
        self.log.info('uploaded to local')

        if self.hasRemote:
            self.remoteUploader.uploadPerSeqNumPlot(*args, **kwargs)
            self.log.info('uploaded to remote')

    def uploadNightReportData(self, *args, **kwargs):
        self.localUploader.uploadNightReportData(*args, **kwargs)

        if self.hasRemote:
            self.remoteUploader.uploadNightReportData(*args, **kwargs)

    def upload(self, *args, **kwargs):
        self.localUploader.upload(*args, **kwargs)

        if self.hasRemote:
            self.remoteUploader.upload(*args, **kwargs)

    def uploadMetdata(self, *args, **kwargs):
        self.localUploader.uploadMetdata(*args, **kwargs)

        if self.hasRemote:
            self.remoteUploader.uploadMetdata(*args, **kwargs)

    def uploadMovie(self, *args, **kwargs):
        self.localUploader.uploadMovie(*args, **kwargs)

        if self.hasRemote:
            self.remoteUploader.uploadMovie(*args, **kwargs)

    def uploadStarTrackerPlot(self, *args, **kwargs):
        self.localUploader.uploadStarTrackerPlot(*args, **kwargs)

        if self.hasRemote:
            self.remoteUploader.uploadStarTrackerPlot(*args, **kwargs)

    def uploadAllSkyStill(self, *args, **kwargs):
        self.localUploader.uploadAllSkyStill(*args, **kwargs)

        if self.hasRemote:
            self.remoteUploader.uploadAllSkyStill(*args, **kwargs)


class S3Uploader(IUploader):
    """
    Uploader to upload files to an S3 bucket

    Parameters
    ----------
    s3Bucket : `ServiceResource`
        S3 Bucket to upload files to.
    """
    def __init__(self, s3Bucket: ServiceResource) -> None:
        super().__init__()
        self._log = _LOG.getChild("S3Uploader")
        self._s3Bucket = s3Bucket

    @classmethod
    def from_bucket(cls, s3Bucket: ServiceResource):
        """S3 Uploader initialization from bucket information.

        Parameters
        ----------
        s3Bucket : `ServiceResource`
            S3 Bucket to upload files to.

        Returns
        ------
        uploader: `S3Uploader`
            instance ready to use for file transfer.
        """
        assert isinstance(s3Bucket, ServiceResource), "Invalid s3Bucket type"
        return cls(s3Bucket=s3Bucket)

    @classmethod
    def from_information(cls,
                         endPoint: EndPoint = EndPoint.USDF,
                         bucket: Bucket = Bucket.USDF,
                         httpsProxy: str = ""):
        """S3 Uploader initialization from bucket information.

        Parameters
        ----------
        endPoint: `EndPoint`
            The complete URL to use to construct the S3 client.
        bucket : `Bucket`
            Bucket identifier to connect to. Available buckets: SUMMIT, TTS,
            USDF or BTS.
        httpsProxy: `str`, optional
            URL of an https proxy if needed. Form should be: host:port.

        Raises
        ------
        ValueError:
            If bucket not valid for endpoint selected.
        ConnectionError:
            When connection could not be stablished with
            S3 server.

        Returns
        -------
        uploader: `S3Uploader`
            S3Uploader instance ready to use for file transfer.
        """
        if bucket not in endPoint.value['buckets_available'].keys():
            raise ValueError('Invalid bucket')
        endPointValue = endPoint.value["end_point"]
        bucketInfo = endPoint.value["buckets_available"][bucket]  # type: BucketInformation
        bucket = cls._createBucketConnection(endPoint=endPointValue,
                                             bucketInfo=bucketInfo,
                                             proxyUrl=httpsProxy)
        return cls(bucket)

    @staticmethod
    def _createBucketConnection(endPoint: str,
                                bucketInfo: BucketInformation,
                                proxyUrl: str = "") -> ServiceResource:
        """Create bucket connection used to upload files.

        Parameters
        ----------
        endPoint: `EndPoint`
            The complete URL to use to construct the S3 client.
        bucket : `Bucket`
            Bucket identifier to connect to. Available buckets: SUMMIT, TTS,
            USDF or BTS.
        httpsProxy : `str`, optional
            URL of an https proxy if needed. Form should be: host:port.

        Raises
        ------
        ConnectionError
            When connection could not be stablished with S3 server.

        Returns
        -------
        s3bucket : `ServiceResource`
            S3 bucket ready to use for file transfer
        """
        try:
            proxyDict = {"http": proxyUrl, "https": proxyUrl}

            # Create a custom botocore session
            session = S3_session(profile_name=bucketInfo.profileName)

            # Create an S3 resource with the custom botocore session
            s3Resource = session.resource('s3', endpoint_url=endPoint,
                                          config=Config(proxies=proxyDict))
            return s3Resource.Bucket(bucketInfo.bucketName)
        except ClientError:
            raise ConnectionError(f"Failed client connection: {bucketInfo.profileName}")

    def uploadPerSeqNumPlot(
        self,
        instrument: str,
        plotName: str,
        dayObs: int,
        seqNum: int,
        filename: str,
    ) -> str:
        """Upload a per-dayObs/seqNum plot to the bucket.

        Parameters
        ----------
        instrument : `str`
            The instrument the plot is for.
        plotName : `str`
            The name of the plot.
        dayObs : `int`
            The dayObs of the plot.
        seqNum : `int`
            The seqNum of the plot.
        filename : `str`
            The full path and filename of the file to upload.

        Raises
        ------
        ValueError
            Raised if the specified channel is not in the list of existing
            channels as specified in CHANNELS.
        UploadError
            Raised if uploading the file to the Bucket was not possible.

        Returns
        -------
        uploadAs: `str``
            Path and filename for the destination file in the bucket
        """
        checkInstrument(instrument)

        dayObsStr = dayObsIntToString(dayObs)
        paddedSeqNum = f"{seqNum:06}"
        extension = os.path.splitext(filename)[1]  # contains the period so don't add back later
        uploadAs = (
            f"{instrument}"
            f"/{dayObsStr}"
            f"/{plotName}"
            f"/{paddedSeqNum}"
            f"/{instrument}_{plotName}_{dayObsStr}_{paddedSeqNum}{extension}"
        )

        try:
            self.upload(destinationFilename=uploadAs, sourceFilename=filename)
            self._log.info(f"Uploaded {filename} to {uploadAs}")
        except Exception as e:
            self._log.exception(
                f"Failed to upload {filename} for {instrument}+{plotName} as {uploadAs}"
            )
            raise e

        return uploadAs

    @override
    def uploadNightReportData(
        self,
        instrument: str,
        dayObs: int,
        filename: str,
        plotGroup: str = None,
    ) -> str:
        """Upload night report type plot or json file
           to a night report channel.

        Parameters
        ----------
        instrument : `str`
            The instrument.
        dayObs : `int`
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
        UploadError
            Raised if uploading the file to the Bucket was not possible

        Returns
        -------
        uploadAs: `str``
            Path and filename for the destination file in the bucket
        """
        # XXX fix use in nightReportPlotBase
        # this is called from createAndUpload() in nightReportPlotBase.py so if
        # you change the args here (like renaming the channel to be the
        # instrument) then make sure to catch it everywhere

        checkInstrument(instrument)

        if plotGroup is None:
            plotGroup = 'default'

        basename = os.path.basename(filename)
        dayObsStr = dayObsIntToString(dayObs)

        # the plot filenames have the channel name saved into them in the form
        # path/channelName-plotName.png, so remove the channel name and dash
        plotName = basename.replace(instrument + '_night_reports' + "-", "")
        plotFilename = f'{instrument}_night_report_{dayObsStr}_{plotGroup}_{plotName}'
        uploadAs = (f"{instrument}/{dayObsStr}/night_report/{plotGroup}/{plotFilename}")
        try:
            self.upload(destinationFilename=uploadAs, sourceFilename=filename)
            self._log.info(f"Uploaded {filename} to {uploadAs}")
        except Exception as ex:
            self._log.exception(
                f"Failed to upload {filename} as {uploadAs} for {instrument} night report"
            )
            raise ex

        return uploadAs

    @override
    def upload(self, destinationFilename: str, sourceFilename: str) -> str:
        """Upload a file to a storage bucket.

        Parameters
        ----------
        destinationFilename : `str`
            The destination filename including channel location.
        sourceFilename : `str`
            The full path and filename of the file to upload.

        Raises
        ------
        UploadError
            Raised if uploading the file to the Bucket was not possible
        """
        try:
            self._s3Bucket.upload_file(
                Filename=sourceFilename, Key=destinationFilename
            )
        except ClientError as e:
            logging.error(e)
            raise UploadError(
                f"Failed uploading file {sourceFilename} as Key: {destinationFilename}"
            )
        return destinationFilename

    def uploadMovie(
        self,
        instrument: str,
        dayObs: int,
        filename: str,
        seqNum: int | None = None,
    ) -> str:
        checkInstrument(instrument)

        dayObsStr = dayObsIntToString(dayObs)
        ext = os.path.splitext(filename)[1]  # contains the perdiod

        if seqNum is None:
            seqNum = 'final'
        else:
            seqNum = f"{seqNum:06}"

        uploadAs = f"{instrument}/{dayObsStr}/movies/{seqNum}/{instrument}_movies_{dayObsStr}_{seqNum}{ext}"

        try:
            self.upload(destinationFilename=uploadAs, sourceFilename=filename)
            self._log.info(f"Uploaded {filename} to {uploadAs}")
        except Exception as ex:
            self._log.exception(f"Failed to upload {filename} as {uploadAs} for {instrument} night report")
            raise ex

        return uploadAs

    def uploadStarTrackerPlot(
        self,
        camera,
        dayObs: int,
        seqNum: int,
        filename: str,
        isFitted: bool,
    ) -> str:
        # XXX write this
        raise NotImplementedError()

    def uploadMetdata(
        self,
        channel: str,  # TODO: DM-43413 change this to instrument
        dayObs: int,
        filename: str
    ) -> str:
        """Upload a file to a storage bucket.

        Parameters
        ----------
        destinationFilename : `str`
            The destination filename including channel location.
        sourceFilename : `str`
            The full path and filename of the file to upload.

        Raises
        ------
        UploadError
            Raised if uploading the file to the Bucket was not possible.
        """
        # TODO: DM-43413 this could be tidied up to not call
        # getCameraAndPlotName by renaming the "channel" on the
        # TimedMetadataServer instances to just give the instrument without
        # _metadata
        dayObsStr = dayObsIntToString(dayObs)
        camera, plotName = getCameraAndPlotName(channel)
        if plotName != "metadata":
            raise ValueError("Tried to upload non-metadata file to metadata channel:"
                             f"{channel}, {filename}")

        uploadAs = f"{camera}/{dayObsStr}/metadata.json"
        self.upload(destinationFilename=uploadAs, sourceFilename=filename)
        return uploadAs

    def __repr__(self):
        return f"S3 uploader Bucket Information: {self._s3Bucket.name}"

    def __str__(self):
        return self.__repr__()


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
        dayObs,
        seqNum,
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

        dayObsStr = dayObsIntToString(dayObs)
        # TODO: sort out this prefix nonsense as part of the plot organization
        # fixup in the new year?
        plotPrefix = channel.replace("_", "-")

        uploadAs = f"{channel}/{plotPrefix}_dayObs_{dayObsStr}_seqNum_{seqNum}.png"

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
