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
from boto3.resources.base import ServiceResource
from enum import Enum
from typing import Optional, override

from lsst.summit.utils.utils import dayObsIntToString

from .channels import CHANNELS

__all__ = [
    'S3Uploader',
    "UploadError"
]

_LOG = logging.getLogger(__name__)

class ConnectionError(Exception):

    def __init__(self, msg: str):
        super().__init__(msg)

class UploadError(Exception):

    def __init__(self, msg: str):
        super().__init__(msg)

class Uploader(ABC):

    def __init__(self):
        super().__init__()

    @abstractmethod
    def upload_sequential_number_plot(self, channel: str, observation_day: int, sequence_number: int,
                                      filename: str, is_live_file: bool = False, is_large_file: bool = False):
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
    def upload_night_report_data(self, channel, observation_day: int, filename: str, plot_group: str = ''):
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
    def upload(self, channel: str, source_filename: str):
        """Upload a file to an storage bucket.

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        source_filename : `str`
            The full path and filename of the file to upload.
        upload_as_filename : `str`, optional
            Optionally rename the file to this upon upload.
        is_live_file : `bool`, optional
            The file is being updated constantly, and so caching should be
            disabled.
        is_large_file : `bool`, optional
            The file is large, so add a longer timeout to the upload.

        Raises
        ------
        UploadError
            Raised if uploading the file to the Bucket was no possible
        """
        raise NotImplementedError()

class Buckets(Enum):
    SUMMIT = "rubin-rubintv-data-tts"
    TTS = "rubin-rubintv-data-tts"
    USDF = "rubin-rubintv-data-usdf"


class S3Uploader(Uploader):

    _ENDPOINT_URL = "https://s3dfrgw.slac.stanford.edu"

    def __init__(self, bucket: Buckets = Buckets.TTS) -> None:
        """
        S3 Uploader initialization. Here the connection with the remote S3 bucket is stablished.
        Raises
        ------


        """
        super().__init__()
        self._log = _LOG.getChild("S3Uploader")
        try:
            self._session = S3_session(profile_name=bucket.value)
            self._S3 = self._session.resource("s3", endpoint_url=S3Uploader._ENDPOINT_URL) # type: ServiceResource
            self._bucket_s3 = self._S3.Bucket(bucket.value) # type: ignore
        except:
            self._log.exception(f"Failed client connection: {bucket.value}")
            raise ConnectionError(f"Failed client connection: {bucket.value}")

    @override
    def upload_sequential_number_plot(self, channel: str, observation_day: int, sequence_number: int,
                                      filename: str, is_live_file: bool = False, is_large_file: bool = False) -> str:
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

        date_observation_str = dayObsIntToString(observation_day)
        # TODO: sort out this prefix nonsense as part of the plot organization
        # fixup in the new year?
        plot_prefix = channel.replace('_', '-')

        upload_as = f"{channel}/{plot_prefix}_dayObs_{date_observation_str}_seqNum_{sequence_number}.png"

        try:
            self.upload(channel = upload_as, source_filename = filename)
            self._log.info(f'Uploaded {filename} to {upload_as}')
        except Exception as ex:
            self._log.exception(f"Failed to upload {filename} as {upload_as} to {channel}")
            raise ex

        return upload_as

    @override
    def upload_night_report_data(self, channel, observation_day: int, filename: str, plot_group: Optional[str] = None) -> str:
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
        basename = basename.replace(channel + '-', '')
        upload_as = f"{channel}/{observation_day}/{plot_group if plot_group else 'default'}/{basename}"
        try:
            self.upload(channel = upload_as, source_filename = filename)
            self._log.info(f'Uploaded {filename} to {upload_as}')
        except Exception as ex:
            self._log.exception(f"Failed to upload {filename} as {upload_as} to {channel}")
            raise ex

        return upload_as

    @override
    def upload(self, channel: str, source_filename: str) -> None:
        """Upload a file to an storage bucket.

        Parameters
        ----------
        channel : `str`
            The RubinTV channel to upload to.
        source_filename : `str`
            The full path and filename of the file to upload.
        upload_as_filename : `str`, optional
            Optionally rename the file to this upon upload.
        is_live_file : `bool`, optional
            The file is being updated constantly, and so caching should be
            disabled.
        is_large_file : `bool`, optional
            The file is large, so add a longer timeout to the upload.

        Raises
        ------
        UploadError
            Raised if uploading the file to the Bucket was no possible
        """
        try:
            response = self._bucket_s3.upload_file(Filename = source_filename, Key = channel)
        except ClientError as e:
            logging.error(e)
            raise UploadError(f"Failed uploadig file {source_filename} as Key: {channel}")
