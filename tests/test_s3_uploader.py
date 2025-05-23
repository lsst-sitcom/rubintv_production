# This file is part of summit_utils.
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
import os
import random
import tempfile
import unittest

import boto3
from moto import mock_aws

from lsst.rubintv.production.uploaders import S3Uploader


class TestS3Uploader(unittest.TestCase):

    def setUp(self) -> None:
        # Create an S3 bucket using moto's mock_aws
        self._mock_aws = mock_aws()
        self._mock_aws.start()
        self._mocked_s3_bucket_name = "mocked_s3_bucket"
        self._s3_resource = boto3.resource("s3", region_name="us-east-1")
        self._mock_bucket = self._s3_resource.create_bucket(Bucket=self._mocked_s3_bucket_name)

        # Create an instance of S3Uploader
        self._s3_uploader = S3Uploader.from_bucket(self._mock_bucket)

    def tearDown(self) -> None:
        # Stop the mock S3 service
        self._mock_aws.stop()

    @staticmethod
    def get_file_content(filePath) -> str:
        content = ""
        with open(filePath, "r") as file:
            content = file.read()
        return content

    def test_connectionTest(self) -> None:
        connected = self._s3_uploader.checkAccess()
        self.assertTrue(connected)

    def test_uploadPerSeqNumPlot(self) -> None:
        """Test uploadPerSeqNumPlot for S3 Uploader"""
        observationDay = 20241515
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files/test_file_0001.txt")
        sequenceNumber = 1

        fileContent = TestS3Uploader.get_file_content(filename)
        uploadedFile = self._s3_uploader.uploadPerSeqNumPlot(
            instrument="auxtel",
            plotName="testPlot",
            dayObs=observationDay,
            seqNum=sequenceNumber,
            filename=filename,
        )

        self.is_correct_check_uploaded_file(uploadedFile, fileContent)

    def test_uploadNightReportData(self) -> None:
        """Test uploadNightReportData for S3 Uploader"""
        observationDay = 20241515
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files/test_file_0001.txt")
        plotGroup = "group_01"

        fileContent = TestS3Uploader.get_file_content(filename)
        uploadedFile = self._s3_uploader.uploadNightReportData(
            instrument="auxtel",
            dayObs=observationDay,
            filename=filename,
            plotGroup=plotGroup,
            uploadAs="test_file_0001.txt",
        )
        self.is_correct_check_uploaded_file(uploadedFile, fileContent)

    def test_uploadMetdata(self) -> None:
        """test uploadMetdata method from S3Uploader"""
        channels = [
            "startracker_metadata",
            "ts8_metadata",
            "comcam_metadata",
            "slac_lsstcam_metadata",
            "tma_metadata",
        ]
        observationDay = 20241515
        channel = random.choice(channels)
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files/test_file_0001.txt")

        fileContent = TestS3Uploader.get_file_content(filename)
        uploadedFile = self._s3_uploader.uploadMetdata(channel, dayObs=observationDay, filename=filename)
        self.is_correct_check_uploaded_file(uploadedFile, fileContent)

    def test_uploadMetdata_fails_on_not_metadata_channel(self) -> None:
        """test uploadMetdata method from S3Uploader
        when using a not metadata channel causes an exception"""
        channel = "auxtel_monitor"
        observationDay = 20241515
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files/test_file_0001.txt")
        with self.assertRaises(ValueError) as context:
            self._s3_uploader.uploadMetdata(channel, dayObs=observationDay, filename=filename)
        self.assertEqual(
            str(context.exception),
            "Tried to upload non-metadata file to metadata channel:" + f"{channel}, {filename}",
        )

    def is_correct_check_uploaded_file(self, uploadedFile, file_content) -> None:
        """Support method to check that the file uploaded is correct"""
        # Verify that the file was uploaded to the S3 bucket
        with tempfile.NamedTemporaryFile() as temp_file:
            self._mock_bucket.download_file(uploadedFile, temp_file.name)
            download_content = TestS3Uploader.get_file_content(temp_file.name)
            # Assert that the content of the uploaded file
            # matches the expected content
        self.assertEqual(download_content, file_content)


if __name__ == "__main__":
    unittest.main()
