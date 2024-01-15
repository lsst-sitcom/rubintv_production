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
import boto3
import os
import random
import tempfile
import unittest

from moto import mock_s3
from lsst.rubintv.production import S3Uploader
from lsst.rubintv.production import CHANNELS


class TestS3Uploader(unittest.TestCase):

    def setUp(self):
        # Create an S3 bucket using moto's mock_s3
        self._mock_s3 = mock_s3()
        self._mock_s3.start()
        self._mocked_s3_bucket_name = 'mocked_s3_bucket'
        self._s3_resource = boto3.resource('s3', region_name='us-east-1')
        self._mock_bucket = self._s3_resource.create_bucket(Bucket=self._mocked_s3_bucket_name)

        # Create an instance of S3Uploader
        self._s3_uploader = S3Uploader.from_bucket(self._mock_bucket)

    def tearDown(self):
        # Stop the mock S3 service
        self._mock_s3.stop()

    @staticmethod
    def get_file_content(filePath):
        content = ""
        with open(filePath, 'r') as file:
            content = file.read()
        return content

    def test_uploadPerSeqNumPlot(self):
        """Test uploadPerSeqNumPlot for S3 Uploader"""
        channel = random.choice(CHANNELS)
        observationDay = 20241515
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "files/test_file_0001.txt")
        sequenceNumber = 1

        fileContent = TestS3Uploader.get_file_content(filename)
        uploadedFile = self._s3_uploader.uploadPerSeqNumPlot(channel,
                                                             observationDay,
                                                             sequenceNumber,
                                                             filename)

        self.is_correct_check_uploaded_file(uploadedFile, fileContent)

    def test_uploadNightReportData(self):
        """Test uploadNightReportData for S3 Uploader"""
        channel = random.choice(CHANNELS)
        observationDay = 20241515
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "files/test_file_0001.txt")
        plotGroup = "group_01"

        fileContent = TestS3Uploader.get_file_content(filename)
        uploadedFile = self._s3_uploader.uploadNightReportData(channel,
                                                               observationDay,
                                                               filename,
                                                               plotGroup
                                                               )
        self.is_correct_check_uploaded_file(uploadedFile, fileContent)

    def test_uploadMetdata(self):
        """test uploadMetdata method from S3Uploader"""
        channels = ["startracker_metadata",
                    "ts8_metadata",
                    "comcam_metadata",
                    "slac_lsstcam_metadata",
                    "tma_metadata"]
        channel = random.choice(channels)
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "files/test_file_0001.txt")

        fileContent = TestS3Uploader.get_file_content(filename)
        uploadedFile = self._s3_uploader.uploadMetdata(channel, filename)
        self.is_correct_check_uploaded_file(uploadedFile, fileContent)

    def test_uploadMetdata_fails_on_not_metadata_channel(self):
        """test uploadMetdata method from S3Uploader
        when using a not metadata channel causes an exception"""
        channel = "auxtel_monitor"
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "files/test_file_0001.txt")
        with self.assertRaises(ValueError) as context:
            self._s3_uploader.uploadMetdata(channel, filename)
        self.assertEqual(str(context.exception), "Tried to upload non-metadata file to metadata channel:" +
                                                 f"{channel}, {filename}")

    def is_correct_check_uploaded_file(self, uploadedFile, file_content):
        """Support method to check that the file uploaded is correct"""
        # Verify that the file was uploaded to the S3 bucket
        with tempfile.NamedTemporaryFile() as temp_file:
            self._mock_bucket.download_file(uploadedFile, temp_file.name)
            download_content = TestS3Uploader.get_file_content(temp_file.name)
            # Assert that the content of the uploaded file
            # matches the expected content
        self.assertEqual(download_content, file_content)


if __name__ == '__main__':
    unittest.main()
