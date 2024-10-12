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

"""Test cases for utils."""
import unittest

import lsst.utils.tests
from lsst.rubintv.production.podDefinition import PodDetails, PodType


class PodDefinitionTestCase(lsst.utils.tests.TestCase):

    def test_podDefinition(self):
        pod = PodDetails(instrument="LSSTCam", podType=PodType.SFM_WORKER, detectorNumber=94, depth=0)
        self.assertIsInstance(pod, PodDetails)
        expectedQueueName = "LSSTCam-SFM_WORKER-094-000"
        self.assertEqual(pod.queueName, expectedQueueName)

        newPod = PodDetails.fromQueueName(expectedQueueName)
        self.assertIsInstance(pod, PodDetails)
        self.assertEqual(pod.queueName, expectedQueueName)
        self.assertEqual(pod, newPod)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
