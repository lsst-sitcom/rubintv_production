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
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor, PodType


class PodDefinitionTestCase(lsst.utils.tests.TestCase):

    def test_podDefinition(self):
        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=94, depth=0)
        self.assertIsInstance(pod, PodDetails)
        expectedQueueName = "SFM_WORKER-LSSTCam-000-094"
        self.assertEqual(pod.queueName, expectedQueueName)

        newPod = PodDetails.fromQueueName(expectedQueueName)
        self.assertIsInstance(pod, PodDetails)
        self.assertEqual(pod.queueName, expectedQueueName)
        self.assertEqual(pod, newPod)

    def test_headNodeInit(self):
        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=None)
        self.assertIsInstance(pod, PodDetails)

        # do we actually want to check this in the tests?
        expectedQueueName = "HEAD_NODE-LSSTCam"
        self.assertEqual(pod.queueName, expectedQueueName)

        with self.assertRaises(ValueError):
            pod = PodDetails(
                instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=1, depth=None
            )
        with self.assertRaises(ValueError):
            pod = PodDetails(
                instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=2
            )
        with self.assertRaises(ValueError):
            pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=3, depth=4)

    def test_perFocalPlaneType(self):
        for podFlavor in [
            PodFlavor.PSF_PLOTTER,
            PodFlavor.NIGHTLYROLLUP_WORKER,
            PodFlavor.STEP2A_WORKER,
            PodFlavor.MOSAIC_WORKER,
        ]:
            pod = PodDetails(instrument="LSSTCam", podFlavor=podFlavor, detectorNumber=None, depth=0)
            self.assertIsInstance(pod, PodDetails)

            with self.assertRaises(ValueError):
                pod = PodDetails(instrument="LSSTCam", podFlavor=podFlavor, detectorNumber=1, depth=None)
            with self.assertRaises(ValueError):
                pod = PodDetails(instrument="LSSTCam", podFlavor=podFlavor, detectorNumber=3, depth=4)

    def test_valid_init_per_detector(self):
        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=94, depth=2)
        self.assertIsInstance(pod, PodDetails)
        self.assertEqual(pod.podType, PodType.PER_DETECTOR)

    def test_invalid_init_per_detector(self):
        with self.assertRaises(ValueError):
            PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=None, depth=0)
        with self.assertRaises(ValueError):
            PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=94, depth=None)

    def test_valid_init_per_instrument_singleton(self):
        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=None)
        self.assertEqual(pod.podType, PodType.PER_INSTRUMENT_SINGLETON)

    def test_invalid_init_per_instrument_singleton(self):
        with self.assertRaises(ValueError):
            PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=1, depth=None)
        with self.assertRaises(ValueError):
            PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=2)
        with self.assertRaises(ValueError):
            PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=3, depth=4)

    def test_valid_init_per_instrument(self):
        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.PSF_PLOTTER, detectorNumber=None, depth=12)
        self.assertEqual(pod.podType, PodType.PER_INSTRUMENT)

    def test_invalid_init_per_instrument(self):
        with self.assertRaises(ValueError):
            PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.PSF_PLOTTER, detectorNumber=1, depth=None)
        with self.assertRaises(ValueError):
            PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.PSF_PLOTTER, detectorNumber=None, depth=None)
        with self.assertRaises(ValueError):
            PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.PSF_PLOTTER, detectorNumber=3, depth=4)

    def test_fromQueueName(self):
        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=94, depth=0)
        newPod = PodDetails.fromQueueName(pod.queueName)
        self.assertEqual(pod, newPod)

        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=None)
        newPod = PodDetails.fromQueueName(pod.queueName)
        self.assertEqual(pod, newPod)

        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.PSF_PLOTTER, detectorNumber=None, depth=12)
        newPod = PodDetails.fromQueueName(pod.queueName)
        self.assertEqual(pod, newPod)

        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=94, depth=2)
        newPod = PodDetails.fromQueueName(pod.queueName)
        self.assertEqual(pod, newPod)

        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=None)
        newPod = PodDetails.fromQueueName(pod.queueName)
        self.assertEqual(pod, newPod)

        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.PSF_PLOTTER, detectorNumber=None, depth=12)
        newPod = PodDetails.fromQueueName(pod.queueName)
        self.assertEqual(pod, newPod)

    def test_fromQueueName_failing(self):
        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=None)
        queueName = pod.queueName
        queueName += "-010"
        with self.assertRaises(ValueError):
            PodDetails.fromQueueName(queueName)

        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.PSF_PLOTTER, detectorNumber=None, depth=0)
        queueName = pod.queueName
        queueName += "-010"
        with self.assertRaises(ValueError):
            PodDetails.fromQueueName(queueName)

        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.PSF_PLOTTER, detectorNumber=None, depth=0)
        queueName = pod.queueName
        newName = queueName.replace("-000", "")
        self.assertLess(len(newName), len(queueName))  # just check we actually removed something
        with self.assertRaises(ValueError):
            PodDetails.fromQueueName(newName)

        pod = PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=12, depth=34)
        queueName = pod.queueName
        newName = queueName.replace("-012", "")
        self.assertLess(len(newName), len(queueName))  # just check we actually removed something
        with self.assertRaises(ValueError):
            PodDetails.fromQueueName(newName)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
