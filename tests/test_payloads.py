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

import copy
import unittest
import lsst.utils.tests
import json

from utils import getSampleExpRecord

from lsst.rubintv.production.payloads import (
    Payload,
    PayloadResult,
)


@unittest.skip('Turn these back on sometime after OR3')
class TestPayload(unittest.TestCase):
    def setUp(self):
        # this got harder because we now need a butler as well
        self.expRecord = getSampleExpRecord()
        self.payload = Payload(expRecord=self.expRecord, detector=3, pipeline="test")
        self.validJson = self.payload.to_json()
        test = Payload.from_json(self.validJson)  # check the validJson is actually valid
        self.assertIsInstance(test, Payload)

    def test_constructor(self):
        payload = Payload(expRecord=self.expRecord, detector=3, pipeline="test")
        self.assertEqual(payload.expRecord, self.expRecord)
        self.assertEqual(payload.detector, 3)
        self.assertEqual(payload.pipeline, "test")

        with self.assertRaises(TypeError):
            payload = Payload(
                expRecord=self.expRecord,
                detector=3,
                pipeline="test",
                illegalKwarg="test",
            )

    def test_equality(self):
        payload1 = Payload(expRecord=self.expRecord, detector=3, pipeline="test")
        payload2 = Payload(expRecord=self.expRecord, detector=3, pipeline="test")
        payloadDiffPipeline = Payload(
            expRecord=self.expRecord, detector=3, pipeline="diff_pipeline"
        )
        payloadDiffDetector = Payload(
            expRecord=self.expRecord, detector=123, pipeline="test"
        )

        diffExpRecord = copy.deepcopy(self.expRecord)
        object.__setattr__(
            diffExpRecord, "dataId", {"instrument": ""}
        )  # mutate expRecord to make it different
        payloadDiffExpRecord = Payload(
            expRecord=diffExpRecord, detector=3, pipeline="test"
        )

        self.assertEqual(payload1, payload2)
        self.assertNotEqual(payload1, payloadDiffPipeline)
        self.assertNotEqual(payload1, payloadDiffDetector)
        self.assertNotEqual(payload1, payloadDiffExpRecord)

    def test_roundtrip(self):
        payload = Payload.from_json(self.validJson)
        payloadJson = payload.to_json()
        reconstructedPayload = Payload.from_json(payloadJson)
        self.assertEqual(payload, reconstructedPayload)

    def test_from_json(self):
        payload = Payload.from_json(self.validJson)
        self.assertEqual(payload.expRecord, self.expRecord)
        self.assertEqual(payload.detector, 3)
        self.assertEqual(payload.pipeline, "test")

        json_str = (
            '{"expRecord": ' +
            json.dumps(self.expRecord.to_simple().json()) +
            ', "detector": 3, "pipeline": "test"}'
        )
        payload = Payload.from_json(json_str)
        self.assertEqual(payload, self.payload)

        json_str = (
            '{"expRecord": ' +
            json.dumps(self.expRecord.to_simple().json()) +
            ', "detector": 3, "pipeline": "test", "illegalItem": "test"}'
        )
        with self.assertRaises(TypeError):
            payload = Payload.from_json(json_str)


@unittest.skip('Turn these back on sometime after OR3')
class TestPayloadResult(unittest.TestCase):
    def setUp(self):
        self.expRecord = getSampleExpRecord()
        self.payload_result = PayloadResult(
            expRecord=self.expRecord,
            detector=3,
            pipeline="test",
            startTime=0.0,
            endTime=1.0,
            splitTimings={"step1": 0.5, "step2": 0.3},
            success=True,
            message="Test message",
        )
        self.validJson = self.payload_result.to_json()
        test = PayloadResult.from_json(self.validJson)  # check the validJson is actually valid
        self.assertIsInstance(test, PayloadResult)

    def test_constructor(self):
        payload_result = PayloadResult(
            expRecord=self.expRecord,
            detector=3,
            pipeline="test",
            startTime=0.0,
            endTime=1.0,
            splitTimings={"step1": 0.5, "step2": 0.3},
            success=True,
            message="Test message",
        )
        self.assertEqual(payload_result.detector, 3)
        self.assertEqual(payload_result.pipeline, "test")
        self.assertEqual(payload_result.startTime, 0.0)
        self.assertEqual(payload_result.endTime, 1.0)
        self.assertEqual(payload_result.splitTimings, {"step1": 0.5, "step2": 0.3})
        self.assertEqual(payload_result.success, True)
        self.assertEqual(payload_result.message, "Test message")

        with self.assertRaises(TypeError):
            payload_result = PayloadResult(
                expRecord=self.expRecord,
                detector=3,
                pipeline="test",
                startTime=0.0,
                endTime=1.0,
                splitTimings={"step1": 0.5, "step2": 0.3},
                success=True,
                message="Test message",
                illegalKwarg="test",
            )

    def test_roundtrip(self):
        payload_result = PayloadResult.from_json(self.validJson)
        payload_result_json = payload_result.to_json()
        reconstructed_payload_result = PayloadResult.from_json(payload_result_json)
        self.assertEqual(payload_result, reconstructed_payload_result)

    def test_from_json(self):
        payload_result = PayloadResult.from_json(self.validJson)
        self.assertEqual(payload_result.detector, 3)
        self.assertEqual(payload_result.pipeline, "test")
        self.assertEqual(payload_result.startTime, 0.0)
        self.assertEqual(payload_result.endTime, 1.0)
        self.assertEqual(payload_result.splitTimings, {"step1": 0.5, "step2": 0.3})
        self.assertEqual(payload_result.success, True)
        self.assertEqual(payload_result.message, "Test message")

        json_str = (
            '{"expRecord": ' +
            json.dumps(self.expRecord.to_simple().json()) +
            ', "detector": 3, "pipeline": "test", "startTime": 0.0, "endTime": 1.0, "splitTimings": ' +
            '{"step1": 0.5, "step2": 0.3}, "success": true, "message": "Test message"}'
        )
        payload_result = PayloadResult.from_json(json_str)
        self.assertEqual(payload_result, self.payload_result)

        json_str = (
            '{"expRecord": ' +
            json.dumps(self.expRecord.to_simple().json()) +
            ', "detector": 3, "pipeline": "test", "startTime": 0.0, "endTime": 1.0, "splitTimings": ' +
            '{"step1": 0.5, "step2": 0.3}, "success": true, "message": "Test message", "illegalItem": ' +
            '"test"}'
        )
        with self.assertRaises(TypeError):
            payload_result = PayloadResult.from_json(json_str)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
