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
import unittest

from pydantic import ValidationError

import lsst.utils.tests
from lsst.rubintv.production.payloads import Payload, PayloadResult

from .utils import getSampleExpRecord


class TestPayload(unittest.TestCase):
    def setUp(self) -> None:
        # this got harder because we now need a butler as well
        self.expRecord = getSampleExpRecord()
        self.pipelineBytes = "test".encode("utf-8")
        self.differentPipelineBytes = "different test".encode("utf-8")
        self.payload = Payload(
            dataId=self.expRecord.dataId, run="test run", pipelineGraphBytes=self.pipelineBytes
        )
        self.validJson = self.payload.to_json()

    def test_constructor(self) -> None:
        payload = Payload(dataId=self.expRecord.dataId, run="test run", pipelineGraphBytes=self.pipelineBytes)
        self.assertEqual(payload.dataId, self.expRecord.dataId)
        self.assertEqual(payload.pipelineGraphBytes, self.pipelineBytes)

        with self.assertRaises(TypeError):
            payload = Payload(
                dataId=self.expRecord.dataId,
                run="test run",
                pipelineGraphBytes=self.pipelineBytes,
                illegalKwarg="test",
            )

    def test_equality(self) -> None:
        payload1 = Payload(
            dataId=self.expRecord.dataId, run="test run", pipelineGraphBytes=self.pipelineBytes
        )
        payload2 = Payload(
            dataId=self.expRecord.dataId, run="test run", pipelineGraphBytes=self.pipelineBytes
        )
        payloadDiffRun = Payload(
            dataId=self.expRecord.dataId, run="other run", pipelineGraphBytes=self.pipelineBytes
        )
        payloadDiffPipeline = Payload(
            dataId=self.expRecord.dataId, run="test run", pipelineGraphBytes=self.differentPipelineBytes
        )

        self.assertEqual(payload1, payload2)
        self.assertNotEqual(payload1, payloadDiffPipeline)
        self.assertNotEqual(payload1, payloadDiffRun)
        self.assertNotEqual(payload1, payloadDiffPipeline)

    @unittest.skip("Turn these back on if you can work out how to do it without a butler")
    def test_roundtrip(self) -> None:
        payload = Payload.from_json(self.validJson)
        payloadJson = payload.to_json()
        reconstructedPayload = Payload.from_json(payloadJson)
        self.assertEqual(payload, reconstructedPayload)

    @unittest.skip("Turn these back on if you can work out how to do it without a butler")
    def test_from_json(self) -> None:
        payload = Payload.from_json(self.validJson)
        self.assertEqual(payload.expRecord, self.expRecord)
        self.assertEqual(payload.detector, 3)
        self.assertEqual(payload.pipeline, "test")

        json_str = (
            '{"expRecord": '
            + json.dumps(self.expRecord.to_simple().json())
            + ', "detector": 3, "pipeline": "test"}'
        )
        payload = Payload.from_json(json_str)
        self.assertEqual(payload, self.payload)

        json_str = (
            '{"expRecord": '
            + json.dumps(self.expRecord.to_simple().json())
            + ', "detector": 3, "pipeline": "test", "illegalItem": "test"}'
        )
        with self.assertRaises(ValidationError):
            payload = Payload.from_json(json_str)


# @unittest.skip("Turn these back on sometime after OR3")
class TestPayloadResult(unittest.TestCase):
    def setUp(self) -> None:
        self.expRecord = getSampleExpRecord()
        self.pipelineBytes = "test".encode("utf-8")

        self.payload_result = PayloadResult(
            dataId=self.expRecord.dataId,
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            startTime=0.0,
            endTime=1.0,
            splitTimings={"step1": 0.5, "step2": 0.3},
            success=True,
            message="Test message",
        )
        self.validJson = self.payload_result.to_json()

    def test_constructor(self) -> None:
        payload_result = PayloadResult(
            dataId=self.expRecord.dataId,
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            startTime=0.0,
            endTime=1.0,
            splitTimings={"step1": 0.5, "step2": 0.3},
            success=True,
            message="Test message",
        )
        self.assertEqual(payload_result.pipelineGraphBytes, self.pipelineBytes)
        self.assertEqual(payload_result.startTime, 0.0)
        self.assertEqual(payload_result.endTime, 1.0)
        self.assertEqual(payload_result.splitTimings, {"step1": 0.5, "step2": 0.3})
        self.assertEqual(payload_result.success, True)
        self.assertEqual(payload_result.message, "Test message")

        with self.assertRaises(TypeError):
            payload_result = PayloadResult(
                dataId=self.expRecord.dataId,
                run="test run",
                pipelineGraphBytes=self.pipelineBytes,
                startTime=0.0,
                endTime=1.0,
                splitTimings={"step1": 0.5, "step2": 0.3},
                success=True,
                message="Test message",
                illegalKwarg="test",
            )

    @unittest.skip("Turn these back on if you can work out how to do it without a butler")
    def test_roundtrip(self) -> None:
        payload_result = PayloadResult.from_json(self.validJson)
        payload_result_json = payload_result.to_json()
        reconstructed_payload_result = PayloadResult.from_json(payload_result_json)
        self.assertEqual(payload_result, reconstructed_payload_result)

    @unittest.skip("Turn these back on if you can work out how to do it without a butler")
    def test_from_json(self) -> None:
        payload_result = PayloadResult.from_json(self.validJson)
        self.assertEqual(payload_result.pipelineGraphBytes, self.pipelineBytes)
        self.assertEqual(payload_result.startTime, 0.0)
        self.assertEqual(payload_result.endTime, 1.0)
        self.assertEqual(payload_result.splitTimings, {"step1": 0.5, "step2": 0.3})
        self.assertEqual(payload_result.success, True)
        self.assertEqual(payload_result.message, "Test message")

        json_str = (
            '{"expRecord": '
            + json.dumps(self.expRecord.to_simple().json())
            + ', "detector": 3, "pipeline": "test", "startTime": 0.0, "endTime": 1.0, "splitTimings": '
            + '{"step1": 0.5, "step2": 0.3}, "success": true, "message": "Test message"}'
        )
        payload_result = PayloadResult.from_json(json_str)
        self.assertEqual(payload_result, self.payload_result)

        json_str = (
            '{"expRecord": '
            + json.dumps(self.expRecord.to_simple().json())
            + ', "detector": 3, "pipeline": "test", "startTime": 0.0, "endTime": 1.0, "splitTimings": '
            + '{"step1": 0.5, "step2": 0.3}, "success": true, "message": "Test message", "illegalItem": '
            + '"test"}'
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
