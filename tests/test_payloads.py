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

import lsst.daf.butler as dafButler
import lsst.utils.tests
from lsst.rubintv.production.payloads import Payload, PayloadResult
from lsst.summit.utils.utils import getSite

from .utils import getSampleExpRecord

NO_BUTLER = True
if getSite() in ["staff-rsp", "rubin-devl"]:
    NO_BUTLER = False


class TestPayload(unittest.TestCase):
    def setUp(self) -> None:
        self.butler = None
        if getSite() in ["staff-rsp", "rubin-devl"]:
            self.butler = dafButler.Butler("embargo_old", instrument="LATISS")  # type: ignore

        # this got harder because we now need a butler as well
        self.expRecord = getSampleExpRecord()
        self.expRecord2 = getSampleExpRecord()  # TODO get a different expRecord
        self.pipelineBytes = "test".encode("utf-8")
        self.differentPipelineBytes = "different test".encode("utf-8")
        self.payload = Payload(
            dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            who="SFM",
        )
        self.validJson = self.payload.to_json()

    def test_constructor(self) -> None:
        payload = Payload(
            dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            who="SFM",
        )
        self.assertEqual(payload.dataIds, [self.expRecord.dataId, self.expRecord2.dataId])
        self.assertEqual(payload.pipelineGraphBytes, self.pipelineBytes)

        with self.assertRaises(TypeError):
            payload = Payload(
                dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
                run="test run",
                pipelineGraphBytes=self.pipelineBytes,
                who="SFM",
                illegalKwarg="test",  # type: ignore[call-arg]  # that's the whole point here
            )

    def test_equality(self) -> None:
        payload1 = Payload(
            dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
            run="test run",
            who="SFM",
            pipelineGraphBytes=self.pipelineBytes,
        )
        payload2 = Payload(
            dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
            run="test run",
            who="SFM",
            pipelineGraphBytes=self.pipelineBytes,
        )
        payloadDiffRun = Payload(
            dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
            run="other run",
            who="SFM",
            pipelineGraphBytes=self.pipelineBytes,
        )
        payloadDiffPipeline = Payload(
            dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
            run="test run",
            who="SFM",
            pipelineGraphBytes=self.differentPipelineBytes,
        )

        self.assertEqual(payload1, payload2)
        self.assertNotEqual(payload1, payloadDiffPipeline)
        self.assertNotEqual(payload1, payloadDiffRun)
        self.assertNotEqual(payload1, payloadDiffPipeline)

    @unittest.skipIf(NO_BUTLER, "Skipping butler-driven tests")
    def test_roundtrip(self) -> None:
        # remove the ignore[arg-type] everywhere once there is a butler
        payload = Payload.from_json(self.validJson, self.butler)  # type: ignore[arg-type]
        payloadJson = payload.to_json()
        reconstructedPayload = Payload.from_json(payloadJson, self.butler)  # type: ignore[arg-type]
        self.assertEqual(payload, reconstructedPayload)

    @unittest.skipIf(NO_BUTLER, "Skipping butler-driven tests")
    def test_from_json(self) -> None:
        # remove the ignore[arg-type] everywhere once there is a butler
        payload = Payload.from_json(self.validJson, self.butler)  # type: ignore[arg-type]
        self.assertEqual(payload.dataIds, [self.expRecord.dataId, self.expRecord2.dataId])
        self.assertEqual(payload.pipelineGraphBytes, self.pipelineBytes)

        json_str = (
            '{"expRecord": '
            + json.dumps(self.expRecord.to_simple().json())
            + ', "detector": 3, "pipeline": "test"}'
        )
        payload = Payload.from_json(json_str, self.butler)  # type: ignore[arg-type]
        self.assertEqual(payload, self.payload)

        json_str = (
            '{"expRecord": '
            + json.dumps(self.expRecord.to_simple().json())
            + ', "detector": 3, "pipeline": "test", "illegalItem": "test"}'
        )
        with self.assertRaises(ValidationError):
            payload = Payload.from_json(json_str, self.butler)  # type: ignore[arg-type]


class TestPayloadResult(unittest.TestCase):
    def setUp(self) -> None:
        self.butler = None
        if getSite() in ["staff-rsp", "rubin-devl"]:
            self.butler = dafButler.Butler("embargo_old", instrument="LATISS")  # type: ignore

        self.expRecord = getSampleExpRecord()
        self.expRecord2 = getSampleExpRecord()  # TODO get a different expRecord
        self.pipelineBytes = "test".encode("utf-8")

        self.payload_result = PayloadResult(
            dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            who="SFM",
            startTime=0.0,
            endTime=1.0,
            splitTimings={"step1": 0.5, "step2": 0.3},
            success=True,
            message="Test message",
        )
        self.validJson = self.payload_result.to_json()

    def test_constructor(self) -> None:
        payload_result = PayloadResult(
            dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            who="SFM",
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
                dataIds=[self.expRecord.dataId, self.expRecord2.dataId],
                run="test run",
                pipelineGraphBytes=self.pipelineBytes,
                who="SFM",
                startTime=0.0,
                endTime=1.0,
                splitTimings={"step1": 0.5, "step2": 0.3},
                success=True,
                message="Test message",
                illegalKwarg="test",  # type: ignore[call-arg]  # that's the whole point here
            )

    @unittest.skipIf(NO_BUTLER, "Skipping butler-driven tests")
    def test_roundtrip(self) -> None:
        # remove the ignore[arg-type] everywhere once there is a butler
        payload_result = PayloadResult.from_json(self.validJson, self.butler)  # type: ignore[arg-type]
        payload_result_json = payload_result.to_json()
        reconstructed_payload_result = PayloadResult.from_json(
            payload_result_json, self.butler  # type: ignore[arg-type]
        )
        self.assertEqual(payload_result, reconstructed_payload_result)

    @unittest.skipIf(NO_BUTLER, "Skipping butler-driven tests")
    def test_from_json(self) -> None:
        # remove the ignore[arg-type] everywhere once there is a butler
        payload_result = PayloadResult.from_json(self.validJson, self.butler)  # type: ignore[arg-type]
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
        payload_result = PayloadResult.from_json(json_str, self.butler)  # type: ignore[arg-type]
        self.assertEqual(payload_result, self.payload_result)

        json_str = (
            '{"expRecord": '
            + json.dumps(self.expRecord.to_simple().json())
            + ', "detector": 3, "pipeline": "test", "startTime": 0.0, "endTime": 1.0, "splitTimings": '
            + '{"step1": 0.5, "step2": 0.3}, "success": true, "message": "Test message", "illegalItem": '
            + '"test"}'
        )
        with self.assertRaises(TypeError):
            payload_result = PayloadResult.from_json(json_str, self.butler)  # type: ignore[arg-type]


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
