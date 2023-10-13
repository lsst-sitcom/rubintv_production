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

import unittest
import lsst.utils.tests

from lsst.rubintv.production.payloads import (
    Payload,
    Pipeline,
)


class TestPayload(unittest.TestCase):
    def test_from_json(self):
        json_str = '{"dayObs": 1, "seqNum": 2, "detector": 3, "pipeline": "test"}'
        payload = Payload.from_json(json_str)
        self.assertEqual(payload.dayObs, 1)
        self.assertEqual(payload.seqNum, 2)
        self.assertEqual(payload.detector, 3)
        self.assertEqual(payload.pipeline, 'test')

        json_str = '{"dayObs": 1, "seqNum": 2, "detector": 3, "illegalItem": "test"}'
        with self.assertRaises(TypeError):
            payload = Payload.from_json(json_str)
            payload = Payload(dayObs=1, seqNum=2, detector=3, illegalKwarg='test')

    def test_to_json(self):
        payload = Payload(dayObs=1, seqNum=2, detector=3, pipeline='test')
        json_str = payload.to_json()
        expected_json_str = '{"dayObs": 1, "seqNum": 2, "detector": 3, "pipeline": "test"}'
        self.assertEqual(json_str, expected_json_str)


class TestPipeline(unittest.TestCase):
    def test_pipeline(self):
        pipeline = Pipeline(pipeline='test')
        self.assertEqual(pipeline.pipeline, 'test')


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
