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
import json
import os
import tempfile
import unittest

import numpy as np

import lsst.utils.tests
from lsst.rubintv.production.timedServices import TimedMetadataServer
from lsst.rubintv.production.utils import writeMetadataShard


class TimedMetadataServerTestCase(lsst.utils.tests.TestCase):
    """Tests for the TimedMetadataServer shard merging and sanitization."""

    def test_mergeShardsAndUpload_sanitizes_and_preserves_structure(self) -> None:
        # real shard files written to temporary directories
        with tempfile.TemporaryDirectory() as tempRoot:
            metadataDir = os.path.join(tempRoot, "metadata")
            shardsDir = os.path.join(metadataDir, "shards")
            os.makedirs(shardsDir, exist_ok=True)

            dayObs = 19700101
            seqNum = 123

            # Shard 1 with NaNs, floats, floats-as-strings, and nested dict
            shard1 = {
                seqNum: {
                    "floatVal": 1.234,
                    "nanVal": float("nan"),
                    "numpyNana": np.nan,
                    "stringFloat": "3.1415",
                    "stringString": "not a float",
                    "nested": {
                        "innerFloat": np.float64(2.5),
                        "innerNan": np.nan,
                        "innerStringFloat": "6.28",
                        "innerStringString": "also not a float",
                    },
                }
            }
            writeMetadataShard(shardsDir, dayObs, shard1)

            # Shard 2 updates (tests deep_update on nested dict and overwrite
            # of stringFloat)
            shard2 = {
                seqNum: {
                    "stringFloat2": "2.71",
                    "nested": {
                        "another": 7.0,
                    },
                }
            }
            writeMetadataShard(shardsDir, dayObs, shard2)

            # A second seqNum to ensure structure across multiple rows
            seqNum2 = 124
            shard3 = {
                seqNum2: {
                    "floatVal": 9.0,
                    "stringFloatSci": "1.0e-3",
                    "nanVal": np.nan,
                    "nested": {
                        "innerStringFloatSci": "2E2",
                        "innerNan": float("nan"),
                    },
                }
            }
            writeMetadataShard(shardsDir, dayObs, shard3)

            # Build server with a no-op uploader
            server = TimedMetadataServer(
                locationConfig=None,  # type: ignore[arg-type]
                metadataDirectory=metadataDir,
                shardsDirectory=shardsDir,
                channelName="test_metadata",
                doRaise=True,
            )

            class _NoopUploader:
                def uploadMetdata(self, *args, **kwargs):
                    return

            server.s3Uploader = _NoopUploader()

            # Execute merge
            server.mergeShardsAndUpload()

            # Validate merged output
            outfile = os.path.join(metadataDir, f"dayObs_{dayObs}.json")
            self.assertTrue(os.path.isfile(outfile))
            with open(outfile) as f:
                merged = json.load(f)

            # Keys are JSON object keys; ensure data exists for each seqNum
            self.assertIn(str(seqNum), merged)
            self.assertIn(str(seqNum2), merged)

            row1 = merged[str(seqNum)]
            row2 = merged[str(seqNum2)]

            # Rows should be dicts (integer-keyed dict in JSON context)
            self.assertIsInstance(row1, dict)
            self.assertIsInstance(row2, dict)

            # No NaNs: NaNs should be converted to None
            self.assertIn("nanVal", row1)
            self.assertIsNone(row1["nanVal"])
            self.assertIn("nanVal", row2)
            self.assertIsNone(row2["nanVal"])

            # Numeric strings should be converted to floats
            self.assertIn("stringFloat", row1)
            self.assertIsInstance(row1["stringFloat"], float)
            self.assertAlmostEqual(row1["stringFloat"], 3.1415, places=6)
            self.assertIn("stringFloat2", row1)
            self.assertIsInstance(row1["stringFloat2"], float)
            self.assertAlmostEqual(row1["stringFloat2"], 2.71, places=6)

            self.assertIn("stringFloatSci", row2)
            self.assertIsInstance(row2["stringFloatSci"], float)
            self.assertAlmostEqual(row2["stringFloatSci"], 1.0e-3, places=12)

            # Nested structure preserved, NaNs sanitized, strings converted to
            # floats
            self.assertIn("nested", row1)
            nested1 = row1["nested"]
            self.assertIsInstance(nested1, dict)
            self.assertIn("innerFloat", nested1)
            self.assertIsInstance(nested1["innerFloat"], float)
            self.assertAlmostEqual(nested1["innerFloat"], 2.5, places=6)
            self.assertIn("innerNan", nested1)
            self.assertIsNone(nested1["innerNan"])
            self.assertIn("innerStringFloat", nested1)
            self.assertIsInstance(nested1["innerStringFloat"], float)
            self.assertAlmostEqual(nested1["innerStringFloat"], 6.28, places=6)
            self.assertIn("another", nested1)
            self.assertIsInstance(nested1["another"], float)

            nested2 = row2["nested"]
            self.assertIsInstance(nested2, dict)
            self.assertIn("innerStringFloatSci", nested2)
            self.assertIsInstance(nested2["innerStringFloatSci"], float)
            self.assertAlmostEqual(nested2["innerStringFloatSci"], 200.0, places=6)
            self.assertIn("innerNan", nested2)
            self.assertIsNone(nested2["innerNan"])

            # check string values are preserved
            self.assertIn("stringString", row1)
            self.assertIsInstance(row1["stringString"], str)
            self.assertEqual(row1["stringString"], "not a float")
            self.assertIn("innerStringString", nested1)
            self.assertIsInstance(nested1["innerStringString"], str)
            self.assertEqual(nested1["innerStringString"], "also not a float")


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
