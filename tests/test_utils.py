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
import os
import tempfile
import unittest

import lsst.utils.tests
from lsst.rubintv.production.utils import isDayObsContiguous, managedTempFile, sanitizeNans


class RubinTVUtilsTestCase(lsst.utils.tests.TestCase):
    """A test case RubinTV utility functions."""

    def test_isDayObsContiguous(self) -> None:
        dayObs = 20220930
        nextDay = 20221001  # next day in a different month
        differentDay = 20221005
        self.assertTrue(isDayObsContiguous(dayObs, nextDay))
        self.assertTrue(isDayObsContiguous(nextDay, dayObs))
        self.assertFalse(isDayObsContiguous(nextDay, differentDay))
        self.assertFalse(isDayObsContiguous(dayObs, dayObs))  # same day

    def test_sanitizeNans(self) -> None:
        self.assertEqual(sanitizeNans({"a": 1.0, "b": float("nan")}), {"a": 1.0, "b": None})
        self.assertEqual(sanitizeNans([1.0, float("nan")]), [1.0, None])
        self.assertIsNone(sanitizeNans(float("nan")))

        # test that a nested dictionary with nan values is sanitized
        nestedDict = {"a": 1.0, "b": {"c": float("nan"), "d": 2.0}}
        result = sanitizeNans(nestedDict)
        self.assertEqual(result["a"], 1.0)
        self.assertEqual(result["b"], {"c": None, "d": 2.0})

        noneKeyedDict = {None: 1.0, "b": {"c": float("nan"), "d": 2.0}}
        self.assertEqual(sanitizeNans(noneKeyedDict), {None: 1.0, "b": {"c": None, "d": 2.0}})


class ManagedTempFileTestCase(lsst.utils.tests.TestCase):
    """Tests for managedTempFile context manager."""

    def setUp(self):
        # Save original environment value if exists
        self.original_ci_env = os.environ.get("RAPID_ANALYSIS_CI", None)

    def tearDown(self):
        # Restore original environment
        if self.original_ci_env is not None:
            os.environ["RAPID_ANALYSIS_CI"] = self.original_ci_env
        elif "RAPID_ANALYSIS_CI" in os.environ:
            del os.environ["RAPID_ANALYSIS_CI"]

    def test_standard_temp_file(self):
        """Test standard operation creates and cleans up temp file."""
        # Ensure CI mode is off
        if "RAPID_ANALYSIS_CI" in os.environ:
            del os.environ["RAPID_ANALYSIS_CI"]

        filename = None
        # Use the context manager
        with managedTempFile(suffix=".txt") as temp_file:
            filename = temp_file
            # File should exist within context
            self.assertTrue(os.path.exists(filename))
            # Write some content
            with open(filename, "w") as f:
                f.write("test content")

        # File should be deleted after context exit
        self.assertFalse(os.path.exists(filename))

    def test_suffix_handling(self):
        """Test that suffix is applied correctly."""
        if "RAPID_ANALYSIS_CI" in os.environ:
            del os.environ["RAPID_ANALYSIS_CI"]

        with managedTempFile(suffix=".test_suffix") as temp_file:
            self.assertTrue(temp_file.endswith(".test_suffix"))

    def test_ci_mode(self):
        """Test CI mode operation."""
        # Set CI mode
        os.environ["RAPID_ANALYSIS_CI"] = "true"

        # Create a test directory that will be cleaned up
        test_dir = tempfile.mkdtemp()
        try:
            test_file = os.path.join(test_dir, "test_ci_file.txt")

            # Use the context manager
            with managedTempFile(suffix=".txt", ciOutputName=test_file) as temp_file:
                self.assertEqual(temp_file, test_file)
                # File should exist
                self.assertTrue(os.path.exists(temp_file))
                # Write some content
                with open(temp_file, "w") as f:
                    f.write("CI test content")

            # File should still exist after context in CI mode
            self.assertTrue(os.path.exists(test_file))

            # Clean up
            os.unlink(test_file)
        finally:
            # Clean up test directory
            if os.path.exists(test_dir):
                os.rmdir(test_dir)

    def test_ci_mode_no_output_name(self):
        """Test CI mode raises ValueError when no output name provided."""
        os.environ["RAPID_ANALYSIS_CI"] = "true"

        with self.assertRaises(ValueError):
            with managedTempFile(suffix=".txt") as _:
                pass

    def test_ci_mode_file_exists(self):
        """Test CI mode raises FileExistsError when file already exists."""
        os.environ["RAPID_ANALYSIS_CI"] = "true"

        # Create a test file
        test_dir = tempfile.mkdtemp()
        try:
            test_file = os.path.join(test_dir, "existing_file.txt")
            with open(test_file, "w") as f:
                f.write("existing content")

            # Should raise FileExistsError
            with self.assertRaises(FileExistsError):
                with managedTempFile(suffix=".txt", ciOutputName=test_file) as _:
                    pass

            # Clean up
            os.unlink(test_file)
        finally:
            # Clean up test directory
            if os.path.exists(test_dir):
                os.rmdir(test_dir)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
