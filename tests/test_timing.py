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
import time
import unittest

import lsst.utils.tests
from lsst.rubintv.production.timing import BoxCarTimer


class BoxCarTimerTestCase(lsst.utils.tests.TestCase):
    def test_lap(self) -> None:
        timer = BoxCarTimer(length=5)
        timer.start()
        time.sleep(0.1)
        timer.lap()
        self.assertEqual(len(timer._buffer), 1)
        self.assertAlmostEqual(timer._buffer[0], 0.1, places=1)

    def test_buffer_length(self) -> None:
        timer = BoxCarTimer(length=3)
        timer.start()
        time.sleep(0.1)
        timer.lap()
        time.sleep(0.1)
        timer.lap()
        time.sleep(0.1)
        timer.lap()
        self.assertEqual(len(timer._buffer), 3)

    def test_min(self) -> None:
        timer = BoxCarTimer(length=3)
        timer.start()
        time.sleep(0.1)
        timer.lap()
        time.sleep(0.2)
        timer.lap()
        minValue = timer.min()
        assert minValue is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(minValue, 0.1, places=1)
        minFreq = timer.min(frequency=True)
        assert minFreq is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(minFreq, 10, places=1)

    def test_max(self) -> None:
        timer = BoxCarTimer(length=3)
        timer.start()
        time.sleep(0.1)
        timer.lap()
        time.sleep(0.2)
        timer.lap()
        maxValue = timer.max()
        assert maxValue is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(maxValue, 0.2, places=1)
        maxFreq = timer.max(frequency=True)
        assert maxFreq is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(maxFreq, 5, places=1)

    def test_mean(self) -> None:
        timer = BoxCarTimer(length=3)
        timer.start()
        time.sleep(0.1)
        timer.lap()
        time.sleep(0.2)
        timer.lap()
        time.sleep(0.15)
        timer.lap()
        meanValue = timer.mean()
        assert meanValue is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(meanValue, 0.15, places=1)
        meanFreq = timer.mean(frequency=True)
        assert meanFreq is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(meanFreq, 6.67, places=1)

    def test_median(self) -> None:
        timer = BoxCarTimer(length=5)
        timer.start()
        time.sleep(0.1)
        timer.lap()
        time.sleep(0.2)
        timer.lap()
        time.sleep(0.15)
        timer.lap()
        time.sleep(0.3)
        timer.lap()
        medianValue = timer.median()
        assert medianValue is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(medianValue, 0.175, places=1)
        medianFreq = timer.median(frequency=True)
        assert medianFreq is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(medianFreq, 5.714, places=1)

    def test_extreme_outliers(self) -> None:
        timer = BoxCarTimer(length=5)
        timer.start()
        time.sleep(0.1)
        timer.lap()
        time.sleep(0.1)
        timer.lap()
        time.sleep(0.1)
        timer.lap()
        time.sleep(5)
        timer.lap()
        meanValue = timer.mean()
        assert meanValue is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(meanValue, 1.325, places=1)
        medianValue = timer.median()
        assert medianValue is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(medianValue, 0.1, places=1)

    def test_overflow(self) -> None:
        timer = BoxCarTimer(length=5)
        timer.start()
        time.sleep(1)
        timer.lap()
        for i in range(5):
            time.sleep(0.1)
            timer.lap()
        mean_value = timer.mean()
        assert mean_value is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(mean_value, 0.1, places=1)

    def test_empty_buffer(self) -> None:
        timer = BoxCarTimer(length=3)
        timer.start()  # need to start for min/max etc to work at all
        self.assertIsNone(timer.min())
        self.assertIsNone(timer.max())
        self.assertIsNone(timer.mean())
        self.assertIsNone(timer.median())

    def test_last_lap_time(self) -> None:
        timer = BoxCarTimer(length=5)
        timer.start()
        time.sleep(0.1)
        timer.lap()
        time.sleep(0.2)
        timer.lap()
        last_lap = timer.lastLapTime()
        assert last_lap is not None  # self.assertIsNotNone doesn't work for mypy as it doesn't narrow type
        self.assertAlmostEqual(last_lap, 0.2, places=1)

    def test_pause_resume(self) -> None:
        timer = BoxCarTimer(length=5)
        timer.start()
        time.sleep(0.1)
        timer.pause()
        time.sleep(0.2)
        timer.resume()
        time.sleep(0.1)
        timer.lap()
        self.assertEqual(len(timer._buffer), 1)
        self.assertAlmostEqual(timer._buffer[0], 0.2, places=1)

        with self.assertRaises(RuntimeError):
            timer.pause()
            timer.lap()

    def test_lap_counting(self) -> None:
        timer = BoxCarTimer(length=5)
        timer.start()
        for i in range(3):  # check the basics
            timer.lap()
        self.assertEqual(timer.totalLaps, 3)
        for i in range(10):  # check it works if we overrun the buffer
            timer.lap()
        self.assertEqual(timer.totalLaps, 13)
        timer.pause()
        timer.resume()
        self.assertEqual(timer.totalLaps, 13)  # check pause/resume doesn't add a lap

    def test_not_started(self) -> None:
        timer = BoxCarTimer(length=5)
        with self.assertRaises(RuntimeError):
            timer.lap()
        with self.assertRaises(RuntimeError):
            timer.pause()
        with self.assertRaises(RuntimeError):
            timer.resume()
        with self.assertRaises(RuntimeError):
            timer.min()
        with self.assertRaises(RuntimeError):
            timer.max()
        with self.assertRaises(RuntimeError):
            timer.mean()
        with self.assertRaises(RuntimeError):
            timer.median()
        with self.assertRaises(RuntimeError):
            timer.lastLapTime()


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
