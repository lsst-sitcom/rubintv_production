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
import statistics
import time
from collections import deque

__all__ = ["BoxCarTimer"]


class BoxCarTimer:
    """A box-car averaging lap-timer.

    The `BoxCarTimer` class is used to measure the elapsed time between laps.
    It provides methods to record the elapsed time, pause and resume the timer,
    and calculate various statistics such as minimum, maximum, mean, and median
    elapsed time.

    Parameters
    ----------
    length : `int`
        The number of lap times to store in the buffer. ``None`` can be passed
        for an infinite buffer, but this is not the default to discourage its
        usage as this is expected to be used for long-running processes.

    Raises
    ------
    RuntimeError
        If the timer is a lap is attempted to be recorded while paused.
    """

    def __init__(self, length):
        self._buffer = deque(maxlen=length)
        self.lastTime = None
        self.paused = False
        self.pauseStartTime = None

    def lap(self):
        """Record the elapsed time since the last lap.

        Raises
        ------
        RuntimeError
            If the timer is paused.
        """
        if self.paused:
            raise RuntimeError("Timer is paused. Cannot record lap.")
        currentTime = time.time()
        if self.lastTime is not None:
            elapsed_time = currentTime - self.lastTime
            self._buffer.append(elapsed_time)
        self.lastTime = currentTime

    def pause(self):
        """Pause the timer."""
        if not self.paused:
            self.pauseStartTime = time.time()
            self.paused = True

    def resume(self):
        """Resume the timer."""
        if self.paused:
            pauseDuration = time.time() - self.pauseStartTime
            self.lastTime += pauseDuration
            self.paused = False
            self.pauseStartTime = None

    def min(self, frequency=False):
        """Get the minimum lap time in the buffer.

        Parameters
        ----------
        frequency : bool, optional
            If True, returns the frequency (1 / elapsed time).

        Returns
        -------
        min : `float`
            The minimum elapsed time or its frequency.
        """
        if not self._buffer:
            return None
        minValue = min(self._buffer)
        if frequency:
            return 1 / minValue if minValue != 0 else float("inf")
        return minValue

    def max(self, frequency=False):
        """Get the minimum lap time in the buffer.

        Parameters
        ----------
        frequency : bool, optional
            If True, returns the frequency (1 / elapsed time).

        Returns
        -------
        max : `float`
            The maximum elapsed time or its frequency.
        """
        if not self._buffer:
            return None
        maxValue = max(self._buffer)
        if frequency:
            return 1 / maxValue if maxValue != 0 else float("inf")
        return maxValue

    def mean(self, frequency=False):
        """Get the mean of the lap times in the buffer.

        Parameters
        ----------
        frequency : bool, optional
            If True, returns the frequency (1 / elapsed time).

        Returns
        -------
        mean : `float`
            The mean elapsed time or its frequency.
        """
        if not self._buffer:
            return None
        meanValue = sum(self._buffer) / len(self._buffer)
        if frequency:
            return 1 / meanValue if meanValue != 0 else float("inf")
        return meanValue

    def median(self, frequency=False):
        """Get the median of the lap times in the buffer.

        Parameters
        ----------
        frequency : bool, optional
            If True, returns the frequency (1 / elapsed time).

        Returns
        -------
        median : `float`
            The median elapsed time or its frequency.
        """
        if not self._buffer:
            return None
        medianValue = statistics.median(self._buffer)
        if frequency:
            return 1 / medianValue if medianValue != 0 else float("inf")
        return medianValue

    def lastLapTime(self):
        """Get the time of the previous lap.

        Returns
        -------
        lastLap : `float`
            The elapsed time of the last lap.
        """
        if not self._buffer:
            return None
        return self._buffer[-1]
