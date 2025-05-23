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

from __future__ import annotations

import statistics
import time
from collections import deque
from typing import Deque

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
        Raised if a lap is attempted to be recorded while paused or before it
        is started.
    """

    def __init__(self, length: int):
        self._buffer: Deque[float] = deque(maxlen=length)
        self.lastTime: float | None = None
        self.paused = False
        self.pauseStartTime: float | None = None
        self.totalLaps = 0
        self.started = False

    def start(self) -> None:
        """Start the timer."""
        self.lastTime = time.time()
        self.started = True

    def lap(self) -> None:
        """Record the elapsed time since the last lap.

        Raises
        ------
        RuntimeError
            If the timer is paused or not started.
        """
        if not self.started:
            raise RuntimeError("Timer has not been started. Cannot record lap.")
        if self.paused:
            raise RuntimeError("Timer is paused. Cannot record lap.")
        currentTime = time.time()
        if self.lastTime is not None:
            elapsed_time = currentTime - self.lastTime
            self._buffer.append(elapsed_time)
        self.lastTime = currentTime
        self.totalLaps += 1

    def pause(self) -> None:
        """Pause the timer."""
        if not self.started:
            raise RuntimeError("Timer has not been started. Cannot pause.")
        if not self.paused:
            self.pauseStartTime = time.time()
            self.paused = True

    def resume(self) -> None:
        """Resume the timer."""
        if not self.started:
            raise RuntimeError("Timer has not been started. Cannot resume.")
        if self.paused:
            assert self.pauseStartTime is not None
            pauseDuration = time.time() - self.pauseStartTime
            assert self.lastTime is not None
            self.lastTime += pauseDuration
            self.paused = False
            self.pauseStartTime = None

    def min(self, frequency: bool = False) -> float | None:
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
        if not self.started:
            raise RuntimeError("Timer has not been started. Cannot get minimum.")
        if not self._buffer:
            return None
        minValue = min(self._buffer)
        if frequency:
            return 1 / minValue if minValue != 0 else float("inf")
        return minValue

    def max(self, frequency: bool = False) -> float | None:
        """Get the maximum lap time in the buffer.

        Parameters
        ----------
        frequency : bool, optional
            If True, returns the frequency (1 / elapsed time).

        Returns
        -------
        max : `float`
            The maximum elapsed time or its frequency.
        """
        if not self.started:
            raise RuntimeError("Timer has not been started. Cannot get maximum.")
        if not self._buffer:
            return None
        maxValue = max(self._buffer)
        if frequency:
            return 1 / maxValue if maxValue != 0 else float("inf")
        return maxValue

    def mean(self, frequency: bool = False) -> float | None:
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
        if not self.started:
            raise RuntimeError("Timer has not been started. Cannot get mean.")
        if not self._buffer:
            return None
        meanValue = sum(self._buffer) / len(self._buffer)
        if frequency:
            return 1 / meanValue if meanValue != 0 else float("inf")
        return meanValue

    def median(self, frequency: bool = False) -> float | None:
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
        if not self.started:
            raise RuntimeError("Timer has not been started. Cannot get median.")
        if not self._buffer:
            return None
        medianValue = statistics.median(self._buffer)
        if frequency:
            return 1 / medianValue if medianValue != 0 else float("inf")
        return medianValue

    def lastLapTime(self) -> float | None:
        """Get the time of the previous lap.

        Returns
        -------
        lastLap : `float`
            The elapsed time of the last lap.
        """
        if not self.started:
            raise RuntimeError("Timer has not been started. Cannot get last lap time.")
        if not self._buffer:
            return None
        return self._buffer[-1]
