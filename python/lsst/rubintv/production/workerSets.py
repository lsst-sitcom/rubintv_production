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

__all__ = ["SfmWorkerSet", "Step1bWorkerSet", "AosWorkerSet"]

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Sequence

from .podDefinition import PodDetails, PodFlavor
from .utils import mapAosWorkerNumber

if TYPE_CHECKING:
    from .clusterManagement import ClusterStatus, WorkerStatus


@dataclass
class WorkerSet:
    """Base class for sets of worker pods."""

    instrument: str
    podFlavor: PodFlavor
    pods: list[PodDetails]
    name: str
    log: logging.Logger = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        self.log = logging.getLogger(__name__)
        for pod in self.pods:
            if pod.instrument != self.instrument:
                raise ValueError(f"Pod {pod} does not match {self.instrument=} - sets can't be mixed")
            if pod.podFlavor != self.podFlavor:
                raise ValueError(f"Pod {pod} does not match {self.podFlavor=} - sets can't be mixed")

    def getWorkerStatuses(self, clusterStatus: ClusterStatus) -> list[WorkerStatus]:
        """Get the worker statuses for this set of pods."""
        fs = clusterStatus.flavorStatuses[self.podFlavor]
        return [workerStatus for workerStatus in fs.workerStatuses if workerStatus.worker in self.pods]

    def allFree(self, clusterStatus: ClusterStatus) -> bool:
        """Check if all workers in this set are free."""
        if not self.allExist(clusterStatus):
            nMissing = len(self.getMissingPods(clusterStatus))
            self.log.warning(
                f"Not all pods in {self.name} exist in the cluster ({nMissing} missing) - "
                f"call getMissingPods(clusterStatus) to see details."
            )
            return False

        for workerStatus in self.getWorkerStatuses(clusterStatus):
            if workerStatus.isBusy:
                return False
        return True

    def allBusy(self, clusterStatus: ClusterStatus) -> bool:
        """Check if all workers in this set are busy."""
        if not self.allExist(clusterStatus):
            nMissing = len(self.getMissingPods(clusterStatus))
            self.log.warning(
                f"Not all pods in {self.name} exist in the cluster ({nMissing} missing) - "
                f"call getMissingPods(clusterStatus) to see details."
            )
            return False

        for workerStatus in self.getWorkerStatuses(clusterStatus):
            if not workerStatus.isBusy:
                return False
        return True

    def maxQueueLength(self, clusterStatus: ClusterStatus) -> int:
        """Get the maximum queue length of all workers in this set."""
        maxLength = 0
        for workerStatus in self.getWorkerStatuses(clusterStatus):
            maxLength = max(maxLength, workerStatus.queueLength)
        return maxLength

    def minQueueLength(self, clusterStatus: ClusterStatus) -> int:
        """Get the minimum queue length of all workers in this set."""
        minLength = 9999999
        for workerStatus in self.getWorkerStatuses(clusterStatus):
            minLength = min(minLength, workerStatus.queueLength)
        return minLength

    def getMissingPods(self, clusterStatus: ClusterStatus) -> list[PodDetails]:
        """Find pods in this set that are missing from the cluster status."""
        # NBL do not use getWorkerStatuses() here as we need check the
        # clusterStatus directly to see if the pod is missing
        flavorStatus = clusterStatus.flavorStatuses[self.podFlavor]

        missingPods = []
        for pod in self.pods:
            if pod not in flavorStatus.workers:
                missingPods.append(pod)
        return missingPods

    def allExist(self, clusterStatus: ClusterStatus) -> bool:
        """Check if all workers in this set exist."""
        return len(self.getMissingPods(clusterStatus)) == 0

    def totalQueuedItems(self, clusterStatus: ClusterStatus) -> int:
        """Get the total queue length of all workers in this set."""
        flavorStatus = clusterStatus.flavorStatuses[self.podFlavor]
        totalLength = 0
        for workerStatus in flavorStatus.workerStatuses:
            if workerStatus.worker in self.pods:
                totalLength += workerStatus.queueLength
        return totalLength

    def nFreeWorkers(self, clusterStatus: ClusterStatus) -> int:
        """Get the number of free workers in this set."""
        nFree = 0
        for workerStatus in self.getWorkerStatuses(clusterStatus):
            if not workerStatus.isBusy:
                nFree += 1
        return nFree

    def nWorkers(self) -> int:
        """Get the number of workers in this set."""
        return len(self.pods)


@dataclass
class SfmWorkerSet(WorkerSet):
    """A set of SFM worker pods."""

    @classmethod
    def create(cls, instrument: str, depth: int) -> SfmWorkerSet:
        """Create a set of SFM workers for all detectors at a given depth."""
        podFlavor = PodFlavor.SFM_WORKER
        pods = [PodDetails(instrument, podFlavor, detectorNumber=d, depth=depth) for d in range(0, 189)]
        name = f"SFM Set {depth + 1}"
        return cls(instrument=instrument, podFlavor=podFlavor, pods=pods, name=name)


@dataclass
class Step1bWorkerSet(WorkerSet):
    """A set of Step1b worker pods."""

    @classmethod
    def create(cls, instrument: str, podFlavor: PodFlavor, count: int) -> Step1bWorkerSet:
        """Create a set of Step1b workers."""
        pods = [PodDetails(instrument, podFlavor, detectorNumber=None, depth=d) for d in range(count)]
        name = f"Step1b {podFlavor.name} Set"
        return cls(instrument=instrument, podFlavor=podFlavor, pods=pods, name=name)


@dataclass
class AosWorkerSet(WorkerSet):
    """A set of AOS worker pods."""

    @classmethod
    def create(cls, instrument: str, workerRange: Sequence[int]) -> AosWorkerSet:
        """Create a set of AOS workers for a range of worker numbers."""
        pods = []
        podFlavor = PodFlavor.AOS_WORKER
        for workerNum in workerRange:
            depth, detNum = mapAosWorkerNumber(workerNum)
            pods.append(PodDetails(instrument, podFlavor, detectorNumber=detNum, depth=depth))
        name = f"AOS Set {1 + workerRange[0] // 8}"
        return cls(instrument=instrument, podFlavor=podFlavor, pods=pods, name=name)


@dataclass
class BacklogWorkerSet(WorkerSet):
    """A set of Step1b worker pods."""

    @classmethod
    def create(cls, instrument: str, count: int) -> BacklogWorkerSet:
        """Create a set of backlog workers."""
        podFlavor = PodFlavor.BACKLOG_WORKER
        pods = [PodDetails(instrument, podFlavor, detectorNumber=None, depth=d) for d in range(count)]
        name = "Backlog Set"
        return cls(instrument=instrument, podFlavor=podFlavor, pods=pods, name=name)
