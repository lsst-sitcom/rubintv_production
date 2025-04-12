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

__all__ = [
    "PerformanceBrowser",
]

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

# TODO Change these back to relative imports
from lsst.rubintv.production.processingControl import PipelineComponents, buildPipelines
from lsst.rubintv.production.utils import LocationConfig
from lsst.summit.utils.utils import getCameraFromInstrumentName

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, ButlerLogRecords, DimensionRecord
    from lsst.pipe.base.pipeline_graph import TaskNode


def isVisitType(task: TaskNode) -> bool:
    return "visit" in task.dimensions


def isExposureType(task: TaskNode) -> bool:
    return "exposure" in task.dimensions


def isDetectorLevel(task: TaskNode) -> bool:
    return "detector" in task.dimensions


def isFocalPlaneLevel(task: TaskNode) -> bool:
    return not isDetectorLevel(task)


def getFail(log: ButlerLogRecords) -> str | None:
    for line in log:
        if line.levelname == "ERROR":
            return line.message
    return None


def getExpRecord(butler: Butler, dayObs: int, seqNum: int) -> DimensionRecord | None:
    try:
        (expRecord,) = butler.registry.queryDimensionRecords(
            "exposure", where=f"exposure.day_obs={dayObs} and exposure.seq_num={seqNum}"
        )
        return expRecord
    except Exception:  # XXX make this a little less broad
        return None


def getVisitRecord(butler: Butler, expRecord: DimensionRecord) -> DimensionRecord | None:
    try:
        (visitRecord,) = butler.registry.queryDimensionRecords("visit", where=f"visit={expRecord.id}")
        return visitRecord
    except Exception:  # XXX make this a little less broad
        return None


def makeWhere(task: TaskNode, record: DimensionRecord) -> str:
    isVisit = isExposureType(task)

    if isVisit == "isr":
        return f"visit={record.id}"
    else:
        return f"exposure={record.id}"


def getTaskTime(logs: ButlerLogRecords, method="first-last") -> float:
    if method == "first-last":
        return (logs[-1].asctime - logs[0].asctime).total_seconds()
    elif method == "parse":
        message = logs[-1].message
        match = re.search(r"\btook\s+(\d+\.\d+)", message)
        if match:
            return float(match.group(1))
        else:
            raise ValueError(f"Failed to parse log line: {message}")
    else:
        raise ValueError(f"Unknown getTaskTime option {method=}")


@dataclass
class TaskResult:
    """Details about task performance.

    Parameters
    ----------
    task : `TaskNode`
        The task node.
    taskName : `str`
        The name of the task.
    detectors : `list[int]`
        The list of detectors for which the task ran.
    detectorTimings : `dict[int, float]`
        The timings for each detector on the task.
    failures : `dict[int, str]`
        The failures for each detector, if present.
    """

    record: DimensionRecord
    task: TaskNode
    taskName: str
    detectors: list[int | None]
    detectorTimings: dict[int | None, float]
    failures: dict[int | None, str]
    logs: dict[int | None, ButlerLogRecords]

    def __init__(self, butler: Butler, record: DimensionRecord, task: TaskNode, debug: bool = False) -> None:
        self.record = record
        self.task = task
        self.taskName = task.label

        self.detectors: list[int | None] = []
        self.logs: dict[int | None, ButlerLogRecords] = {}
        self.detectorTimings: dict[int | None, float] = {}
        self.failures: dict[int | None, str] = {}
        self.logs: dict[int | None, ButlerLogRecords] = {}

        where = makeWhere(task, record)
        dRefs = list(
            butler.registry.queryDatasets(
                f"{self.taskName}_log",
                findFirst=True,
                where=where,
            )
        )

        if debug:
            print(f"Loading logs for {self.taskName=} with {where=} from {len(dRefs)=} dRefs")

        for i, dRef in enumerate(dRefs):
            _d = dRef.dataId.get("detector")  # `None` for focalPlane level
            detector = int(_d) if _d is not None else None
            self.logs[detector] = butler.get(dRef)
            self.detectors.append(detector)

        if debug:
            print(f"{self.detectors=}")

        if not self.logs:
            return

        if isDetectorLevel(task):
            for detector in self.detectors:
                failMessage = getFail(self.logs[detector])
                time = getTaskTime(self.logs[detector])
                self.detectorTimings[detector] = time
                if failMessage:
                    self.failures[detector] = failMessage
        else:
            detector = None
            self.detectors = [None]
            failMessage = getFail(self.logs[detector])
            time = getTaskTime(self.logs[detector])
            self.detectorTimings[detector] = time
            if failMessage:
                self.failures[detector] = failMessage

    @property
    def isVisitType(self) -> bool:
        return isVisitType(self.task)

    @property
    def isExposureType(self) -> bool:
        return isExposureType(self.task)

    @property
    def isDetectorLevel(self) -> bool:
        return isDetectorLevel(self.task)

    @property
    def isFocalPlaneLevel(self) -> bool:
        return isFocalPlaneLevel(self.task)

    @property
    def dayObs(self) -> bool:
        return self.record.day_obs

    @property
    def recordType(self) -> str:
        if self.isExposureType:
            return "exposure"
        elif self.isVisitType:
            return "visit"
        else:
            raise RuntimeError(f"Unknown record type for {self.taskName=}")

    @property
    def seqNum(self) -> bool:
        if self.isExposureType:
            return self.record.seq_num
        else:
            # TODO: improve this?
            return self.record.id % 10000

    @property
    def maxTime(self) -> float:
        """Maximum time taken by any detector."""
        return max(self.detectorTimings.values()) if self.detectorTimings else float("nan")

    @property
    def minTime(self) -> float:
        """Minimum time taken by any detector."""
        return min(self.detectorTimings.values()) if self.detectorTimings else float("nan")

    @property
    def meanTime(self) -> float:
        """Average time taken by all detectors."""
        if not self.detectorTimings:
            return float("nan")
        return float(np.mean(list(self.detectorTimings.values())))

    @property
    def stdDevTime(self) -> float:
        """Standard deviation of time taken by all detectors."""
        if not self.detectorTimings:
            return 0.0
        return float(np.std(list(self.detectorTimings.values())))

    @property
    def numDetectors(self) -> int:
        """Number of detectors."""
        return len(self.detectors)

    @property
    def numFailures(self) -> int:
        """Number of failures."""
        return len(self.failures)


class PerformanceBrowser:
    def __init__(
        self,
        butler,
        instrument: str,
        locationConfig: LocationConfig,
        debug: bool = False,
    ) -> None:
        self.butler = butler
        self.instrument = instrument
        self.locationConfig = locationConfig
        self.debug = debug
        self.camera = getCameraFromInstrumentName(instrument)
        self.detNums = [d.getId() for d in self.camera]
        self.pipelines: dict[str, PipelineComponents] = {}
        self.whos = list(self.pipelines.keys())

        _, pipelines = buildPipelines(
            instrument=instrument,
            locationConfig=locationConfig,
            butler=butler,
        )
        self.pipelines = pipelines
        self.data: dict[tuple[int, int], dict[str, TaskResult]] = {}

        self.taskDict: dict[str, TaskNode] = {}
        people = self.pipelines.keys()
        for who in people:
            self.taskDict.update(self.pipelines[who].getTasks())

    def loadData(self, dayObs: int, seqNum: int, reload=False) -> None:
        """Load data for the given day and sequence number.

        Parameters
        ----------
        dayObs : `int`
            The day of observation.
        seqNum : `int`
            The sequence number.
        """
        # have data and not reloading
        if (dayObs, seqNum) in self.data and not reload:
            return

        # don't have data, so try loading regardless
        if (dayObs, seqNum) not in self.data:
            reload = True

        self.data[(dayObs, seqNum)] = {}

        expRecord = getExpRecord(self.butler, dayObs, seqNum)
        if not expRecord:
            raise ValueError(f"Failed to get expRecord records for {seqNum=} - has the image been taken yet?")
        visitRecord = getVisitRecord(self.butler, expRecord)

        for taskName, task in self.taskDict.items():
            isVisit = isVisitType(task)

            if isVisit and visitRecord is None:
                if self.debug:
                    print(f"Skipping {taskName} - no visit record")
                continue

            record = visitRecord if isVisit else expRecord
            assert record is not None

            taskResult = TaskResult(
                butler=self.butler,
                record=record,
                task=task,
            )
            self.data[(dayObs, seqNum)][taskName] = taskResult

    def getResults(self, dayObs: int, seqNum: int, taskName: str, reload: bool = False) -> TaskResult:
        """Get the results for a specific task.

        Parameters
        ----------
        dayObs : `int`
            The day of observation.
        seqNum : `int`
            The sequence number.
        taskName : `str`
            The name of the task.

        Returns
        -------
        TaskResult
            The results for the specified task.
        """
        self.loadData(dayObs, seqNum, reload=reload)
        try:
            return self.data[(dayObs, seqNum)][taskName]
        except KeyError:
            raise ValueError(f"No data found for {taskName} found for {dayObs=}, {seqNum=}")

    def printLogs(self, dayObs: int, seqNum: int, full=False, reload=False) -> None:
        self.loadData(dayObs, seqNum, reload)
        data = self.data[(dayObs, seqNum)]

        for taskName, taskResult in data.items():
            nItems = len(taskResult.detectors)
            if nItems == 0:
                continue
            print(f"{taskResult.taskName}: {nItems} datasets")
            timings = (
                f"  min={taskResult.minTime:.2f}s - "
                f"mean={taskResult.meanTime:.2f}s - "
                f"max={taskResult.maxTime:.2f}s"
            )
            print(timings)
            if full:
                for detector, timing in sorted(taskResult.detectorTimings.items()):
                    success = "✅" if detector not in taskResult.failures else "❌"
                    print(f"  {success} {detector if detector is not None else 'None':>3}: {timing:.1f}s")

            if taskResult.numFailures > 0:
                print(f"  {taskResult.numFailures} failures")
                for detector, failMessage in taskResult.failures.items():
                    print(f"    {detector}: {failMessage}")
