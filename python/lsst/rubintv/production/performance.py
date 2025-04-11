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

import re
from typing import TYPE_CHECKING

# TODO Change these back to relative imports
from lsst.rubintv.production.processingControl import PipelineComponents, buildPipelines
from lsst.rubintv.production.utils import LocationConfig
from lsst.summit.utils.utils import getCameraFromInstrumentName

if TYPE_CHECKING:
    from lsst.daf.butler import ButlerLogRecords, DatasetRef, DimensionRecord
    from lsst.pipe.base.pipeline_graph import TaskNode


def isVisitLevel(task: TaskNode) -> bool:
    return "visit" in task.dimensions


def isExposureLevel(task: TaskNode) -> bool:
    return "exposure" in task.dimensions


class ErrorBrowser:
    def __init__(
        self,
        butler,
        dayObs: int,
        instrument: str,
        locationConfig: LocationConfig,
    ) -> None:
        self.butler = butler
        self.dayObs = dayObs
        self.instrument = instrument
        self.locationConfig = locationConfig
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

    def getErrors(self, seqNum: int, taskName: str) -> None:
        raise NotImplementedError("getErrors not implemented")

    def getExpRecord(self, seqNum: int) -> DimensionRecord | None:
        try:
            (expRecord,) = self.butler.registry.queryDimensionRecords(
                "exposure", where=f"exposure.day_obs={self.dayObs} and exposure.seq_num={seqNum}"
            )
            return expRecord
        except Exception:
            return None

    def getVisitRecord(self, expRecord: DimensionRecord) -> DimensionRecord | None:
        try:
            (visitRecord,) = self.butler.registry.queryDimensionRecords(
                "visit", where=f"visit={expRecord.id}"
            )
            return visitRecord
        except Exception:
            return None

    def makeWhere(self, task: TaskNode, record: DimensionRecord) -> str:
        isVisit = isExposureLevel(task)

        if isVisit == "isr":
            return f"visit={record.id}"
        else:
            return f"exposure={record.id}"

    @staticmethod
    def getTaskTime(logs, method="first-last") -> float:
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

    @staticmethod
    def getDataId(dRef: DatasetRef):
        detector = dRef.dataId.get("detector")
        if "visit" in dRef.dataId:
            return dRef.dataId["visit"], detector
        elif "exposure" in dRef.dataId:
            return dRef.dataId["exposure"], detector
        elif "group" in dRef.dataId:
            return dRef.dataId["group"], detector
        raise RuntimeError(f"Failed to get dataId from {dRef}")

    @staticmethod
    def getFail(log: ButlerLogRecords) -> str | None:
        for line in log:
            if line.levelname == "ERROR":
                return line.message
        return None

    def printLogs(self, seqNum: int):
        expRecord = self.getExpRecord(seqNum)
        if not expRecord:
            raise ValueError(f"Failed to get expRecord records for {seqNum=} - has the image been taken yet?")

        visitRecord = self.getVisitRecord(expRecord)

        taskDict: dict[str, TaskNode] = {}
        people = self.pipelines.keys()
        for who in people:
            taskDict.update(self.pipelines[who].getTasks())

        for taskName, task in taskDict.items():
            isVisit = isVisitLevel(task)

            print(taskName)

            if isVisit and visitRecord is None:
                print(f"Skipping {taskName} - no visit record")
                continue

            record = visitRecord if isVisit else expRecord
            assert record is not None
            where = self.makeWhere(task, record)

            dRefs = list(
                self.butler.registry.queryDatasets(
                    f"{taskName}_log",
                    findFirst=True,
                    where=where,
                )
            )
            print(f"task: {taskName} - {len(dRefs)} results")
            dRefs = sorted(dRefs, key=lambda x: x.dataId.get("detector"))
            for i, dRef in enumerate(dRefs):
                primaryId, detector = self.getDataId(dRef)
                logs = self.butler.get(dRef)
                failMessage = self.getFail(logs)
                passFail = "✅" if not failMessage else "❌"
                print(f"{passFail} Detector {detector} took {self.getTaskTime(logs):.2f} seconds")
                if failMessage:
                    print(failMessage)
            print()
