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
    "PerformanceMonitor",
]

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import matplotlib.dates as mdates
import numpy as np
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import Patch

from lsst.rubintv.production.baseChannels import BaseButlerChannel

# TODO Change these back to relative imports
from lsst.rubintv.production.processingControl import PipelineComponents, buildPipelines
from lsst.rubintv.production.utils import LocationConfig, makePlotFile, writeMetadataShard
from lsst.summit.utils.dateTime import dayObsIntToString
from lsst.summit.utils.efdUtils import getEfdData, makeEfdClient
from lsst.summit.utils.utils import getCameraFromInstrumentName
from lsst.utils.plotting.figures import make_figure

if TYPE_CHECKING:
    from lsst_efd_client import EfdClient

    from lsst.daf.butler import Butler, ButlerLogRecords, DimensionRecord
    from lsst.pipe.base.pipeline_graph import TaskNode

    from .payloads import Payload
    from .podDefinition import PodDetails
    from .processingControl import CameraControlConfig


AOS_DEFAULT_TASKS: dict[str, str] = {
    "isr": "k",
    "generateDonutDirectDetectTask": "r",
    "cutOutDonutsCwfsPairTask": "g",
    "reassignCwfsCutoutsPairTask": "orange",
    "calcZernikesTask": "y",
}

CWFS_SENSOR_NAMES = ("SW0", "SW1")  # these exclude the raft prefix so can't easily come from the camera
IMAGING_SENSOR_NAMES = ("S00", "S01", "S02", "S10", "S11", "S12", "S20", "S21", "S22")


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


def makeTitle(record: DimensionRecord) -> str:
    """Make a title for a plot based on the exp/visit record and detector.

    Parameters
    ----------
    record : `lsst.daf.butler.DimensionRecord`
        The exposure or visit record.

    Returns
    -------
    title : `str`
        The title for the plot.
    """
    r = record
    title = f"dayObs={r.day_obs} - seqNum={r.seq_num}\n"
    title += (
        f"{r.exposure_time:.1f}s {r.observation_type} ({r.physical_filter}) image, {r.observation_reason}"
    )
    return title


def plotGantt(
    expRecord: DimensionRecord,
    taskResults: list[TaskResult],
    ignoreTasks: list[str] | None = None,
    timings: list[str] | None = None,
    figsize=(10, 6),
    barHeight=0.6,
):
    """Plot a Gantt chart of task results.

    For each task, puts a vertical mark at the absolute start and end time,
    and a horizontal bar in the middle of that region, with a width of the
    standard deviation of the time taken by that task.

    Parameters
    ----------
    expRecord : `DimensionRecord`
        The exposure or visit record.
    taskResults : `list[TaskResult]`
        The list of task results to plot.
    ignoreTasks : `list[str]`, optional
        A list of task names to exclude from the plot.
    timings : `list[(datetime, str)]`, optional
        A list of tuples containing a datetime and a string to plot as
        vertical lines on the Gantt chart and text in the textbox.
    figsize : `tuple`
        The size of the figure.
    barHeight : `float`
        The height of the bars in the Gantt chart.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        The figure object.
    """
    fig = make_figure(figsize=figsize)
    ax = fig.gca()

    if ignoreTasks is None:
        ignoreTasks = []

    if timings is None:
        timings = []

    valid = [
        tr
        for tr in taskResults
        if isinstance(tr.startTimeOverall, datetime)
        and isinstance(tr.endTimeOverall, datetime)
        and tr.taskName not in ignoreTasks
    ]
    shutterClose: datetime = expRecord.timespan.end.utc.to_datetime()
    shutterCloseNum = mdates.date2num(shutterClose)

    for i, tr in enumerate(valid):
        startNum = mdates.date2num(tr.startTimeOverall)
        endNum = mdates.date2num(tr.endTimeOverall)

        # Plot vertical marks at start and end times
        ax.plot(startNum, i, marker="|", c="k")
        ax.plot(endNum, i, marker="|", c="k")
        ax.plot([startNum, endNum], [i, i], c="k", lw=0.5)

        duration = endNum - startNum
        ax.barh(i, width=duration, left=startNum, height=barHeight)

    ax.set_yticks(range(len(valid)))
    # Align labels with the start of each bar
    ax.set_yticklabels([tr.taskName for tr in valid], ha="right")
    fig.subplots_adjust(left=0.2)

    # Configure the primary axis for date display at the top
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M:%S"))
    ax.xaxis.set_ticks_position("top")
    ax.xaxis.set_label_position("top")
    # align and rotate top ticks correctly
    ax.tick_params(
        axis="x", which="major", pad=2, rotation=30, labeltop=True, labelbottom=False, labelrotation=30
    )
    # left‑align each label to its tick, keeping the rotated anchor
    for lbl in ax.xaxis.get_ticklabels():
        lbl.set_horizontalalignment("left")
        lbl.set_rotation_mode("anchor")

    # Shutter-close reference line
    ax.axvline(x=shutterCloseNum, color="r", linestyle="--", alpha=0.7, label="Shutter Close")

    # legend & stats boxes
    ax.legend(loc="upper right", fontsize="small")  # moved to top-right

    textBoxText = "\n".join(timings)
    if textBoxText:
        ax.text(
            0.98,
            0.85,
            textBoxText,
            transform=ax.transAxes,
            ha="right",
            va="top",
            bbox=dict(facecolor="w", alpha=0.8),
        )

    # Semi-transparent grid every 30 s after shutterClose
    if valid:
        lastEndNum = max(mdates.date2num(tr.endTimeOverall) for tr in valid)
        stepDays = 30 / (24 * 60 * 60)  # 30 s in Matplotlib date units
        t = shutterCloseNum + stepDays
        while t <= lastEndNum:
            ax.axvline(x=t, color="gray", linestyle=":", alpha=0.4, lw=0.5)
            t += stepDays

        def date2sec(x):
            return (x - shutterCloseNum) * 24 * 60 * 60

        def sec2date(x):
            return shutterCloseNum + x / (24 * 60 * 60)

        secax = ax.secondary_xaxis("bottom", functions=(date2sec, sec2date))
        secax.set_xlabel("Time (seconds from shutter close)")

    fig.tight_layout()
    # Legend for shutter close and grid lines
    from matplotlib.lines import Line2D

    legend_elements = [
        Line2D([0], [0], color="r", linestyle="--", alpha=0.9, label="Shutter Close"),
        Line2D([0], [0], color="gray", linestyle=":", alpha=0.7, lw=0.5, label="30s intervals"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize="small")

    # Create a separate title outside the plot area
    title = makeTitle(expRecord)
    # Position title in the top-left corner outside the main axes
    # Using figure coordinates (0,0 is bottom-left of figure)
    fig.text(0.02, 0.98, title, ha="left", va="top", fontsize="medium")

    return fig


def calcTimeSinceShutterClose(
    expRecord: DimensionRecord,
    taskResult: TaskResult,
    startOrEnd: str = "start",
) -> float:
    """Calculate the time since shutter close for a task result.

    Parameters
    ----------
    expRecord : `DimensionRecord`
        The exposure record.
    taskResult : `TaskResult`
        The task result.

    Returns
    -------
    timeSinceShutterClose : `float` or `None`
        The time since shutter close in seconds, or None if not applicable.
    """
    if startOrEnd not in ["start", "end"]:
        raise ValueError(f"Invalid option {startOrEnd=}")

    if taskResult.startTimeOverall is None:  # if it has a start it has an end, and vice versa
        log = logging.getLogger("lsst.rubintv.production.performance.calcTimeSinceShutterClose")
        log.warning(f"Task {taskResult.taskName} has no {startOrEnd} time")
        return float("nan")

    shutterClose: datetime = expRecord.timespan.end.utc.to_datetime()
    if startOrEnd == "start":
        taskTime = taskResult.startTimeOverall.astimezone(timezone.utc).replace(tzinfo=None)
    else:  # naked else for mypy, start/end checked on function entry
        assert taskResult.endTimeOverall is not None, "endTimeOverall should not be None if start is not None"
        taskTime = taskResult.endTimeOverall.astimezone(timezone.utc).replace(tzinfo=None)

    return (taskTime - shutterClose).total_seconds()


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

    @property
    def startTimeOverall(self) -> datetime | None:
        """Overall start time of the first quantum in the set of logs"""
        if not self.logs:
            return None
        return min(log[0].asctime for log in self.logs.values())

    @property
    def endTimeOverall(self) -> datetime | None:
        """Overall end time of the last quantum in the set of logs"""
        if not self.logs:
            return None
        return max(log[-1].asctime for log in self.logs.values())

    @property
    def endTimeAfterShutterClose(self) -> float | None:
        """Elapsed time of the end of the last quantum in the set of logs since
        shutter close
        """
        return calcTimeSinceShutterClose(self.record, self, startOrEnd="end")

    @property
    def startTimeAfterShutterClose(self) -> float | None:
        """Elapsed time of the start of the first quantum in the set of logs
        since shutter close
        """
        return calcTimeSinceShutterClose(self.record, self, startOrEnd="start")

    def printLog(self, detector: int | None, differentialMode: bool = True) -> None:
        """Print the per-line log times for a task.

        Prints the log for a given detector (if it's a detector level) task,
        with the associated time for each line. This is useful for debugging
        and understanding the time taken by each step.
        """
        if not self.logs:
            return

        if isDetectorLevel(self.task):
            if detector is None:
                raise ValueError("detector must be specified for detector level tasks")
            if detector not in self.logs:
                raise ValueError(f"Detector {detector} not found in logs")
            logs = self.logs[detector]
        else:
            if detector is not None:
                raise ValueError("detector must be None for non-detector level tasks")
            logs = self.logs[None]

        detStr = f"for detector {detector} " if detector is not None else ""  # contains trailing space
        if differentialMode:
            print(f"Differential mode logs for {self.taskName} {detStr}on {self.record.id}:")
            print("<time **since previous** log message> - log message")
        else:
            print(f"ISO-format logs for {self.taskName} {detStr}on {self.record.id}:")
            print("<time of log> - log message")

        firstLine = logs[0]
        timestamp = firstLine.asctime.isoformat() if not differentialMode else "0.0"
        print(timestamp, logs[0].message)
        for i, line in enumerate(logs[1:], start=1):  # start=1 to match the actual index in logs
            if differentialMode:
                timestamp = f"{(line.asctime - logs[i - 1].asctime).total_seconds():.2f}s"
            else:
                timestamp = line.asctime.isoformat()
            print(f"{timestamp} {line.message}")


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
        self.data: dict[DimensionRecord, dict[str, TaskResult]] = {}

        self.taskDict: dict[str, TaskNode] = {}
        people = self.pipelines.keys()
        for who in people:
            self.taskDict.update(self.pipelines[who].getTasks())

    def loadData(self, expRecord: DimensionRecord, reload=False) -> None:
        """Load data for the given day and sequence number.

        Parameters
        ----------
        dayObs : `int`
            The day of observation.
        seqNum : `int`
            The sequence number.
        """
        # have data and not reloading
        if expRecord in self.data and not reload:
            return

        # don't have data, so try loading regardless
        if expRecord not in self.data:
            reload = True

        data: dict[str, TaskResult] = {}
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
            data[taskName] = taskResult
        self.data[expRecord] = data

    def getResults(self, expRecord: DimensionRecord, taskName: str, reload: bool = False) -> TaskResult:
        """Get the results for a specific task.

        Parameters
        ----------
        expRecord : `DimensionRecord`
            The exposure record.
        taskName : `str`
            The name of the task.

        Returns
        -------
        TaskResult
            The results for the specified task.
        """
        self.loadData(expRecord, reload=reload)
        try:
            return self.data[expRecord][taskName]
        except KeyError:
            raise ValueError(f"No data found for {taskName} found for {expRecord.id=}")

    def plot(
        self, expRecord: DimensionRecord, reload: bool = False, ignoreTasks: list[str] | None = None
    ) -> None:
        """Plot the results for all tasks."""
        self.loadData(expRecord, reload=reload)
        data = self.data[expRecord]
        if not data:
            raise ValueError(f"No data found for {expRecord.id=}")

        taskResults = list(data.values())

        resultsDict = {tr.taskName: tr for tr in taskResults}
        textItems = []

        isrDt = calcTimeSinceShutterClose(expRecord, resultsDict["isr"], startOrEnd="start")
        textItems.append(f"Shutter close to isr start: {isrDt:.1f} s")

        if "calcZernikesTask" in resultsDict:
            zernikeDt = calcTimeSinceShutterClose(
                expRecord, resultsDict["calcZernikesTask"], startOrEnd="end"
            )
            textItems.append(f"Shutter close to zernike end: {zernikeDt:.1f} s")

        fig = plotGantt(expRecord, taskResults, ignoreTasks=ignoreTasks, timings=textItems)
        return fig

    def printLogs(self, expRecord: DimensionRecord, full=False, reload=False) -> None:
        self.loadData(expRecord, reload)
        data = self.data[expRecord]

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


class PerformanceMonitor(BaseButlerChannel):
    def __init__(
        self,
        locationConfig: LocationConfig,
        butler: Butler,
        instrument: str,
        podDetails: PodDetails,
        *,
        doRaise=False,
    ) -> None:
        super().__init__(
            locationConfig=locationConfig,
            butler=butler,
            # TODO: DM-43764 this shouldn't be necessary on the
            # base class after this ticket, I think.
            detectors=None,  # unused
            dataProduct=None,  # unused
            # TODO: DM-43764 should also be able to fix needing
            # channelName when tidying up the base class. Needed
            # in some contexts but not all. Maybe default it to
            # ''?
            channelName="",  # unused
            podDetails=podDetails,
            doRaise=doRaise,
            addUploader=True,
        )
        assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
        assert self.podDetails is not None  # XXX why is this necessary? Fix mypy better!
        self.log.info(f"Performance monitor running, consuming from {self.podDetails.queueName}")
        self.perf = PerformanceBrowser(butler, instrument, locationConfig)
        self.shardsDirectory = locationConfig.raPerformanceShardsDirectory
        self.instrument = instrument  # why isn't this being set in the base class?!
        self.efdClient = makeEfdClient()
        self.cameraControl = CameraControlConfig()

    def callback(self, payload: Payload) -> None:
        """Callback function to be called when a new exposure is available."""
        dataId = payload.dataIds[0]
        record = None
        if "exposure" in dataId.dimensions:
            record = dataId.records["exposure"]
        elif "visit" in dataId.dimensions:
            record = dataId.records["visit"]

        if record is None:
            raise RuntimeError(f"Failed to find exposure or visit record in {dataId=}")

        self.log.info(f"Analyzing timing info for {record.id}...")
        t0 = time.time()
        self.perf.loadData(record)
        loadTime = time.time() - t0
        self.log.info(f"Loaded data for {record.id} in {loadTime:.2f}s")

        data = self.perf.data[record]
        if not data:
            raise ValueError(f"No data found for {record.id=}")

        taskResults = list(data.values())
        resultsDict = {tr.taskName: tr for tr in taskResults}

        textItems = []
        rubinTVtableItems: dict[str, str | dict[str, str]] = {}

        isrTaskNames = [k for k in resultsDict.keys() if "isr" in k.lower()]
        # isr runs on the AOS chips, cpVerifyIsr runs on the imaging chips for
        # calib type images, so deal with the keys and pick the quickest one to
        # start
        if len(isrTaskNames) != 0:
            minTime = 9999999.0
            for isrTaskName in isrTaskNames:
                isrDt = calcTimeSinceShutterClose(record, resultsDict[isrTaskName], startOrEnd="start")
                if isrDt < minTime:
                    minTime = isrDt
            textItems.append(f"Shutter close to isr start: {minTime:.1f} s")
            rubinTVtableItems["ISR start time (shutter)"] = f"{minTime:.2f}"

        if "calcZernikesTask" in resultsDict:
            zernikeDt = calcTimeSinceShutterClose(record, resultsDict["calcZernikesTask"], startOrEnd="end")
            textItems.append(f"Shutter close to zernike end: {zernikeDt:.1f} s")
            rubinTVtableItems["Zernike delivery time (shutter)"] = f"{zernikeDt:.2f}"

        rubinTVtableItems["Exposure time"] = record.exposure_time
        rubinTVtableItems["Image type"] = record.observation_type
        rubinTVtableItems["Target"] = record.target_name

        fig = plotGantt(record, taskResults, timings=textItems)

        plotName = "timing_diagram"
        plotFile = makePlotFile(
            self.locationConfig, self.instrument, record.day_obs, record.seq_num, plotName, "jpg"
        )
        fig.tight_layout()
        fig.savefig(plotFile)
        assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
        self.s3Uploader.uploadPerSeqNumPlot(
            instrument="ra_performance",
            plotName=plotName,
            dayObs=record.day_obs,
            seqNum=record.seq_num,
            filename=plotFile,
        )

        for taskName, taskResult in data.items():
            nItems = len(taskResult.detectors)
            if nItems == 0:
                continue
            rubinTVtableItems[taskName] = f"{nItems} datasets"
            rubinTVtableItems[f"{taskName} min runtime"] = f"{taskResult.minTime:.2f}"
            rubinTVtableItems[f"{taskName} mean runtime"] = f"{taskResult.meanTime:.2f}"
            rubinTVtableItems[f"{taskName} max runtime"] = f"{taskResult.maxTime:.2f}"
            rubinTVtableItems[f"{taskName} std dev"] = f"{taskResult.stdDevTime:.2f}"
            rubinTVtableItems[f"{taskName} fail count"] = f"{taskResult.numFailures}"

            timingDict = {}
            timingDict["DISPLAY_VALUE"] = "⏱"
            for detector, timing in sorted(taskResult.detectorTimings.items()):
                success = "✅" if detector not in taskResult.failures else "❌"
                timingDict[f"{detector}"] = f"{success} in {timing:.1f}"
            rubinTVtableItems[f"{taskName} timings"] = timingDict

            if taskResult.numFailures > 0:
                failDict = {}
                failDict["DISPLAY_VALUE"] = "📖"
                for detector, failMessage in taskResult.failures.items():
                    failDict[f"{detector}"] = failMessage
                rubinTVtableItems[f"{taskName} failures"] = failDict

        md = {record.seq_num: rubinTVtableItems}
        writeMetadataShard(self.shardsDirectory, record.day_obs, md)

        # self.makeAosPlot(record)

        # callback() is only called for the long-running RA process, so clear
        # the cache so we don't have ever increasing memory usage
        self.perf.data = {}


def getEndReadoutTime(client: EfdClient, expRecord: DimensionRecord) -> float:
    data = getEfdData(client, "lsst.sal.MTCamera.logevent_endReadout", expRecord=expRecord, postPadding=30)
    timestamp = data[data["imageName"] == expRecord.obs_id]["timestampEndOfReadout"].iloc[0]
    return timestamp - expRecord.timespan.end.unix_tai


def getIngestTimes(
    client: EfdClient,
    expRecord: DimensionRecord,
    key="private_kafkaStamp",
) -> tuple[dict[str, float], dict[str, float]]:
    oodsData = getEfdData(client, "lsst.sal.MTOODS.logevent_imageInOODS", expRecord=expRecord, postPadding=60)

    endExposure = expRecord.timespan.end.unix_tai
    thisImageData = oodsData[oodsData["obsid"] == expRecord.obs_id]

    wavefronts = thisImageData[thisImageData["sensor"].isin(CWFS_SENSOR_NAMES)]
    sciences = thisImageData[thisImageData["sensor"].isin(IMAGING_SENSOR_NAMES)]

    wfTimes = {f"{row['raft']}_{row['sensor']}": row[key] - endExposure for _, row in wavefronts.iterrows()}
    sciTimes = {f"{row['raft']}_{row['sensor']}": row[key] - endExposure for _, row in sciences.iterrows()}

    return wfTimes, sciTimes


def makeAosPlot(
    efdClient: EfdClient,
    expRecord: DimensionRecord,
    taskResults: dict[str, TaskResult],
    cwfsDetNums: list[int],
) -> None:
    wfTimes, sciTimes = getIngestTimes(efdClient, expRecord)

    readoutDelay = getEndReadoutTime(efdClient, expRecord)
    zernikeDelivery = taskResults["calcZernikesTask"].endTimeAfterShutterClose
    isrStart = taskResults["isr"].startTimeAfterShutterClose
    wfIngestStart = min(wfTimes.values())
    wfIngestEnd = max(wfTimes.values())
    calcZernMean = np.nanmedian(list(taskResults["calcZernikesTask"].detectorTimings.values()))
    isrTimes = taskResults["isr"].detectorTimings  # this includes the imaging chips
    cwfsIsrTimes = [isrTimes[detNum] for detNum in cwfsDetNums]
    isrMean = np.nanmedian(cwfsIsrTimes)

    timings = {  # for the staircase plot
        "Readout start": 0,
        "Readout (effective)": readoutDelay,
        "WFS ingest start": min(wfTimes.values()),
        "WFS ingest finished": max(wfTimes.values()),
        "Imaging ingest finished": max(sciTimes.values()),
    }

    assert isrStart is not None, "isrStart should not be None"

    legendItems = {  # for the legend box
        "Readout (effective)": readoutDelay,
        "WF ingestion duration": (wfIngestEnd - wfIngestStart),
        "First WF available to isr start": (isrStart - wfIngestStart),
        "Mean isr runtime": isrMean,
        "Mean calcZernikes runtime": calcZernMean,
        "Readout end to isr start": (isrStart - readoutDelay),
        "Shutter close to zernikes": zernikeDelivery,
    }
    legendExtraLines = [f"{k}: {v:.2f}s" for k, v in legendItems.items()]

    fig, axTop, axBottom = plotAosTaskTimings(
        detectorList=cwfsDetNums,
        taskMap=AOS_DEFAULT_TASKS,
        results=taskResults,
        expRecord=expRecord,
        timings=timings,
        legendExtraLines=legendExtraLines,
        figsize=(12, 8),
        heightRatios=(1, 2.5),
    )
    return fig


def addEventStaircase(
    axTop: Axes, axBottom: Axes, timings: dict[str, float], *, yMax: float = 1.0, yMin: float = 0.08
) -> None:
    """Top panel: dashed verticals that step down; full-height lines drawn in
    bottom panel. Labels on arrows show the *later* event name and +Δt.
    """
    if not timings:
        axTop.set_axis_off()
        return

    items: list[tuple[str, float]] = sorted(timings.items(), key=lambda kv: kv[1])
    names: list[str] = [k for k, _ in items]
    times: list[float] = [v for _, v in items]
    n = len(times)

    heights = np.linspace(yMax, yMin, n, dtype=float)

    # top: staircase heights; bottom: full-height guides
    for i, t in enumerate(times):
        axTop.vlines(t, 0.0, float(heights[i]), linestyles="--", linewidth=1.2)
        axBottom.axvline(t, color="black", linestyle="--", linewidth=1.2, ymin=0, ymax=1)

    axTop.text(
        times[0],
        1,
        f"{names[0]} (+{times[0]:.2f}s)",
        rotation=45,
        rotation_mode="anchor",
        ha="left",
        va="bottom",
    )

    # arrows + labels between consecutive events
    labelDy = 0.03 * (yMax - yMin)
    for i in range(n - 1):
        t0, t1 = times[i], times[i + 1]
        yRight = float(heights[i + 1])
        dt = t1 - t0

        # Horizontal arrow pointing to the top of the right event line
        axTop.annotate(
            "",
            xy=(t1, yRight),
            xytext=(t0, yRight),
            arrowprops=dict(arrowstyle="<->", linewidth=1.2),
        )

        # Place the label above the right event point (not at the midpoint)
        axTop.text(
            t1,
            yRight + labelDy,
            f"{names[i + 1]} (+{dt:.2f}s)",
            rotation=45,
            rotation_mode="anchor",
            ha="left",
            va="bottom",
        )

    axTop.set_ylim(0.0, yMax + 2 * labelDy)
    axTop.set_yticks([])
    axTop.set_ylabel("Events", labelpad=6)
    axTop.grid(False)

    # Remove top x-axis and side axis lines
    for spine in ("top", "left", "right"):
        axTop.spines[spine].set_visible(False)
    axTop.tick_params(axis="x", top=False)


def createLegendBoxes(
    axTop: Axes,
    colors: dict[str, str],
    extraLines: list[str] | None = None,
) -> None:
    """Two axis-anchored legends at the bottom-right of *axTop*.

    Left block = colored task entries; right block = free-text lines.
    Both are placed relative to axTop's axes coordinates (0..1).
    """
    # Left: colored task entries (placed just to the *left* of the text legend)
    colorHandles = [Patch(facecolor=v, label=k) for k, v in colors.items()]
    tasksLegend = axTop.legend(
        handles=colorHandles,
        loc="lower right",
        bbox_to_anchor=(1.0, 0.0),  # bottom-right corner of axTop
        bbox_transform=axTop.transAxes,
        frameon=False,
        ncol=1,
        borderaxespad=0.0,
        fontsize="medium",
    )

    if extraLines:
        # Force a draw so we can compute the first legend's bbox.
        fig = axTop.figure
        fig.canvas.draw()
        # type ignore because get_renderer() only exists with Agg backend
        renderer = fig.canvas.get_renderer()  # type: ignore[attr-defined]
        bboxDisplay = tasksLegend.get_window_extent(renderer=renderer)
        bboxAxes = bboxDisplay.transformed(axTop.transAxes.inverted())

        # Anchor the right edge of the text legend to the left edge of the
        # tasks legend.
        gapAxes = 0.02  # small horizontal gap in axes coords to avoid touching
        anchorX = max(bboxAxes.x0 - gapAxes, 0.0)

        textHandles = [Patch(facecolor="none", edgecolor="none", label=line) for line in extraLines]
        axTop.legend(
            handles=textHandles,
            loc="lower right",
            bbox_to_anchor=(anchorX, 0.0),
            bbox_transform=axTop.transAxes,
            frameon=False,
            ncol=1,
            borderaxespad=0.0,
            handlelength=0.0,
            handletextpad=0.0,
            fontsize="medium",
        )
        # Re-add the first legend so both legends are visible.
        axTop.add_artist(tasksLegend)


def plotAosTaskTimings(
    detectorList: list[int],
    taskMap: dict[str, str],
    results: dict[str, TaskResult],
    expRecord: DimensionRecord,
    timings: dict[str, float],
    *,
    barHalf: float = 0.3,
    touchHalf: float = 0.5,
    figsize: tuple[float, float] = (12, 5.0),
    heightRatios: tuple[float, float] = (1, 2.5),
    legendExtraLines: list[str] | None = None,
) -> tuple[Figure, Axes, Axes]:
    """Render the AOS task timing plot with an event staircase panel.

    Returns (fig, axTop, axBottom).
    """
    fig = make_figure(figsize=figsize)
    axTop, axBottom = fig.subplots(
        2,
        1,
        sharex=True,
        gridspec_kw={"height_ratios": list(heightRatios), "hspace": 0.0},
    )

    t0 = expRecord.timespan.end.utc.to_datetime().astimezone(timezone.utc)

    detMap = {det: i for i, det in enumerate(detectorList)}
    bottoms: list[float] = [i - barHalf for i in range(len(detectorList))]
    tops: list[float] = [i + barHalf for i in range(len(detectorList))]

    # make consecutive detectors touch
    for i in range(len(detectorList) - 1):
        if detectorList[i + 1] == detectorList[i] + 1:
            tops[i] = i + touchHalf  # raise earlier one
            bottoms[i + 1] = i + touchHalf  # lower later one

    taskMins: dict[str, float] = {}
    taskMaxs: dict[str, float] = {}

    for task, color in taskMap.items():
        taskResults = results[task]
        taskMins[task] = 999.0
        taskMaxs[task] = -1.0

        for detNum in detectorList:
            if detNum not in taskResults.logs:
                continue
            start = (taskResults.logs[detNum][0].asctime - t0).total_seconds()
            end = (taskResults.logs[detNum][-1].asctime - t0).total_seconds()

            taskMins[task] = min(taskMins[task], start)
            taskMaxs[task] = max(taskMaxs[task], end)

            idx = detMap[detNum]
            axBottom.fill_between([start, end], bottoms[idx], tops[idx], color=color)

    if taskMins.get("isr", None) is not None:
        timings["ISR Start"] = taskMins["isr"]

    # legends anchored to bottom-right of the TOP axis
    createLegendBoxes(axTop, taskMap, extraLines=legendExtraLines)

    axBottom.set_xlim(0, None)
    axBottom.set_yticks(list(detMap.values()))
    axBottom.set_yticklabels(list(detMap.keys()))
    axBottom.set_xlabel("Time since end integration (s)")
    axBottom.set_ylabel("Detector number #")

    # move title to very bottom, centered under the x-axis of the bottom plot
    dayObsStr = dayObsIntToString(expRecord.day_obs)
    bottomTitle = f"AOS pipeline timings for {dayObsStr} - seq {expRecord.seq_num}"

    # Clear any axes titles and draw a figure-level bottom title
    axBottom.set_title("")
    axTop.set_title("")
    fig.text(0.5, 0.02, bottomTitle, ha="center", va="bottom")

    addEventStaircase(axTop, axBottom, timings)

    # Layout: no extra bottom legend space needed; keep room for bottom title
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    fig.subplots_adjust(bottom=0.14)
    return fig, axTop, axBottom
