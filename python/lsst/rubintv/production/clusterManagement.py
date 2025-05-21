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

__all__ = "ClusterManager"


import json
import logging
from dataclasses import dataclass
from time import sleep
from typing import TYPE_CHECKING, Any

from tabulate import tabulate

from .payloads import Payload, RestartPayload, isRestartPayload
from .podDefinition import PodDetails, PodFlavor
from .redisUtils import RedisHelper
from .workerSets import AosWorkerSet, SfmWorkerSet, Step1bWorkerSet

if TYPE_CHECKING:
    from lsst.daf.butler import Butler

    from .utils import LocationConfig


step1aMap = {
    PodFlavor.SFM_WORKER: "CLUSTER_STATUS_SFM_SET",
    PodFlavor.AOS_WORKER: "CLUSTER_STATUS_AOS_SET",
}
flatSetMap = {
    PodFlavor.STEP1B_WORKER: "CLUSTER_STATUS_SFM_STEP1B_SET_0",
    PodFlavor.STEP1B_AOS_WORKER: "CLUSTER_STATUS_AOS_STEP1B_SET_0",
    PodFlavor.BACKLOG_WORKER: "CLUSTER_STATUS_SPAREWORKERS_SET_0",
}


@dataclass
class QueueItem:
    """Information about a single item in a queue."""

    index: int
    who: str
    dataIdInfo: str


@dataclass
class WorkerStatus:
    """Status information for a single worker."""

    worker: PodDetails
    queueLength: int
    isBusy: bool
    queueItems: list[QueueItem]


@dataclass
class FlavorStatus:
    """Status information for all workers of a specific flavor."""

    name: str
    nFreeWorkers: int
    workerStatuses: list[WorkerStatus]

    @property
    def workers(self) -> list[PodDetails]:
        """Get the list of workers in this flavor."""
        return [ws.worker for ws in self.workerStatuses]

    @property
    def totalWorkers(self) -> int:
        """Get the total number of workers in this flavor."""
        return len(self.workerStatuses)


@dataclass
class ClusterStatus:
    """Overall cluster status information."""

    instrument: str
    flavorStatuses: dict[PodFlavor, FlavorStatus]
    rawQueueLength: int


class ClusterManager:
    def __init__(self, locationConfig: LocationConfig, butler: Butler, doRaise: bool = False) -> None:
        self.locationConfig = locationConfig
        self.butler = butler
        self.doRaise = doRaise
        self.rh = RedisHelper(butler, locationConfig)
        self.redis = self.rh.redis
        self._lastRubinTVStates: dict[str, dict[str, Any]] = {}
        self.log = logging.getLogger("lsst.rubintv.produciton.clusterManager")

    def drainWorker(self, pod: PodDetails, newQueue: str | None = None, noWarn: bool = False) -> None:
        """Drain all the work from a worker, optionally moving to a new queue.

        Raw queue names are used here instead of PodDetails because we are not
        necessarily assigning it for immediate work, so allow it to sit on at
        an arbitrary address for later distribution.

        Parameters
        ----------
        pod : `PodDetails`
            The pod to drain.
        newQueue : `str`, optional
            The queue to move the work to. If ``None``, the work is discarded.
        """
        counter = 0
        payload = self.rh.dequeuePayload(pod)
        while payload:
            counter += 1
            if newQueue:
                self.rh.redis.lpush(newQueue, payload.to_json())
            else:
                if not noWarn:
                    self.log.warn(f"Discarding payload from {pod.queueName}")
            payload = self.rh.dequeuePayload(pod)
        self.log.info(
            f"Drained {counter} payloads from {pod.queueName} {'to ' + newQueue if newQueue else ''}"
        )

    def getQueueItems(self, queueName: str) -> list[QueueItem]:
        """Get detailed information about items in a queue without removing.

        Parameters
        ----------
        queueName : str
            Name of the queue to inspect.
        detailed : bool
            Whether to fetch detailed information about queue items.

        Returns
        -------
        items: `list[QueueItem]`
            Information about each item in the queue.
        """
        queueItems = []
        rawItems = self.redis.lrange(queueName, 0, -1)

        for i, item in enumerate(rawItems):
            who = "unparseable payload"
            dataIdInfo = ""

            try:
                payload = Payload.from_json(item, self.butler)
                dataIds = payload.dataIds

                # Extract the most relevant ID info from each dataId
                id_parts = []
                for dataId in dataIds:
                    # Try different dimension identifiers in order of
                    # preference
                    for dim in ["exposure", "visit"]:
                        if dim in dataId.required:
                            id_parts.append(str(dataId.required[dim]))
                            break
                    else:
                        # If none of the preferred dimensions exist, use the
                        # string representation
                        id_parts.append(str(dataId))

                dataIdInfo = "+".join(id_parts)
                who = payload.who
            except Exception:
                try:
                    decodedItem = item.decode("utf-8")
                    payloadData = json.loads(decodedItem)
                    if "dataIds" in payloadData:
                        dataIdInfo = str(payloadData["dataIds"])
                except Exception:
                    pass

            queueItems.append(QueueItem(index=i + 1, who=who, dataIdInfo=dataIdInfo))

        return queueItems

    def getWorkerStatus(self, worker: PodDetails, detailed: bool = False) -> WorkerStatus:
        """Get status information for a single worker.

        Parameters
        ----------
        worker : PodDetails
            Worker pod to get status for
        detailed : bool
            Whether to include detailed queue information

        Returns
        -------
        WorkerStatus
            Status information for the worker
        """
        queueLength = self.rh.getQueueLength(worker)
        isBusy = bool(self.redis.exists(f"{worker.queueName}+IS_BUSY"))
        queueItems = self.getQueueItems(worker.queueName) if detailed and queueLength > 0 else []

        return WorkerStatus(
            worker=worker,
            queueLength=queueLength,
            isBusy=isBusy,
            queueItems=queueItems,
        )

    def getStatusForPodFlavor(
        self, flavor: PodFlavor, instrument: str, detailed: bool = False
    ) -> FlavorStatus:
        """Get status information for all workers of a specific flavor.

        Parameters
        ----------
        flavor : PodFlavor
            Flavor of workers to get status for
        instrument : str
            Instrument to filter workers by
        detailed : bool
            Whether to include detailed queue information

        Returns
        -------
        FlavorStatus
            Status information for all workers of the flavor
        """
        workers = self.rh.getAllWorkers(instrument=instrument, podFlavor=flavor)

        if not workers:
            return FlavorStatus(name=flavor.name, nFreeWorkers=0, workerStatuses=[])

        workerStatuses = []
        freeWorkers = 0

        for worker in sorted(workers):
            workerStatus = self.getWorkerStatus(worker, detailed)
            workerStatuses.append(workerStatus)

            if not workerStatus.isBusy:
                freeWorkers += 1

        return FlavorStatus(
            name=flavor.name,
            nFreeWorkers=freeWorkers,
            workerStatuses=workerStatuses,
        )

    def getClusterStatus(self, instrument: str = "LSSTCam", detailed: bool = False) -> ClusterStatus:
        """Get comprehensive status information for the cluster.

        Parameters
        ----------
        instrument : str, optional
            Instrument to get status for
        detailed : bool, optional
            Whether to include detailed queue information

        Returns
        -------
        ClusterStatus
            Comprehensive status information for the cluster
        """
        # Check all pod flavors except HEAD_NODE
        flavors = [f for f in PodFlavor if f != PodFlavor.HEAD_NODE]
        flavorStatuses: dict[PodFlavor, FlavorStatus] = {}

        # Get information for each pod flavor
        for flavor in flavors:
            flavorStatus = self.getStatusForPodFlavor(flavor, instrument, detailed)
            flavorStatuses[flavor] = flavorStatus

        # Check if any raw data queues exist
        rawQueue = f"INCOMING-{instrument}-raw"
        rawQueueLength = self.redis.llen(rawQueue)

        return ClusterStatus(
            instrument=instrument, flavorStatuses=flavorStatuses, rawQueueLength=rawQueueLength
        )

    def printClusterStatus(
        self, clusterStatus: ClusterStatus | None = None, detailed: bool = False, ignoreFree: bool = True
    ) -> None:
        """Print status information for the cluster.

        Print status provided, if provided, otherwise fetch the status and
        print it.

        Parameters
        ----------
        detailed : bool, optional
            Whether to print detailed queue information
        ignoreFree : bool, optional
            Whether to ignore free workers with empty queues
        """
        if clusterStatus is None:
            clusterStatus = self.getClusterStatus(detailed=detailed)
        assert clusterStatus is not None

        allTables = []
        summaryTable = []

        # Process each flavor's data for display
        for podFlavour, flavorStatus in clusterStatus.flavorStatuses.items():
            if flavorStatus.totalWorkers == 0:
                continue

            tableData = []

            for wStatus in flavorStatus.workerStatuses:
                # Skip free workers if requested
                if ignoreFree and not wStatus.isBusy and wStatus.queueLength == 0:
                    continue

                # Add visual indicators
                status = "⚠️ BUSY" if wStatus.isBusy else "✅ FREE"
                queueIndicator = "❌" * min(wStatus.queueLength, 8)  # Limit to 8 crosses max

                # Format detector info
                detectorInfo = (
                    f"{wStatus.worker.detectorNumber}" if wStatus.worker.detectorNumber is not None else "N/A"
                )

                tableData.append(
                    [
                        wStatus.worker.queueName,
                        detectorInfo,
                        f"{wStatus.queueLength} {queueIndicator}",
                        status,
                    ]
                )

                # If there are items in the queue, show them
                if detailed:
                    for item in wStatus.queueItems:
                        tableData.append([f"  └─ Item {item.index}", "", f"{item.who}", f"{item.dataIdInfo}"])

            if tableData:
                allTables.append((podFlavour.name, tableData))

            # Add to summary table
            summaryTable.append(
                [podFlavour.name, f"{flavorStatus.totalWorkers} (Free: {flavorStatus.nFreeWorkers})"]
            )

        # Print results
        print(f"\nQueue Status for {clusterStatus.instrument} Workers:")
        print("=" * 80)

        for podFlavourName, tableData in allTables:
            print(f"\n{podFlavourName} Workers:")
            print(
                tabulate(
                    tableData, headers=["Queue Name", "Detector", "Queue Length", "Status"], tablefmt="grid"
                )
            )

        # Print summary
        print("\nWorker Summary:")
        print(tabulate(summaryTable, headers=["Worker Type", "Count"], tablefmt="simple"))

        # Show raw queue info if any
        if clusterStatus.rawQueueLength > 0:
            queueIndicator = "❌" * min(clusterStatus.rawQueueLength, 10)
            print(f"\nIncoming Raw Data Queue: {clusterStatus.rawQueueLength} items {queueIndicator}")

    def executeRubinTvCommands(self) -> None:
        """Dispatch RestartPayloads to pods based on commands from RubinTV.

        This method checks for specific Redis keys and sends RestartPayloads to
        the appropriate workers based on the keys that are set.
        """
        inst = "LSSTCam"
        restartPayload = RestartPayload()

        if self.redis.getdel("RUBINTV_CONTROL_RESET_SFM_SET_0"):
            self.log.info("Resetting SFM Set 0 workers (Imaging Worker Set 1 on RubinTV)")
            sfmSet0set = SfmWorkerSet.create(inst, depth=0)
            for pod in sfmSet0set.pods:
                self.rh.enqueuePayload(restartPayload, pod)

        if self.redis.getdel("RUBINTV_CONTROL_RESET_SFM_SET_1"):
            self.log.info("Resetting SFM Set 1 workers (Imaging Worker Set 2 on RubinTV)")
            sfmSet1set = SfmWorkerSet.create(inst, depth=1)
            for pod in sfmSet1set.pods:
                self.rh.enqueuePayload(restartPayload, pod)

        if self.redis.getdel("RUBINTV_CONTROL_RESET_SFM_STEP1B_SET_0"):
            status = self.getClusterStatus()
            nStep1b = len(status.flavorStatuses[PodFlavor.STEP1B_WORKER].workerStatuses)
            self.log.info(f"Resetting {nStep1b} SFM Step1b workers")
            sfmStep1bStep = Step1bWorkerSet.create(inst, PodFlavor.STEP1B_WORKER, nStep1b)
            for pod in sfmStep1bStep.pods:
                self.rh.enqueuePayload(restartPayload, pod)

        if self.redis.getdel("RUBINTV_CONTROL_RESET_AOS_SET_0"):
            self.log.info("Resetting AOS Set 0 workers (CWFS worker set 1 on RubinTV)")
            aosSet0set = AosWorkerSet.create(inst, range(0, 8))
            for pod in aosSet0set.pods:
                self.rh.enqueuePayload(restartPayload, pod)

        if self.redis.getdel("RUBINTV_CONTROL_RESET_AOS_SET_1"):
            self.log.info("Resetting AOS Set 1 workers (CWFS worker set 2 on RubinTV)")
            aosSet1set = AosWorkerSet.create(inst, range(8, 16))
            for pod in aosSet1set.pods:
                self.rh.enqueuePayload(restartPayload, pod)

        if self.redis.getdel("RUBINTV_CONTROL_RESET_AOS_SET_2"):
            self.log.info("Resetting AOS Set 2 workers (CWFS worker set 3 on RubinTV)")
            aosSet2set = AosWorkerSet.create(inst, range(16, 24))
            for pod in aosSet2set.pods:
                self.rh.enqueuePayload(restartPayload, pod)

        if self.redis.getdel("RUBINTV_CONTROL_RESET_AOS_SET_3"):
            self.log.info("Resetting AOS Set 3 workers (CWFS worker set 4 on RubinTV)")
            aosSet3set = AosWorkerSet.create(inst, range(24, 32))
            for pod in aosSet3set.pods:
                self.rh.enqueuePayload(restartPayload, pod)

        if self.redis.getdel("RUBINTV_CONTROL_RESET_AOS_STEP1B_SET_0"):
            status = self.getClusterStatus()
            nStep1b = len(status.flavorStatuses[PodFlavor.STEP1B_AOS_WORKER].workerStatuses)
            self.log.info(f"Resetting {nStep1b} AOS Step1b workers")
            aosStep1bset = Step1bWorkerSet.create(inst, PodFlavor.STEP1B_AOS_WORKER, nStep1b)
            for pod in aosStep1bset.pods:
                self.rh.enqueuePayload(restartPayload, pod)

    def sendStatusToRubinTV(self, status: ClusterStatus) -> None:
        pipe = self.redis.pipeline()
        totalUpdates = 0

        # Process SFM sets and AOS sets
        for flavor, redisKey in step1aMap.items():
            statuses = status.flavorStatuses[flavor]
            statesByDepth: dict[str, dict[str, Any]] = {}  # Group states by depth-specific streams

            # Collect all detector states for this set
            for workerStatus in statuses.workerStatuses:
                w = workerStatus.worker
                streamKey = f"stream:{redisKey}_{w.depth}"
                det = w.detectorNumber if w.detectorNumber else 0

                # Initialize state dict for this depth if not exists
                if streamKey not in statesByDepth:
                    statesByDepth[streamKey] = {}

                # Format status
                if workerStatus.isBusy is False:
                    value = "free"
                else:
                    value = str(workerStatus.queueLength) if workerStatus.queueLength > 0 else "busy"

                statesByDepth[streamKey][str(det)] = {
                    "status": value,
                    "type": "worker_status",
                }

            # Send updates for each depth-specific stream
            for streamKey, currentState in statesByDepth.items():
                if currentState != self._lastRubinTVStates.get(streamKey, {}):
                    pipe.xadd(
                        streamKey,
                        {"data": json.dumps(currentState)},
                        maxlen=2,
                        approximate=True,
                    )
                    self._lastRubinTVStates[streamKey] = currentState
                    totalUpdates += 1

        # Process step1b sets and spare workers
        for flavor, redisKey in flatSetMap.items():
            streamKey = f"stream:{redisKey}"
            currentState = {}

            for workerStatus in status.flavorStatuses[flavor].workerStatuses:
                w = workerStatus.worker
                if workerStatus.isBusy is False:
                    value = "free"
                else:
                    value = str(workerStatus.queueLength) if workerStatus.queueLength > 0 else "busy"

                currentState[str(w.depth)] = {"status": value, "type": "worker_status"}

            if currentState != self._lastRubinTVStates.get(streamKey, {}):
                pipe.xadd(
                    streamKey,
                    {"data": json.dumps(currentState)},
                    maxlen=2,
                    approximate=True,
                )
                self._lastRubinTVStates[streamKey] = currentState
                totalUpdates += 1

        # Handle remaining queues
        streamKey = "stream:CLUSTER_STATUS_OTHER_QUEUES"
        currentState = {}

        exclude = step1aMap.keys() | flatSetMap.keys()
        for flavor, flavorStatus in status.flavorStatuses.items():
            if flavor in exclude:
                continue

            totalQueue = sum(w.queueLength for w in flavorStatus.workerStatuses)
            if totalQueue > 0:
                currentState[flavor.name] = {
                    "status": str(totalQueue),
                    "type": "text_status",
                }

        if currentState != self._lastRubinTVStates.get(streamKey, {}):
            pipe.xadd(streamKey, {"data": json.dumps(currentState)}, maxlen=2, approximate=True)
            self._lastRubinTVStates[streamKey] = currentState
            totalUpdates += 1

        # Execute all updates in a single transaction
        if totalUpdates > 0:
            pipe.execute()

    def rebalanceSfmWorkers(self, status: ClusterStatus) -> None:
        nBacklogFree = status.flavorStatuses[PodFlavor.BACKLOG_WORKER].nFreeWorkers
        if nBacklogFree == 0:
            return

        freeBacklogWorkers = [
            ws.worker
            for ws in status.flavorStatuses[PodFlavor.BACKLOG_WORKER].workerStatuses
            if ws.isBusy is False
        ]
        assert len(freeBacklogWorkers) == nBacklogFree

        sfmStatuses = status.flavorStatuses[PodFlavor.SFM_WORKER]
        # make an ordered dict of queue lengths so we always rebalance the
        # worst backlogs first
        queueLengths = dict(
            sorted(
                {ws.worker.queueName: ws.queueLength for ws in sfmStatuses.workerStatuses}.items(),
                key=lambda x: x[1],
                reverse=True,
            )
        )

        i = 0
        for queueName, length in queueLengths.items():
            if length == 0:
                return

            if i == nBacklogFree:  # check this *before* dequeueing!
                self.log.info("Finished rebalancing to all the available backlog workers")
                return

            # grab the destination worker before dequeueing, just in case
            backlogWorker = freeBacklogWorkers[i]
            pod = PodDetails.fromQueueName(queueName)
            payload = self.rh.dequeuePayload(pod)
            if payload is None:  # if length is 1 this happens quite often, but above that is much weirder
                if length > 1:
                    warning = f"Dequeued empty payload from {queueName} which should have had {length=}"
                    self.log.warning(warning)
                continue

            if isRestartPayload(payload):  # restart's must not be moved - they're targeted to a specific pod
                self.rh.enqueuePayload(payload, pod)
            else:
                self.log.info(f"Rebalancing payload from {queueName} with queue {length=} to {backlogWorker}")
                self.rh.enqueuePayload(payload, backlogWorker)
                i += 1

        return

    def run(self):
        """Main loop to monitor and manage the cluster."""
        while True:
            try:
                self.executeRubinTvCommands()
                status = self.getClusterStatus()
                self.sendStatusToRubinTV(status)
                self.rebalanceSfmWorkers(status)
            except Exception as e:
                self.log.exception(f"Error in cluster management: {e}")
            finally:
                sleep(0.5)
