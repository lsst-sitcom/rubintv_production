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
from typing import TYPE_CHECKING

from tabulate import tabulate

from .payloads import Payload
from .podDefinition import PodDetails, PodFlavor
from .redisUtils import RedisHelper

if TYPE_CHECKING:
    from lsst.daf.butler import Butler

    from .utils import LocationConfig


step1aMap = {
    "SFM_WORKER": "CLUSTER_STATUS_SFM_SET",
    "AOS_WORKER": "CLUSTER_STATUS_AOS_SET",
}
step1bMap = {
    "STEP1B_WORKER": "CLUSTER_STATUS_SFM_STEP1B_SET_0",
    "STEP1B_AOS_WORKER": "CLUSTER_STATUS_AOS_STEP1B_SET_0",
    # "???": "CLUSTER_STATUS_SPAREWORKERS_SET_0",
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
    totalWorkers: int
    freeWorkers: int
    workers: list[WorkerStatus]


@dataclass
class ClusterStatus:
    """Overall cluster status information."""

    instrument: str
    flavorStatuses: dict[str, FlavorStatus]
    rawQueueLength: int


class ClusterManager:
    def __init__(self, locationConfig: LocationConfig, butler: Butler, doRaise: bool = False) -> None:
        self.locationConfig = locationConfig
        self.butler = butler
        self.doRaise = doRaise
        self.rh = RedisHelper(butler, locationConfig)
        self.redis = self.rh.redis
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
            return FlavorStatus(name=flavor.name, totalWorkers=0, freeWorkers=0, workers=[])

        workerStatuses = []
        freeWorkers = 0

        for worker in sorted(workers):
            workerStatus = self.getWorkerStatus(worker, detailed)
            workerStatuses.append(workerStatus)

            if not workerStatus.isBusy:
                freeWorkers += 1

        return FlavorStatus(
            name=flavor.name, totalWorkers=len(workers), freeWorkers=freeWorkers, workers=workerStatuses
        )

    def getClusterStatus(self, instrument: str = "LSSTCam", detailed: bool = False) -> ClusterStatus:
        """Get comprehensive status information for the cluster.

        Parameters
        ----------
        instrument : str, default="LSSTCam"
            Instrument to get status for
        detailed : bool, default=False
            Whether to include detailed queue information

        Returns
        -------
        ClusterStatus
            Comprehensive status information for the cluster
        """
        # Check all pod flavors except HEAD_NODE
        flavors = [f for f in PodFlavor if f != PodFlavor.HEAD_NODE]
        flavorStatuses: dict[str, FlavorStatus] = {}

        # Get information for each pod flavor
        for flavor in flavors:
            flavorStatus = self.getStatusForPodFlavor(flavor, instrument, detailed)
            flavorStatuses[flavor.name] = flavorStatus

        # Check if any raw data queues exist
        rawQueue = f"INCOMING-{instrument}-raw"
        rawQueueLength = self.redis.llen(rawQueue)

        return ClusterStatus(
            instrument=instrument, flavorStatuses=flavorStatuses, rawQueueLength=rawQueueLength
        )

    def printClusterStatus(self, detailed: bool = False, ignoreFree: bool = True) -> None:
        """Print status information for the cluster.

        Parameters
        ----------
        detailed : bool, default=False
            Whether to print detailed queue information
        ignoreFree : bool, default=True
            Whether to ignore free workers with empty queues
        """
        clusterStatus = self.getClusterStatus(detailed=detailed)

        allTables = []
        summaryTable = []

        # Process each flavor's data for display
        for flavorName, flavorStatus in clusterStatus.flavorStatuses.items():
            if flavorStatus.totalWorkers == 0:
                continue

            tableData = []

            for wStatus in flavorStatus.workers:
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
                allTables.append((flavorName, tableData))

            # Add to summary table
            summaryTable.append(
                [flavorName, f"{flavorStatus.totalWorkers} (Free: {flavorStatus.freeWorkers})"]
            )

        # Print results
        print(f"\nQueue Status for {clusterStatus.instrument} Workers:")
        print("=" * 80)

        for flavorName, tableData in allTables:
            print(f"\n{flavorName} Workers:")
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

    def sendStatusToRubinTV(self, status: ClusterStatus) -> None:
        for flavor, redisKey in step1aMap.items():
            statuses = status.flavorStatuses[flavor]

            for workerStatus in statuses.workers:
                w = workerStatus.worker
                key = f"{redisKey}_{w.depth}"
                det = w.detectorNumber if w.detectorNumber else 0
                if workerStatus.isBusy is False:
                    value = "free"
                else:
                    value = str(workerStatus.queueLength) if workerStatus.queueLength > 0 else "busy"
                self.redis.hset(key, str(det), value)

        for flavor, redisKey in step1bMap.items():
            statuses = status.flavorStatuses[flavor]

            for workerStatus in statuses.workers:
                w = workerStatus.worker
                depth = w.depth
                if workerStatus.isBusy is False:
                    value = "free"
                else:
                    value = str(workerStatus.queueLength) if workerStatus.queueLength > 0 else "busy"

                self.redis.hset(redisKey, str(depth), value)
            self.redis.hset(redisKey, "num_workers", len(statuses.workers))

    def run(self):
        """Main loop to monitor and manage the cluster."""
        while True:
            try:
                status = self.getClusterStatus()
                self.sendStatusToRubinTV(status)
            except Exception as e:
                self.log.exception(f"Error in cluster management: {e}")
            finally:
                sleep(0.5)
