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

__all__ = "RedisHelper"


import json
import logging
import os
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable

import redis

from lsst.daf.butler import DataCoordinate, DimensionRecord

from .payloads import Payload
from .podDefinition import PodDetails, PodFlavor, getQueueName
from .utils import expRecordFromJson, removeDetector

# Check if the environment is a notebook
clear_output: Callable | None = None
IN_NOTEBOOK = False
try:
    from IPython import get_ipython

    ipython_instance = get_ipython()
    # Check if notebook and not just IPython terminal. ipython_instance is None
    # if not in IPython environment, and if IPKernelApp is not in the config
    # then it's not a notebook, just an ipython terminal.
    if ipython_instance is None or "IPKernelApp" not in ipython_instance.config:
        IN_NOTEBOOK = False
    else:
        from IPython.display import clear_output

        IN_NOTEBOOK = True
except (ImportError, NameError):
    pass


if TYPE_CHECKING:
    from lsst.daf.butler import Butler

    from .utils import LocationConfig


CONSDB_ANNOUNCE_EXPIRY_TIME = 86400 * 2


def decode_string(value: bytes) -> str:
    """Decode a string from bytes to UTF-8.

    Parameters
    ----------
    value : bytes
        Bytes value to decode.

    Returns
    -------
    str
        Decoded string.
    """
    return value.decode("utf-8")


def decode_hash(hash_dict: dict[bytes, bytes]) -> dict[str, str]:
    """Decode a hash dictionary from bytes to UTF-8.

    Parameters
    ----------
    hash_dict : dict
        Dictionary with bytes keys and values.

    Returns
    -------
    dict
        Dictionary with decoded keys and values.
    """
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in hash_dict.items()}


def decode_list(value_list: list[bytes]) -> list[str]:
    """Decode a list of values from bytes to UTF-8.

    Parameters
    ----------
    value_list : list
        List of bytes values to decode.

    Returns
    -------
    list
        List of decoded values.
    """
    return [item.decode("utf-8") for item in value_list]


def decode_set(value_set: set[bytes]) -> set[str]:
    """Decode a set of values from bytes to UTF-8.

    Parameters
    ----------
    value_set : set
        Set of bytes values to decode.

    Returns
    -------
    set
        Set of decoded values.
    """
    return {item.decode("utf-8") for item in value_set}


def decode_zset(value_zset: list[tuple[Any, float]]) -> list[tuple[str, float]]:
    """Decode a zset of values from bytes to UTF-8.

    Parameters
    ----------
    value_zset : list
        List of tuple with bytes values and scores to decode.

    Returns
    -------
    list
        List of tuples with decoded values and scores.
    """
    return [(item[0].decode("utf-8"), item[1]) for item in value_zset]


def getRedisSecret(filename: str = "$HOME/.lsst/redis_secret.ini") -> str:
    filename = os.path.expandvars(filename)
    with open(filename) as f:
        return f.read().strip()


def getNewDataQueueName(instrument: str) -> str:
    return f"INCOMING-{instrument}-raw"


def _extractExposureIds(exposureBytes: bytes, instrument: str) -> list[int]:
    """Extract the exposure IDs from the byte string.

    Parameters
    ----------
    exposureBytes : `bytes`
        The byte string containing the exposure IDs.

    Returns
    -------
    expIds : `list` of `int`
        A list of two exposure IDs extracted from the byte string.

    Raises
    ------
    ValueError
        If the number of exposure IDs extracted is not equal to 2.
    """
    exposureIdStrs = exposureBytes.decode("utf-8").split(",")
    exposureIds = [int(v) for v in exposureIdStrs]

    if instrument == "LSSTComCamSim":
        # simulated exp ids are in the year 702X so add this manually, as
        # OCS doesn't know about the fact the butler will add this on. This
        # is only true for LSSTComCamSim though.
        log = logging.getLogger("lsst.rubintv.production.redisUtils._extractExposureIds")
        log.info(f"Adding 5000000000000 to {exposureIds=} to adjust for simulated LSSTComCamSim data")
        exposureIds = [expId + 5000000000000 for expId in exposureIds]
    return exposureIds


class RedisHelper:
    def __init__(self, butler: Butler, locationConfig: LocationConfig, isHeadNode: bool = False) -> None:
        self.butler = butler  # needed to expand dataIds when dequeuing payloads
        self.locationConfig = locationConfig
        self.isHeadNode = isHeadNode
        self.redis = self._makeRedis()
        self._testRedisConnection()
        self.log = logging.getLogger("lsst.rubintv.production.redisUtils.RedisHelper")
        self._loggedAbout: set[str] = set()

    def _makeRedis(self) -> redis.Redis:
        """Create a redis connection.

        Returns
        -------
        redis.Redis
            The redis connection.
        """
        host: str = os.getenv("REDIS_HOST", "")
        password = os.getenv("REDIS_PASSWORD")
        port: int = int(os.getenv("REDIS_PORT", 6379))
        return redis.Redis(host=host, password=password, port=port)

    def _testRedisConnection(self) -> None:
        """Check that redis is online and can be contacted.

        Raises
        ------
        RuntimeError:
            Raised if redis can't be contacted.
            Raised on any other unexpected error.
        """
        try:
            self.redis.ping()
        except redis.exceptions.ConnectionError as e:
            raise RuntimeError("Could not connect to redis - is it running?") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error connecting to redis: {e}") from e

    def affirmRunning(self, pod: PodDetails, timePeriod: int | float) -> None:
        """Affirm that the named pod is running OK and should not be considered
        dead for `timePeriod` seconds.

        Parameters
        ----------
        podName : `str`
            The name of the pod.
        timePeriod : `float`
            The amount of time after which the pod would be considered dead if
            not reaffirmed by.
        """
        self.redis.setex(f"{pod.queueName}+IS_RUNNING", timedelta(seconds=timePeriod), value=1)

    def confirmRunning(self, pod: PodDetails) -> bool:
        """Check whether the named pod is running or should be considered dead.

        Parameters
        ----------
        podName : `str`
            The name of the pod.
        """
        isRunning = self.redis.get(f"{pod.queueName}+IS_RUNNING")
        return bool(isRunning)  # 0 and None both bool() to False

    def announceBusy(self, pod: PodDetails) -> None:
        """Announce that a worker is busy processing a queue.

        Parameters
        ----------
        queueName : `str`
            The name of the queue the worker is processing.
        """
        self.redis.set(f"{pod.queueName}+IS_BUSY", value=1)

    def announceFree(self, pod: PodDetails) -> None:
        """Announce that a worker is free to process a queue.

        Implies a call to `announceExistence` as you have to exist to be free.

        Parameters
        ----------
        queueName : `str`
            The name of the queue the worker is processing.
        """
        self.announceExistence(pod)
        self.redis.delete(f"{pod.queueName}+IS_BUSY")

    def announceExistence(self, pod: PodDetails, remove: bool = False) -> None:
        """Announce that a worker is present in the pool.

        Currently this is set to 30s expiry, and is reasserted with each call
        to `announceFree`. This is to ensure that if a worker dies, the queue
        will be freed up for another worker to take over. It shouldn't matter
        much if this expires during processing, so this doesn't need to be
        greater than the longest SFM pipeline execution, which would be ~200s.
        If we were to pick that, then work could land on a dead queue and sit
        around for quite some time.

        Parameters
        ----------
        queueName : `str`
            The name of the queue the worker is processing.
        remove : `bool`, optional
            Remove the worker from pool. Default is ``False``.
        """
        if not remove:
            self.redis.setex(f"{pod.queueName}+EXISTS", timedelta(seconds=30), value=1)
        else:
            self.redis.delete(f"{pod.queueName}+EXISTS")

    def getAllWorkers(self, instrument: str, podFlavor: PodFlavor) -> list[PodDetails]:
        """Get the list of workers that are currently active.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        podFlavor : `PodFlavor`
            The type of worker to get, e.g. "SFM". The default of ``None``
            will return all worker types.

        Returns
        -------
        workers : `list` of `PodDetails`
            The list of workers that are currently active.
        """
        # need to get the set of things that exist, or are busy, because
        # things "cease to exist" during long processing runs, but they do
        # still show as busy

        queueName = getQueueName(podFlavor, instrument, "*", "*")

        existing = self.redis.keys(f"{queueName}+EXISTS")
        existing = [key.decode("utf-8").replace("+EXISTS", "") for key in existing]

        busy = self.redis.keys(f"{queueName}+IS_BUSY")
        busy = [key.decode("utf-8").replace("+IS_BUSY", "") for key in busy]

        allWorkerQueues = sorted(set(existing + busy))

        allWorkers = [PodDetails.fromQueueName(queueName) for queueName in allWorkerQueues]

        return allWorkers

    def getFreeWorkers(self, instrument, podFlavor: PodFlavor) -> list[PodDetails]:
        """Get the list of workers that are currently free.

        Parameters
        ----------
        workerType : `str`, optional
            The type of worker to get, e.g. "SFM". The default of ``None``
            will return all worker types.

        Returns
        -------
        workers : `list` of `PodDetails`
            The list of workers that are currently free.
        """
        workers = []
        allWorkers = self.getAllWorkers(instrument=instrument, podFlavor=podFlavor)
        for worker in allWorkers:
            if not self.redis.get(f"{worker.queueName}+IS_BUSY"):
                workers.append(worker)
        return sorted(workers)

    def pushToButlerWatcherList(self, instrument, expRecord) -> None:
        """Keep a record of what's been found by the butler watcher for all
        time.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord`
            The exposure record to push to the list.
        """
        expRecordJson = expRecord.to_simple().json()
        self.redis.lpush(f"{instrument}-fromButlerWacher", expRecordJson)

    def reportTaskFinished(
        self, instrument: str, taskName: str, dataId: DataCoordinate, failed=False
    ) -> None:
        """Report that a task has finished, be that a real DM Task or simply
        that something has happened which needs to be triggered on.

        The detector number is removed from the dataId, and we count the number
        of tasks which finish for that dataId.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        taskName : `str`
            The name of the task that has finished processing.
        dataId : `DataCoordinate`
            The dataId the task has finished processing.
        """
        key = f"{instrument}-{taskName}-FINISHEDCOUNTER"
        dataIdNoDetector = removeDetector(dataId, self.butler)
        processingId = dataIdNoDetector.to_json().encode("utf-8")
        self.redis.hincrby(key, processingId, 1)  # creates the key if it doesn't exist

        if failed:  # fails have finished too, so increment finished and failed
            key = key.replace("FINISHEDCOUNTER", "FAILEDCOUNTER")
            self.redis.hincrby(key, processingId, 1)  # creates the key if it doesn't exist

    def getNumTaskFinished(self, instrument: str, taskName: str, dataId: DataCoordinate) -> int:
        """Get the number of items finished for a given task and id.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        taskName : `str`
            The name of the task that has finished processing.
        dataId : `DataCoordinate`
            The dataId the task has finished processing.

        Returns
        -------
        numFinished : `int`
            The number of times the task has finished.
        """
        key = f"{instrument}-{taskName}-FINISHEDCOUNTER"
        dataIdNoDetector = removeDetector(dataId, self.butler)
        processingId = dataIdNoDetector.to_json().encode("utf-8")
        value = self.redis.hget(key, processingId)
        return int(value or 0)

    def getAllDataIdsForTask(self, instrument: str, taskName: str) -> list[DataCoordinate]:
        """Get a list of processed ids for the specified task

        These are returned
        """
        key = f"{instrument}-{taskName}-FINISHEDCOUNTER"
        idList = self.redis.hgetall(key).keys()  # list of bytes
        return [
            DataCoordinate.from_json(dataCoordJson, universe=self.butler.dimensions)
            for dataCoordJson in idList
        ]

    def removeTaskCounter(self, instrument: str, taskName: str, dataId: DataCoordinate) -> None:
        """Once a gather step is finished with all the expected data present,
        remove the counter from the tracking dictionary to save reprocessing it
        each time.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        taskName : `str`
            The name of the task that has finished processing.
        dataId : `DataCoordinate`
            The dataId the task has finished processing.
        """
        key = f"{instrument}-{taskName}-FINISHEDCOUNTER"
        dataIdNoDetector = removeDetector(dataId, self.butler)
        processingId = dataIdNoDetector.to_json().encode("utf-8")
        if self.redis.hexists(key, processingId):
            self.redis.hdel(key, processingId)
        else:
            self.log.warning(
                f"Key {key} with processingId {processingId.decode('utf-8')} from {dataId} for {taskName}"
                " did not exist when removal was attempted"
            )

    def reportDetectorLevelFinished(
        self, instrument: str, step: str, who: str, processingId: str, failed=False
    ) -> None:
        """Count the number of times a detector-level pipeline has finished.

        Increments the FINISHEDCOUNTER for the key corresponding to the
        instrument, step, and who. If the processing failed, the FAILEDCOUNTER
        is also incremented.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        step : `str`
            The name of the step which finished processing.
        who : `str`
            Who are we running the pipeline for, e.g. "SFM" or "AOS".
        processingId : `str`
            The processing id, either single or compound.
        failed : `bool`
            True if the processing did not fail to complete
        """
        key = f"{instrument}-{step}-{who}-FINISHEDCOUNTER"
        self.redis.hincrby(key, processingId, 1)  # creates the key if it doesn't exist

        if failed:  # fails have finished too, so increment finished and failed
            key = key.replace("FINISHEDCOUNTER", "FAILEDCOUNTER")
            self.redis.hincrby(key, processingId, 1)  # creates the key if it doesn't exist

    def getNumDetectorLevelFinished(self, instrument: str, step: str, who: str, processingId: str) -> int:
        """Get the number of times a visit-level pipeline has finished.

        Returns the number of times the step has finished for the given
        processingId.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        step : `str`
            The name of the step which finished processing.
        who : `str`
            Whose pipeline is the step counter for e.g. "SFM" or "AOS".
        processingId : `str`
            The processing id, either single or compound.

        Returns
        -------
        numFinished : `int`
            The number of times the step has finished.
        """
        key = f"{instrument}-{step}-{who}-FINISHEDCOUNTER"
        if not self.redis.hexists(key, processingId):
            self.log.warning(f"Key {key} with processingId {processingId} does not exist")
        return int(self.redis.hget(key, processingId) or 0)

    def getAllIdsForDetectorLevel(self, instrument: str, step: str, who: str) -> list[str]:
        """Get a list of processed ids for the specified step."""
        key = f"{instrument}-{step}-{who}-FINISHEDCOUNTER"
        idList = self.redis.hgetall(key).keys()
        return [procId.decode("utf-8") for procId in idList]

    def removeFinishedIdDetectorLevel(self, instrument: str, step: str, who: str, processingId: str) -> None:
        """Remove the specified counter for the processingId from the list of
        finishing ids.
        """
        key = f"{instrument}-{step}-{who}-FINISHEDCOUNTER"
        if self.redis.hexists(key, processingId):
            self.redis.hdel(key, processingId)
        else:
            self.log.warning(
                f"Key {key} with processingId {processingId} did not exist when removal was attempted"
            )

    def reportVisitLevelFinished(self, instrument: str, step: str, who: str, failed=False) -> None:
        """Count the number of times a visit-level pipeline has finished.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        step : `str`
            The name of the step which finished processing.
        who : `str`
            Who are we running the pipeline for, e.g. "SFM" or "AOS".
        failed : `bool`
            True if the processing did not fail to complete
        """
        key = f"{instrument}-{step}-{who}-FINISHEDCOUNTER"
        self.redis.incr(key, 1)  # creates the key if it doesn't exist

        if failed:  # fails have finished too, so increment finished and failed
            key = key.replace("FINISHEDCOUNTER", "FAILEDCOUNTER")
            self.redis.incr(key, 1)  # creates the key if it doesn't exist

    def getNumVisitLevelFinished(self, instrument: str, step: str, who: str) -> int:
        """Get the number of times a visit-level pipeline has finished.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        step : `str`
            The name of the step which finished processing.
        who : `str`
            Whose pipeline is the step counter for e.g. "SFM" or "AOS".

        Returns
        -------
        numFinished : `int`
            The number of times the step has finished.
        """
        key = f"{instrument}-{step}-{who}-FINISHEDCOUNTER"
        return int(self.redis.get(key) or 0)

    def reportNightLevelFinished(self, instrument: str, who: str, failed=False) -> None:
        """Count the number of times a night-level pipeline has finished.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        failed : `bool`
            True if the processing did not fail to complete
        """
        key = f"{instrument}-{who}-NIGHTLYROLLUP-FINISHEDCOUNTER"
        self.redis.incr(key, 1)

    def checkButlerWatcherList(self, instrument: str, expRecord) -> bool:
        """Check if an exposure record has already been processed because it
        was seen by the ButlerWatcher.

        This is because, when a butler watcher restarts, it will always find
        the most recent exposure record in the repo. We don't want to always
        issue these for processing, so we keep a list of what's been seen.

        Note that this is checked by the ButlerWatcher itself, and thus,
        *intentionally*, a general RedisHelper can still call
        `helper.pushNewExposureToHeadNode(record)` in order to send anything
        for processing, as this will be pulled in and fanned out as usual.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord`
            The exposure record to check.

        Returns
        -------
        bool
            Whether the exposure record has already been processed.
        """
        expRecordJson = expRecord.to_simple().json()

        data = self.redis.lrange(f"{instrument}-fromButlerWacher", 0, -1)
        recordStrings = [item.decode("utf-8") for item in data]
        return expRecordJson in recordStrings

    def _checkIsHeadNode(self) -> None:
        """Note: this isn't how atomicity of transactions is ensured, this is
        just to make sure workers don't accidentally try to pop straight from
        the main queue.
        """
        if not self.isHeadNode:
            raise RuntimeError("This function is only for the head node - consume your queue, worker!")

    def getRemoteCommands(self) -> list[dict]:
        """Get any remote commands that have been sent to the head node.

        Returns
        -------
        commands : `list` of `dict` : `dict`
            The commands that have been sent. As a list of dicts, where each
            dict has a single string key, which is the function to be called,
            and dict value, optionally populated with kwargs to call that
            function with.

            For example, to ensure the guiders are on, the wavefronts are off,
            and we have a phase-0 per-CCD checkerboard, the received commands
            would look like:

            commands = [
                {'setAllOff': {}},
                {'setFullCheckerboard': {'phase': 0}},
                {'setGuidersOn': {}},
            ]
        """
        commands = []
        while command := self.redis.rpop("commands"):  # rpop for FIFO
            commands.append(json.loads(command.decode("utf-8")))

        return commands

    def pushNewExposureToHeadNode(self, expRecord: DimensionRecord) -> None:
        """Send an exposure record for processing.

        This queue is consumed by the head node, which fans it out for
        processing by the workers. Which detetors for the exposure will be
        processed is determined by state of the ``focalPlaneControl`` on the
        head node at the time the exposure record is fanned out.

        The queue can have any length, and will be consumed last-in-first-out.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record to process.
        """
        instrument = expRecord.instrument
        queueName = getNewDataQueueName(instrument)
        expRecordJson = expRecord.to_simple().json()
        self.redis.lpush(queueName, expRecordJson)

    def announceResultInConsDb(self, instrument: str, table: str, obsId: int) -> None:
        """
        Parameters
        ----------
        instrument : `str`
            The instrument name.
        table : `str`
            The table name.
        obsId : `int`
            The obsId that was used in the consDbClient.insert() call.
        """
        # The call to .lower() is because consDB is all lower case, and we
        # don't want to be sensitive to table capitalization or regular butler
        # instrument name capitalization.
        key = f"consdb-{instrument}-{table}-{obsId}".lower()

        if not self.redis.exists(key):
            self.redis.lpush(key, 1)
        self.redis.expire(key, CONSDB_ANNOUNCE_EXPIRY_TIME)

    def waitForResultInConsdDb(self, instrument: str, table: str, obsId: int, timeout=None) -> bool:
        """Wait for an item to be available in consDB.

        NB: this function is only appropriate for items less than 2 days old,
        anything older than that should be assumed to be there, or not, but not
        waited for.

        This function blocks execution and waits for the data to land. It does
        not retrieve the item, but simply provides the necessary wait for the
        item to be available. The default timeout of ``None`` is an indefinite
        wait.

        If the result landed, ``True`` is returned, otherwise, if the timeout
        elapsed, ``False`` is returned.

        Parameters
        ----------
        instrument : `str`
            The instrument name.
        table : `str`
            The table name.
        obsId : `int`
            The obsId that was used in the consDbClient.insert() call.
        timeout : `float`, optional
            The maximum time to wait for the item to appear, in seconds. The
            default of ``None`` is to wait indefinitely.

        Return
        ------
        found : `bool`
            Was the item found, or did we return because of a timeout?
        """
        # this key needs to match the key used in announceResultInConsDb -
        # maybe add a private method for that (and all the other keys that are
        # tied together)
        key = f"consdb-{instrument}-{table}-{obsId}".lower()

        if timeout is None:
            timeout = 0

        # Wait for an item to appear and put it straight back for others to use
        item = self.redis.blmove(key, key, timeout, "LEFT", "RIGHT")
        if item:
            return True
        else:
            return False

    def getExposureForFanout(self, instrument: str) -> DimensionRecord | None:
        """Get the next exposure to process for the specified instrument.

        Parameters
        ----------
        instrument : `str`
            The instrument to get the next exposure for.

        Returns
        -------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector, or
            ``None`` if the queue is empty.
        """
        self._checkIsHeadNode()
        queueName = getNewDataQueueName(instrument)
        expRecordJson = self.redis.lpop(queueName)
        if expRecordJson is None:
            return None
        return expRecordFromJson(expRecordJson, self.locationConfig)

    def checkForOcsDonutPair(self, instrument: str) -> tuple[int, int] | None:
        """Get the next exposure to process for the specified instrument.

        Parameters
        ----------
        instrument : `str`
            The instrument to get the next exposure for.

        Returns
        -------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector, or
            ``None`` if the queue is empty.
        """
        self._checkIsHeadNode()
        queueName = f"{instrument}-FROM-OCS_DONUTPAIR"  # defined by Tiago, so just hard-coded here
        exposurePairBytes = self.redis.lpop(queueName)
        if exposurePairBytes is not None:
            exposureIds = _extractExposureIds(exposurePairBytes, instrument)
            if len(exposureIds) != 2:
                raise ValueError(f"Expected two exposureIds, got {exposureIds}")
            expId1, expId2 = exposureIds
            return expId1, expId2
        else:
            return None

    def enqueuePayload(self, payload: Payload, destinationPod: PodDetails, top=True) -> None:
        """Send a unit of work to a specific worker-queue.

        Parameters
        ----------
        payload : `lsst.rubintv.production.payloads.Payload`
            The payload to enqueue.
        queueName : `str`
            The name of the queue to enqueue the payload to.
        top : `bool`, optional
            Whether to add the payload to the top of the queue. Default is
            ``True``.
        """
        if top:
            self.redis.lpush(destinationPod.queueName, payload.to_json())
        else:
            self.redis.rpush(destinationPod.queueName, payload.to_json())

    def dequeuePayload(self, pod: PodDetails) -> Payload | None:
        """Get the next unit of work from a specific worker queue.

        Returns
        -------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector, or
            ``None`` if the queue is empty.
        """
        payLoadJson = self.redis.lpop(pod.queueName)
        if payLoadJson is None:
            return None
        return Payload.from_json(payLoadJson, self.butler)

    def clearTaskCounters(self) -> None:
        # TODO: DM-44102 check if .keys() is OK here
        keys = self.redis.keys("*EDCOUNTER*")  # FINISHEDCOUNTER and FAILEDCOUNTER
        for key in keys:
            self.redis.delete(key)

    def writeDetectorsToExpect(
        self, instrument: str, indentifier: int | str, detectors: list[int], who: str
    ) -> None:
        """Write the detectors we are processing for a given exposureId.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        indentifier : `int` or `str`
            The exposure or visit ID(s) the detectors are being processed for.
        detectors : `list` of `int`
            The list of detectors to expect.
        who : `str`
            Who are we running the pipeline for, e.g. "SFM" or "AOS".
        """
        key = f"{instrument}-EXPECTED_DETECTORS-{who}-{indentifier}"
        self.redis.set(key, ",".join(str(det) for det in detectors))
        self.redis.expire(key, 86400 * 2)  # expire in 2 days

    def getExpectedDetectors(self, instrument: str, indentifier: int | str, who: str) -> list[int]:
        """Get the expected detectors for a given exposure or visit ID.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        indentifier : `int` or `str`
            The exposure or visit ID(s).
        who : `str`
            Who are we running the pipeline for, e.g. "SFM" or "AOS".

        Returns
        -------
        detectors : `list` of `int` or `None`
            The list of expected detectors, or ``None`` if not found.
        """
        key = f"{instrument}-EXPECTED_DETECTORS-{who}-{indentifier}"
        value = self.redis.get(key)
        if value is None:
            if key not in self._loggedAbout:
                self._loggedAbout.add(key)
                self.log.warning(f"Key {key} not found in redis! Are you processing stale data?")
            return []
        return [int(det) for det in value.decode("utf-8").split(",")]

    def recordAosPipelineConfig(self, instrument: str, expId: int, pipelineName: str) -> None:
        """Record the pipeline configuration used for a given exposure ID.

        e.g. AOS_TIE or AOS_DANISH

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        expId : `int`
            The exposure ID.
        pipelineName : `str`
            The name of the pipeline configuration used.
        """
        key = f"{instrument}-AOS_PIPELINE_CONFIG-{expId}"
        self.redis.set(key, pipelineName)
        self.redis.expire(key, 86400 * 2)

    def getAosPipelineConfig(self, instrument: str, expId: int) -> str | None:
        """Get the pipeline configuration used for a given exposure ID.

        e.g. AOS_TIE or AOS_DANISH

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        expId : `int`
            The exposure ID.

        Returns
        -------
        pipelineName : `str` or `None`
            The name of the pipeline configuration used, or ``None`` if not
            found.
        """
        key = f"{instrument}-AOS_PIPELINE_CONFIG-{expId}"
        value = self.redis.get(key)
        if value is None:
            self.log.warning(f"Key {key} not found in redis! Are you processing stale data?")
            return None
        return value.decode("utf-8")

    def displayRedisContents(self, instrument: str | None = None, ignorePods: bool = True) -> None:
        """Get the next unit of work from a specific worker queue.

        Returns
        -------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector, or
            ``None`` if the queue is empty.
        """

        def _isPayload(jsonData) -> bool:
            try:
                loaded = json.loads(jsonData)
                _ = loaded["dataId"]
                return True
            except (KeyError, json.JSONDecodeError, TypeError):
                pass
            return False

        def _isExpRecord(jsonData) -> bool:
            try:
                loaded = json.loads(jsonData)
                _ = loaded["definition"]
                return True
            except (KeyError, json.JSONDecodeError, TypeError):
                pass
            return False

        def getPayloadDataId(jsonData) -> str:
            # XXX pretty sure this now crashes due to it being dataIds plural
            # check if you can get mypy to catch this when fixing
            loaded = json.loads(jsonData)
            return f"{loaded['dataId']}, run={loaded['run']}"

        def getExpRecordDataId(jsonData) -> str:
            loaded = json.loads(jsonData)
            expRecordStr = f"{loaded['record']['instrument']}, {loaded['record']['id']}"
            return expRecordStr

        def isPod(key: str) -> bool:
            return key.endswith("+EXISTS") or key.endswith("+IS_BUSY")

        r = self.redis

        # Get all keys in the database
        # TODO: .keys is a blocking operation - consider using .scan instead
        keys = sorted(r.keys("*"))

        if ignorePods:
            keys = [key for key in keys if not isPod(key.decode("utf-8"))]

        if not keys:
            print("Nothing in the Redis database.")
            return

        # Remove consDB announcements from the list
        keys = [key for key in keys if "consdb" not in decode_string(key)]
        if not keys:
            print("Nothing but consDB announcements in the Redis database.")
            return

        if instrument is not None:
            # filter to only the instrument-relevant ones if specified
            keys = [key for key in keys if instrument.lower() in decode_string(key).lower()]

        # TODO: DM-44102 Improve how all the redis monitoring stuff is done

        # any keys containing these strings will just have their lengths
        # printed, not the full contents of the lists
        lengthKeyPatterns = [
            "fromButlerWacher",
        ]

        def handleLengthKeys(key: str) -> None:
            # just print the key and the length of the list
            print(f"{key}: {r.llen(key)} items")

        for key in keys:
            key = decode_string(key)
            type_of_key = r.type(key).decode("utf-8")

            if any(pattern in key for pattern in lengthKeyPatterns):
                handleLengthKeys(key)
                continue

            # Handle different Redis data types
            if type_of_key == "string":
                toDecode = r.get(key)
                assert toDecode is not None
                value = decode_string(toDecode)
                print(f"{key}: {value}")
            elif type_of_key == "hash":
                hValues = decode_hash(r.hgetall(key))
                print(f"{key}: {hValues}")
            elif type_of_key == "list":
                listValues = decode_list(r.lrange(key, 0, -1))
                print(f"{key}:")
                indent = 2 * " "
                for item in listValues:
                    if _isPayload(item):
                        print(f"{indent}{getPayloadDataId(item)}")
                    elif _isExpRecord(item):
                        print(f"{indent}{getExpRecordDataId(item)}")
                    else:
                        print(f"{indent}{item}")
            elif type_of_key == "set":
                sValues = decode_set(r.smembers(key))
                print(f"{key}: {sValues}")
            elif type_of_key == "zset":
                zValues = decode_zset(r.zrange(key, 0, -1, withscores=True))
                print(f"{key}: {zValues}")
            else:
                print(f"Unsupported type for key: {key}")

    def clearRedis(self, force: bool = False, keepButlerWatcherHistory: bool = True) -> None:
        """Clear all keys in the Redis database.

        Parameters
        ----------
        force : `bool`, optional
            Whether to clear the Redis database without user confirmation.
            Default is ``False``.
        keepButlerWatcherHistory : `bool`, optional
            Whether to keep keys matching "*fromButlerWacher*". Default is
            ``True``.
        """
        if not force:
            print("Are you sure you want to clear the Redis database? This action cannot be undone.")
            print("Type 'yes' to confirm.")
            response = input()
            if response != "yes":
                print("Clearing aborted.")
                return

        if not keepButlerWatcherHistory:
            self.redis.flushdb()
            print("Redis database cleared.")
        else:
            # Get all keys and delete them selectively
            all_keys = self.redis.keys("*")
            for key in all_keys:
                key_str = key.decode("utf-8")
                if "fromButlerWacher" not in key_str:
                    self.redis.delete(key)
            print("Redis database cleared, but ButlerWatcher history retained.")

    def clearWorkerQueues(self, force: bool = False) -> None:
        """Clear all keys in the Redis database.

        Parameters
        ----------
        force : `bool`, optional
            Whether to clear the Redis database without user confirmation.
            Default is ``False``.
        """
        if not force:
            print("Are you sure you want to clear the worker queues?")
            print("Type 'yes' to confirm.")
            response = input()
            if response != "yes":
                print("Clearing aborted.")
                return

        # TODO: DM-44102 check if .keys() is OK here
        keys = self.redis.keys("*WORKER*")
        for key in keys:
            self.redis.delete(key)

    def monitorRedis(self, interval: float = 1) -> None:
        """Continuously display the contents of Redis database in the console.

        This function prints the entire contents of the redis database to
        either the console or the notebook every ``interval`` seconds. The
        console/notebook cell is cleared before each update.

        Parameters
        ----------
        interval : `float`, optional
            The time interval between each update of the Redis database
            contents. Default is every second.
        """
        while True:
            self.displayRedisContents()
            time.sleep(interval)

            # Clear the screen according to the environment
            if IN_NOTEBOOK:
                assert clear_output is not None
                clear_output(wait=True)
            else:
                print("\033c", end="")  # clear the terminal
