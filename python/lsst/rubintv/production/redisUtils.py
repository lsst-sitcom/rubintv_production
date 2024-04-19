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

import json
import redis
import logging
from datetime import timedelta
import os
import time

from lsst.summit.utils.utils import getSite

from .payloads import Payload
from .utils import expRecordFromJson

# Check if the environment is a notebook
clear_output = None
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

__all__ = (
    'RedisHelper'
)


def decode_string(value):
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
    return value.decode('utf-8')


def decode_hash(hash_dict):
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
    return {k.decode('utf-8'): v.decode('utf-8') for k, v in hash_dict.items()}


def decode_list(value_list):
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
    return [item.decode('utf-8') for item in value_list]


def decode_set(value_set):
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
    return {item.decode('utf-8') for item in value_set}


def decode_zset(value_zset):
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
    return [(item[0].decode('utf-8'), item[1]) for item in value_zset]


def getRedisSecret(filename='$HOME/.lsst/redis_secret.ini'):
    filename = os.path.expandvars(filename)
    with open(filename) as f:
        return f.read().strip()


def getNewDataQueueName(instrument):
    return f'INCOMING-{instrument}-raw'


class RedisHelper:
    def __init__(self, butler, locationConfig, isHeadNode=False):
        self.butler = butler  # needed to expand dataIds when dequeuing payloads
        self.locationConfig = locationConfig
        self.isHeadNode = isHeadNode
        self.redis = self._makeRedis()
        self._testRedisConnection()
        self.log = logging.getLogger('lsst.rubintv.production.redisUtils.RedisHelper')

    def _makeRedis(self):
        """Create a redis connection.

        Returns
        -------
        redis.Redis
            The redis connection.
        """
        site = getSite()
        match site:
            case site if site in ('rubin-devl', 'staff-rsp'):
                # XXX put this IP in the locationConfig instead?
                return redis.Redis(host='172.24.5.216', password=getRedisSecret())
            case 'usdf-k8s':
                password = os.getenv('REDIS_PASSWORD')
                host = os.getenv('REDIS_HOST')
                return redis.Redis(host=host, password=password)
            case 'summit':
                host = os.getenv('REDIS_HOST')
                password = os.getenv('REDIS_PASSWORD')
                return redis.Redis(host=host, password=password)
            case 'base':
                host = os.getenv('REDIS_HOST')
                password = os.getenv('REDIS_PASSWORD')
                return redis.Redis(host=host, password=password)
            case 'tucson':
                host = os.getenv('REDIS_HOST')
                password = os.getenv('REDIS_PASSWORD')
                return redis.Redis(host=host, password=password)
            case _:
                raise RuntimeError('Unknown site, cannot connect to redis')

    def _testRedisConnection(self):
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
            raise RuntimeError('Could not connect to redis - is it running?') from e
        except Exception as e:
            raise RuntimeError(f'Unexpected error connecting to redis: {e}')

    def affirmRunning(self, podName, timePeriod):
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
        self.redis.setex(f'{podName}_IS-RUNNING', timedelta(seconds=timePeriod), value=1)

    def confirmRunning(self, podName):
        """Check whether the named pod is running or should be considered dead.

        Parameters
        ----------
        podName : `str`
            The name of the pod.
        """
        isRunning = self.redis.get(f'{podName}_IS-RUNNING')
        return bool(isRunning)  # 0 and None both bool() to False

    def announceBusy(self, queueName):
        """Announce that a worker is busy processing a queue.

        Parameters
        ----------
        queueName : `str`
            The name of the queue the worker is processing.
        """
        self.redis.set(f'{queueName}_IS-BUSY', value=1)

    def announceFree(self, queueName):
        """Announce that a worker is free to process a queue.

        Implies a call to `announceExistence` as you have to exist to be free.

        Parameters
        ----------
        queueName : `str`
            The name of the queue the worker is processing.
        """
        self.announceExistence(queueName)
        self.redis.delete(f'{queueName}_IS-BUSY')

    def announceExistence(self, queueName, remove=False):
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
            Remove the worker from pool. Default is ``True``.
        """
        if not remove:
            self.redis.setex(f'{queueName}_EXISTS', timedelta(seconds=30), value=1)
        else:
            self.redis.delete(f'{queueName}_EXISTS')

    def getAllWorkers(self, workerType=None):
        """Get the list of workers that are currently active.

        Parameters
        ----------
        workerType : `str`, optional
            The type of worker to get, e.g. "SFM". The default of ``None``
            will return all worker types.

        Returns
        -------
        workers : `list` of `str`
            The list of workers that are currently active.
        """
        if workerType is None:
            workerType = '*'

        # need to get the set of things that exist, or are busy, because
        # things "cease to exist" during long processing runs, but they do
        # still show as busy
        existing = self.redis.keys(f'{workerType}*WORKER*_EXISTS')
        existing = [key.decode('utf-8').replace('_EXISTS', '') for key in existing]

        busy = self.redis.keys(f'{workerType}*WORKER*_IS-BUSY')
        busy = [key.decode('utf-8').replace('_IS-BUSY', '') for key in busy]

        allWorkers = sorted(set(existing + busy))

        return allWorkers

    def getFreeWorkers(self, workerType=None):
        """Get the list of workers that are currently free.

        Parameters
        ----------
        workerType : `str`, optional
            The type of worker to get, e.g. "SFM". The default of ``None``
            will return all worker types.

        Returns
        -------
        workers : `list` of `str`
            The list of workers that are currently free.
        """
        workers = []
        allWorkers = self.getAllWorkers(workerType=workerType)
        for worker in allWorkers:
            if not self.redis.get(f'{worker}_IS-BUSY'):
                workers.append(worker)
        return sorted(workers)

    def pushToButlerWatcherList(self, instrument, expRecord):
        """Keep a record of what's been found by the butler watcher for all
        time.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord`
            The exposure record to push to the list.
        """
        expRecordJson = expRecord.to_simple().json()
        self.redis.lpush(f'{instrument}-fromButlerWacher', expRecordJson)

    def reportFinished(self, instrument, taskName, processingId, failed=False):
        """Report that a task has finished.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        taskName : `str`
            The name of the task that has finished processing.
        processingId : `int`
            Either the exposureId or visitId of the payload that has finished
            being processed for the specified task.
        """
        key = f'{instrument}-{taskName}-FINISHEDCOUNTER'
        self.redis.hincrby(key, processingId, 1)  # creates the key if it doesn't exist

        if failed:  # fails have finished too, so increment finished and failed
            key = key.replace('FINISHEDCOUNTER', 'FAILEDCOUNTER')
            self.redis.hincrby(key, processingId, 1)  # creates the key if it doesn't exist

    def getNumFinished(self, instrument, taskName, processingId):
        """Get the number of items finished for a given task and id.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        taskName : `str`
            The name of the task that has finished processing.
        processingId : `int`
            Either the exposureId or visitId of the payload that has finished
            being processed for the specified task.

        Returns
        -------
        numFinished : `int`
            The number of times the task has finished.
        """
        key = f'{instrument}-{taskName}-FINISHEDCOUNTER'
        return int(self.redis.hget(key, processingId))

    def reportVisitLevelFinished(self, instrument, step, failed=False):
        """Count the number of times a visit-level pipeline has finished.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        step : `str`
            The name of the step which finished processing.
        failed : `bool`
            True if the processing did not fail to complete
        """
        key = f'{instrument}-{step}-FINISHEDCOUNTER'
        self.redis.incr(key, 1)  # creates the key if it doesn't exist

        if failed:  # fails have finished too, so increment finished and failed
            key = key.replace('FINISHEDCOUNTER', 'FAILEDCOUNTER')
            self.redis.incr(key, 1)  # creates the key if it doesn't exist

    def getNumVisitLevelFinished(self, instrument, step):
        """Get the number of times a visit-level pipeline has finished.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        step : `str`
            The name of the step which finished processing.

        Returns
        -------
        numFinished : `int`
            The number of times the step has finished.
        """
        key = f'{instrument}-{step}-FINISHEDCOUNTER'
        return int(self.redis.get(key) or 0)

    def reportNightLevelFinished(self, instrument, failed=False):
        """Count the number of times a night-level pipeline has finished.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        failed : `bool`
            True if the processing did not fail to complete
        """
        key = f'{instrument}-NIGHTLYROLLUP-FINISHEDCOUNTER'
        self.redis.incr(key, 1)

    def getIdsForTask(self, instrument, taskName):
        key = f'{instrument}-{taskName}-FINISHEDCOUNTER'
        idList = self.redis.hgetall(key).keys()  # list of bytes
        return [int(procId) for procId in idList]

    def removeTaskCounter(self, instrument, taskName, processId):
        """Once a gather step is finished with all the expected data present,
        remove the counter from the tracking dictionary to save reprocessing it
        each time.

        Parameters
        ----------
        instrument : `str`
            The name of the instrument.
        taskName : `str`
            The name of the task that has finished processing.
        processingId : `int`
            Either the exposureId or visitId of the payload that has finished
            being processed for the specified task.
        """
        key = f'{instrument}-{taskName}-FINISHEDCOUNTER'
        self.redis.hdel(key, processId)

    def checkButlerWatcherList(self, instrument, expRecord):
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

        data = self.redis.lrange(f'{instrument}-fromButlerWacher', 0, -1)
        recordStrings = [item.decode('utf-8') for item in data]
        return expRecordJson in recordStrings

    def _checkIsHeadNode(self):
        """Note: this isn't how atomicity of transactions is ensured, this is
        just to make sure workers don't accidentally try to pop straight from
        the main queue.
        """
        if not self.isHeadNode:
            raise RuntimeError('This function is only for the head node - consume your queue, worker!')

    def getRemoteCommands(self):
        """Get any remote commands that have been sent to the head node.

        Returns
        -------
        commands : `list` of `dict` : `dict`
            The commands that have been sent. As a list of dicts, where each
            dict has a single string key, which is the function to be called,
            and dict value, optionally populated with kwargs to call that
            function with.

            For example, to ensure the guiders are on, the wavefronts are off,
            and we have a phase-0 per-CCD chequerboard, the received commands
            would look like:

            commands = [
                {'setAllOff': {}},
                {'setFullChequerboard': {'phase': 0}},
                {'setGuidersOn': {}},
            ]
        """
        commands = []
        while command := self.redis.rpop('commands'):  # rpop for FIFO
            commands.append(json.loads(command.decode('utf-8')))

        return commands

    def pushNewExposureToHeadNode(self, expRecord):
        """Call to send an expRecord for processing.

        This queue is consumed by the head node, which fans it out for
        processing by the workers.

        The queue can have any length, and will be consumed last-in-first-out.

        Parameters
        ----------
        XXX
        """
        instrument = expRecord.instrument
        queueName = getNewDataQueueName(instrument)
        expRecordJson = expRecord.to_simple().json()
        self.redis.lpush(queueName, expRecordJson)

    def getExposureForFanout(self, instrument):
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

    def enqueuePayload(self, payload, queueName, top=True):
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
            self.redis.lpush(queueName, payload.to_json())
        else:
            self.redis.rpush(queueName, payload.to_json())

    def dequeuePayload(self, queueName):
        """Get the next unit of work from a specific worker queue.

        Returns
        -------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector, or
            ``None`` if the queue is empty.
        """
        payLoadJson = self.redis.lpop(queueName)
        if payLoadJson is None:
            return None
        return Payload.from_json(payLoadJson, self.butler)

    def clearTaskCounters(self):
        # XXX is keys OK here?
        keys = self.redis.keys('*EDCOUNTER*')  # FINISHEDCOUNTER and FAILEDCOUNTER
        for key in keys:
            self.redis.delete(key)

    def displayRedisContents(self):
        """Get the next unit of work from a specific worker queue.

        Returns
        -------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector, or
            ``None`` if the queue is empty.
        """
        def _isPayload(jsonData):
            try:
                loaded = json.loads(jsonData)
                _ = loaded['dataId']
                return True
            except (KeyError, json.JSONDecodeError):
                pass
            return False

        def _isExpRecord(jsonData):
            try:
                loaded = json.loads(jsonData)
                _ = loaded['definition']
                return True
            except (KeyError, json.JSONDecodeError):
                pass
            return False

        def getPayloadDataId(jsonData):
            loaded = json.loads(jsonData)
            return f"{loaded['dataId']}, run={loaded['run']}"

        def getExpRecordDataId(jsonData):
            loaded = json.loads(jsonData)
            expRecordStr = f"{loaded['record']['instrument']}, {loaded['record']['id']}"
            return expRecordStr

        r = self.redis

        # Get all keys in the database
        # TODO: .keys is a blocking operation - consider using .scan instead
        keys = sorted(r.keys('*'))
        if not keys:
            print("Nothing in the Redis database.")
            return

        # any keys containing these strings will just have their lengths
        # printed, not the full contents of the lists
        lengthKeyPatterns = [
            'fromButlerWacher',
        ]

        def handleLengthKeys(key):
            # just print the key and the length of the list
            print(f"{key}: {r.llen(key)} items")

        for key in keys:
            key = decode_string(key)
            type_of_key = r.type(key).decode('utf-8')

            if any(pattern in key for pattern in lengthKeyPatterns):
                handleLengthKeys(key)
                continue

            # Handle different Redis data types
            if type_of_key == 'string':
                value = decode_string(r.get(key))
                print(f"{key}: {value}")
            elif type_of_key == 'hash':
                values = decode_hash(r.hgetall(key))
                print(f"{key}: {values}")
            elif type_of_key == 'list':
                values = decode_list(r.lrange(key, 0, -1))
                print(f"{key}:")
                indent = 2 * ' '
                for item in values:
                    if _isPayload(item):
                        print(f"{indent}{getPayloadDataId(item)}")
                    elif _isExpRecord(item):
                        print(f"{indent}{getExpRecordDataId(item)}")
                    else:
                        print(f"{indent}{item}")
            elif type_of_key == 'set':
                values = decode_set(r.smembers(key))
                print(f"{key}: {values}")
            elif type_of_key == 'zset':
                values = decode_zset(r.zrange(key, 0, -1, withscores=True))
                print(f"{key}: {values}")
            else:
                print(f"Unsupported type for key: {key}")

    def clearRedis(self, force=False):
        """Clear all keys in the Redis database.

        Parameters
        ----------
        force : `bool`, optional
            Whether to clear the Redis database without user confirmation.
            Default is ``False``.
        """
        if not force:
            print("Are you sure you want to clear the Redis database? This action cannot be undone.")
            print("Type 'yes' to confirm.")
            response = input()
            if response != 'yes':
                print("Clearing aborted.")
                return
        self.redis.flushdb()

    def clearWorkerQueues(self, force=False):
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
            if response != 'yes':
                print("Clearing aborted.")
                return

        keys = self.redis.keys('*WORKER*')  # XXX check if this is OK to use .keys() here
        for key in keys:
            self.redis.delete(key)

    def monitorRedis(self, interval=1):
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
                clear_output(wait=True)
            else:
                print('\033c', end='')  # clear the terminal
