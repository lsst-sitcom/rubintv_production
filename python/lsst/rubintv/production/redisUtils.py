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


class RedisHelper:
    def __init__(self, isHeadNode=False):
        self.isHeadNode = isHeadNode
        self.redis = redis.Redis()
        self._testRedisConnection()
        self.log = logging.getLogger('lsst.rubintv.production.redisUtils.RedisHelper')

        self._mostRecents = {}

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

    def updateMostRecent(self, dataProduct, expRecord):
        if dataProduct not in self._mostRecents:
            self._mostRecents[dataProduct] = expRecord.timespan.end
        else:
            if self._mostRecents[dataProduct] > expRecord.timespan.end:
                self._mostRecents[dataProduct] = expRecord.timespan.end
            else:
                return

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
        self.redis.setex(podName, timedelta(seconds=timePeriod), value=1)

    def confirmRunning(self, podName):
        """Check whether the named pod is running or should be considered dead.

        Parameters
        ----------
        podName : `str`
            The name of the pod.
        """
        isRunning = self.r.get(podName)
        return bool(isRunning)  # 0 and None both bool() to False

    def _checkIsHeadNode(self):
        """Note: this isn't how atomicity of transactions is ensured, this is
        just to make sure workers don't accidentally try to pop straight from
        the main queue.
        """
        if not self.isHeadNode:
            raise RuntimeError('This function is only for the head node - consume your queue, worker!')

    def pushDataId(self, dataProduct, expRecord):
        """Put an exposure on the `current` stack for immediate processing.

        If there is an unprocessed item on the current stack when the new item
        is pushed (and there can only ever be only one) the previous item is
        moved to the top of the backlog stack.

        Each data product has its own `current` stack, and putting an exposure
        record onto its stack indicates that it is time to start processing
        that data product for that exposure. Note that the dataProduct can be
        any string - it need not be restricted to dafButler dataProducts.

        Note, his function may only be called from the head node or a
        ``ButlerWatcher``. If this function is called from elsewhere a
        ``RuntimeError`` is raised.

        Parameters
        ----------
        dataProduct : `str`
            The data product to write the dataId file for.
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record to write the dataId file for.
        log : `logging.Logger`, optional
            The logger to use. If provided, and info-level message is logged
            about where what file was written.

        Raises
        ------
        RuntimeError
            Raised if called from a worker node.
        """
        self._checkIsHeadNode()

        expRecordJson = expRecord.to_simple().json()
        with self.redis.pipeline() as pipe:
            while True:  # continue trying until we break out, though this should usually work first try
                try:
                    # Start a transactional block to ensure atomicity. Take the
                    # current expRecord off the current list if there is one,
                    # move it to the top of the backlog, and put the new one on
                    # the current stack in its place
                    pipe.watch(dataProduct)
                    pipe.multi()
                    numExisting = pipe.llen(dataProduct)
                    if numExisting > 1:
                        raise RuntimeError('Too many current expRecords on current stack - check the logic')
                    old = None
                    if numExisting == 1:
                        old = pipe.lpop(dataProduct)
                    nRemaining = pipe.llen(dataProduct)
                    assert nRemaining == 0  # this *must* always be true, so a very hard check here
                    pipe.lpush(f'{dataProduct}-backlog', old)
                    pipe.lpush(dataProduct, expRecordJson)
                    pipe.execute()
                    break  # success!

                except redis.WatchError:
                    # No break here, this is the expected flow for a retry
                    dayObs = expRecord.day_obs
                    seqNum = expRecord.seq_num
                    self.log.info(f'redis.WatchError putting expRecord for {dayObs=}, {seqNum=}, retrying...')

                except Exception as e:
                    dayObs = expRecord.day_obs
                    seqNum = expRecord.seq_num
                    self.log.exception(f'Error putting expRecord for {dayObs=}, {seqNum=}: {e}'
                                       ' This image will not be processed!')
                    break  # an unexpected (real) exception, so don't retry

        self.updateMostRecent(dataProduct, expRecord)

    def popDataId(self, dataProduct):
        """Get the most recent image from the top of the stack.

        If includeBacklog is set, items which were not the most recently put
        will also be returned. Otherwise, an exposure record will only be
        returned if it's the most recently landed image.
        """
        self._checkIsHeadNode()
        expRecordJson = self.redis.lpop(f'{dataProduct}')
        if expRecordJson is None:
            return
        return expRecordFromJson(expRecordJson.decode('utf-8'))

    def insertDataId(self, dataProduct, expRecord, index=1):
        """Insert an exposure into the queue.

        By default, this will go at the top of the backlog queue. If the
        exposure should instead be processed only once the backlog is cleared,
        set index=-1 to put at the very end of the queue.
        """
        self._checkIsHeadNode()
        if index == -1:
            expRecordJson = expRecord.to_simple().json()
            self.redis.rpush(f'{dataProduct}', expRecordJson)
        else:
            # Need to work out how to do this, LINSERT seemed to need the value
            # to push it next to rather than an index, which feels a bit tricky
            raise NotImplementedError

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

    def enqueueCurrentWork(self, expRecord, detector):
        """Send a unit of work to a specific worker-queue.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector.
        detector : `int`
            The detector to enqueue the work for.
        """
        # XXX I think this should work the same as the main pushDataId function
        # so that there can only ever be a single item on the current queue per
        # detector, and then the workers themselves can decide whether to
        # consume from their backlog queues depending on their
        # WorkerProcessingMode
        expRecordJson = expRecord.to_simple().json()
        self.redis.lpush(f'current-{detector}', expRecordJson)

    def enqueueBacklogWork(self, expRecord, detector):
        """Send a unit of work to a specific worker-queue.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector.
        detector : `int`
            The detector to enqueue the work for.
        """
        expRecordJson = expRecord.to_simple().json()
        self.redis.lpush(f'backlog-{detector}', expRecordJson)

    def getWork(self, detector, includeBacklog=False):
        """Get the next unit of work from a specific worker queue.

        Returns
        -------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector, or
            ``None`` if the queue is empty.
        """
        expRecordJson = self.redis.lpop(f'current-{detector}')
        if expRecordJson is None and includeBacklog:
            expRecordJson = self.redis.lpop(f'backlog-{detector}')
        if expRecordJson is None:
            return None
        return expRecordFromJson(expRecordJson.decode('utf-8'))

    def displayRedisContents(self):
        """Get the next unit of work from a specific worker queue.

        Returns
        -------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector, or
            ``None`` if the queue is empty.
        """
        r = self.redis

        # Get all keys in the database
        keys = r.keys('*')

        for key in keys:
            key = decode_string(key)
            type_of_key = r.type(key).decode('utf-8')

            # Handle different Redis data types
            if type_of_key == 'string':
                value = decode_string(r.get(key))
                print(f"{key}: {value}")
            elif type_of_key == 'hash':
                values = decode_hash(r.hgetall(key))
                print(f"{key}: {values}")
            elif type_of_key == 'list':
                values = decode_list(r.lrange(key, 0, -1))
                print(f"{key}: {values}")
            elif type_of_key == 'set':
                values = decode_set(r.smembers(key))
                print(f"{key}: {values}")
            elif type_of_key == 'zset':
                values = decode_zset(r.zrange(key, 0, -1, withscores=True))
                print(f"{key}: {values}")
            else:
                print(f"Unsupported type for key: {key}")

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
