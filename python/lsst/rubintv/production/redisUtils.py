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

from .utils import expRecordFromJson

__all__ = (
    'RedisHelper'
)


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

    def _checkIsHeadNode(self):
        """Note: this isn't how atomicity of transactions is ensured, this is
        just to make sure workers don't accidentally try to pop straight from
        the main queue.
        """
        if not self.isHeadNode:
            raise RuntimeError('This function is only for the head node - consume your queue, worker!')

    def pushDataId(self, dataProduct, expRecord):
        """Put an exposure on the top of the stack for immediate processing.

        Each data product has its own stack, and putting an exposure record
        onto its stack indicates that it is time to start processing that
        exposure. Note that the dataProduct can be any string - it need not be
        restricted to dafButler dataProducts.

        Parameters
        ----------
        dataProduct : `str`
            The data product to write the dataId file for.
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record to write the dataId file for.
        log : `logging.Logger`, optional
            The logger to use. If provided, and info-level message is logged
            about where what file was written.
        """
        self._checkIsHeadNode()

        expRecordJson = json.dumps(expRecord.to_simple().json())
        with self.redis.pipeline() as pipe:
            while True:  # continue trying until we break out
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
                    dayObs = expRecord.day_obs
                    seqNum = expRecord.seq_num
                    self.log.info(f'redis.WatchError putting expRecord for {dayObs=}, {seqNum=}, retrying...')
                    # WatchError so no break here

                except Exception as e:
                    dayObs = expRecord.day_obs
                    seqNum = expRecord.seq_num
                    self.log.exception(f'Error putting expRecord for {dayObs=}, {seqNum=}: {e}'
                                       ' This image will not be processed!')
                    break  # a real exception, so don't retry

        self.updateMostRecent(dataProduct, expRecord)

    def popDataId(self, dataProduct):
        """Get the most recent image from the top of the stack.

        If includeBacklog is set, items which were not the most recently put
        will also be returned. Otherwise, an exposure record will only be returned
        if it's the most recently landed image.
        """
        self._checkIsHeadNode()
        expRecordJson = self.redis.lpop(f'{dataProduct}')
        if expRecordJson is None:
            return
        return expRecordFromJson(expRecordJson)

    def insertDataId(self, dataProduct, expRecord, index=1):
        """Insert an exposure into the queue.

        By default, this will go at the top of the backlog queue. If the
        exposure should instead be processed only once the backlog is cleared,
        set index=-1 to put at the very end of the queue.
        """
        self._checkIsHeadNode()
        if index == -1:
            expRecordJson = json.dumps(expRecord.to_simple().json())
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
        expRecordJson = json.dumps(expRecord.to_simple().json())
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
        expRecordJson = json.dumps(expRecord.to_simple().json())
        self.redis.lpush(f'backlog-{detector}', expRecordJson)

    def getWork(self, detector, includeBacklog=False):
        """Get the next unit of work from a specific worker queue.

        Returns
        -------
        expRecord : `lsst.daf.butler.dimensions.ExposureRecord` or `None`
            The next exposure to process for the specified detector, or
            ``None`` if the queue is empty.
        """
        expRecordJson = self.redis.lpop(f'current-{detector}').decode('utf-8')
        if not expRecordJson and includeBacklog:
            expRecordJson = self.redis.lpop(f'backlog-{detector}').decode('utf-8')
        return expRecordFromJson(expRecordJson)
