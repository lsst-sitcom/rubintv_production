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
    'writeDataIdToRedis'
)


class RedisHelper:
    def __init__(self, isHeadNode=False):
        self.isHeadNode = isHeadNode
        self.redis = redis.Redis()
        self.log = logging.getLogger('lsst.rubintv.production.redisUtils.RedisHelper')

        self._mostRecents = {}

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
            while True:
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
                    break
                except redis.WatchError:
                    dayObs = expRecord.day_obs
                    seqNum = expRecord.seq_num
                    self.log.info(f'redis.WatchError putting expRecord for {dayObs=}, {seqNum=}, retrying...')

                except Exception as e:
                    dayObs = expRecord.day_obs
                    seqNum = expRecord.seq_num
                    self.log.exception(f'Error putting expRecord for {dayObs=}, {seqNum=}: {e}'
                                       ' This image will not be processed!')
                    break

        self.updateMostRecent(dataProduct, expRecord)

    def popDataId(self, dataProduct, expRecord, includeBacklog=False):
        """Get the most recent image from the top of the stack.

        If includeBacklog is set, items which were not the most recently put
        will also be returned. Otherwise, an exposure record will only be returned
        if it's the most recently landed image.
        """
        self._checkIsHeadNode()
        expRecordJson = self.redis.lpop(f'{dataProduct}')
        expRecord = expRecordFromJson(expRecordJson)
        return expRecord

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
        commands : `list` of `str`
            The commands that have been sent.
        """
        commands = []
        while command := self.redis.rpop('commands'):
            commands.append(json.loads(command.decode('utf-8')))

        return commands
