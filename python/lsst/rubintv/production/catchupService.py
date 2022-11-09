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

import os
import copy
import time
from time import sleep
import logging
import shutil
import json

from lsst.summit.utils.bestEffort import BestEffortIsr
import lsst.summit.utils.butlerUtils as butlerUtils
from lsst.summit.utils.utils import getCurrentDayObs_int
from lsst.summit.extras.animation import animateDay
from .rubinTv import Uploader, Heartbeater, MetadataServer
from lsst.rubintv.production.allSky import cleanupAllSkyIntermediates
from lsst.rubintv.production.utils import remakeDay, isDayObsContiguous

__all__ = ['RubinTvBackgroundService']

_LOG = logging.getLogger(__name__)

# TODO:
# Add imExam catchup
# Add specExam catchup
# Add metadata server catchup
#    - this will require loading the local json, checking for gaps and
#      just adding those. Hold off on doing this to see if there even are
#      ever any gaps - there might not be because the service is probably
#      quick enough that nothing is ever missed.


class RubinTvBackgroundService():
    """Sits in the background, performing catchups, and performs a specific end
    of day action when the day rolls over.

    This model assumes that all the existing channels services will never
    be so far behind that this service will saturate. At present, this is
    *easily* true, and should always be true. To that end, if/when this
    service starts logging warnings that it has a growing backlog, that is
    a sign that other summit services are too slow and are falling too far
    behind/are not keeping up.

    Parameters
    ----------
    allSkyPngRoot : `str`
        The path at which the all sky movie channel is writing its images to.
    moviePngRoot : `str`
        The root path to write all pngs and movies to. It need not exist,
        but must be creatable with write privileges.
    doRaise : `bool`
        Raise on error?
    **kwargs
        Additional keyword arguments passed to ``BestEffortIsr`` instantiation.
    """
    catchupPeriod = 300  # in seconds, so 5 mins
    loopSleep = 30
    endOfDayDelay = 600
    allSkyDeletionExtraSleep = 1800  # 30 mins

    HEARTBEAT_HANDLE = "backgroundService"
    HEARTBEAT_UPLOAD_PERIOD = 30
    HEARTBEAT_FLATLINE_PERIOD = 600

    def __init__(self, *,
                 allSkyPngRoot=None,
                 moviePngRoot=None,
                 metadataOutputRoot=None,
                 doRaise=False,
                 **kwargs):
        self.uploader = Uploader()
        self.log = _LOG.getChild("backgroundService")
        self.allSkyPngRoot = allSkyPngRoot
        self.moviePngRoot = moviePngRoot
        self.doRaise = doRaise
        self.butler = butlerUtils.makeDefaultLatissButler()
        self.bestEffort = BestEffortIsr(**kwargs)
        self.uploader = Uploader()
        self.heartbeater = Heartbeater(self.HEARTBEAT_HANDLE,
                                       self.HEARTBEAT_UPLOAD_PERIOD,
                                       self.HEARTBEAT_FLATLINE_PERIOD)
        self.mdServer = MetadataServer(outputRoot=metadataOutputRoot)  # costly-ish to create, so put in class

    def _raiseIf(self, error):
        """Raises the error if ``self.doRaise`` otherwise logs it as a warning.

        Parameters
        ----------
        error : `Exception`
            The error that has been raised.

        Raises
        ------
        AnyException
            Raised if ``self.doRaise`` is True, otherwise swallows and warns.
        """
        msg = f'Background service error: {error}'
        if self.doRaise:
            raise RuntimeError(msg) from error
        else:
            self.log.warn(msg)

    def hasDayRolledOver(self):
        """Check if the dayObs has rolled over.

        Checks if the class' dayObs is the current dayObs and returns False
        if it is. Note that this does not update the attribute itself.

        Returns
        -------
        hasDayRolledOver : `bool`
            Whether the day has rolled over?
        """
        currentDay = getCurrentDayObs_int()
        if currentDay == self.dayObs:
            return False
        elif currentDay == self.dayObs+1:
            return True
        else:
            if not isDayObsContiguous(currentDay, self.dayObs):
                self.log.warning(f"Encountered non-linear time! Day cleaner was started with {self.dayObs} "
                                 f"and now current dayObs is {currentDay}!")
            return True  # the day has still rolled over, just in an unexpected way

    def getMissingQuickLookIds(self):
        """Get a list of the dataIds for the current dayObs for which
        quickLookExps do not exist in the repo.

        Returns
        -------
        dataIds : `list` [`dict]
            A list of the missing dataIds.
        """
        allSeqNums = butlerUtils.getSeqNumsForDayObs(self.butler, self.dayObs)

        where = "exposure.day_obs=dayObs AND instrument='LATISS'"
        expRecords = self.butler.registry.queryDimensionRecords("exposure",
                                                                where=where,
                                                                bind={'dayObs': self.dayObs},
                                                                datasets='quickLookExp')
        expRecords = list(set(expRecords))
        foundSeqNums = [r.seq_num for r in expRecords]
        toMakeSeqNums = [s for s in allSeqNums if s not in foundSeqNums]
        return [{'day_obs': self.dayObs, 'seq_num': s, 'detector': 0} for s in toMakeSeqNums]

    @staticmethod
    def _makeMinimalDataId(dataId):
        """Given a dataId, strip it to contain only ``day_obs``, ``seq_num``
        and ``detector``.

        This is necessary because the set of keys used must be consistent so
        that removal from a list works, as superfluous keys would mean the
        items do not match.

        Parameters
        ----------
        dataId : `dict`
            The dataId.
        """
        # Need to have this exact set of keys to make removing from work
        keys = ['day_obs', 'seq_num', 'detector']
        for key in keys:
            if key not in dataId:
                raise ValueError(f'Failed to minimize dataId {dataId}')
        return {'day_obs': dataId['day_obs'], 'seq_num': dataId['seq_num'], 'detector': dataId['detector']}

    def catchupIsrRunner(self):
        """Create any missing quickLookExps for the current dayObs.
        """
        # check latest dataId and remove that and previous
        # and then do *not* do that in end of day
        self.log.info(f'Catching up quickLook exposures for {self.dayObs}')
        missingQuickLooks = self.getMissingQuickLookIds()

        mostRecent = butlerUtils.getMostRecentDataId(self.butler)
        # reduce to keys that exactly matches missingQuickLooks
        mostRecent = self._makeMinimalDataId(mostRecent)

        secondMostRecent = copy.copy(mostRecent)
        secondMostRecent['seq_num'] -= 1

        for d in [mostRecent, secondMostRecent]:
            if d in missingQuickLooks:
                missingQuickLooks.remove(d)

        self.log.info(f'Catchup service found {len(missingQuickLooks)} missing quickLookExps')

        for dataId in missingQuickLooks:
            self.log.info(f"Producing quickLookExp for {dataId}")
            exp = self.bestEffort.getExposure(dataId)
            del exp

    def catchupMountTorques(self):
        """Create and upload any missing mount torque plots for the current
        dayObs.
        """
        self.log.info(f'Catching up mount torques for {self.dayObs}')
        remakeDay('auxtel_mount_torques', self.dayObs, remakeExisting=False, notebook=False)

    def catchupMonitor(self):
        """Create and upload any missing monitor images for the current dayObs.
        """
        self.log.info(f'Catching up monitor images for {self.dayObs}')
        remakeDay('auxtel_monitor', self.dayObs, remakeExisting=False, notebook=False)

    def catchupMetadata(self):
        """Create shards for any seqNums missing their metadata. Do not upload.
        """
        self.log.info(f'Catching up metadata for {self.dayObs}')
        mdFilename = self.mdServer.getSidecarFilename(self.dayObs)
        if not os.path.isfile(mdFilename):
            # we haven't taken any data yet today
            self.log.info(f"Metadata file {mdFilename} for {self.dayObs} does not exist yet, waiting...")
            return
        else:
            with open(mdFilename) as f:
                data = json.load(f)

        seqNums = [int(k) for k in data.keys()]
        maxVal = max(seqNums)
        missing = [k for k in range(1, maxVal) if k not in seqNums]
        self.log.info(f'Found {len(missing)} rows missing metadata to create shards for')

        for seqNum in missing:
            dataId = {'day_obs': self.dayObs, 'seq_num': seqNum, 'detector': 0}
            self.mdServer.writeShardForDataId(dataId)
        # note we do *not* call mdServer.mergeShardsAndUpload() here
        # as that writes to the main file, which could collide with the main
        # channel. Instead, we leave the shards in place to be uploaded with
        # the next image. We do, however, call it on end-of-day to catch any
        # leftover shards

    def runCatchup(self):
        """Run all the catchup routines: isr, monitor images, mount torques.
        """
        startTime = time.time()

        # a little ugly but saves copy/pasting the try block 4 times
        # we need to try each one because raising here has bad conqquences
        # on the try block in run()
        # (the day doesn't roll over, we constantly hammer on the same images...)
        for component in [self.catchupMetadata,
                          self.catchupIsrRunner,
                          self.catchupMonitor,
                          self.catchupMountTorques]:
            try:
                component.__call__()
            except Exception as e:
                self._raiseIf(e)

        endTime = time.time()
        self.log.info(f"Catchup for all channels took {(endTime-startTime):.2f} seconds")

    def deleteAllSkyPngs(self):
        """Delete all the intermediate on-disk files created when making the
        all sky movie for the current day.
        """
        if self.allSkyPngRoot is not None:
            directory = os.path.join(self.allSkyPngRoot, str(self.dayObs))
            if os.path.isdir(directory):
                shutil.rmtree(directory)
                self.log.info(f"Deleted all-sky png directory {directory}")
            else:
                self.log.warning(f"Failed to find assumed all-sky png directory {directory}")

    def runEndOfDay(self):
        """Routine to run when the summit dayObs rolls over.

        Makes the per-day animation of all the on-sky images and uploads to the
        auxtel_movies channel. Deletes all the intermediate on-disk files
        created when making the all sky movie. Deletes all the intermediate
        movies uploaded during the day for the all sky channel from the bucket.
        """
        self.mdServer.mergeShardsAndUpload()  # just catch any leftover shards
        try:
            # TODO: this will move to its own channel to be done routinely
            # during the night, but this is super easy for now, so add here
            self.log.info(f'Creating movie for {self.dayObs}')
            outputPath = self.moviePngRoot
            writtenMovie = animateDay(self.butler, self.dayObs, outputPath)

            if writtenMovie:
                channel = 'auxtel_movies'
                uploadAs = f'dayObs_{self.dayObs}.mp4'
                self.uploader.googleUpload(channel, writtenMovie, uploadAs)
            else:
                self.log.warning(f'Failed to find movie for {self.dayObs}')
            # clean up animation pngs here?
            # 27k images on lsst-dev is 47G, so not too big and they're
            # useful in other places sometimes, so leave for now.

            # all sky movie creation wants an extra safety margin due to
            # its loop cadence and animation time etc and there's no hurry
            # since we're no longer on sky as the day has just rolled over.
            sleep(self.allSkyDeletionExtraSleep)
            self.log.info('Deleting rescaled pngs from all-sky camera...')
            self.deleteAllSkyPngs()

            self.log.info('Deleting intermediate all-sky movies from GCS bucket')
            cleanupAllSkyIntermediates()

        except Exception as e:
            self._raiseIf(e)

        finally:
            self.dayObs = getCurrentDayObs_int()

    def runEndOfDayManual(self, dayObs):
        """Manually run the end of day routine for a specific dayObs by hand.

        Useful for if the final catchup and end of day animation/clearup have
        failed to run and this needs to be redone by manually.

        Parameters
        ----------
        dayObs : `int`
            The dayObs to rerun the end of day routine for.
        """
        self.dayObs = dayObs
        self.runCatchup()
        self.runEndOfDay()
        return

    def run(self):
        """Runs forever, running the catchup services during the day and the
        end of day service when the day ends.

        Raises
        ------
        RuntimeError:
            Raised from the root error on any error if ``self.doRaise`` is
            True.
        """
        lastRun = time.time()
        self.dayObs = getCurrentDayObs_int()

        while True:
            try:
                timeSince = time.time() - lastRun
                if timeSince >= self.catchupPeriod:
                    self.runCatchup()
                    self.heartbeater.beat()
                    lastRun = time.time()
                    if self.hasDayRolledOver():
                        self.log.info(f'Day has rolled over, sleeping for {self.endOfDayDelay}s before '
                                      'running end of day routine.')
                        # endOfDayDelay is long so send a heartbeat first
                        self.heartbeater.beat(customFlatlinePeriod=self.endOfDayDelay*1.5)
                        sleep(self.endOfDayDelay)  # give time for anything running elsewhere to finish
                        # animation can take a very long time
                        self.heartbeater.beat(customFlatlinePeriod=1.5*60*60)
                        self.runEndOfDay()  # sets new dayObs in a finally block
                        self.heartbeater.beat()
                else:
                    remaining = self.catchupPeriod - timeSince
                    self.log.info(f'Waiting for catchup period to elapse, {remaining:.2f}s to go...')
                    sleep(self.loopSleep)

                self.heartbeater.beat()

            except Exception as e:
                self._raiseIf(e)
