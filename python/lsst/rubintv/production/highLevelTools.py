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
import logging
import glob
import time

from lsst.utils import getPackageDir

from lsst.summit.utils.butlerUtils import getSeqNumsForDayObs, makeDefaultLatissButler, getExpRecordFromDataId
from lsst.summit.utils.utils import dayObsIntToString, setupLogging
from .channels import CHANNELS, PREFIXES
from .utils import _expRecordToFilename, LocationConfig, FakeDataCoordinate
from .uploaders import Uploader

__all__ = ['getPlotSeqNumsForDayObs',
           'createChannelByName',
           'remakePlotByDataId',
           'remakeDay',
           'pushTestImageToCurrent',
           'remakeStarTrackerDay',
           ]

# this file is for higher level utilities for use in notebooks, but also
# can be imported by other scripts and channels, especially the catchup ones.


def getPlotSeqNumsForDayObs(channel, dayObs, bucket=None):
    """Return the list of seqNums for which the plot exists in the bucket for
    the specified channel.

    Parameters
    ----------
    channel : `str`
        The channel.
    dayObs : `int`
        The dayObs.
    bucket : `google.cloud.storage.bucket.Bucket`, optional
        The GCS bucket, created if not supplied.

    Returns
    -------
    seqNums : `list` [`int`]
        Sorted list of ints of the seqNums for which the specified plot exists.

    Raises
    ------
    ValueError:
        Raised if the channel is unknown.
    """
    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}.")

    if not bucket:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.get_bucket('rubintv_data')

    dayObsStr = dayObsIntToString(dayObs)

    prefix = f'{channel}/{PREFIXES[channel]}_dayObs_{dayObsStr}'
    blobs = list(bucket.list_blobs(prefix=prefix))
    existing = [int(b.name.split(f'{prefix}_seqNum_')[1].replace('.png', '')) for b in blobs]
    return sorted(existing)


def createChannelByName(location, channel, *, embargo=False, doRaise=False):
    """Create a RubinTV Channel object using the name of the channel.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    channel : `str`
        The name of the channel, as found in lsst.rubintv.production.CHANNELS.
    embargo : `bool`, optional
        If True, use the embargo repo.
    doRaise : `bool`, optional
        Have the channel ``raise`` if errors are encountered while it runs.

    Returns
    -------
    channel : `lsst.rubintv.production.<Channel>`
        The lsst.rubintv.production Channel object.

    Raises
    ------
    ValueError:
        Raised if the channel is unknown, or creating by name is not supported
        for the channel in question.
    """
    from .rubinTv import (ImExaminerChannel,
                          SpecExaminerChannel,
                          MountTorqueChannel,
                          MonitorChannel,
                          MetadataCreator,
                          )
    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}.")

    locationConfig = LocationConfig(location)

    match channel:
        case "summit_imexam":
            return ImExaminerChannel(locationConfig=locationConfig, embargo=embargo, doRaise=doRaise)
        case "summit_specexam":
            return SpecExaminerChannel(locationConfig=locationConfig, embargo=embargo, doRaise=doRaise)
        case "auxtel_mount_torques":
            return MountTorqueChannel(locationConfig=locationConfig, embargo=embargo, doRaise=doRaise)
        case "auxtel_monitor":
            return MonitorChannel(locationConfig=locationConfig, embargo=embargo, doRaise=doRaise)
        case "auxtel_metadata":
            return MetadataCreator(locationConfig=locationConfig, embargo=embargo, doRaise=doRaise)
        case "all_sky_current":
            raise ValueError(f"{channel} is not a creatable by name.")
        case "all_sky_movies":
            raise ValueError(f"{channel} is not a creatable by name.")
        case _:
            raise ValueError(f"Unrecognized channel {channel}.")


def remakePlotByDataId(location, channel, dataId, embargo=False):
    """Remake the plot for the given channel for a single dataId.
    Reproduces the plot regardless of whether it exists. Raises on error.

    This method is very slow and inefficient for bulk processing, as it
    creates a Channel object for each plot - do *not* use in loops, use
    remakeDay() or write a custom scripts for bulk remaking.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    channel : `str`
        The name of the channel.
    dataId : `dict`
        The dataId.
    embargo : `bool`, optional
        Use the embargo repo?
    """
    tvChannel = createChannelByName(location, channel, embargo=embargo, doRaise=True)
    expRecord = getExpRecordFromDataId(tvChannel.butler, dataId)
    tvChannel.callback(expRecord)


def remakeDay(location,
              channel,
              dayObs,
              *,
              remakeExisting=False,
              notebook=True,
              logger=None,
              embargo=False):
    """Remake all the plots for a given day.

    Currently auxtel_metadata does not pull from the bucket to check what is
    in there, so remakeExisting is not supported.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    channel : `str`
        The name of the lsst.rubintv.production channel. The actual channel
        object is created internally.
    dayObs : `int`
        The dayObs.
    remakeExisting : `bool`, optional
        Remake all plots, regardless of whether they already exist in the
        bucket?
    notebook : `bool`, optional
        Is the code being run from within a notebook? Needed to correctly nest
        asyncio event loops in notebook-type environments.
    embargo : `bool`, optional
        Use the embargoed repo?

    Raises
    ------
    ValueError:
        Raised if the channel is unknown.
        Raised if remakeExisting is False and channel is auxtel_metadata.
    """
    if not logger:
        logger = logging.getLogger(__name__)

    from google.cloud import storage

    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}")

    if remakeExisting is False and channel in ['auxtel_metadata']:
        raise ValueError(f"Channel {channel} can currently only remake everything or nothing. "
                         "If you would like to remake everything, please explicitly pass "
                         "remakeExisting=True.")

    if notebook:
        # notebooks have their own eventloops, so this is necessary if the
        # function is being run from within a notebook type environment
        import nest_asyncio
        nest_asyncio.apply()
        setupLogging()

    client = storage.Client()
    bucket = client.get_bucket('rubintv_data')
    butler = makeDefaultLatissButler(embargo=embargo)

    allSeqNums = set(getSeqNumsForDayObs(butler, dayObs))
    logger.info(f"Found {len(allSeqNums)} seqNums to potentially create plots for.")
    existing = set()
    if not remakeExisting:
        existing = set(getPlotSeqNumsForDayObs(channel, dayObs, bucket=bucket))
        nToMake = len(allSeqNums) - len(existing)
        logger.info(f"Found {len(existing)} in the bucket which will be skipped, "
                    f"leaving {nToMake} to create.")

    toMake = sorted(allSeqNums - existing)
    if not toMake:
        logger.info(f"Nothing to do for {channel} on {dayObs}")
        return

    # doRaise is False because during bulk plot remaking we expect many fails
    # due to image types, short exposures, etc.
    tvChannel = createChannelByName(location, channel, doRaise=False, embargo=embargo)
    if channel in ['auxtel_metadata']:
        # special case so we only upload once after all the shards are made
        for seqNum in toMake:
            dataId = {'day_obs': dayObs, 'seq_num': seqNum, 'detector': 0}
            expRecord = getExpRecordFromDataId(butler, dataId)
            tvChannel.writeShardForExpRecord(expRecord)
        tvChannel.mergeShardsAndUpload()

    else:
        for seqNum in toMake:
            dataId = {'day_obs': dayObs, 'seq_num': seqNum, 'detector': 0}
            expRecord = getExpRecordFromDataId(butler, dataId)
            tvChannel.callback(expRecord)


def pushTestImageToCurrent(channel, bucketName, duration=15):
    """Push a test image to a channel to see if it shows up automatically.

    Leaves the test image in the bucket for ``duration`` seconds and then
    removes it. ``duration`` cannot be more than 60s, as test images should not
    be left in the bucket for long, and a minute should easily be long enough
    to see if things are working.

    NB: this function is designed for interactive use in notebooks and blocks
    for ``duration`` and then deletes the file.

    Parameters
    ----------
    channel : `str`
        The name of the lsst.rubintv.production channel. The actual channel
        object is created internally.
    bucketName : `str`
        The name of the GCS bucket to push the test image to.
    duration : `float`, optional
        The duration to leave the test image up for, in seconds.

    Raises
    ------
    ValueError: Raised when the channel is unknown or the channel does support
        test images being pushed to it, or is the requested duration for the
        test image to remain is too long (max of 60s).
    """
    logger = logging.getLogger(__name__)

    from google.cloud import storage

    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}")
    if channel in ['auxtel_metadata', 'auxtel_isr_runner', 'all_sky_current', 'all_sky_movies',
                   'auxtel_movies']:
        raise ValueError(f'Pushing test data not supported for {channel}')
    if duration > 60:
        raise ValueError(f'Maximum time to leave test images in buckets is 60s, got {duration}')

    client = storage.Client()
    bucket = client.get_bucket(bucketName)
    prefix = f'{channel}/{PREFIXES[channel]}'
    blobs = list(bucket.list_blobs(prefix=prefix))

    logger.info(f'Found {len(blobs)} for channel {channel} in bucket')

    # names are like
    # 'auxtel_monitor/auxtel-monitor_dayObs_2021-07-06_seqNum_100.png'
    days = set([b.name.split(f'{prefix}_dayObs_')[1].split('_seqNum')[0] for b in blobs])
    days = [int(d.replace('-', '')) for d in days]  # days are like 2022-01-02
    recentDay = max(days)

    seqNums = getPlotSeqNumsForDayObs(channel, recentDay, bucket)
    newSeqNum = max(seqNums) + 1

    mockDataCoord = FakeDataCoordinate(seq_num=newSeqNum, day_obs=recentDay)
    testCardFile = os.path.join(getPackageDir('rubintv_production'), 'assets', 'testcard_f.jpg')
    uploadAs = _expRecordToFilename(channel, mockDataCoord)
    uploader = Uploader(bucketName)

    logger.info(f'Uploading test card to {mockDataCoord} for channel {channel}')
    blob = uploader.googleUpload(channel, testCardFile, uploadAs, isLiveFile=True)

    logger.info(f'Upload complete, sleeping for {duration} for you to check...')
    time.sleep(duration)
    blob.delete()
    logger.info('Test card removed')


def remakeStarTrackerDay(*, dayObs,
                         rootDataPath,
                         outputRoot,
                         metadataRoot,
                         astrometryNetRefCatRoot,
                         wide,
                         remakeExisting=False,
                         logger=None,
                         forceMaxNum=None
                         ):
    """Remake all the star tracker plots for a given day.

    Parameters
    ----------
    dayObs : `int`
        The dayObs.
    rootDataPath : `str`
        The path at which to find the data, passed through to the channel.
    outputRoot : str``
        The path to write the results out to, passed through to the channel.
    metadataRoot : `str`
        The path to write metadata to, passed through to the channel.
    astrometryNetRefCatRoot : `str`
        The path to the astrometry.net reference catalogs. Do not include
        the /4100 or /4200, just the base directory.
    wide : `bool`
        Do this for the wide or narrow camera?
    remakeExisting : `bool`, optional
        Remake all plots, regardless of whether they already exist in the
        bucket?
    logger : `logging.Logger`, optional
        The logger.
    forceMaxNum : `int`
        Force the maximum seqNum to be this value. This is useful for remaking
        days from scratch or in full, rather than running as a catchup.
    """
    from .starTracker import StarTrackerChannel, getRawDataDirForDayObs

    if not logger:
        logger = logging.getLogger('lsst.starTracker.remake')

    # doRaise is False because during bulk plot remaking we expect many fails
    tvChannel = StarTrackerChannel(wide=wide,
                                   rootDataPath=rootDataPath,
                                   metadataRoot=metadataRoot,
                                   outputRoot=outputRoot,
                                   astrometryNetRefCatRoot=astrometryNetRefCatRoot,
                                   doRaise=False,
                                   )

    _ifWide = '_wide' if wide else ''
    rawChannel = f"startracker{_ifWide}_raw"

    existing = getPlotSeqNumsForDayObs(rawChannel, dayObs)
    maxSeqNum = max(existing) if not forceMaxNum else forceMaxNum
    missing = [_ for _ in range(1, maxSeqNum) if _ not in existing]
    logger.info(f"Most recent = {maxSeqNum}, found {len(missing)} missing to create plots for: {missing}")

    dayPath = getRawDataDirForDayObs(rootDataPath=rootDataPath,
                                     wide=wide,
                                     dayObs=dayObs)

    files = glob.glob(os.path.join(dayPath, '*.fits'))
    foundFiles = {}
    for filename in files:
        # filenames are like GC101_O_20221114_000005.fits
        _, _, dayObs, seqNumAndSuffix = filename.split('_')
        seqNum = int(seqNumAndSuffix.removesuffix('.fits'))
        foundFiles[seqNum] = filename

    toRemake = missing if not remakeExisting else list(range(1, maxSeqNum))
    toRemake.reverse()  # always do the most recent ones first, burning down the list, not up

    for seqNum in toRemake:
        if seqNum not in foundFiles.keys():
            logger.warning(f'Failed to find raw file for {seqNum}, skipping...')
            continue
        filename = foundFiles[seqNum]
        logger.info(f'Processing {seqNum} from {filename}')
        tvChannel.callback(filename)
