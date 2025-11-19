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

import glob
import io
import logging
import os
import pickle
import time
from typing import TYPE_CHECKING, Any

import pandas as pd

from lsst.summit.utils.butlerUtils import getExpRecordFromDataId, getSeqNumsForDayObs, makeDefaultLatissButler
from lsst.summit.utils.dateTime import calcPreviousDay, dayObsIntToString, getCurrentDayObsInt
from lsst.summit.utils.utils import setupLogging
from lsst.utils import getPackageDir

from .channels import CHANNELS, PREFIXES
from .uploaders import Uploader
from .utils import FakeExposureRecord, LocationConfig, expRecordToUploadFilename

if TYPE_CHECKING:
    from lsst.summit.utils import ConsDbClient

    from .uploaders import MultiUploader

__all__ = [
    "getPlotSeqNumsForDayObs",
    "createChannelByName",
    "remakePlotByDataId",
    "remakeDay",
    "pushTestImageToCurrent",
    "remakeStarTrackerDay",
    "getDaysWithDataForPlotting",
    "getPlottingArgs",
    "syncBuckets",
]

# this file is for higher level utilities for use in notebooks, but also
# can be imported by other scripts and channels, especially the catchup ones.


def getDaysWithDataForPlotting(path):
    """Get a list of the days for which we have data for prototyping plots.

    Parameters
    ----------
    path : `str`
        The path to look for data in.

    Returns
    -------
    days : `list` [`int`]
        The days for which we have data.
    """
    reportFiles = glob.glob(os.path.join(path, "report_*.pickle"))
    mdTableFiles = glob.glob(os.path.join(path, "dayObs_*.json"))
    ccdVisitTableFiles = glob.glob(os.path.join(path, "ccdVisitTable_*.pickle"))

    reportDays = [
        int(filename.removeprefix(path + "/report_").removesuffix(".pickle")) for filename in reportFiles
    ]
    mdDays = [
        int(filename.removeprefix(path + "/dayObs_").removesuffix(".json")) for filename in mdTableFiles
    ]
    ccdVisitDays = [
        int(filename.removeprefix(path + "/ccdVisitTable_").removesuffix(".pickle"))
        for filename in ccdVisitTableFiles
    ]

    days = set.intersection(set(reportDays), set(mdDays), set(ccdVisitDays))
    return list(days)


def getPlottingArgs(butler, path, dayObs):
    """Get the args which are passed to a night report plot.

    Checks if the data is available for the specified ``dayObs`` at the
    specified ``path``, and returns the args which are passed to the plot
    function. The ``butler`` is largely unused, but we must pass one to
    reinstantiate a NightReport.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    path : `str`
        The path to look for data in.
    dayObs : `int`
        The dayObs to get the data for.

    Returns
    -------
    plottingArgs : `tuple`
        The args which are passed to the plot function. ``(report, mdTable,
        ccdVisitTable)`` A NightReport, a metadata table as a pandas dataframe,
        and a ccdVisit table.
    """
    from lsst.summit.utils import NightReport

    if dayObs not in getDaysWithDataForPlotting(path):
        raise ValueError(f"Data not available for {dayObs=} in {path}")

    reportFilename = os.path.join(path, f"report_{dayObs}.pickle")
    mdFilename = os.path.join(path, f"dayObs_{dayObs}.json")
    ccdVisitFilename = os.path.join(path, f"ccdVisitTable_{dayObs}.pickle")

    report = NightReport(butler, dayObs, reportFilename)

    mdTable = pd.read_json(mdFilename).T
    mdTable = mdTable.sort_index()

    with open(ccdVisitFilename, "rb") as input_file:
        ccdVisitTable = pickle.load(input_file)

    return report, mdTable, ccdVisitTable


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
        # TODO: either make bucket a mandatory arg or take a locationConfig and
        # create it from bucketName
        bucket = client.get_bucket("rubintv_data")

    dayObsStr = dayObsIntToString(dayObs)

    prefix = f"{channel}/{PREFIXES[channel]}_dayObs_{dayObsStr}"
    blobs = list(bucket.list_blobs(prefix=prefix))
    existing = [int(b.name.split(f"{prefix}_seqNum_")[1].replace(".png", "")) for b in blobs]
    return sorted(existing)


def createChannelByName(location, instrument, channel, *, embargo=False, doRaise=False):
    """Create a RubinTV Channel object using the name of the channel.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    instrument : `str`
        The instrument, e.g. 'LATISS' or 'LSSTComCam'.
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
    from .rubinTv import (
        ImExaminerChannel,
        MetadataCreator,
        MonitorChannel,
        MountTorqueChannel,
        SpecExaminerChannel,
    )

    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}.")

    locationConfig = LocationConfig(location)

    match channel:
        case "summit_imexam":
            return ImExaminerChannel(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "summit_specexam":
            return SpecExaminerChannel(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "auxtel_mount_torques":
            return MountTorqueChannel(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "auxtel_monitor":
            return MonitorChannel(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "auxtel_metadata":
            return MetadataCreator(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "all_sky_current":
            raise ValueError(f"{channel} is not a creatable by name.")
        case "all_sky_movies":
            raise ValueError(f"{channel} is not a creatable by name.")
        case _:
            raise ValueError(f"Unrecognized channel {channel}.")


def remakePlotByDataId(location, instrument, channel, dataId, embargo=False):
    """Remake the plot for the given channel for a single dataId.
    Reproduces the plot regardless of whether it exists. Raises on error.

    This method is very slow and inefficient for bulk processing, as it
    creates a Channel object for each plot - do *not* use in loops, use
    remakeDay() or write a custom scripts for bulk remaking.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    instrument : `str`
        The instrument, e.g. 'LATISS' or 'LSSTComCam'.
    channel : `str`
        The name of the channel.
    dataId : `dict`
        The dataId.
    embargo : `bool`, optional
        Use the embargo repo?
    """
    tvChannel = createChannelByName(location, instrument, channel, embargo=embargo, doRaise=True)
    expRecord = getExpRecordFromDataId(tvChannel.butler, dataId)
    tvChannel.callback(expRecord)


def remakeDay(
    location, instrument, channel, dayObs, *, remakeExisting=False, notebook=True, logger=None, embargo=False
):
    """Remake all the plots for a given day.

    Currently auxtel_metadata does not pull from the bucket to check what is
    in there, so remakeExisting is not supported.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    instrument : `str`
        The instrument, e.g. 'LATISS' or 'LSSTComCam'.
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
    logger : `logging.Logger`, optional
        The logger to use, created if not provided.
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

    if remakeExisting is False and channel in ["auxtel_metadata"]:
        raise ValueError(
            f"Channel {channel} can currently only remake everything or nothing. "
            "If you would like to remake everything, please explicitly pass "
            "remakeExisting=True."
        )

    if notebook:
        # notebooks have their own eventloops, so this is necessary if the
        # function is being run from within a notebook type environment
        import nest_asyncio

        nest_asyncio.apply()
        setupLogging()

    client = storage.Client()
    locationConfig = LocationConfig(location)
    bucket = client.get_bucket(locationConfig.bucketName)
    butler = makeDefaultLatissButler(embargo=embargo)

    allSeqNums = set(getSeqNumsForDayObs(butler, dayObs))
    logger.info(f"Found {len(allSeqNums)} seqNums to potentially create plots for.")
    existing = set()
    if not remakeExisting:
        existing = set(getPlotSeqNumsForDayObs(channel, dayObs, bucket=bucket))
        nToMake = len(allSeqNums) - len(existing)
        logger.info(
            f"Found {len(existing)} in the bucket which will be skipped, " f"leaving {nToMake} to create."
        )

    toMake = sorted(allSeqNums - existing)
    if not toMake:
        logger.info(f"Nothing to do for {channel} on {dayObs}")
        return

    # doRaise is False because during bulk plot remaking we expect many fails
    # due to image types, short exposures, etc.
    tvChannel = createChannelByName(location, instrument, channel, doRaise=False, embargo=embargo)
    for seqNum in toMake:
        dataId = {"day_obs": dayObs, "seq_num": seqNum, "detector": 0}
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
    # TODO: DM-43413 think about how you want the alternative to this to work
    # for S3 uploads, given there's a local and remote and many locations.
    # probably just want to use the MultiUploader auto magic, and then manually
    # set one of them to None if we don't want to use it, or something like
    # that. Will always have to be the remote which gets set to none, as local
    # is currently mandatory for the MultiUploader (though you could swap them)
    # in here if you wanted to sneakily use the same object.
    logger = logging.getLogger(__name__)

    from google.cloud import storage

    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}")
    if channel in [
        "auxtel_metadata",
        "auxtel_isr_runner",
        "all_sky_current",
        "all_sky_movies",
        "auxtel_movies",
    ]:
        raise ValueError(f"Pushing test data not supported for {channel}")
    if duration > 60:
        raise ValueError(f"Maximum time to leave test images in buckets is 60s, got {duration}")

    client = storage.Client()
    bucket = client.get_bucket(bucketName)
    prefix = f"{channel}/{PREFIXES[channel]}"
    blobs = list(bucket.list_blobs(prefix=prefix))

    logger.info(f"Found {len(blobs)} for channel {channel} in bucket")

    # names are like
    # 'auxtel_monitor/auxtel-monitor_dayObs_2021-07-06_seqNum_100.png'
    days = set([b.name.split(f"{prefix}_dayObs_")[1].split("_seqNum")[0] for b in blobs])
    days = [int(d.replace("-", "")) for d in days]  # days are like 2022-01-02
    recentDay = max(days)

    seqNums = getPlotSeqNumsForDayObs(channel, recentDay, bucket)
    newSeqNum = max(seqNums) + 1

    mockDataCoord = FakeExposureRecord(seq_num=newSeqNum, day_obs=recentDay)
    testCardFile = os.path.join(getPackageDir("rubintv_production"), "assets", "testcard_f.jpg")
    uploadAs = expRecordToUploadFilename(channel, mockDataCoord)
    uploader = Uploader(bucketName)

    logger.info(f"Uploading test card to {mockDataCoord} for channel {channel}")
    blob = uploader.googleUpload(channel, testCardFile, uploadAs, isLiveFile=True)

    logger.info(f"Upload complete, sleeping for {duration} for you to check...")
    time.sleep(duration)
    blob.delete()
    logger.info("Test card removed")


def remakeStarTrackerDay(
    *,
    dayObs,
    rootDataPath,
    outputRoot,
    metadataRoot,
    astrometryNetRefCatRoot,
    wide,
    remakeExisting=False,
    logger=None,
    forceMaxNum=None,
):
    """Remake all the star tracker plots for a given day.

    TODO: This needs updating post-refactor, but can wait for another ticket
    for now, as other work is required on the StarTracker side, and this will
    fit well with doing that.

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
    raise NotImplementedError("This needs updating post-refactor")
    from .starTracker import StarTrackerChannel, getRawDataDirForDayObs

    if not logger:
        logger = logging.getLogger("lsst.starTracker.remake")

    # doRaise is False because during bulk plot remaking we expect many fails
    tvChannel = StarTrackerChannel(
        wide=wide,
        rootDataPath=rootDataPath,
        metadataRoot=metadataRoot,
        outputRoot=outputRoot,
        astrometryNetRefCatRoot=astrometryNetRefCatRoot,
        doRaise=False,
    )

    _ifWide = "_wide" if wide else ""
    rawChannel = f"startracker{_ifWide}_raw"

    existing = getPlotSeqNumsForDayObs(rawChannel, dayObs)
    maxSeqNum = max(existing) if not forceMaxNum else forceMaxNum
    missing = [_ for _ in range(1, maxSeqNum) if _ not in existing]
    logger.info(f"Most recent = {maxSeqNum}, found {len(missing)} missing to create plots for: {missing}")

    dayPath = getRawDataDirForDayObs(rootDataPath=rootDataPath, wide=wide, dayObs=dayObs)

    files = glob.glob(os.path.join(dayPath, "*.fits"))
    foundFiles = {}
    for filename in files:
        # filenames are like GC101_O_20221114_000005.fits
        _, _, dayObs, seqNumAndSuffix = filename.split("_")
        seqNum = int(seqNumAndSuffix.removesuffix(".fits"))
        foundFiles[seqNum] = filename

    toRemake = missing if not remakeExisting else list(range(1, maxSeqNum))
    toRemake.reverse()  # always do the most recent ones first, burning down the list, not up

    for seqNum in toRemake:
        if seqNum not in foundFiles.keys():
            logger.warning(f"Failed to find raw file for {seqNum}, skipping...")
            continue
        filename = foundFiles[seqNum]
        logger.info(f"Processing {seqNum} from {filename}")
        tvChannel.callback(filename)


def syncBuckets(multiUploader: MultiUploader, locationConfig: LocationConfig) -> None:
    """Make sure all objects in the local bucket are also in the remote bucket.

    Call this function after a bad night to (slowly) send all the plots that
    didn't make it to USDF.

    Parameters
    ----------
    multiUploader : `MultiUploader`
        The multiUploader to use to sync the buckets.
    locationConfig : `LocationConfig`
        The location configuration to use, which contains the scratch path
        to exclude from the sync.
    """
    log = logging.getLogger(__name__)

    t0 = time.time()
    remoteBucket = multiUploader.remoteUploader._s3Bucket
    remoteObjects = set(o for o in remoteBucket.objects.all())
    log.info(f"Found {len(remoteObjects)} remote objects in {(time.time() - t0):.2f}s")

    t0 = time.time()
    localBucket = multiUploader.localUploader._s3Bucket
    localObjects = set(o for o in localBucket.objects.all())
    log.info(f"Found {len(localObjects)} local objects in {(time.time() - t0):.2f}s")

    # these are temp files, for local use only, and will be deleted in due
    # course anyway, so never sync the scratch area
    exclude = {o for o in localObjects if o.key.startswith(f"{locationConfig.scratchPath}")}

    remoteKeys = {o.key for o in remoteObjects}
    missing = {o for o in localObjects if o.key not in remoteKeys}
    missing -= exclude  # remove the scratch area from the missing list
    nMissing = len(missing)
    log.info(f"of which {nMissing} were missing from the remote. Copying missing items...")

    t0 = time.time()
    for i, obj in enumerate(missing):
        body = localBucket.Object(obj.key).get()["Body"].read()
        remoteBucket.Object(obj.key).upload_fileobj(io.BytesIO(body))
        del body
        if i % 100 == 0:
            log.info(f"Copied {i + 1} items of {len(missing)}, elapsed: {(time.time() - t0):.2f}s")

    log.info(f"Full copying took {(time.time() - t0):.2f} seconds")


def deleteAllSkyStills(bucket: Any) -> None:
    log = logging.getLogger(__name__)

    today = getCurrentDayObsInt()
    yesterday = calcPreviousDay(today)
    todayStr = dayObsIntToString(today)
    yesterdayStr = dayObsIntToString(yesterday)

    allSkyAll = [o for o in bucket.objects.filter(Prefix="allsky/")]
    stills = [o for o in allSkyAll if "still" in o.key]
    log.info(f"Found {len(stills)} all sky stills in total")
    filtered = [o for o in stills if todayStr not in o.key]
    filtered = [o for o in filtered if yesterdayStr not in o.key]
    log.info(f" of which {len(filtered)} are from before {yesterdayStr}")
    for i, obj in enumerate(filtered):
        obj.delete()
        if (i + 1) % 100 == 0:
            log.info(f"Deleted {i + 1} of {len(filtered)} stills...")
    log.info("Finished deleting stills")


def deleteNonFinalAllSkyMovies(bucket: Any) -> None:
    log = logging.getLogger(__name__)

    today = getCurrentDayObsInt()
    yesterday = calcPreviousDay(today)
    todayStr = dayObsIntToString(today)
    yesterdayStr = dayObsIntToString(yesterday)

    allSkyAll = [o for o in bucket.objects.filter(Prefix="allsky/")]
    movies = [o for o in allSkyAll if o.key.endswith(".mp4") and "final" not in o.key]
    log.info(f"Found {len(movies)} non-final all sky movies in total")
    filtered = [o for o in movies if todayStr not in o.key]
    filtered = [o for o in filtered if yesterdayStr not in o.key]
    log.info(f" of which {len(filtered)} are from before {yesterdayStr}")
    for i, obj in enumerate(filtered):
        obj.delete()
        if (i + 1) % 100 == 0:
            log.info(f"Deleted {i + 1} of {len(filtered)} non-final movies...")
    log.info("Finished deleting non-final movies")


def deleteConsDbColumn(client: ConsDbClient, instrument: str, table: str, column: str) -> None:
    """Delete all values in a column in a ConsDB table by setting them to NULL.

    Can only be used interactively, as it requires typing 'yes' to confirm.

    Parameters
    ----------
    client : `ConsDbClient`
        The ConsDbClient to use to connect to the database.
    instrument : `str`
        The instrument, e.g. 'latiss' or 'LSSTCam', case insensitive.
    table : `str`
        The table name, without the cdb_<instrument>. prefix.
    column : `str`
        The column name to delete values from.
    """
    inst = instrument.lower()
    table = table.lower()
    col = column.lower()

    print(f"Are you sure you want to clear the entire {col} column from cdb_{inst}.{table}?")
    print("This action cannot be undone. Type 'yes' to confirm.")
    response = input()
    if response != "yes":
        print("Aborted.")
        return

    query = (
        f"UPDATE cdb_{inst}.{table} SET {col} = NULL; "
        f"SELECT COUNT(*) FROM cdb_{inst}.{table} WHERE {col} IS NOT NULL;"
    )
    ret = client.query(query)
    print(f"{ret['count'].value[0]} rows remain with non-null values in {col}.")
