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

import logging
from time import sleep

from lsst.summit.utils.butlerUtils import getSeqNumsForDayObs, makeDefaultLatissButler
from lsst.summit.utils.utils import dayObsIntToString, setupLogging
from . import CHANNELS, PREFIXES
from . import (ImExaminerChannel,
               SpecExaminerChannel,
               MountTorqueChannel,
               MonitorChannel,
               MetadataServer,
               )

__all__ = ["checkRubinTvExternalPackages",
           ]

EFD_CLIENT_MISSING_MSG = ('ImportError: lsst_efd_client not found. Please install with:\n'
                          '    pip install lsst-efd-client')

GOOGLE_CLOUD_MISSING_MSG = ('ImportError: Google cloud storage not found. Please install with:\n'
                            '    pip install google-cloud-storage')


def checkRubinTvExternalPackages(exitIfNotFound=True, logger=None):
    """Check whether the prerequsite installs for RubinTV are present.

    Some packages which aren't distributed with any metapackage are required
    to run RubinTV. This function is used to check if they're present so
    that unprotected imports don't cause the package to fail to import. It also
    allows checking in a singple place, given that all are necessary for
    RubinTV's running.

    Parameters
    ----------
    exitIfNotFound : `bool`
        Terminate execution if imports are not present? Useful in bin scripts.
    logger : `logging.Log`
        The logger used to warn is packages are not present.
    """
    if not logger:
        logger = logging.getLogger(__name__)

    hasGoogleStorage = False
    hasEfdClient = False
    try:
        from google.cloud import storage  # noqa: F401
        hasGoogleStorage = True
    except ImportError:
        pass

    try:
        from lsst_efd_client import EfdClient  # noqa: F401
        hasEfdClient = True
    except ImportError:
        pass

    if not hasGoogleStorage:
        logger.warning(GOOGLE_CLOUD_MISSING_MSG)

    if not hasEfdClient:
        logger.warning(EFD_CLIENT_MISSING_MSG)

    if exitIfNotFound and (not hasGoogleStorage or not hasEfdClient):
        exit()


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


def createChannelByName(channel, doRaise, **kwargs):
    """Create a RubinTV Channel object using the name of the channel.

    Parameters
    ----------
    channel : `str`
        The name of the channel, as found in lsst.rubintv.production.CHANNELS.
    doRaise : `bool`
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
    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}.")

    match channel:
        case "summit_imexam":
            return ImExaminerChannel(doRaise=doRaise)
        case "summit_specexam":
            return SpecExaminerChannel(doRaise=doRaise)
        case "auxtel_mount_torques":
            return MountTorqueChannel(doRaise=doRaise)
        case "auxtel_monitor":
            return MonitorChannel(doRaise=doRaise)
        case "auxtel_metadata":
            if 'outputRoot' not in kwargs:
                raise RuntimeError("Must provide writeable output root outputRoot via kwargs for "
                                   "auxtel_metadata channel")
            return MetadataServer(doRaise=doRaise, **kwargs)
        case "all_sky_current":
            raise ValueError(f"{channel} is not a creatable by name.")
        case "all_sky_movies":
            raise ValueError(f"{channel} is not a creatable by name.")
        case _:
            raise ValueError(f"Unrecognized channel {channel}.")


def remakePlotByDataId(channel, dataId):
    """Remake the plot for the given channel for a single dataId.
    Reproduces the plot regardless of whether it exists. Raises on error.

    This method is very slow and inefficient for bulk processing, as it
    creates a Channel object for each plot - do *not* use in loops, use
    remakeDay() or write a custom scripts for bulk remaking.

    Parameters
    ----------
    channel : `str`
        The name of the channel.
    dataId : `dict`
        The dataId.
    """
    tvChannel = createChannelByName(channel, doRaise=True)
    tvChannel.callback(dataId)


def remakeDay(channel, dayObs, remakeExisting=False, notebook=True, logger=None, **kwargs):
    """Remake all the plots for a given day.

    Currently auxtel_metadata does not pull from the bucket to check what is
    in there, so remakeExisting is not supported.

    Parameters
    ----------
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

    Raises
    ------
    ValueError:
        Raised if the channel is unknown.
        Raised if remakeExisting is False and channel is auxtel_metadata
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
    butler = makeDefaultLatissButler()

    allSeqNums = set(getSeqNumsForDayObs(butler, dayObs))
    logger.info(f"Found {len(allSeqNums)} seqNums to potentially create plots for.")
    existing = set()
    if not remakeExisting:
        existing = set(getPlotSeqNumsForDayObs(channel, dayObs, bucket=bucket))
        nToMake = len(allSeqNums) - len(existing)
        logger.info(f"Found {len(existing)} in the bucket which will be skipped, "
                    f"leaving {nToMake} to create.")

    toMake = sorted(allSeqNums - existing)

    # doRaise is False because during bulk plot remaking we expect many fails
    # due to image types, short exposures, etc.
    tvChannel = createChannelByName(channel, doRaise=False, **kwargs)
    for seqNum in toMake:
        dataId = {'day_obs': dayObs, 'seq_num': seqNum, 'detector': 0}
        tvChannel.callback(dataId)
        if channel in ['auxtel_metadata']:
            sleep(1.5)  # metadata creation is fast enough that we hit the GCS rate limit without a pause
