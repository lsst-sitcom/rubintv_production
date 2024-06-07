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

import glob
import io
import json
import logging
import math
import os
import sys
import time
import uuid
from contextlib import redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property

import numpy as np
import yaml

from lsst.daf.butler import DimensionConfig, DimensionRecord, DimensionUniverse
from lsst.resources import ResourcePath
from lsst.summit.utils.utils import dayObsIntToString, getCurrentDayObs_int
from lsst.utils import getPackageDir

from .channels import PREFIXES

__all__ = [
    "writeDimensionUniverseFile",
    "getDimensionUniverse",
    "writeDataIdFile",
    "getGlobPatternForDataProduct",
    "expRecordToUploadFilename",
    "checkRubinTvExternalPackages",
    "raiseIf",
    "getDoRaise",
    "isDayObsContiguous",
    "writeMetadataShard",
    "writeDataShard",
    "getShardedData",
    "isFileWorldWritable",
    "LocationConfig",
    "getAutomaticLocationConfig",
    "sanitizeNans",
    "safeJsonOpen",
    "getNumExpectedItems",
    "ALLOWED_DATASET_TYPES",
    "NumpyEncoder",
]

EFD_CLIENT_MISSING_MSG = (
    "ImportError: lsst_efd_client not found. Please install with:\n" "    pip install lsst-efd-client"
)

GOOGLE_CLOUD_MISSING_MSG = (
    "ImportError: Google cloud storage not found. Please install with:\n"
    "    pip install google-cloud-storage"
)

DATA_ID_TEMPLATE = os.path.join("{path}", "{instrument}_{dataProduct}_{expId}.json")

# ALLOWED_DATASET_TYPES is the types of data that can be written as sharded
# data. In principle, there is no reason this can't be or shouldn't be totally
# free-form, but because we need to delete any pre-existing data before
# processing starts, we maintain a list of allowed types so that we can loop
# over them and delete them. Having this list here and checking before writing
# ensures that the list is maintained, as new products can't be written without
# being added here first.
ALLOWED_DATASET_TYPES = ["rawNoises", "binnedImage"]
SEQNUM_PADDING = 6
SHARDED_DATA_TEMPLATE = os.path.join(
    "{path}", "dataShard-{dataSetName}-{instrument}-dayObs_{dayObs}" "_seqNum_{seqNum}_{suffix}.json"
)

# this file is for low level tools and should therefore not import
# anything from elsewhere in the package, this is strictly for importing from
# only.


def writeDimensionUniverseFile(butler, locationConfig):
    """Run on butler watcher startup.

    This assumes that all repos in a give location are on the same version, but
    we will make sure to keep that always true.
    """
    with open(locationConfig.dimensionUniverseFile, "w") as f:
        f.write(json.dumps(butler.dimensions.dimensionConfig.toDict()))


def getDimensionUniverse(locationConfig):
    duJson = safeJsonOpen(locationConfig.dimensionUniverseFile)
    return DimensionUniverse(DimensionConfig(duJson))


def writeDataIdFile(dataIdPath, dataProduct, expRecord, log=None):
    """Write a dataId file for a dataProduct to be consumed by a FileWatcher.

    Note that the dataProduct can be any string, it need not be restricted to
    dafButler dataProducts.

    Parameters
    ----------
    dataIdPath : `str`
        The path to write the dataId file to.
    dataProduct : `str`
        The data product to write the dataId file for.
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to write the dataId file for.
    log : `logging.Logger`, optional
        The logger to use. If provided, and info-level message is logged about
        where what file was written.
    """
    expId = expRecord.id
    dayObs = expRecord.day_obs
    seqNum = expRecord.seq_num
    outFile = DATA_ID_TEMPLATE.format(
        path=dataIdPath, instrument=expRecord.instrument, dataProduct=dataProduct, expId=expId
    )
    with open(outFile, "w") as f:
        f.write(json.dumps(expRecord.to_simple().json()))
    if log:
        log.info(f"Wrote dataId file for {dataProduct} {dayObs}/{seqNum}, {expId} to {outFile}")


def getGlobPatternForDataProduct(dataIdPath, dataProduct, instrument):
    """Get a glob-style pattern for finding dataId files for a dataProduct.

    These are the dataId files used to signal that a given dataId or
    dataProduct is ready for use.

    Parameters
    ----------
    dataIdPath : `str`
        The path find the dataIds in.
    dataProduct : `str`
        The data product to find the dataIds for.
    """
    return DATA_ID_TEMPLATE.format(path=dataIdPath, instrument=instrument, dataProduct=dataProduct, expId="*")


def getGlobPatternForShardedData(path, dataSetName, instrument, dayObs, seqNum):
    """Get a glob-style pattern for finding sharded data.

    These are the sharded data files used to store parts of the output data
    from the scatter part of the processing, ready to be gathered.

    Parameters
    ----------
    path : `str`
        The path find the sharded data.
    dataSetName : `str`
        The dataSetName to find the dataIds for, e.g. binnedImage.
    instrument : `str`
        The instrument to find the sharded data for.
    dayObs : `int`
        The dayObs to find the sharded data for.
    seqNum : `int`
        The seqNum to find the sharded data for.
    """
    seqNumFormatted = f"{seqNum:0{SEQNUM_PADDING}}" if seqNum != "*" else "*"
    return SHARDED_DATA_TEMPLATE.format(
        path=path,
        dataSetName=dataSetName,
        instrument=instrument,
        dayObs=dayObs,
        seqNum=seqNumFormatted,
        suffix="*",
    )


@dataclass
class FakeExposureRecord:
    """A minimal dataclass for passing to expRecordToUploadFilename.

    expRecordToUploadFilename accesses seq_num and day_obs as properties rather
    than dict items, so this dataclass exists to allow using the same naming
    function when we do not have a real dataId.
    """

    seq_num: int
    day_obs: int

    def __repr__(self):
        return f"{{day_obs={self.day_obs}, seq_num={self.seq_num}}}"


def expRecordFromJson(expRecordJson, locationConfig):
    """Deserialize a DimensionRecord from a JSON string.

    expRecordJson : `str`
        The JSON string to deserialize.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration, used to determine the dimension universe.
    """
    if not expRecordJson:
        return None
    return DimensionRecord.from_json(expRecordJson, universe=getDimensionUniverse(locationConfig))


def expRecordToUploadFilename(channel, expRecord, extension=".png", zeroPad=False):
    """Convert an expRecord to a filename, for use when uploading to a channel.

    Names the file in way the frontend app expects, zeropadding the seqNum if
    ``zeroPad`` is set (because the All Sky Cam channel has zero-padded seqNums
    but others do not).

    Parameters
    ----------
    channel : `str`
        The name of the RubinTV channel.
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to convert to a filename.

    Returns
    -------
    filename : `str`
        The filename.
    """
    dayObs = expRecord.day_obs
    seqNum = expRecord.seq_num
    dayObsStr = dayObsIntToString(dayObs)
    seqNumStr = f"{seqNum:05}" if zeroPad else str(seqNum)
    filename = f"{PREFIXES[channel]}_dayObs_{dayObsStr}_seqNum_{seqNumStr}{extension}"
    return filename


@dataclass(frozen=True)
class LocationConfig:
    """A frozen dataclass for holding location-based configurations.

    Note that all items which are used as paths *must* be decorated with
    @cached_property, otherwise they are will be method-type rather than
    str-type when they are accessed.
    """

    location: str
    log: logging.Logger = logging.getLogger("lsst.rubintv.production.utils.LocationConfig")

    def __post_init__(self) -> None:
        # Touch the _config after init to make sure the config file can be
        # read.
        # Any essential items can be touched here, but note they must all
        # exist in all the different locations, otherwise it will fail in some
        # locations and not others, so add things with caution.
        self._config

    def _checkDir(self, dirName: str, createIfMissing=True) -> None:
        """Check that a directory exists, optionally creating if it does not.

        Parameters
        ----------
        dirName : `str`
            The directory to check.
        createIfMissing : `bool`, optional
            If True, create the directory if it does not exist.

        Raises
            RuntimeError: raised if the directory does not exist and could not
            be created.

        TODO: Add a check for being world-writable here, and make the dir
        creation chmod to 777.
        """
        if not os.path.isdir(dirName):
            if createIfMissing:
                self.log.info(f"Directory {dirName} does not exist, creating it.")
                os.makedirs(dirName, exist_ok=True)  # exist_ok necessary due to potential startup race

                if not os.access(dirName, os.W_OK):  # check if dir is 777 but only when creating
                    try:  # attempt to chmod to 777
                        os.chmod(dirName, 0o777)
                    except PermissionError:
                        raise RuntimeError(
                            f"Directory {dirName} is not world-writable " "and could not be made so."
                        )

        # check whether we succeeded
        if not os.path.isdir(dirName):
            msg = f"Directory {dirName} does not exist"
            if createIfMissing:
                msg += " and could not be created."
            raise RuntimeError(msg)

    def _checkFile(self, filename):
        """Check that a file exists.

        Parameters
        ----------
        filename : `str`
            The file to check.

        Raises
            RuntimeError: raised if the file does not exist.
        """
        expanded = os.path.expandvars(filename)
        if not os.path.isfile(expanded):
            raise RuntimeError(f"Could not find file {filename} at {expanded}")

    @cached_property
    def _config(self):
        return _loadConfigFile(self.location)

    @cached_property
    def dimensionUniverseFile(self):
        file = self._config["dimensionUniverseFile"]
        return file

    @cached_property
    def sfmPipelineFile(self):
        file = self._config["sfmPipelineFile"]
        self._checkFile(file)
        return file

    def butlerPath(self):
        file = self._config["butlerPath"]
        self._checkFile(file)
        return file

    @cached_property
    def ts8ButlerPath(self):
        file = self._config["ts8ButlerPath"]
        self._checkFile(file)
        return file

    @cached_property
    def botButlerPath(self):
        file = self._config["botButlerPath"]
        self._checkFile(file)
        return file

    @cached_property
    def dataIdScanPath(self):
        directory = self._config["dataIdScanPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def metadataPath(self):
        directory = self._config["metadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def auxTelMetadataPath(self):
        directory = self._config["auxTelMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def metadataShardPath(self):
        directory = self._config["metadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def auxTelMetadataShardPath(self):
        directory = self._config["auxTelMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def ts8MetadataPath(self):
        directory = self._config["ts8MetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def ts8MetadataShardPath(self):
        directory = self._config["ts8MetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def plotPath(self):
        directory = self._config["plotPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def calculatedDataPath(self):
        directory = self._config["calculatedDataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def binnedCalexpPath(self):
        directory = self._config["binnedCalexpPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def bucketName(self):
        if self._config["bucketName"] == "":
            raise RuntimeError("Bucket name not set in config file")
        return self._config["bucketName"]

    @cached_property
    def binning(self):
        return self._config["binning"]

    # start of the summit migration stuff:
    # star tracker paths
    @cached_property
    def starTrackerDataPath(self):
        directory = self._config["starTrackerDataPath"]
        self._checkDir(directory, createIfMissing=False)
        return directory

    @cached_property
    def starTrackerMetadataPath(self):
        directory = self._config["starTrackerMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def starTrackerMetadataShardPath(self):
        directory = self._config["starTrackerMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def starTrackerOutputPath(self):
        directory = self._config["starTrackerOutputPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def astrometryNetRefCatPath(self):
        directory = self._config["astrometryNetRefCatPath"]
        self._checkDir(directory, createIfMissing=False)
        return directory

    # animation paths
    @cached_property
    def moviePngPath(self):
        directory = self._config["moviePngPath"]
        self._checkDir(directory)
        return directory

    # all sky cam paths
    @cached_property
    def allSkyRootDataPath(self):
        directory = self._config["allSkyRootDataPath"]
        self._checkDir(directory, createIfMissing=False)
        return directory

    @cached_property
    def allSkyOutputPath(self):
        directory = self._config["allSkyOutputPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def nightReportPath(self):
        directory = self._config["nightReportPath"]
        self._checkDir(directory)
        return directory

    # ComCam stuff:
    @cached_property
    def comCamButlerPath(self):
        file = self._config["comCamButlerPath"]
        self._checkFile(file)
        return file

    @cached_property
    def comCamMetadataPath(self):
        directory = self._config["comCamMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamMetadataShardPath(self):
        directory = self._config["comCamMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimMetadataPath(self):
        directory = self._config["comCamSimMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimMetadataShardPath(self):
        directory = self._config["comCamSimMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def botMetadataPath(self):
        directory = self._config["botMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def botMetadataShardPath(self):
        directory = self._config["botMetadataShardPath"]
        self._checkDir(directory)
        return directory

    # TMA config:
    @cached_property
    def tmaMetadataPath(self):
        directory = self._config["tmaMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def tmaMetadataShardPath(self):
        directory = self._config["tmaMetadataShardPath"]
        self._checkDir(directory)
        return directory

    def getOutputChain(self, instrument):
        return self._config["outputChains"][instrument]


def getAutomaticLocationConfig():
    """Get a location config, based on RA location and command line args.

    If no command line args have been supplied, get the LocationConfig based on
    where the code is being run. If a command line arg was supplied, use that
    as an override value.

    Returns
    -------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration.
    """
    if len(sys.argv) >= 2:
        try:  # try using this, because anything could be in argv[1]
            location = sys.argv[1]
            return LocationConfig(location)
        except FileNotFoundError:
            pass

    location = os.getenv("RAPID_ANALYSIS_LOCATION")
    if not location:
        raise RuntimeError("No location was supplied on the command line or via RAPID_ANALYSIS_LOCATION.")
    return LocationConfig(location)


def _loadConfigFile(site):
    """Get the site configuration, given a site name.

    Parameters
    ----------
    site : `str`, optional
        The site. If not provided, the default is 'summit'.

    Returns
    -------
    config : `dict`
        The configuration, as a dict.
    """
    packageDir = getPackageDir("rubintv_production")
    configFile = os.path.join(packageDir, "config", f"config_{site}.yaml")
    config = yaml.safe_load(open(configFile, "rb"))
    return config


def checkRubinTvExternalPackages(exitIfNotFound=True, logger=None):
    """Check whether the prerequsite installs for RubinTV are present.

    Some packages which aren't distributed with any metapackage are required
    to run RubinTV. This function is used to check if they're present so
    that unprotected imports don't cause the package to fail to import. It also
    allows checking in a single place, given that all are necessary for
    RubinTV's running.

    Parameters
    ----------
    exitIfNotFound : `bool`
        Terminate execution if imports are not present? Useful in bin scripts.
    logger : `logging.Logger`, optional
        The logger used to warn if packages are not present.
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


def raiseIf(doRaise, error, logger, msg=""):
    """Raises the error if ``doRaise`` otherwise logs it as a warning.

    Parameters
    ----------
    doRaise : `bool`
        Raise the error if True, otherwise logs it as a warning.
    error : `Exception`
        The error that has been raised.
    logger : `logging.Logger`
        The logger to warn with if ``doRaise`` is False.
    msg : `str`, optional
        Additional error message to log with the error.

    Raises
    ------
    AnyException
        Raised if ``self.doRaise`` is True, otherwise swallows and warns.
    """
    if not msg:
        msg = f"{error}"
    if doRaise:
        raise RuntimeError(msg) from error
    else:
        logger.exception(msg)


def getDoRaise():
    """Get the value of ``RAPID_ANALYSIS_DO_RAISE`` as a bool from the env.

    Defaults to False if not present or if the value cannot be interpreted as a
    boolean.

    Returns
    -------
    do_raise : `bool`
        Whether to raise exceptions or not.
    """
    doRaiseString = os.getenv("RAPID_ANALYSIS_DO_RAISE", "False").strip().lower()
    return doRaiseString in ["true", "1", "yes"]


def isDayObsContiguous(dayObs, otherDayObs):
    """Check if two dayObs integers are coniguous or not.

    DayObs take forms like 20220727 and therefore don't trivially compare.

    Parameters
    ----------
    dayObs : `int`
        The first dayObs to compare.
    otherDayObs : `int`
        The second dayObs to compare.

    Returns
    -------
    contiguous : `bool`
        Are the days contiguous?
    """
    d1 = datetime.strptime(str(dayObs), "%Y%m%d")
    d2 = datetime.strptime(str(otherDayObs), "%Y%m%d")
    deltaDays = d2.date() - d1.date()
    return deltaDays == timedelta(days=1) or deltaDays == timedelta(days=-1)


def hasDayRolledOver(dayObs, logger=None):
    """Check if the dayObs has rolled over when running constantly.

    Checks if supplied dayObs is the current dayObs and returns False
    if it is.

    Parameters
    ----------
    dayObs : `int`
        The dayObs to check if current
    logger : `logging.Logger`, optional
        The logger, created if not supplied

    Returns
    -------
    hasDayRolledOver : `bool`
        Whether the day has rolled over?
    """
    if not logger:
        logger = logging.getLogger(__name__)
    currentDay = getCurrentDayObs_int()
    if currentDay == dayObs:
        return False
    elif currentDay == dayObs + 1:
        return True
    else:
        if not isDayObsContiguous(currentDay, dayObs):
            logger.warning(
                f"Encountered non-linear time! dayObs supplied was {dayObs}"
                f" and now the current dayObs is {currentDay}!"
            )
        return True  # the day has still rolled over, just in an unexpected way


def catchPrintOutput(functionToCall, *args, **kwargs):
    f = io.StringIO()
    with redirect_stdout(f):
        functionToCall(*args, **kwargs)
    return f.getvalue()


def sanitizeNans(obj):
    """Recursively sanitize an object of any NaN-valued items.

    Nans are not JSON de-serializable, so this function replaces them with
    ``None``.

    Parameters
    ----------
    obj : `object`
        The object to santitize, expected to be dict-like: either a single
        ``dict`` or a ``list`` or ``dict`` of ``dict``s.

    Returns
    -------
    obj : `object`
        The object with any NaNs replaced with ``None``.
    """
    if isinstance(obj, list):
        return [sanitizeNans(o) for o in obj]
    elif isinstance(obj, dict):
        return {k: sanitizeNans(v) for k, v in obj.items()}
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    else:
        return obj


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def writeMetadataShard(path, dayObs, mdDict):
    """Write a piece of metadata for uploading to the main table.

    Parameters
    ----------
    path : `str`
        The path to write the file to.
    dayObs : `int`
        The dayObs.
    mdDict : `dict` of `dict`
        The metadata items to write, as a dict of dicts. Each key in the main
        dict should be a sequence number. Each value is a dict of values for
        that seqNum, as {'measurement_name': value}.

    Raises
    ------
    TypeError
        Raised if mdDict is not a dictionary.
    """
    if not isinstance(mdDict, dict):
        raise TypeError(f"mdDict must be a dict, not {type(mdDict)}")

    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)  # exist_ok True despite check to be concurrency-safe just in case

    suffix = uuid.uuid1()
    # this pattern is relied up elsewhere, so don't change it without updating
    # in at least the following places: mergeShardsAndUpload
    filename = os.path.join(path, f"metadata-dayObs_{dayObs}_{suffix}.json")

    with open(filename, "w") as f:
        json.dump(mdDict, f, cls=NumpyEncoder)
    if not isFileWorldWritable(filename):
        os.chmod(filename, 0o777)  # file may be deleted by another process, so make it world writable

    return


def writeExpRecordMetadataShard(expRecord, metadataShardPath):
    """Write the exposure record metedata to a shard.

    Only fires once, based on the value of TS8_METADATA_DETECTOR or
    LSSTCOMCAM_METADATA_DETECTOR, depending on the instrument.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record.
    """
    md = {}
    md["Exposure time"] = expRecord.exposure_time
    md["Image type"] = expRecord.observation_type
    md["Reason"] = expRecord.observation_reason
    md["Date begin"] = expRecord.timespan.begin.isot
    md["Program"] = expRecord.science_program
    md["Group name"] = expRecord.group_name
    md["Filter"] = expRecord.physical_filter
    md["Target"] = expRecord.target_name
    md["RA"] = expRecord.tracking_ra
    md["Dec"] = expRecord.tracking_dec
    md["Sky angle"] = expRecord.sky_angle
    md["Azimuth"] = expRecord.azimuth
    md["Elevation"] = 90 - expRecord.zenith_angle if expRecord.zenith_angle else None

    seqNum = expRecord.seq_num
    dayObs = expRecord.day_obs
    shardData = {seqNum: md}
    writeMetadataShard(metadataShardPath, dayObs, shardData)


def writeDataShard(path, instrument, dayObs, seqNum, dataSetName, dataDict):
    """Write some per-image data for merging later.

    Parameters
    ----------
    path : `str`
        The path to write to.
    instrument : `str`
        The instrument name, e.g. 'LSSTCam'.
    dayObs : `int`
        The dayObs.
    seqNum : `int`
        The seqNum.
    dataSetName : `str`
        The name of the data, e.g. 'perAmpNoise'
    dataDict : `dict` of `dict`
        The data to write.

    Raises
    ------
    TypeError
        Raised if dataDict is not a dictionary.
    """
    if dataSetName not in ALLOWED_DATASET_TYPES:
        raise ValueError(
            f"dataSetName must be one of {ALLOWED_DATASET_TYPES}, not {dataSetName}. If you are"
            " trying to add a new one, simply add it to the list."
        )

    if not isinstance(dataDict, dict):
        raise TypeError(f"dataDict must be a dict, not {type(dataDict)}")

    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)  # exist_ok True despite check to be concurrency-safe just in case

    filename = createFilenameForDataShard(
        path=path,
        instrument=instrument,
        dataSetName=dataSetName,
        dayObs=dayObs,
        seqNum=seqNum,
    )

    with open(filename, "w") as f:
        json.dump(dataDict, f, cls=NumpyEncoder)
    if not isFileWorldWritable(filename):
        os.chmod(filename, 0o777)  # file may be deleted by another process, so make it world writable

    return


def createFilenameForDataShard(path, dataSetName, instrument, dayObs, seqNum):
    """Get a filename to use for writing sharded data to.

    A filename is built from the SHARDED_DATA_TEMPLATE, with a random suffix
    added for sharding.

    Parameters
    ----------
    path : `str`
        The path to write to.
    dataSetName : `str`
        The name of the dataset, e.g. 'rawNoises'.
    instrument : `str`
        The name of the instrument, e.g. 'LSSTCam'.
    dayObs : `int`
        The dayObs.
    seqNum : `int`
        The seqNum.

    Returns
    -------
    filename : `str`
        The filename to write the data to.
    """
    suffix = uuid.uuid1()
    seqNumFormatted = f"{seqNum:0{SEQNUM_PADDING}}"
    filename = SHARDED_DATA_TEMPLATE.format(
        path=path,
        dataSetName=dataSetName,
        instrument=instrument,
        dayObs=dayObs,
        seqNum=seqNumFormatted,
        suffix=suffix,
    )
    return filename


def getShardedData(
    path,
    instrument,
    dayObs,
    seqNum,
    dataSetName,
    nExpected,
    timeout,
    logger=None,
    deleteIfComplete=True,
    deleteRegardless=False,
):
    """Read back the sharded data for a given dayObs, seqNum, and dataset.

    Looks for ``nExpected`` files in the directory ``path``, merges their
    contents and returns the merged data. If ``nExpected`` files are not found
    after ``timeout`` seconds, the items which have been found within the time
    limit are merged and returned.

    Parameters
    ----------
    path : `str`
        The path to write to.
    instrument : `str`
        The name of the instrument, e.g. 'LSSTCam'.
    dayObs : `int`
        The dayObs.
    seqNum : `int`
        The seqNum.
    dataSetName : `str`
        The name of the data, e.g. 'perAmpNoise'
    nExpected : `int`
        The number of expected items to wait for. Once ``nExpected`` files have
        been collected, their contents are merged and returned immediately. If
        ``nExpected`` items are not found after ``timeout`` seconds, the items
        which have been found within the time limit are merged and returned.
    timeout : `float`
        The timeout period after which to give up waiting for files to land.
    logger : `logging.Logger`, optional
        The logger for logging warnings if files don't appear.
    deleteIfComplete : `bool`, optional
        Delete the input datafiles if there were the number expected?
    deleteRegardless : `bool`, optional
        If True, delete the files after reading them, regardless of whether the
        expected number of items were found.

    Returns
    -------
    data : `dict` of `dict`
        The merged data.
    nFiles : `int`
        The number of shard files found and merged into ``data``.

    Raises
    ------
    TypeError
        Raised if dataDict is not a dictionary.
    """
    pattern = getGlobPatternForShardedData(
        path=path, instrument=instrument, dataSetName=dataSetName, dayObs=dayObs, seqNum=seqNum
    )

    start = time.time()
    firstLoop = True
    while firstLoop or (time.time() - start < timeout):
        firstLoop = False  # ensure we always run at least once
        files = glob.glob(pattern)
        if len(files) > nExpected:
            # it is ambiguous which to use to form a coherent set, so raise
            raise RuntimeError(f"Too many data files found for {dataSetName} for {dayObs=}-{seqNum=}")
        if len(files) == nExpected:
            break
        time.sleep(0.2)

    if len(files) != nExpected:
        if not logger:
            logger = logging.getLogger(__name__)
        logger.warning(
            f"Found {len(files)} files after waiting {timeout}s for {dataSetName}"
            f" for {dayObs=}-{seqNum=} but expected {nExpected}"
        )

    if not files:
        return None, 0

    data = {}
    for dataShard in files:
        with open(dataShard) as f:
            shard = json.load(f)
        data.update(shard)
        if deleteRegardless or (deleteIfComplete and len(files) == nExpected):
            os.remove(dataShard)
    return data, len(files)


def isFileWorldWritable(filename):
    """Check that the file has the correct permissions for write access.

    Parameters
    ----------
    filename : `str`
        The filename to check.

    Returns
    -------
    ok : `bool`
        True if the file has the correct permissions, False otherwise.
    """
    stat = os.stat(filename)
    return stat.st_mode & 0o777 == 0o777


def safeJsonOpen(filename, timeout=0.3):
    """Open a JSON file, waiting for it to be populated if necessary.

    JSON doesn't like opening zero-byte files, so try to open it, and if it's
    empty, add a series of small waits until it's not empty and reads
    correctly, or the timeout is reachecd.

    Parameters
    ----------
    filename : `str`
        The filename to open.
    timeout : `float`, optional
        The timeout period after which to give up waiting for the contents to
        populate.

    Returns
    -------
    jsonData : `dict`
        The data from the json file as (potentially very nested) a dict.

    Raises
    ------
    RuntimeError: Raised if the file is not populated within the timeout.
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            with open(filename, "r") as f:
                jsonData = json.load(f)
                return jsonData
        except (RuntimeError, json.decoder.JSONDecodeError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"Failed to load data from {filename} after {timeout}s")


def getNumExpectedItems(expRecord, logger=None):
    """A placeholder function for getting the number of expected items.

    For a given instrument, get the number of detectors which were read out or
    for which we otherwise expect to have data for.

    This method will be updated once we have a way of knowing, from the camera,
    how many detectors were actually read out (the plan is the CCS writes a
    JSON file with this info).

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record. This is currently unused, but will be used once
        we are doing this properly.
    logger : `logging.Logger`
        The logger, created if not supplied.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    instrument = expRecord.instrument

    fallbackValue = None
    if instrument == "LATISS":
        fallbackValue = 1
    elif instrument == "LSSTCam":
        fallbackValue = 201
    elif instrument in ["LSST-TS8", "LSSTComCam", "LSSTComCamSim"]:
        fallbackValue = 9
    else:
        raise ValueError(f"Unknown instrument {instrument}")

    if instrument == "LSSTComCamSim":
        return fallbackValue  # it's always nine (it's simulated), and this will all be redone soon anyway

    try:
        resourcePath = (
            f"s3://rubin-sts/{expRecord.instrument}/{expRecord.day_obs}/{expRecord.obs_id}/"
            f"{expRecord.obs_id}_expectedSensors.json"
        )
        url = ResourcePath(resourcePath)
        jsonData = url.read()
        data = json.loads(jsonData)
        nExpected = len(data["expectedSensors"])
        if nExpected != fallbackValue:
            # not a warning because this is it working as expected, but it's
            # nice to see when we have a partial readout
            logger.debug(
                f"Partial focal plane readout detected: expected number of items ({nExpected}) "
                f" is different from the nominal value of {fallbackValue} for {instrument}"
            )
        return nExpected
    except FileNotFoundError:
        if instrument in ["LSSTCam", "LSST-TS8"]:
            # these instruments are expected to have this info, the other are
            # not yet, so only warn when the file is expected and not found.
            logger.warning(
                f"Unable to get number of expected items from {resourcePath}, "
                f"using fallback value of {fallbackValue}"
            )
        return fallbackValue
    except Exception:
        logger.exception(
            "Error calculating expected number of items, using fallback value " f"of {fallbackValue}"
        )
        return fallbackValue


def getShardPath(locationConfig, expRecord):
    """Get the path to the metadata shard for the given exposure record.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to get the shard path for.

    Returns
    -------
    shardPath : `str`
        The path to write the metadata shard to.
    """
    match expRecord.instrument:
        case "LATISS":
            return locationConfig.auxTelMetadataShardPath
        case "LSSTComCam":
            return locationConfig.comCamMetadataShardPath
        case "LSSTComCamSim":
            return locationConfig.comCamSimMetadataShardPath
        case _:
            raise ValueError(f"Unknown instrument {expRecord.instrument=}")
