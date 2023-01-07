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
import os
import logging
from glob import glob
from time import sleep
import datetime
import traceback

from lsst.summit.utils.utils import (getCurrentDayObs_int,
                                     getAltAzFromSkyPosition,
                                     dayObsIntToString,
                                     starTrackerFileToExposure,
                                     )
from lsst.summit.utils.astrometry import CommandLineSolver
from lsst.summit.utils.astrometry.plotting import plot
from lsst.summit.utils.astrometry.utils import (runCharactierizeImage,
                                                filterSourceCatOnBrightest,
                                                getAverageAzFromHeader,
                                                getAverageElFromHeader,
                                                )

from .channels import PREFIXES
from .utils import writeMetadataShard, isFileWorldWritable, raiseIf
from .uploaders import Uploader, Heartbeater
from .baseChannels import BaseChannel

_LOG = logging.getLogger(__name__)


def getCurrentRawDataDir(rootDataPath, wide):
    """Get the raw data dir corresponding to the current dayObs.

    Returns
    -------
    path : `str`
        The raw data dir for today.
    wide : `bool`
        Is this the wide field camera?
    """
    todayInt = getCurrentDayObs_int()
    return getRawDataDirForDayObs(rootDataPath, wide, todayInt)


def getRawDataDirForDayObs(rootDataPath, wide, dayObs):
    """Get the raw data dir for a given dayObs.

    Parameters
    ----------
    rootDataPath : `str`
        The root data path.
    wide : `bool`
        Is this the wide field camera?
    dayObs : `int`
        The dayObs.
    """
    camNum = 101 if wide else 102  # TODO move this config to the top somehow?
    dayObsDateTime = datetime.datetime.strptime(str(dayObs), '%Y%m%d')
    dirSuffix = (f'GenericCamera/{camNum}/{dayObsDateTime.year}/'
                 f'{dayObsDateTime.month:02}/{dayObsDateTime.day:02}/')
    return os.path.join(rootDataPath, dirSuffix)


def dayObsToDateTime(dayObs):
    """Convert a dayObs to a datetime.

    Parameters
    ----------
    dayObs : `int`
        The dayObs.

    Returns
    -------
    datetime : `datetime`
        The datetime.
    """
    return datetime.datetime.strptime(dayObsIntToString(dayObs), '%Y%m%d')


def dayObsSeqNumFromFilename(filename):
    """Get the dayObs and seqNum from a filename.

    Parameters
    ----------
    filename : `str`
        The filename.

    Returns
    -------
    dayObs : `int`
        The dayObs.
    seqNum : `int`
        The seqNum.
    """
    # filenames are like GC101_O_20221114_000005.fits
    filename = os.path.basename(filename)  # in case we're passed a full path
    _, _, dayObs, seqNumAndSuffix = filename.split('_')
    dayObs = int(dayObs)
    seqNum = int(seqNumAndSuffix.removesuffix('.fits'))
    return dayObs, seqNum


class StarTrackerWatcher():
    """Class for continuously watching for new files landing in the directory.
    Uploads a heartbeat to the bucket every ``HEARTBEAT_PERIOD`` seconds.

    Parameters
    ----------
    rootDataPath : `str`
        The root directory to watch for files landing in. Should not include
        the GenericCamera/101/ or GenericCamera/102/ part, just the base
        directory that these are being written to, as visible from k8s.
    bucketName : `str`
        The bucket to upload the heartbeats to.
    wide : `bool`
        Whether to watch the wide or narrow camera.
    """
    cadence = 1  # in seconds

    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 240

    def __init__(self, *, rootDataPath, bucketName, wide):
        self.rootDataPath = rootDataPath
        self.wide = wide
        self.uploader = Uploader(bucketName)
        self.log = _LOG.getChild("watcher")
        self.heartbeater = None

    def _getLatestImageDataIdAndExpId(self):
        """Get the dataId and expId for the most recent image in the repo.
        """
        currentDir = getCurrentRawDataDir(self.rootDataPath, self.wide)
        files = glob(os.path.join(currentDir, '*.fits'))
        files = sorted(files, reverse=True)  # everything is zero-padded so sorts nicely
        if not files:
            return None, None, None, None

        latestFile = files[0]

        # filenames are like GC101_O_20221114_000005.fits
        _, _, dayObs, seqNumAndSuffix = latestFile.split('_')
        dayObs = int(dayObs)
        seqNum = int(seqNumAndSuffix.removesuffix('.fits'))
        expId = int(str(dayObs) + seqNumAndSuffix.removesuffix('.fits'))
        return latestFile, dayObs, seqNum, expId

    def run(self, callback):
        """Wait for the image to land, then run callback(filename).

        Parameters
        ----------
        callback : `callable`
            The method to call, with the latest filename as the argument.
        """
        lastFound = -1
        filename = 'no filename'
        while True:
            try:
                filename, _, _, expId = self._getLatestImageDataIdAndExpId()
                self.log.debug(f"{filename}")
                if self.heartbeater:  # gets set by the channel post-init
                    self.heartbeater.beat()

                if (filename is None) or (lastFound == expId):
                    self.log.debug('Found nothing, sleeping')
                    sleep(self.cadence)
                    continue
                else:
                    lastFound = expId
                    callback(filename)

            except Exception as e:
                self.log.warning(f'Skipped {filename} due to {e}')
                traceback.print_exc()


class StarTrackerChannel(BaseChannel):
    """Class for serving star tracker images to RubinTV.

    These channels are somewhat hybrid channels which serve both the raw
    images and their analyses. The metadata is also written as shards from
    these channels, with the metadata server itself just functioning to collate
    the shards and upload them.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    wide : `bool`
        Do this for the wide or narrow camera?
    doRaise : `bool`, optional
        Raise on error? Default False, useful for debugging.
    """
    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 240

    def __init__(self,
                 locationConfig,
                 *,
                 wide,
                 doRaise=False):

        name = 'starTracker' + ('_wide' if wide else '')
        log = logging.getLogger(f'lsst.rubintv.production.{name}')
        self.rootDataPath = locationConfig.starTrackerDataPath
        watcher = StarTrackerWatcher(rootDataPath=self.rootDataPath,
                                     bucketName=locationConfig.bucketName,
                                     wide=wide)

        super().__init__(locationConfig=locationConfig,
                         log=log,
                         watcher=watcher,
                         doRaise=doRaise)

        self.channelRaw = f"startracker{'_wide' if wide else ''}_raw"
        self.channelAnalysis = f"startracker{'_wide' if wide else ''}_analysis"
        self.wide = wide

        self.outputRoot = self.locationConfig.starTrackerOutputPath
        self.metadataRoot = self.locationConfig.starTrackerMetadataPath
        self.astrometryNetRefCatRoot = self.locationConfig.astrometryNetRefCatPath
        self.doRaise = doRaise
        self.shardsDir = os.path.join(self.metadataRoot, 'shards')
        for path in (self.outputRoot, self.shardsDir, self.metadataRoot):
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                raise RuntimeError(f"Failed to find/create {path}") from e

        self.heartbeaterAnalysis = Heartbeater(self.channelAnalysis,
                                               self.locationConfig.bucketName,
                                               self.HEARTBEAT_UPLOAD_PERIOD,
                                               self.HEARTBEAT_FLATLINE_PERIOD)
        self.heartbeaterRaw = Heartbeater(self.channelRaw,
                                          self.locationConfig.bucketName,
                                          self.HEARTBEAT_UPLOAD_PERIOD,
                                          self.HEARTBEAT_FLATLINE_PERIOD)
        self.watcher.heartbeater = self.heartbeaterRaw  # so that it can be called in the watch loop

        self.solver = CommandLineSolver(indexFilePath=self.locationConfig.astrometryNetRefCatPath,
                                        checkInParallel=True)

    def writeDefaultPointingShardForFilename(self, exp, filename):
        """Write a metadata shard for the given filename.
        """
        _ifWide = ' wide' if self.wide else ''
        dayObs, seqNum = dayObsSeqNumFromFilename(filename)
        expMd = exp.getMetadata().toDict()
        expTime = exp.visitInfo.exposureTime
        contents = {}
        ra = exp.getWcs().getSkyOrigin().getRa().asDegrees()
        dec = exp.getWcs().getSkyOrigin().getDec().asDegrees()

        az = None
        try:
            az = getAverageAzFromHeader(expMd)
        except RuntimeError:
            self.log.warning(f'Failed to get az from header for {filename}')

        alt = None
        try:
            alt = getAverageElFromHeader(expMd)
        except RuntimeError:
            self.log.warning(f'Failed to get alt from header for {filename}')

        contents = {
            f"Exposure Time{_ifWide}": expTime,
            f"Ra{_ifWide}": ra,
            f"Dec{_ifWide}": dec,
            f"Alt{_ifWide}": alt,
            f"Az{_ifWide}": az,
        }
        md = {seqNum: contents}
        writeMetadataShard(self.shardsDir, dayObs, md)

    def _getUploadFilename(self, channel, filename):
        """Calculate the filename to use for uploading.
        """
        dayObs, seqNum = dayObsSeqNumFromFilename(filename)
        dayObsStr = dayObsIntToString(dayObs)
        filename = f"{PREFIXES[channel]}_dayObs_{dayObsStr}_seqNum_{seqNum}.png"
        return filename

    def runAnalysis(self, exp, filename):
        """Run the analysis and upload the results.
        """
        _ifWide = ' wide' if self.wide else ''  # the string to append to md keys
        oldWcs = exp.getWcs()

        basename = os.path.basename(filename).removesuffix('.fits')
        fittedPngFilename = os.path.join(self.outputRoot, basename + '_fitted.png')
        dayObs, seqNum = dayObsSeqNumFromFilename(filename)
        self.heartbeaterAnalysis.beat()  # we're alive and at least trying to solve

        # TODO: Need to pull these wide/non-wide config options into a
        # centralised place. snr, minPix, scaleError, indexFiles on init
        snr = 5 if self.wide else 2.5
        minPix = 25 if self.wide else 10
        brightSourceFraction = 0.8 if self.wide else 0.95
        imCharResult = runCharactierizeImage(exp, snr, minPix)

        sourceCatalog = imCharResult.sourceCat
        md = {seqNum: {f"nSources{_ifWide}": len(sourceCatalog)}}
        writeMetadataShard(self.shardsDir, dayObs, md)
        if not sourceCatalog:
            raise RuntimeError('Failed to find any sources in image')

        filteredSources = filterSourceCatOnBrightest(sourceCatalog, brightSourceFraction)
        md = {seqNum: {f"nSources filtered{_ifWide}": len(filteredSources)}}
        writeMetadataShard(self.shardsDir, dayObs, md)

        plot(exp, sourceCatalog, filteredSources, saveAs=fittedPngFilename)
        uploadAs = self._getUploadFilename(self.channelAnalysis, filename)
        self.uploader.googleUpload(self.channelAnalysis, fittedPngFilename, uploadAs)

        scaleError = 10 if self.wide else 40
        result = self.solver.run(exp, filteredSources,
                                 isWideField=self.wide,
                                 percentageScaleError=scaleError,
                                 silent=True)

        if not result:
            self.log.warning(f"Failed to find solution for {basename}")
            return

        newWcs = result.wcs

        calculatedRa, calculatedDec = newWcs.getSkyOrigin()
        nominalRa, nominalDec = oldWcs.getSkyOrigin()

        deltaRa = calculatedRa - nominalRa
        deltaDec = calculatedDec - nominalDec

        oldAlt, oldAz = getAltAzFromSkyPosition(oldWcs.getSkyOrigin(), exp.visitInfo)
        newAlt, newAz = getAltAzFromSkyPosition(newWcs.getSkyOrigin(), exp.visitInfo)

        deltaAlt = newAlt - oldAlt
        deltaAz = newAz - oldAz

        deltaRot = newWcs.getRelativeRotationToWcs(oldWcs).asArcseconds()

        result = {
            'Calculated Ra': calculatedRa.asDegrees(),
            'Calculated Dec': calculatedDec.asDegrees(),
            'Calculated Alt': newAlt.asDegrees(),
            'Calculated Az': newAz.asDegrees(),
            'Delta Ra Arcsec': deltaRa.asArcseconds(),
            'Delta Dec Arcsec': deltaDec.asArcseconds(),
            'Delta Alt Arcsec': deltaAlt.asArcseconds(),
            'Delta Az Arcsec': deltaAz.asArcseconds(),
            'Delta Rot Arcsec': deltaRot,
            'RMS scatter arcsec': result.rmsErrorArsec,
            'RMS scatter pixels': result.rmsErrorPixels,
        }
        contents = {k + _ifWide: v for k, v in result.items()}
        md = {seqNum: contents}
        writeMetadataShard(self.shardsDir, dayObs, md)

    def callback(self, filename):
        """Callback for the watcher, called when a new image lands.
        """
        exp = starTrackerFileToExposure(filename, self.log)  # make the exp and set the wcs from the header
        self.heartbeaterRaw.beat()  # we loaded the file, so we're alive and running for raws

        # plot the raw file and upload it
        basename = os.path.basename(filename).removesuffix('.fits')
        rawPngFilename = os.path.join(self.outputRoot, basename + '_raw.png')  # for saving to disk
        plot(exp, saveAs=rawPngFilename)
        uploadFilename = self._getUploadFilename(self.channelRaw, filename)  # get the filename for the bucket
        self.uploader.googleUpload(self.channelRaw, rawPngFilename, uploadFilename)

        if not exp.wcs:
            self.log.info(f"Skipping {filename} as it has no WCS")
            return
        if not exp.visitInfo.date.isValid():
            self.log.warning(f"exp.visitInfo.date is not valid. {filename} will still be fitted"
                             " but the alt/az values reported will be garbage")

        # metadata a shard with just the pointing info etc
        self.writeDefaultPointingShardForFilename(exp, filename)
        try:
            # writes shards as it goes
            self.runAnalysis(exp, filename)
        except Exception as e:
            self.log.warning(f"Failed to run analysis on {filename}: {repr(e)}")
            traceback.print_exc()


class StarTrackerMetadataServer():
    # TODO: Change this to use the TimedMetadataServer
    """Class for serving star tracker metadata to RubinTV.
    """
    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 120

    def __init__(self,
                 locationConfig,
                 *,
                 doRaise=False):
        self.locationConfig = locationConfig
        self.uploader = Uploader(self.locationConfig.bucketName)
        self.heartbeater = Heartbeater('startracker_metadata',
                                       self.locationConfig.bucketName,
                                       self.HEARTBEAT_UPLOAD_PERIOD,
                                       self.HEARTBEAT_FLATLINE_PERIOD)
        self.channel = 'startracker_metadata'
        self.log = _LOG.getChild(f"{self.channel}")
        self.metadataRoot = self.locationConfig.starTrackerMetadataPath
        self.shardsDir = os.path.join(self.metadataRoot, 'shards')
        self.doRaise = doRaise
        try:
            os.makedirs(self.shardsDir, exist_ok=True)
        except Exception as e:
            raise RuntimeError(f"Failed to find/create {self.shardsDir}") from e

    def mergeShardsAndUpload(self):
        """Merge all the shards in the shard directory into their respective
        files and upload the updated files.

        For each file found in the shard directory, merge its contents into the
        main json file for the corresponding dayObs, and for each file updated,
        upload it.
        """
        filesTouched = set()
        shardFiles = sorted(glob(os.path.join(self.shardsDir, "metadata-*")))
        if shardFiles:
            self.log.debug(f'Found {len(shardFiles)} shardFiles')
            sleep(0.1)  # just in case a shard is in the process of being written

        for shardFile in shardFiles:
            # filenames look like
            # metadata-dayObs_20221027_049a5f12-5b96-11ed-80f0-348002f0628.json
            filename = os.path.basename(shardFile)
            dayObs = int(filename.split("_", 2)[1])
            mainFile = self.getSidecarFilename(dayObs)
            filesTouched.add(mainFile)

            data = {}
            # json.load() doesn't like empty files so check size is non-zero
            if os.path.isfile(mainFile) and os.path.getsize(mainFile) > 0:
                with open(mainFile) as f:
                    data = json.load(f)

            with open(shardFile) as f:
                shard = json.load(f)
            if shard:
                for row in shard:
                    if row in data:
                        data[row].update(shard[row])
                    else:
                        data.update({row: shard[row]})
            os.remove(shardFile)

            with open(mainFile, 'w') as f:
                json.dump(data, f)
            if not isFileWorldWritable(mainFile):
                os.chmod(mainFile, 0o777)  # file may be amended by another process

        if filesTouched:
            self.log.info(f"Uploading {len(filesTouched)} metadata files")
            for file in filesTouched:
                self.uploader.googleUpload(self.channel, file, isLiveFile=True)
        return

    def getSidecarFilename(self, dayObs):
        """Get the name of the metadata sidecar file for the dayObs.

        Returns
        -------
        dayObs : `int`
            The dayObs.
        """
        return os.path.join(self.metadataRoot, f'dayObs_{dayObs}.json')

    def callback(self):
        """Method called on each new dataId as it is found in the repo.

        Add the metadata to the sidecar for the dataId and upload.
        """
        try:
            self.log.info('Getting metadata from shards')
            self.mergeShardsAndUpload()  # updates all shards everywhere

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def run(self):
        """Run continuously, looking for metadata and uploading.
        """
        while True:
            self.callback()
            self.heartbeater.beat()
            sleep(1.5)
