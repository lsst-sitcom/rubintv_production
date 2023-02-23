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
from glob import glob
from time import sleep
import datetime
import traceback
from dataclasses import dataclass
import pandas as pd

import lsst.geom as geom

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
from .utils import writeMetadataShard, hasDayRolledOver, raiseIf
from .uploaders import Uploader, Heartbeater
from .baseChannels import BaseChannel
from .plotting import starTrackerNightReportPlots

__all__ = ('StarTrackerCamera',
           'StarTrackerWatcher',
           'StarTrackerChannel',
           'StarTrackerNightReportChannel')

_LOG = logging.getLogger(__name__)
KNOWN_CAMERAS = ("regular", "wide", "fast")


@dataclass(frozen=True)
class StarTrackerCamera:
    """A frozen dataclass for StarTracker camera configs
    """
    cameraType: str
    suffix: str
    suffixWithSpace: str
    doAstrometry: bool
    cameraNumber: int
    snr: float
    minPix: int
    brightSourceFraction: float
    scaleError: float
    doSmoothPlot: bool


regularCam = StarTrackerCamera('regular', '', '', True, 102, 5, 25, 0.95, 5, True)
wideCam = StarTrackerCamera('wide', '_wide', ' wide', True, 101, 5, 25, 0.8, 5, True)
fastCam = StarTrackerCamera('fast', '_fast', ' fast', True, 103, 2.5, 10, 0.95, 60, False)


def getCurrentRawDataDir(rootDataPath, camera):
    """Get the raw data dir corresponding to the current dayObs.

    Returns
    -------
    path : `str`
        The raw data dir for today.
    camera : `lsst.rubintv.production.starTracker.StarTrackerCamera`
        The camera to get the raw data for.
    """
    todayInt = getCurrentDayObs_int()
    return getRawDataDirForDayObs(rootDataPath, camera, todayInt)


def getRawDataDirForDayObs(rootDataPath, camera, dayObs):
    """Get the raw data dir for a given dayObs.

    Parameters
    ----------
    rootDataPath : `str`
        The root data path.
    camera : `lsst.rubintv.production.starTracker.StarTrackerCamera`
        The camera to get the raw data for.
    dayObs : `int`
        The dayObs.
    """
    camNum = camera.cameraNumber
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


class StarTrackerWatcher:
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
    camera : `lsst.rubintv.production.starTracker.StarTrackerCamera`
        The camera to watch for raw data for.
    """
    cadence = 1  # in seconds

    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 240

    def __init__(self, *, rootDataPath, bucketName, camera):
        self.rootDataPath = rootDataPath
        self.camera = camera
        self.uploader = Uploader(bucketName)
        self.log = _LOG.getChild("watcher")
        self.heartbeater = None

    def _getLatestImageDataIdAndExpId(self):
        """Get the dataId and expId for the most recent image in the repo.

        Returns
        -------
        latestFile : `str` or `None`
            The filename of the most recent file or ``None`` is nothing is
            found.
        dayObs : `int` or `None`
            The dayObs of the ``latestFile`` or ``None`` is nothing is found.
        seqNum : `int` or `None`
            The seqNum of the ``latestFile`` or ``None`` is nothing is found.
        expId : `int` or `None`
            The expId of the ``latestFile`` or ``None`` is nothing is found.
        """
        currentDir = getCurrentRawDataDir(self.rootDataPath, self.camera)
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

    These channels are somewhat hybrid channels which serve both the raw images
    and their analyses. The metadata is also written as shards from these
    channels, with a TimedMetadataServer collating and uploading them as a
    separate service.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The LocationConfig containing the relevant paths.
    cameraType : `str`
        Which camera to run the channel for. Allowed values are 'regular',
        'wide', 'fast'.
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
                 cameraType,
                 doRaise=False):
        if cameraType not in KNOWN_CAMERAS:
            raise ValueError(f"Invalid camera type {cameraType}, known types are {KNOWN_CAMERAS}")

        if cameraType == 'regular':
            self.camera = regularCam
        elif cameraType == 'wide':
            self.camera = wideCam
        elif cameraType == 'fast':
            self.camera = fastCam
        else:
            raise RuntimeError('This should be impossible, camera type already checked.')

        name = 'starTracker' + self.camera.suffix
        log = logging.getLogger(f'lsst.rubintv.production.{name}')
        self.rootDataPath = locationConfig.starTrackerDataPath
        watcher = StarTrackerWatcher(rootDataPath=self.rootDataPath,
                                     bucketName=locationConfig.bucketName,
                                     camera=self.camera)

        super().__init__(locationConfig=locationConfig,
                         log=log,
                         watcher=watcher,
                         doRaise=doRaise)

        self.channelRaw = f"startracker{self.camera.suffix}_raw"
        self.channelAnalysis = f"startracker{self.camera.suffix}_analysis"

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

        Parameters
        ----------
        exp : `lsst.afw.image.Exposure`
            The exposure.
        filename : `str`
            The filename.
        """
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

        # We use MJD as a float because neither astropy.Time nor python
        # datetime.datetime are natively JSON serializable so just use the
        # float for now. Once this data is in the butler we can simply get the
        # datetimes from the exposure records when we need them.
        mjd = exp.visitInfo.getDate().toAstropy().mjd

        datetime = exp.visitInfo.date.toPython()
        taiString = datetime.time().isoformat().split('.')[0]

        contents = {
            f"Exposure Time{self.camera.suffixWithSpace}": expTime,
            f"MJD{self.camera.suffixWithSpace}": mjd,
            f"Ra{self.camera.suffixWithSpace}": ra,
            f"Dec{self.camera.suffixWithSpace}": dec,
            f"Alt{self.camera.suffixWithSpace}": alt,
            f"Az{self.camera.suffixWithSpace}": az,
            f"UTC{self.camera.suffixWithSpace}": taiString,
        }
        md = {seqNum: contents}
        writeMetadataShard(self.shardsDir, dayObs, md)

    def _getUploadFilename(self, channel, filename):
        """Calculate the filename to use for uploading.

        Parameters
        ----------
        channel : `str`
            The channel name.
        filename : `str`
            The filename.
        """
        dayObs, seqNum = dayObsSeqNumFromFilename(filename)
        dayObsStr = dayObsIntToString(dayObs)
        filename = f"{PREFIXES[channel]}_dayObs_{dayObsStr}_seqNum_{seqNum}.png"
        return filename

    def runAnalysis(self, exp, filename):
        """Run the analysis and upload the results.

        Parameters
        ----------
        exp : `lsst.afw.image.Exposure`
            The exposure.
        filename : `str`
            The filename.
        """
        oldWcs = exp.getWcs()

        basename = os.path.basename(filename).removesuffix('.fits')
        fittedPngFilename = os.path.join(self.outputRoot, basename + '_fitted.png')
        dayObs, seqNum = dayObsSeqNumFromFilename(filename)
        self.heartbeaterAnalysis.beat()  # we're alive and at least trying to solve

        snr = self.camera.snr
        minPix = self.camera.minPix
        brightSourceFraction = self.camera.brightSourceFraction
        imCharResult = runCharactierizeImage(exp, snr, minPix)

        sourceCatalog = imCharResult.sourceCat
        md = {seqNum: {f"nSources{self.camera.suffixWithSpace}": len(sourceCatalog)}}
        writeMetadataShard(self.shardsDir, dayObs, md)
        if not sourceCatalog:
            raise RuntimeError('Failed to find any sources in image')

        filteredSources = filterSourceCatOnBrightest(sourceCatalog, brightSourceFraction)
        md = {seqNum: {f"nSources filtered{self.camera.suffixWithSpace}": len(filteredSources)}}
        writeMetadataShard(self.shardsDir, dayObs, md)

        plot(exp, sourceCatalog, filteredSources, saveAs=fittedPngFilename, doSmooth=self.camera.doSmoothPlot)
        uploadAs = self._getUploadFilename(self.channelAnalysis, filename)
        self.uploader.googleUpload(self.channelAnalysis, fittedPngFilename, uploadAs)

        scaleError = self.camera.scaleError
        # hard coding to the wide field solver seems to be much faster even for
        # the regular camera, so try this and revert only if we start seeing
        # fit failures.
        isWide = True
        result = self.solver.run(exp, filteredSources,
                                 isWideField=isWide,
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

        # pull the alt/az from the header *not* by calculating from the ra/dec,
        # mjd and location. We want the difference between where the telescope
        # thinks it was pointing and where it was actually pointing.
        oldAz = getAverageAzFromHeader(exp.getMetadata().toDict())
        oldAlt = getAverageElFromHeader(exp.getMetadata().toDict())
        oldAz = geom.Angle(oldAz, geom.degrees)
        oldAlt = geom.Angle(oldAlt, geom.degrees)

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
        contents = {k + self.camera.suffixWithSpace: v for k, v in result.items()}
        md = {seqNum: contents}
        writeMetadataShard(self.shardsDir, dayObs, md)

    def callback(self, filename):
        """Callback for the watcher, called when a new image lands.

        Parameters
        ----------
        filename : `str`
            The filename.
        """
        exp = starTrackerFileToExposure(filename, self.log)  # make the exp and set the wcs from the header
        self.heartbeaterRaw.beat()  # we loaded the file, so we're alive and running for raws

        # plot the raw file and upload it
        basename = os.path.basename(filename).removesuffix('.fits')
        rawPngFilename = os.path.join(self.outputRoot, basename + '_raw.png')  # for saving to disk
        plot(exp, saveAs=rawPngFilename, doSmooth=self.camera.doSmoothPlot)
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
        if self.camera.doAstrometry is False:
            del exp
            return

        try:
            # writes shards as it goes
            self.runAnalysis(exp, filename)
        except Exception as e:
            self.log.warning(f"Failed to run analysis on {filename}: {repr(e)}")
            traceback.print_exc()
        finally:
            del exp


class StarTrackerNightReportChannel(BaseChannel):
    """Class for running the Star Tracker Night Report channel on RubinTV.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    dayObs : `int`, optional
        TODO: XXX document this!!! XXX
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(self, locationConfig, *, dayObs=None, doRaise=False):
        name = 'starTrackerNightReport'
        log = logging.getLogger(f'lsst.rubintv.production.{name}')
        self.rootDataPath = locationConfig.starTrackerDataPath

        # we have to pick a camera to watch for data on for now, and while we
        # are always reading all three out together it doesn't matter which we
        # pick, so pick the regular one for now (but don't choose fast as that
        # is also a DIMM and so is more likely to be relocated). This won't be
        # a problem once we move to ingesting data and using the butler, so the
        # temp/hacky nature of this is fine for now.
        watcher = StarTrackerWatcher(rootDataPath=self.rootDataPath,
                                     bucketName=locationConfig.bucketName,
                                     camera=regularCam)  # tie the night report to the regular cam for now

        super().__init__(locationConfig=locationConfig,
                         log=log,
                         watcher=watcher,
                         doRaise=doRaise
                         )

        self.dayObs = dayObs if dayObs else getCurrentDayObs_int()

    def getMetadataTableContents(self):
        """Get the measured data for the current night.

        Returns
        -------
        mdTable : `pandas.DataFrame`
            The contents of the metdata table from the front end.
        """
        sidecarFilename = os.path.join(self.locationConfig.starTrackerMetadataPath,
                                       f'dayObs_{self.dayObs}.json')

        try:
            mdTable = pd.read_json(sidecarFilename).T
            mdTable = mdTable.sort_index()
        except Exception as e:
            self.log.warning(f"Failed to load metadata table from {sidecarFilename}: {e}")
            return None

        if mdTable.empty:
            return None

        return mdTable

    def createPlotsAndUpload(self):
        """Create and upload all plots defined in nightReportPlots.

        All plots defined in __all__ in nightReportPlots are discovered,
        created and uploaded. If any fail, the exception is logged and the next
        plot is created and uploaded.
        """
        md = self.getMetadataTableContents()
        self.log.info(f'Creating plots for dayObs {self.dayObs} with: '
                      f'{0 if md is None else len(md)} items in the metadata table')

        for plotClassName in starTrackerNightReportPlots.__all__:
            try:
                self.log.info(f'Creating plot {plotClassName}')
                PlotClass = getattr(starTrackerNightReportPlots, plotClassName)
                plot = PlotClass(dayObs=self.dayObs,
                                 locationConfig=self.locationConfig,
                                 uploader=self.uploader)
                plot.createAndUpload(md)
            except Exception:
                self.log.exception(f"Failed to create plot {plotClassName}")
                continue

    def finalizeDay(self):
        """XXX Docs
        """
        self.log.info(f'Creating final plots for {self.dayObs}')
        self.createPlotsAndUpload()
        self.log.info(f'Starting new star tracker night report for dayObs {self.dayObs}')
        self.dayObs = getCurrentDayObs_int()
        return

    def callback(self, filename, doCheckDay=True):
        """Method called on each new expRecord as it is found in the repo.

        Parameters
        ----------
        filename : `str`
            The filename of the most recently taken image on the nominal camera
            we're using to watch for data for.
        doCheckDay : `bool`, optional
            Whether to check if the day has rolled over. This should be left as
            True for normal operation, but set to False when manually running
            on past exposures to save triggering on the fact it is no longer
            that day, e.g. during testing or doing catch-up/backfilling.
        """
        try:
            if doCheckDay and hasDayRolledOver(self.dayObs):
                self.log.info(f'Day has rolled over, finalizing report for dayObs {self.dayObs}')
                self.finalizeDay()

            else:
                # make plots here, uploading one by one
                # make all the automagic plots from nightReportPlots.py
                self.createPlotsAndUpload()
                self.log.info(f'Finished updating plots and table with most recent file {filename}')

        except Exception as e:
            msg = f"Skipped updating the night report for {filename}:"
            raiseIf(self.doRaise, e, self.log, msg=msg)
