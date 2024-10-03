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
import os
import time
import traceback
from glob import glob
from time import sleep

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

import lsst.geom as geom
from lsst.summit.utils.astrometry import CommandLineSolver
from lsst.summit.utils.astrometry.plotting import plot
from lsst.summit.utils.astrometry.utils import (
    filterSourceCatOnBrightest,
    getAverageAzFromHeader,
    getAverageElFromHeader,
    runCharactierizeImage,
)
from lsst.summit.utils.starTracker import (
    KNOWN_CAMERAS,
    dayObsSeqNumFromFilename,
    fastCam,
    getRawDataDirForDayObs,
    isStreamingModeFile,
    narrowCam,
    wideCam,
)
from lsst.summit.utils.utils import (
    dayObsIntToString,
    getAltAzFromSkyPosition,
    getCurrentDayObs_int,
    starTrackerFileToExposure,
)

from .baseChannels import BaseChannel
from .plotting import starTrackerNightReportPlots
from .uploaders import MultiUploader
from .utils import hasDayRolledOver, raiseIf, writeMetadataShard

__all__ = (
    "getCurrentRawDataDir",
    "getDataDir",
    "getFilename",
    "StarTrackerWatcher",
    "StarTrackerChannel",
    "StarTrackerNightReportChannel",
    "StarTrackerCatchup",
)

_LOG = logging.getLogger(__name__)


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


def getDataDir(rootPath, camera, dayObs):
    """Get the path to the data for a given camera and dayObs.

    Parameters
    -------
    rootPath : `str`
        The root data path.
    camera : `lsst.rubintv.production.starTracker.StarTrackerCamera`
        The camera.
    dayObs : `int`
        The dayObs.

    Returns
    -------
    path : `str`
        The path to the data.
    """
    rootPath = os.path.join(rootPath, "GenericCamera")
    datePath = dayObsIntToString(dayObs).replace("-", "/")
    path = os.path.join(rootPath, f"{camera.cameraNumber}/{datePath}/")
    return path


def getFilename(rootPath, camera, dayObs, seqNum):
    """Get the filename for a given camera, dayObs and seqNum.

    Parameters
    ----------
    rootPath : `str`
        The root data path.
    camera : `lsst.rubintv.production.starTracker.StarTrackerCamera`
        The camera.
    dayObs : `int`
        The dayObs.
    seqNum : `int`
        The seqNum.

    Returns
    -------
    filename : `str`
        The filename.
    """
    path = getDataDir(rootPath, camera, dayObs)
    filename = f"GC{camera.cameraNumber}_O_{dayObs}_{seqNum:06}.fits"
    return os.path.join(path, filename)


class StarTrackerWatcher:
    """Class for continuously watching for new files landing in the directory.

    Parameters
    ----------
    rootDataPath : `str`
        The root directory to watch for files landing in. Should not include
        the GenericCamera/101/ or GenericCamera/102/ part, just the base
        directory that these are being written to, as visible from k8s.
    camera : `lsst.rubintv.production.starTracker.StarTrackerCamera`
        The camera to watch for raw data for.
    """

    cadence = 1  # in seconds

    def __init__(self, *, rootDataPath, camera):
        self.rootDataPath = rootDataPath
        self.camera = camera
        self.s3Uploader = MultiUploader()
        self.log = _LOG.getChild("watcher")

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
        files = glob(os.path.join(currentDir, "*.fits"))
        files = sorted(files, reverse=True)  # everything is zero-padded so sorts nicely
        if not files:
            return None, None, None, None

        latestFile = files[0]
        if isStreamingModeFile(latestFile):  # sadly these go in the same directory
            self.log.info(f"Skipping {latestFile} as it is a streaming mode file")
            return None, None, None, None

        # filenames are like GC101_O_20221114_000005.fits
        _, _, dayObs, seqNumAndSuffix = latestFile.split("_")
        dayObs = int(dayObs)
        seqNum = int(seqNumAndSuffix.removesuffix(".fits"))
        expId = int(str(dayObs) + seqNumAndSuffix.removesuffix(".fits"))
        return latestFile, dayObs, seqNum, expId

    def run(self, callback):
        """Wait for the image to land, then run callback(filename).

        Parameters
        ----------
        callback : `callable`
            The method to call, with the latest filename as the argument.
        """
        lastFound = -1
        filename = "no filename"
        while True:
            try:
                filename, _, _, expId = self._getLatestImageDataIdAndExpId()
                self.log.debug(f"{filename}")

                if (filename is None) or (lastFound == expId):
                    self.log.debug("Found nothing, sleeping")
                    sleep(self.cadence)
                    continue
                else:
                    lastFound = expId
                    callback(filename)

            except Exception as e:
                self.log.warning(f"Skipped {filename} due to {e}")
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

    def __init__(self, locationConfig, *, cameraType, doRaise=False):
        if cameraType not in KNOWN_CAMERAS:
            raise ValueError(f"Invalid camera type {cameraType}, known types are {KNOWN_CAMERAS}")

        if cameraType == "narrow":
            self.camera = narrowCam
        elif cameraType == "wide":
            self.camera = wideCam
        elif cameraType == "fast":
            self.camera = fastCam
        else:
            raise RuntimeError("This should be impossible, camera type already checked.")

        name = "starTracker" + self.camera.suffix
        log = logging.getLogger(f"lsst.rubintv.production.{name}")
        self.rootDataPath = locationConfig.starTrackerDataPath
        watcher = StarTrackerWatcher(rootDataPath=self.rootDataPath, camera=self.camera)

        super().__init__(locationConfig=locationConfig, log=log, watcher=watcher, doRaise=doRaise)

        self.channelRaw = f"startracker{self.camera.suffix}_raw"  # TODO: DM-43413 remove?
        self.channelAnalysis = f"startracker{self.camera.suffix}_analysis"  # TODO: DM-43413 remove?

        self.outputRoot = self.locationConfig.starTrackerOutputPath
        self.metadataRoot = self.locationConfig.starTrackerMetadataPath
        self.astrometryNetRefCatRoot = self.locationConfig.astrometryNetRefCatPath
        self.doRaise = doRaise
        self.shardsDir = os.path.join(self.metadataRoot, "shards")
        for path in (self.outputRoot, self.shardsDir, self.metadataRoot):
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                raise RuntimeError(f"Failed to find/create {path}") from e

        self.solver = CommandLineSolver(
            indexFilePath=self.locationConfig.astrometryNetRefCatPath, checkInParallel=True, timeout=30
        )
        self.fig = plt.figure(figsize=(16, 16))

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
        if seqNum is None:
            return  # skip streaming mode files
        expMd = exp.getMetadata().toDict()
        expTime = exp.visitInfo.exposureTime
        contents = {}
        ra = exp.getWcs().getSkyOrigin().getRa().asDegrees()
        dec = exp.getWcs().getSkyOrigin().getDec().asDegrees()

        az = None
        try:
            az = getAverageAzFromHeader(expMd)
        except RuntimeError:
            self.log.warning(f"Failed to get az from header for {filename}")

        alt = None
        try:
            alt = getAverageElFromHeader(expMd)
        except RuntimeError:
            self.log.warning(f"Failed to get alt from header for {filename}")

        # We use MJD as a float because neither astropy.Time nor python
        # datetime.datetime are natively JSON serializable so just use the
        # float for now. Once this data is in the butler we can simply get the
        # datetimes from the exposure records when we need them.
        mjd = exp.visitInfo.getDate().toAstropy().mjd

        datetime = exp.visitInfo.date.toPython()
        taiString = datetime.time().isoformat().split(".")[0]

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

        basename = os.path.basename(filename).removesuffix(".fits")
        fittedPngFilename = os.path.join(self.outputRoot, basename + "_fitted.png")
        dayObs, seqNum = dayObsSeqNumFromFilename(filename)

        snr = self.camera.snr
        minPix = self.camera.minPix
        brightSourceFraction = self.camera.brightSourceFraction
        imCharResult = runCharactierizeImage(exp, snr, minPix)

        sourceCatalog = imCharResult.sourceCat
        md = {seqNum: {f"nSources{self.camera.suffixWithSpace}": len(sourceCatalog)}}
        writeMetadataShard(self.shardsDir, dayObs, md)
        if not sourceCatalog:
            raise RuntimeError("Failed to find any sources in image")

        filteredSources = filterSourceCatOnBrightest(sourceCatalog, brightSourceFraction)
        md = {seqNum: {f"nSources filtered{self.camera.suffixWithSpace}": len(filteredSources)}}
        writeMetadataShard(self.shardsDir, dayObs, md)

        plot(
            exp,
            sourceCatalog,
            filteredSources,
            saveAs=fittedPngFilename,
            doSmooth=self.camera.doSmoothPlot,
            fig=self.fig,
        )

        dayObs, seqNum = dayObsSeqNumFromFilename(filename)
        self.s3Uploader.uploadPerSeqNumPlot(
            instrument="startracker" + self.camera.suffix,
            plotName="analysis",
            dayObs=dayObs,
            seqNum=seqNum,
            filename=fittedPngFilename,
        )

        scaleError = self.camera.scaleError
        # hard coding to the wide field solver seems to be much faster even for
        # the regular camera, so try this and revert only if we start seeing
        # fit failures.
        isWide = True
        result = self.solver.run(
            exp, filteredSources, isWideField=isWide, percentageScaleError=scaleError, silent=True
        )

        if not result:
            self.log.warning(f"Failed to find solution for {basename}")
            return

        newWcs = result.wcs

        calculatedRa, calculatedDec = newWcs.getSkyOrigin()
        nominalRa, nominalDec = oldWcs.getSkyOrigin()

        deltaRa = geom.Angle.separation(calculatedRa, nominalRa)
        deltaDec = geom.Angle.separation(calculatedDec, nominalDec)

        # pull the alt/az from the header *not* by calculating from the ra/dec,
        # mjd and location. We want the difference between where the telescope
        # thinks it was pointing and where it was actually pointing.
        oldAz = getAverageAzFromHeader(exp.getMetadata().toDict())
        oldAlt = getAverageElFromHeader(exp.getMetadata().toDict())
        oldAz = geom.Angle(oldAz, geom.degrees)
        oldAlt = geom.Angle(oldAlt, geom.degrees)

        atmosphericOverrides = {}
        pressure = exp.visitInfo.weather.getAirPressure()
        if not np.isfinite(pressure):
            self.log.warning("Pressure not found in header, falling back nominal value=0.770 bar")
            atmosphericOverrides["pressureOverride"] = 0.770

        temp = exp.visitInfo.weather.getAirTemperature()
        if not np.isfinite(temp):
            self.log.warning("Temperature not found in header, falling back nominal value=10 C")
            atmosphericOverrides["temperatureOverride"] = 10

        humidity = exp.visitInfo.weather.getHumidity()
        if not np.isfinite(humidity):
            self.log.warning("Humidity not found in header, falling back nominal value=0.1")
            atmosphericOverrides["relativeHumidityOverride"] = 0.1

        newAlt, newAz = getAltAzFromSkyPosition(
            newWcs.getSkyOrigin(), exp.visitInfo, doCorrectRefraction=True, **atmosphericOverrides
        )

        deltaAlt = geom.Angle.separation(newAlt, oldAlt)
        deltaAz = geom.Angle.separation(newAz, oldAz)

        deltaRot = newWcs.getRelativeRotationToWcs(oldWcs).asArcseconds()

        result = {
            "Calculated Ra": calculatedRa.asDegrees(),
            "Calculated Dec": calculatedDec.asDegrees(),
            "Calculated Alt": newAlt.asDegrees(),
            "Calculated Az": newAz.asDegrees(),
            "Delta Ra Arcsec": deltaRa.asArcseconds(),
            "Delta Dec Arcsec": deltaDec.asArcseconds(),
            "Delta Alt Arcsec": deltaAlt.asArcseconds(),
            "Delta Az Arcsec": deltaAz.asArcseconds(),
            "Delta Rot Arcsec": deltaRot,
            "RMS scatter arcsec": result.rmsErrorArsec,
            "RMS scatter pixels": result.rmsErrorPixels,
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
        if isStreamingModeFile(filename):  # sadly these go in the same directory
            self.log.info(f"Skipping {filename} as it is a streaming mode file")
            return

        exp = starTrackerFileToExposure(filename, self.log)  # make the exp and set the wcs from the header

        # plot the raw file and upload it
        basename = os.path.basename(filename).removesuffix(".fits")
        rawPngFilename = os.path.join(self.outputRoot, basename + "_raw.png")  # for saving to disk
        plot(exp, saveAs=rawPngFilename, doSmooth=self.camera.doSmoothPlot, fig=self.fig)

        dayObs, seqNum = dayObsSeqNumFromFilename(filename)
        self.s3Uploader.uploadPerSeqNumPlot(
            instrument="startracker" + self.camera.suffix,
            plotName="raw",
            dayObs=dayObs,
            seqNum=seqNum,
            filename=rawPngFilename,
        )

        if not exp.wcs:
            self.log.info(f"Skipping {filename} as it has no WCS")
            del exp
            return
        if not exp.visitInfo.date.isValid():
            self.log.warning(
                f"exp.visitInfo.date is not valid. {filename} will still be fitted"
                " but the alt/az values reported will be garbage"
            )

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
        The dayObs. If not provided, will be calculated from the current time.
        This should be supplied manually if running catchup or similar, but
        when running live it will be set automatically so that the current day
        is processed.
    embargo : `bool`, optional
        Use the embargo repo?
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(self, locationConfig, *, dayObs=None, doRaise=False):
        name = "starTrackerNightReport"
        log = logging.getLogger(f"lsst.rubintv.production.{name}")
        self.rootDataPath = locationConfig.starTrackerDataPath

        # we have to pick a camera to watch for data on for now, and while we
        # are always reading all three out together it doesn't matter which we
        # pick, so pick the regular one for now (but don't choose fast as that
        # is also a DIMM and so is more likely to be relocated). This won't be
        # a problem once we move to ingesting data and using the butler, so the
        # temp/hacky nature of this is fine for now.
        watcher = StarTrackerWatcher(rootDataPath=self.rootDataPath, camera=narrowCam)

        super().__init__(locationConfig=locationConfig, log=log, watcher=watcher, doRaise=doRaise)

        self.dayObs = dayObs if dayObs else getCurrentDayObs_int()

    def getMetadataTableContents(self):
        """Get the measured data for the current night.

        Returns
        -------
        mdTable : `pandas.DataFrame` or `None`
            The contents of the metadata table from the front end, or `None` if
            the file for ``self.dayObs`` does not exist (yet).
        """
        sidecarFilename = os.path.join(
            self.locationConfig.starTrackerMetadataPath, f"dayObs_{self.dayObs}.json"
        )
        if not os.path.isfile(sidecarFilename):
            self.log.info(
                f"No metadata table found for this night at {sidecarFilename}, "
                "so nothing to catch up on (yet)"
            )
            return None

        try:
            mdTable = pd.read_json(sidecarFilename).T
            mdTable = mdTable.sort_index()
        except Exception as e:
            self.log.warning(f"Failed to load metadata table from {sidecarFilename}: {e}")
            return None

        if mdTable.empty:
            self.log.warning(f"Loaded metadata from {sidecarFilename} but it was found to be empty.")
            return None

        return mdTable

    def createPlotsAndUpload(self):
        """Create and upload all plots defined in nightReportPlots.

        All plots defined in PLOT_FACTORIES in nightReportPlots are discovered,
        created and uploaded. If any fail, the exception is logged and the next
        plot is created and uploaded.
        """
        md = self.getMetadataTableContents()
        if md is None:  # getMetadataTableContents logs about the lack of a file so no need to do it here
            return

        self.log.info(
            f"Creating plots for dayObs {self.dayObs} with: "
            f"{0 if md is None else len(md)} items in the metadata table"
        )

        for plotName in starTrackerNightReportPlots.PLOT_FACTORIES:
            try:
                self.log.info(f"Creating plot {plotName}")
                plotFactory = getattr(starTrackerNightReportPlots, plotName)
                plot = plotFactory(
                    dayObs=self.dayObs,
                    locationConfig=self.locationConfig,
                    s3Uploader=self.s3Uploader,
                )
                plot.createAndUpload(md)
            except Exception:
                self.log.exception(f"Failed to create plot {plotName}")
                continue

    def finalizeDay(self):
        """Perform the end of day actions and roll the day over.

        Creates a final version of the plots at the end of the day and rolls
        ``self.dayObs`` over.
        """
        self.log.info(f"Creating final plots for {self.dayObs}")
        self.createPlotsAndUpload()
        self.log.info(f"Starting new star tracker night report for dayObs {self.dayObs}")
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
                self.log.info(f"Day has rolled over, finalizing report for dayObs {self.dayObs}")
                self.finalizeDay()

            else:
                # make plots here, uploading one by one
                # make all the automagic plots from nightReportPlots.py
                self.createPlotsAndUpload()
                self.log.info(f"Finished updating plots and table with most recent file {filename}")

        except Exception as e:
            msg = f"Skipped updating the night report for {filename}:"
            raiseIf(self.doRaise, e, self.log, msg=msg)


class StarTrackerCatchup:
    """Class for catching up on skipped images in the StarTrackers.

    For now, one catchup service for two cameras, but in future this could
    easily be split out if that becomes necessary.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The LocationConfig containing the relevant paths.
    dayObs : `int`, optional
        The dayObs to catchup. If not provided, will be calculated from the
        current time, and this is the default behaviour when running from k8s.
    doRaise : `bool`, optional
        Raise on error? Default False, useful for debugging.
    """

    loopSleep = 30
    catchupPeriod = 60
    endOfDayDelay = 200

    def __init__(self, locationConfig, doRaise=False):
        self.locationConfig = locationConfig
        self.doRaise = doRaise

        self.cameras = [narrowCam, wideCam, fastCam]
        self.log = _LOG.getChild("catchup")

    def getFullyProcessedSeqNums(self, camera, dayObs):
        """Get the seqNums for images which were fully processed.

        Parameters
        ----------
        camera : `lsst.rubintv.production.starTracker.StarTrackerCamera`
            The camera to get the missing seqNums for.
        dayObs : `int`
            The dayObs to get the processed seqNums for.

        Returns
        -------
        processed : `list` [`int`]
            The processed seqNums.
        """
        sidecarFilename = os.path.join(self.locationConfig.starTrackerMetadataPath, f"dayObs_{dayObs}.json")
        if not os.path.isfile(sidecarFilename):
            self.log.info(
                f"No metadata table found for this night at {sidecarFilename}, "
                "so nothing to catch up on (yet)"
            )
            return []

        mdTable = pd.read_json(sidecarFilename).T
        mdTable = mdTable.sort_index()

        seqNums = list(mdTable.index)
        successfulFitColumn = "Calculated Ra" + camera.suffixWithSpace
        if successfulFitColumn not in mdTable.columns:
            # if the table exists but nothing has fitted yet for a given
            # camera then the column won't exist and the process thrashes a bit
            return []

        processed = [s for s in seqNums if mdTable[successfulFitColumn][s] is not np.nan]
        return processed

    def catchupCamera(self, camera, dayObs):
        """Catch up a single camera.

        TODO: DM-38313 Add a way of recording fails and skipping them in future
        so that we don't keep trying to process them over and over.

        Parameters
        ----------
        camera : `lsst.rubintv.production.starTracker.StarTrackerCamera`
            The camera to catch up.
        """
        dataPath = getDataDir(self.locationConfig.starTrackerDataPath, camera, dayObs)
        allFiles = sorted(glob(os.path.join(dataPath, "*.fits")))
        # filter before getting the seqNums as streaming mode must be excluded
        nonStreamingFiles = [f for f in allFiles if not isStreamingModeFile(f)]
        seqNums = [dayObsSeqNumFromFilename(f)[1] for f in nonStreamingFiles]

        processed = self.getFullyProcessedSeqNums(camera, dayObs)
        toProcess = [s for s in seqNums if s not in processed]
        self.log.info(
            f"Found {len(processed)} processed files out of {len(nonStreamingFiles)} total,"
            f" leaving {len(toProcess)} left to process for {camera.cameraType}"
        )
        if not toProcess:
            return

        filenames = [
            getFilename(self.locationConfig.starTrackerDataPath, camera, self.dayObs, seqNum)
            for seqNum in toProcess
        ]
        filenames = [f for f in filenames if os.path.isfile(f)]
        self.log.info(f"of which {len(filenames)} had corresponding files")

        starTrackerChannel = StarTrackerChannel(
            locationConfig=self.locationConfig, cameraType=camera.cameraType
        )
        for file in filenames:
            self.log.info(f"Catching up {file}")
            try:
                starTrackerChannel.callback(file)
            except Exception as e:
                raiseIf(self.doRaise, e, self.log)

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
        return

    def runCatchup(self):
        """Run the catchup for all cameras."""
        for camera in self.cameras:
            self.log.info(f"Starting catchup for the {camera.cameraType} camera")
            self.catchupCamera(camera, self.dayObs)

    def runEndOfDay(self):
        """Routine to run when the summit dayObs rolls over.

        Sets the new dayObs.
        """
        try:
            self.runCatchup()
        except Exception as e:
            raiseIf(self.doRaise, e, self.log)
        finally:
            self.dayObs = getCurrentDayObs_int()

    def run(self):
        """Runs forever, running the catchup services during the night, and
        rolls the dayObs over at the end of the night.

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
                    lastRun = time.time()
                    if hasDayRolledOver(self.dayObs):
                        sleep(self.endOfDayDelay)  # give time for anything running elsewhere to finish
                        self.runEndOfDay()  # sets new dayObs in a finally block
                else:
                    remaining = self.catchupPeriod - timeSince
                    self.log.info(f"Waiting for catchup period to elapse, {remaining:.2f}s to go...")
                    sleep(self.loopSleep)

            except Exception as e:
                raiseIf(self.doRaise, e, self.log)
