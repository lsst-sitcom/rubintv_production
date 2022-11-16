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
import time
from glob import glob
from time import sleep
import datetime
import traceback

import lsst.geom as geom
import lsst.afw.image as afwImage
from astroquery.astrometry_net import AstrometryNet
from lsst.afw.geom import SkyWcs
from lsst.daf.base import PropertySet
from lsst.summit.utils.utils import getCurrentDayObs_int
from lsst.summit.utils.utils import dayObsIntToString
from lsst.summit.utils.blindSolving import plot, runImchar, _filterSourceCatalog, getApiKey

from .channels import PREFIXES
from .utils import writeMetadataShard, isFileWorldWritable
from .rubinTv import Uploader, Heartbeater

_LOG = logging.getLogger(__name__)


def getCurrentRawDataDir(rootDataPath, wide):
    """Get the raw data dir corresponding to the current dayObs.

    Returns
    -------
    path : `str`
        The raw data dir for today.
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
        Whether this is a wide field camera or not
    dayObs : `int`
        The dayObs
    """
    camNum = 101 if wide else 102  # TODO move this config to the top somehow?
    dayObsDateTime = datetime.datetime.strptime(str(dayObs), '%Y%m%d')
    dirSuffix = f'GenericCamera/{camNum}/{dayObsDateTime.year}/{dayObsDateTime.month}/{dayObsDateTime.day}/'
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


# don't be tempted to get cute and try to combine these 4 functions. It would
# be a easy to do but it's not unlikley they will diverge in the future.
def getAverageRaFromHeader(header):
    raStart = header.get('RASTART')
    raEnd = header.get('RAEND')
    if not raStart or not raEnd:
        raise RuntimeError(f'Failed to get RA from header due to missing RASTART/END {raStart} {raEnd}')
    raStart = float(raStart)
    raEnd = float(raEnd)
    return (raStart + raEnd) / 2


def getAverageDecFromHeader(header):
    decStart = header.get('DECSTART')
    decEnd = header.get('DECEND')
    if not decStart or not decEnd:
        raise RuntimeError(f'Failed to get DEC from header due to missing DECSTART/END {decStart} {decEnd}')
    decStart = float(decStart)
    decEnd = float(decEnd)
    return (decStart + decEnd) / 2


def getAverageAzFromHeader(header):
    azStart = header.get('AZSTART')
    azEnd = header.get('AZEND')
    if not azStart or not azEnd:
        raise RuntimeError(f'Failed to get az from header due to missing AZSTART/END {azStart} {azEnd}')
    azStart = float(azStart)
    azEnd = float(azEnd)
    return (azStart + azEnd) / 2


def getAverageElFromHeader(header):
    elStart = header.get('ELSTART')
    elEnd = header.get('ELEND')
    if not elStart or not elEnd:
        raise RuntimeError(f'Failed to get el from header due to missing ELSTART/END {elStart} {elEnd}')
    elStart = float(elStart)
    elEnd = float(elEnd)
    return (elStart + elEnd) / 2


def genericCameraHeaderToWcs(exp):
    header = exp.getMetadata().toDict()
    width, height = exp.image.array.shape
    header['CRPIX1'] = width/2
    header['CRPIX2'] = height/2

    header['CTYPE1'] = 'RA---TAN-SIP'
    header['CTYPE2'] = 'DEC--TAN-SIP'

    header['CRVAL1'] = getAverageRaFromHeader(header)
    header['CRVAL2'] = getAverageDecFromHeader(header)

    plateScale = header.get('SECPIX')
    if not plateScale:
        raise RuntimeError('Failed to find platescale in header')
    plateScale = float(plateScale)

    header['CD1_1'] = plateScale / 3600
    header['CD1_2'] = 0
    header['CD2_1'] = 0
    header['CD2_2'] = plateScale / 3600

    wcsPropSet = PropertySet.from_mapping(header)
    wcs = SkyWcs(wcsPropSet)
    return wcs


class StarTrackerWatcher():
    """Class for continuously watching for new files landing in the directory.
    Uploads a heartbeat to the bucket every ``HEARTBEAT_PERIOD`` seconds.

    Parameters
    ----------
    rootDataPath : `str`
        The root directory to watch for files landing in. Should not include
        the GenericCamera/101/ or GenericCamera/102/ part, just the base
        directory that these are being written to, as visible from k8s.
    wide : `bool`
        Whether to watch the wide or narrow camera.
    """
    cadence = 1  # in seconds

    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 240

    def __init__(self, *, rootDataPath, wide):
        self.rootDataPath = rootDataPath
        self.wide = wide
        self.uploader = Uploader()
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

                if lastFound == expId:
                    sleep(self.cadence)
                    self.log.debug('Found nothing, sleeping')
                    continue
                else:
                    lastFound = expId
                    self.log.debug(f'Calling back with {filename}')
                    callback(filename)

            except Exception as e:
                self.log.warning(f'Skipped {filename} due to {e}')
                traceback.print_exc()


class StarTrackerChannel():
    """Class for serving star tracker images to RubinTV.

    These channels are somewhat hybrid channels which serve both the raw
    images and their analyses. The metadata is also written as shards from
    these channels, with the metadata server itseld just functioning to collate
    the shards and upload them.
    """
    # upload heartbeat every n seconds
    HEARTBEAT_UPLOAD_PERIOD = 30
    # consider service 'dead' if this time exceeded between heartbeats
    HEARTBEAT_FLATLINE_PERIOD = 240

    def __init__(self, *,
                 rootDataPath,
                 outputRoot,
                 metadataRoot,
                 wide,
                 doRaise=False):
        self.uploader = Uploader()
        self.log = _LOG.getChild(f"starTracker{'_wide' if wide else ''}")
        self.channelRaw = f"startracker{'_wide' if wide else ''}_raw"
        self.channelAnalysis = f"startracker{'_wide' if wide else ''}_analysis"
        self.wide = wide
        self.rootDataPath = rootDataPath
        self.watcher = StarTrackerWatcher(rootDataPath=rootDataPath,
                                          wide=wide)
        self.outputRoot = outputRoot
        self.metadataRoot = metadataRoot
        self.doRaise = doRaise
        self.shardsDir = os.path.join(self.metadataRoot, 'shards')
        for path in (self.outputRoot, self.shardsDir, self.metadataRoot):
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                raise RuntimeError(f"Failed to find/create {path}") from e

        self.heartbeaterAnalysis = Heartbeater(self.channelAnalysis,
                                               self.HEARTBEAT_UPLOAD_PERIOD,
                                               self.HEARTBEAT_FLATLINE_PERIOD)
        self.heartbeaterRaw = Heartbeater(self.channelRaw,
                                          self.HEARTBEAT_UPLOAD_PERIOD,
                                          self.HEARTBEAT_FLATLINE_PERIOD)
        self.watcher.heartbeater = self.heartbeaterRaw  # so that it can be called in the watch loop

    def filenameToExposure(self, filename):
        """Read the exposure from the file and set the wcs from the header.
        """
        exp = afwImage.ExposureF(filename)
        wcs = genericCameraHeaderToWcs(exp)
        exp.setWcs(wcs)
        return exp

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

        el = None
        try:
            el = getAverageElFromHeader(expMd)
        except RuntimeError:
            self.log.warning(f'Failed to get el from header for {filename}')

        contents = {
            f"Exposure Time{_ifWide}": expTime,
            f"Ra{_ifWide}": ra,
            f"Dec{_ifWide}": dec,
            f"Az{_ifWide}": az,
            f"El{_ifWide}": el,
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
        adn = AstrometryNet()
        adn.api_key = getApiKey()

        t0 = time.time()
        basename = os.path.basename(filename).removesuffix('.fits')
        fittedPngFilename = os.path.join(self.outputRoot, basename + '_fitted.png')
        dayObs, seqNum = dayObsSeqNumFromFilename(filename)
        self.heartbeaterAnalysis.beat()  # we're alive and at least trying to solve

        snr = 5
        minPix = 25
        radiusInDegrees = 5
        brightSourceFraction = 0.1
        imCharResult = runImchar(exp, snr, minPix)

        sourceCatalog = imCharResult.sourceCat
        md = {seqNum: {f"nSources{_ifWide}": len(sourceCatalog)}}
        writeMetadataShard(self.shardsDir, dayObs, md)
        if not sourceCatalog:
            raise RuntimeError('Failed to find any sources in image')

        filteredSources = _filterSourceCatalog(sourceCatalog, brightSourceFraction)
        md = {seqNum: {f"nSources filtered{_ifWide}": len(filteredSources)}}
        writeMetadataShard(self.shardsDir, dayObs, md)

        plot(exp, sourceCatalog, filteredSources, saveAs=fittedPngFilename)
        uploadAs = self._getUploadFilename(self.channelAnalysis, filename)
        self.uploader.googleUpload(self.channelAnalysis, fittedPngFilename, uploadAs)

        image_height, image_width = exp.image.array.shape
        nominalRa, nominalDec = exp.getWcs().getSkyOrigin()
        scale_units = 'arcsecperpix'
        scale_type = 'ev'  # ev means submit estimate and % error
        scale_err = 3.0  # error as percentage
        center_ra = nominalRa.asDegrees()
        center_dec = nominalDec.asDegrees()
        scaleEstimate = exp.getWcs().getPixelScale().asArcseconds()
        wcs_header = adn.solve_from_source_list(filteredSources['base_SdssCentroid_x'],
                                                filteredSources['base_SdssCentroid_y'],
                                                image_width, image_height,
                                                scale_units=scale_units,
                                                scale_type=scale_type,
                                                scale_est=scaleEstimate,
                                                scale_err=scale_err,
                                                center_ra=center_ra,
                                                center_dec=center_dec,
                                                radius=radiusInDegrees,
                                                solve_timeout=240)
        t1 = time.time()

        if 'CRVAL1' not in wcs_header:  # this is a failed solve
            self.log.warning(f"Failed to find solution for {basename}")
            return
        else:
            self.log.info(f"Finished source finding and solving for {basename} in {t1-t0:.2f}s")

        calculatedRa = geom.Angle(wcs_header['CRVAL1'], geom.degrees)
        calculatedDec = geom.Angle(wcs_header['CRVAL2'], geom.degrees)

        deltaRa = geom.Angle(wcs_header['CRVAL1'] - nominalRa.asDegrees(), geom.degrees)
        deltaDec = geom.Angle(wcs_header['CRVAL2'] - nominalDec.asDegrees(), geom.degrees)

        result = {
            'Calculated Ra': calculatedRa.asDegrees(),
            'Calculated Dec': calculatedDec.asDegrees(),
            'Delta Ra Arcsec': deltaRa.asArcseconds(),
            'Delta Dec Arcsec': deltaDec.asArcseconds(),
        }
        contents = {k + _ifWide: v for k, v in result.items()}
        md = {seqNum: contents}
        writeMetadataShard(self.shardsDir, dayObs, md)

    def callback(self, filename):
        """Callback for the watcher, called when a new image lands.
        """
        exp = self.filenameToExposure(filename)  # make the exp and set the wcs from the header
        self.heartbeaterRaw.beat()  # we loaded the file, so we're alive and running for raws

        # plot the raw file and upload it
        basename = os.path.basename(filename).removesuffix('.fits')
        rawPngFilename = os.path.join(self.outputRoot, basename + '_raw.png')  # for saving to disk
        plot(exp, saveAs=rawPngFilename)
        uploadFilename = self._getUploadFilename(self.channelRaw, filename)  # get the filename for the bucket
        self.uploader.googleUpload(self.channelRaw, rawPngFilename, uploadFilename)

        # metadata a shard with just the pointing info etc
        self.writeDefaultPointingShardForFilename(exp, filename)
        try:
            # writes shards as it goes
            self.runAnalysis(exp, filename)
        except Exception as e:
            self.log.warning(f"Failed to run analysis on {filename}: {repr(e)}")
            traceback.print_exc()

    def run(self):
        """Run continuously, calling the callback method on the latest filename
        """
        self.watcher.run(self.callback)


class StarTrackerMetadataServer():
    """Class for serving star tracker metadata to RubinTV.
    """

    def __init__(self, *,
                 metadataRoot,
                 doRaise=False):
        self.uploader = Uploader()
        self.heartbeater = Heartbeater('startracker_metadata')
        self.channel = 'startracker_metadata'
        self.log = _LOG.getChild(f"{self.channel}")
        self.metadataRoot = metadataRoot
        self.doRaise = doRaise
        self.shardsDir = os.path.join(self.metadataRoot, 'shards')
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
            if self.doRaise:
                raise RuntimeError("Error when collection metadata") from e
            self.log.warning(f"Error when collection metadata because {repr(e)}")
            return None

    def run(self):
        """Run continuously, looking for metadata and uploading.
        """
        while True:
            self.callback()
            self.heartbeater.beat()
            sleep(1.5)
