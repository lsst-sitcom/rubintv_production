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
import sys
import time
from time import sleep
import logging
import subprocess

from lsst.summit.utils.utils import (dayObsIntToString,
                                     getCurrentDayObs_int,
                                     getCurrentDayObs_datetime,
                                     )
from lsst.rubintv.production import Uploader
from lsst.rubintv.production.rubinTv import _dataIdToFilename

__all__ = ['DayAnimator', 'AllSkyMovieChannel', 'dayObsFromDirName']

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


def _createWritableDir(path):
    """Create a writeable directory with the specified path.

    Parameters
    ----------
    path : `str`
        The path to create.

    Raises
    ------
    RuntimeError
        Raised if the path either can't be created, or exists and is not
        writeable.
    """
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f'Error creating/accessing output path {path}') from e
    if not os.access(path, os.W_OK):
        raise RuntimeError(f"Output path {path} is not writable.")


def dayObsFromDirName(fullDirName, logger):
    """Get the dayObs from the directory name.

    Parses the directory path, returning the dayObs as an int and a string if
    possible, otherwise (None, None) should it fail, to allow directories to be
    easily skipped.

    Paths look like "/lsstdata/offline/allsky/storage/ut220503".

    Not used in this code, but useful in notebooks/when regenerating historical
    data.

    Parameters
    ----------
    fullDirName : `str`
        The full directory name.
    logger : `logging.logger`
        The logger.

    Returns
    -------
    dayObsInt, dayObsStr : `tuple` of `int, str`
        The dayObs as an int and a str, or ``None, None`` is parsing failed.
    """
    dirname = os.path.basename(fullDirName)
    dirname = dirname.replace('ut', '')
    try:
        # days are of the format YYMMDD, make it YYYYMMDD
        dirname = '20' + dirname
        dayObsInt = int(dirname)
        dayObsStr = dayObsIntToString(dayObsInt)
        return dayObsInt, dayObsStr
    except Exception:
        logger.warning(f"Failed to parse directory name {fullDirName}")
        return None, None


def _convertJpgScale(inFilename, outFilename):
    """Convert an image file, cropping and stretching for correctly for use
    in the all sky cam TV channel.

    Parameters
    ----------
    inFilename : `str`
        The input filename.
    outFilename : `str`
        The output filename.
    """
    cmd = ['convert',
           inFilename,
           '-crop 2970x2970+747+0',  # crops to square
           '-contrast-stretch .5%x.5%',  # approximately the same as 99.5% scale in ds9
           outFilename,
           ]
    subprocess.check_call(r' '.join(cmd), shell=True)


def _imagesToMp4(indir, outfile, framerate, verbose=False):
    """Create the movie with ffmpeg, from files.

    Parameters
    ----------
    indir : `str`
        The directory containing the files to animate.
    outfile : `str`
        The full path and filename for the output movie.
    framerate : `int`
        The framerate, in frames per second.
    verbose : `bool`
        Be verbose?
    """
    # NOTE: the order of ffmpeg arguments *REALLY MATTERS*.
    # Reorder them at your own peril!
    pathPattern = f'\"{os.path.join(indir, "*.jpg")}\"'
    if verbose:
        ffmpeg_verbose = 'info'
    else:
        ffmpeg_verbose = 'error'
    cmd = ['ffmpeg',
           '-v', ffmpeg_verbose,
           '-f', 'image2',
           '-y',
           '-pattern_type glob',
           '-framerate', f'{framerate}',
           '-i', pathPattern,
           '-vcodec', 'libx264',
           '-b:v', '20000k',
           '-profile:v', 'main',
           '-pix_fmt', 'yuv420p',
           '-threads', '10',
           '-r', f'{framerate}',
           os.path.join(outfile)]

    subprocess.check_call(r' '.join(cmd), shell=True)


def _seqNumFromFilename(filename):
    """Get the seqNum from a filename.

    Parameters
    ----------
    filename : `str`
        The filename to get the seqNum from.

    Returns
    -------
    seqNum : `int`
        The seqNum.
    """
    # filenames look like /some/path/asc2204290657.jpg
    seqNum = os.path.basename(filename)[:-4][-4:]  # 0-padded 4 digit string
    seqNum = int(seqNum)
    return seqNum


def _getSortedSubDirs(path):
    """Get an alphabetically sorted list of directories from a given path.

    Parameters
    ----------
    path : `str`
        The path to get the sorted subdirectories from.

    Returns
    -------
    dirs : `list` of `str`
        The sorted list of directories.
    """
    if not os.path.isdir(path):
        raise RuntimeError(f"Cannot get directories from {path}: it is not a path")
    dirs = os.listdir(path)
    return sorted([p for d in dirs if (os.path.isdir(p := os.path.join(path, d)))])


def _getFilesetFromDir(path, filetype='jpg'):
    """Get an alphabetically sorted list of files of a given type from a dir.

    Parameters
    ----------
    path : `str`
        The path to get the files from.
    filetype : `str`, optional
        The filetype.

    Returns
    -------
    files : `set` of `str`
        The set of files in the directory.
    """
    if not os.path.isdir(path):
        raise RuntimeError(f"Cannot get files from {path}: it is not a directory")
    files = [f for fname in os.listdir(path) if (os.path.isfile(f := os.path.join(path, fname)))]
    files = [f for f in files if f.endswith(filetype)]
    return set(files)


class DayAnimator():
    """A class for creating all sky camera stills and animations for a single
    specified day.

    The run() method lasts until the dayObs rolls over, doing the file
    conversions and animations, and then returns.

    Set historical=True to not monitor the directory and dayObs values, and
    just process the entire directory as if it were complete. Skips
    intermediate uploading of stills, and just generates and uploads the final
    movie.

    Parameters
    ----------
    dayObsInt : `int`
        The dayObs, as an integer
    todaysDataDir : `str`
        The directory holding the raw jpgs for the day.
    outputImageDir : `str`
        The path to write the converted images out to. Need not exist, but must
        be creatable with write privileges.
    outputMovieDir : `str`
        The path to write the movies. Need not exist, but must be creatable
        with write privileges.
    uploader : `lsst.rubintv.production.Uploader`
        The uploader for sending images and movies to GCS.
    channel : `str`
        The name of the channel. Must match a channel name in rubinTv.py.
    historical : `bool`, optional
        Is this historical or live data?

    Notes
    -----
    TODO: DM-34631 Add GCS cleanup of old files at the end of each day.
    """
    FPS = 10
    DRY_RUN = False

    def __init__(self, *,
                 dayObsInt,
                 todaysDataDir,
                 outputImageDir,
                 outputMovieDir,
                 uploader,
                 channel,
                 historical=False):
        self.dayObsInt = dayObsInt
        self.todaysDataDir = todaysDataDir
        self.outputImageDir = outputImageDir
        self.outputMovieDir = outputMovieDir
        self.uploader = uploader
        self.channel = channel
        self.historical = historical
        self.log = logging.getLogger("allSkyDayAnimator")
        self.log.setLevel(logging.DEBUG)

    def hasDayRolledOver(self):
        """Check if the dayObs has rolled over.

        Checks if the current dayObs is the one the class was instantiated with
        and returns False if it is.

        Returns
        -------
        hasDayRolledOver : `bool`
            Whether the day has rolled over.
        """
        currentDay = getCurrentDayObs_int()
        if currentDay == self.dayObsInt:
            return False
        elif currentDay == self.dayObsInt+1:
            return True
        else:
            raise RuntimeError(f"Encountered non-linear time! Day animation was started with {self.dayObsInt}"
                               f"and now current dayObs is {currentDay}!")

    def _getConvertedFilename(self, filename):
        """Get the filename and path to write the converted images to.

        Parameters
        ----------
        filename : `str`
            The filename to convert.

        Returns
        -------
        convertedFilename : `str`
            The converted filename.
        """
        return os.path.join(self.outputImageDir, os.path.basename(filename))

    def convertFiles(self, files, forceRegen=False):
        """Convert a list of files using _convertJpgScale(), writing the
        converted files to self.outputImageDir

        Parameters
        ----------
        files : `Iterable` of `str`
            The set of files to convert
        forceRegen : `bool`
            Recreate the files even is they exist?

        Returns
        -------
        files : `set`
            The files which were converted.
        """
        convertedFiles = set()
        for file in sorted(files):  # sort just helps debug
            outputFilename = self._getConvertedFilename(file)
            self.log.debug(f"Converting {file} to {outputFilename}")
            if not self.DRY_RUN:
                if os.path.exists(outputFilename):
                    self.log.warning(f"Found already converted {outputFilename}")
                    if forceRegen:
                        _convertJpgScale(file, outputFilename)
                else:
                    _convertJpgScale(file, outputFilename)
            convertedFiles.add(file)
        return set(convertedFiles)

    def animateFilesAndUpload(self, isFinal=True):
        """Animate all the files in self.outputImageDir and upload to GCS.

        If isFinal is False the filename will end with largest input seqNum in
        the animation. If isFinal is True then it will end with seqNum_final.

        Parameters
        ----------
        isFinal : `bool`, optional
            Is this a final animation?
        """
        files = sorted(_getFilesetFromDir(self.outputImageDir))
        lastfile = files[-1]
        if isFinal:
            seqNumStr = 'final'
        else:
            seqNum = _seqNumFromFilename(lastfile)
            seqNumStr = f"{seqNum:05}"

        channel = 'all_sky_movies'
        fakeDataId = {'day_obs': self.dayObsInt, 'seq_num': seqNumStr}
        uploadAsFilename = _dataIdToFilename(channel, fakeDataId, extension='.mp4')
        creationFilename = os.path.join(self.outputMovieDir, uploadAsFilename)
        self.log.info(f"Creating movie from {self.outputImageDir} as {creationFilename}...")
        if not self.DRY_RUN:
            _imagesToMp4(self.outputImageDir, creationFilename, self.FPS)
            if not os.path.isfile(creationFilename):
                raise RuntimeError(f'Failed to find movie {creationFilename}')

        if not self.DRY_RUN:
            self.uploader.googleUpload(self.channel, creationFilename, uploadAsFilename)
        else:
            self.log.info(f"Would have uploaded {creationFilename} as {uploadAsFilename}")
        return

    def uploadLastStill(self, convertedFiles):
        """Upload the most recently created still image to GCS.

        Parameters
        ----------
        convertedFiles : `Iterable` of `str`
            The set of files from which to upload the most recent.
        """
        channel = 'all_sky_current'
        sourceFilename = sorted(convertedFiles)[-1]
        sourceFilename = self._getConvertedFilename(sourceFilename)
        seqNum = _seqNumFromFilename(sourceFilename)
        seqNumStr = f"{seqNum:05}"
        fakeDataId = {'day_obs': self.dayObsInt, 'seq_num': seqNumStr}
        uploadAsFilename = _dataIdToFilename(channel, fakeDataId, extension='.jpg')
        self.log.debug(f"Uploading {sourceFilename} as {uploadAsFilename}")
        if not self.DRY_RUN:
            self.uploader.googleUpload(channel=channel,
                                       sourceFilename=sourceFilename,
                                       uploadAsFilename=uploadAsFilename)
        else:
            self.log.info(f"Would have uploaded {sourceFilename} as {uploadAsFilename}")

    def run(self, animationPeriod=600):
        """The main entry point.

        Keeps watching for new files in self.todaysDataDir. Each time a new
        file lands it is converted and written out immediately. Then, once
        `animationPeriod` has elapsed, a new movie is created containing all
        stills from that current day and is uploaded to GCS.

        At the end of the day, any remaining images and converted, and a movie
        is uploaded with the filename ending seqNum_final, which gets added
        to the historical all sky movies on the frontend.

        Parameters
        ----------
        animationPeriod : `int` or `float`, optional
            How frequently to upload a new movie, in seconds.
        """
        if self.historical:  # all files are ready, so do it all in one go
            allFiles = _getFilesetFromDir(self.todaysDataDir)
            convertedFiles = self.convertFiles(allFiles)
            self.animateFilesAndUpload(isFinal=True)
            return

        convertedFiles = set()
        lastAnimationTime = time.time()
        while True:
            allFiles = _getFilesetFromDir(self.todaysDataDir)
            sleep(1)  # small sleep in case one of the files was being transferred when we listed it

            # convert any new files
            newFiles = allFiles - convertedFiles
            if newFiles:
                newFiles = sorted(newFiles)
                # Never do more than 200 without making a movie along the way
                # This useful when restarting the service.
                if len(newFiles) > 200:
                    newFiles = newFiles[0:200]
                self.log.debug(f"Converting {len(newFiles)} images...")
                convertedFiles |= self.convertFiles(newFiles)
                self.uploadLastStill(convertedFiles)
            else:
                # we're up to speed, files are ~1/min so sleep for a bit
                self.log.debug('Sleeping 20s waiting for new files')
                sleep(20)

            # TODO: Add wait time message here for how long till next movie
            if newFiles and (time.time() - lastAnimationTime > animationPeriod):
                self.log.info(f"Starting periodic animation of {len(allFiles)} images.")
                self.animateFilesAndUpload(isFinal=False)
                lastAnimationTime = time.time()

            if self.hasDayRolledOver():
                # final sweep for new images
                allFiles = _getFilesetFromDir(self.todaysDataDir)
                newFiles = allFiles - convertedFiles
                convertedFiles |= self.convertFiles(newFiles)
                self.uploadLastStill(convertedFiles)

                # make the movie and upload as final
                self.log.info(f"Starting final animation of {len(allFiles)} for {self.dayObsInt}")
                self.animateFilesAndUpload(isFinal=True)
                return


class AllSkyMovieChannel():
    """Class for running the All Sky Camera channels on RubinTV.

    Throughout the day/night it monitors the rootDataPath for new directories.
    When a new day's data directory is created, a new DayAnimator is spawned.

    In the DayAnimator, when a new file lands, it re-stretches the file
    to improve the contrast, copying that restretched image to a directory for
    animation.

    As each new file is found it is added to the end of the movie, which is
    uploaded with its "seq_num" being the number of the final input image in
    the movie, such that new movies are picked up with the same logic as the
    other "current" channels on the front end.

    At the end of each day, the final movie crystallizes and is uploaded as
    _final.mp4 for use in the historical data section.

    Parameters
    ----------
    rootDataPath : `str`
        The path to the all sky camera data directory, containing the
        utYYMMDD directories.
    outputRoot : `str`
        The root path to write all outputs to. It need not exist, but must be
         creatable with write privileges.
    doRaise `bool`
        Raise on error?
    """

    def __init__(self, rootDataPath, outputRoot, doRaise=False):
        self.uploader = Uploader()
        self.log = logging.getLogger("allSkyMovieMaker")
        self.channel = 'all_sky_movies'
        self.doRaise = doRaise

        self.rootDataPath = rootDataPath
        if not os.path.exists(rootDataPath):
            raise RuntimeError(f"Root data path {rootDataPath} not found")

        self.outputRoot = outputRoot
        _createWritableDir(outputRoot)

    def getCurrentRawDataDir(self):
        """Get the raw data dir corresponding to the current dayObs.

        Returns
        -------
        path : `str`
            The raw data dir for today.
        """
        # NB lower case %y as dates are like YYMMDD
        today = getCurrentDayObs_datetime().strftime("%y%m%d")
        return os.path.join(self.rootDataPath, f"ut{today}")

    def runDay(self, dayObsInt, todaysDataDir):
        """Create a DayAnimator for the current day and run it.

        Parameters
        ----------
        dayObsInt : `int`
            The dayObs as an int.
        todaysDataDir : `str`
            The data dir containing the files for today.
        """
        outputMovieDir = os.path.join(self.outputRoot, str(dayObsInt))
        outputJpgDir = os.path.join(self.outputRoot, str(dayObsInt), 'jpgs')
        _createWritableDir(outputMovieDir)
        _createWritableDir(outputJpgDir)
        self.log.info(f"Creating new day animator for {dayObsInt}")
        animator = DayAnimator(dayObsInt=dayObsInt,
                               todaysDataDir=todaysDataDir,
                               outputImageDir=outputJpgDir,
                               outputMovieDir=outputMovieDir,
                               uploader=self.uploader,
                               channel=self.channel,)
        animator.run()

    def run(self):
        """The main entry point - start running the all sky camera TV channels.
        See class init docs for details.
        """
        while True:
            try:
                dirs = _getSortedSubDirs(self.rootDataPath)
                mostRecentDir = dirs[-1]
                todaysDataDir = self.getCurrentRawDataDir()
                dayObsInt = getCurrentDayObs_int()
                self.log.debug(f"mostRecentDir={mostRecentDir}, todaysDataDir={todaysDataDir}")
                if mostRecentDir == todaysDataDir:
                    self.log.info(f"Starting day's animation for {todaysDataDir}.")
                    self.runDay(dayObsInt, todaysDataDir)
                elif mostRecentDir < todaysDataDir:
                    self.log.info(f'Waiting 30s for {todaysDataDir} to be created...')
                    sleep(30)
                elif mostRecentDir > todaysDataDir:
                    raise RuntimeError('Running in the past but mode is not historical')
            except Exception as e:
                if self.doRaise:
                    raise RuntimeError from e
                else:
                    info = f"mostRecentDir: {mostRecentDir}\n"
                    info += f"todaysDataDir: {todaysDataDir}\n"
                    info += f"dayObsInt: {dayObsInt}\n"
                    self.log.warning(f"Error processing all sky data: caught {repr(e)}. Info: {info}")
