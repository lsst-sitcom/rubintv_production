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

__all__ = ["TimedMetadataServer", "TmaTelemetryChannel"]

import json
import logging
import os
import subprocess
from functools import partial
from glob import glob
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt

from .uploaders import MultiUploader
from .utils import hasDayRolledOver, isFileWorldWritable, raiseIf, sanitizeNans, writeMetadataShard

try:
    from lsst_efd_client import EfdClient  # noqa: F401 just check we have it, but don't use it

    HAS_EFD_CLIENT = True
except ImportError:
    HAS_EFD_CLIENT = False

from lsst.summit.utils.efdUtils import clipDataToEvent, makeEfdClient
from lsst.summit.utils.m1m3.inertia_compensation_system import M1M3ICSAnalysis
from lsst.summit.utils.m1m3.plots.inertia_compensation_system import plot_hp_measured_data
from lsst.summit.utils.tmaUtils import (
    TMAEventMaker,
    getAzimuthElevationDataForEvent,
    getCommandsDuringEvent,
    plotEvent,
)
from lsst.summit.utils.utils import getCurrentDayObs_int

if TYPE_CHECKING:
    from lsst.rubintv.production.utils import LocationConfig
    from lsst.summit.utils.tmaUtils import TMAEvent

_LOG = logging.getLogger(__name__)


class TimedMetadataServer:
    """Class for serving metadata to RubinTV.

    Metadata shards are written to a /shards directory, which are collated on a
    timer and uploaded if new shards were found. This happens on a timer,
    defined by ``self.cadenceSeconds``.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration.
    metadataDirectory : `str`
        The name of the directory for which the metadata is being served. Note
        that this directory and the ``shardsDirectory`` are passed in because
        although the ``LocationConfig`` holds all the location based path info
        (and the name of the bucket to upload to), many directories containg
        shards exist, and each one goes to a different page on the web app, so
        this class must be told which set of files to be collating and
        uploading to which channel.
    shardsDirectory : `str`
        The directory to find the shards in, usually of the form
        ``metadataDirectory`` + ``'/shards'``.
    channelName : `str`
        The name of the channel to serve the metadata files to.
    doRaise : `bool`
        If True, raise exceptions instead of logging them.
    """

    # The time between searches of the metadata shard directory to merge the
    # shards and upload.
    cadenceSeconds = 1.5

    def __init__(
        self,
        *,
        locationConfig: LocationConfig,
        metadataDirectory: str,
        shardsDirectory: str,
        channelName: str,
        doRaise: bool = False,
    ):
        self.locationConfig = locationConfig
        self.metadataDirectory = metadataDirectory
        self.shardsDirectory = shardsDirectory
        self.channelName = channelName
        self.doRaise = doRaise
        self.log = _LOG.getChild(self.channelName)
        self.s3Uploader = MultiUploader()

        if not os.path.isdir(self.metadataDirectory):
            # created by the LocationConfig init so this should be impossible
            raise RuntimeError(f"Failed to find/create {self.metadataDirectory}")

    def mergeShardsAndUpload(self) -> None:
        """Merge all the shards in the shard directory into their respective
        files and upload the updated files.

        For each file found in the shard directory, merge its contents into the
        main json file for the corresponding dayObs, and for each file updated,
        upload it.
        """
        filesTouched = set()
        shardFiles = sorted(glob(os.path.join(self.shardsDirectory, "metadata-*")))
        if shardFiles:
            self.log.debug(f"Found {len(shardFiles)} shardFiles")
            sleep(0.1)  # just in case a shard is in the process of being written

        updating = set()

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
            if shard:  # each is a dict of dicts, keyed by seqNum
                for seqNum, seqNumData in shard.items():
                    seqNumData = sanitizeNans(seqNumData)  # remove NaNs
                    if seqNum not in data.keys():
                        data[seqNum] = {}
                    data[seqNum].update(seqNumData)
                    updating.add((dayObs, seqNum))
            os.remove(shardFile)

            with open(mainFile, "w") as f:
                json.dump(data, f)
            if not isFileWorldWritable(mainFile):
                os.chmod(mainFile, 0o777)  # file may be amended by another process

        if updating:
            for dayObs, seqNum in sorted(updating, key=lambda x: (x[0], x[1])):
                self.log.info(f"Updating metadata tables for: {dayObs=}, {seqNum=}")

        if filesTouched:
            self.log.info(f"Uploading {len(filesTouched)} metadata files")
            for file in filesTouched:
                dayObs = self.dayObsFromFilename(file)
                self.s3Uploader.uploadMetdata(self.channelName, dayObs, file)
        return

    def dayObsFromFilename(self, filename: str) -> int:
        """Get the dayObs from a metadata sidecar filename.

        Parameters
        ----------
        filename : `str`
            The filename.

        Returns
        -------
        dayObs : `int`
            The dayObs.
        """
        return int(os.path.basename(filename).split("_")[1].split(".")[0])

    def getSidecarFilename(self, dayObs: int) -> str:
        """Get the name of the metadata sidecar file for the dayObs.

        Parameters
        ----------
        dayObs : `int`
            The dayObs.

        Returns
        -------
        filename : `str`
            The filename.
        """
        return os.path.join(self.metadataDirectory, f"dayObs_{dayObs}.json")

    def callback(self) -> None:
        """Method called on a timer to gather the shards and upload as needed.

        Adds the metadata to the sidecar file for the dataId and uploads it.
        """
        try:
            self.log.debug("Getting metadata from shards")
            self.mergeShardsAndUpload()  # updates all shards everywhere

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def run(self) -> None:
        """Run continuously, looking for metadata and uploading."""
        while True:
            self.callback()
            sleep(self.cadenceSeconds)


class TmaTelemetryChannel(TimedMetadataServer):
    """Class for generating TMA events and plotting their telemetry.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration.
    metadataDirectory : `str`
        The name of the directory for which the metadata is being served. Note
        that this directory and the ``shardsDirectory`` are passed in because
        although the ``LocationConfig`` holds all the location based path info
        (and the name of the bucket to upload to), many directories containing
        shards exist, and each one goes to a different page on the web app, so
        this class must be told which set of files to be collating and
        uploading to which channel.
    shardsDirectory : `str`
        The directory to find the shards in, usually of the form
        ``metadataDirectory`` + ``'/shards'``.
    doRaise : `bool`
        If True, raise exceptions instead of logging them.
    """

    # The time between sweeps of the EFD for today's data.
    cadenceSeconds = 10

    def __init__(
        self,
        *,
        locationConfig: LocationConfig,
        metadataDirectory: str,
        shardsDirectory: str,
        doRaise: bool = False,
    ) -> None:

        self.plotChannelName = "tma_mount_motion_profile"
        self.metadataChannelName = "tma_metadata"
        self.doRaise = doRaise

        super().__init__(
            locationConfig=locationConfig,
            metadataDirectory=metadataDirectory,
            shardsDirectory=shardsDirectory,
            channelName=self.metadataChannelName,  # this is the one for mergeSharsAndUpload
            doRaise=self.doRaise,
        )

        self.client = makeEfdClient()
        self.eventMaker = TMAEventMaker(client=self.client)
        self.figure = plt.figure(figsize=(10, 8))
        self.slewPrePadding = 1
        self.trackPrePadding = 1
        self.slewPostPadding = 2
        self.trackPostPadding = 0
        self.commandsToPlot = ["raDecTarget", "moveToTarget", "startTracking", "stopTracking"]
        self.hardpointCommandsToPlot = [
            "lsst.sal.MTM1M3.command_setSlewFlag",
            "lsst.sal.MTM1M3.command_enableHardpointCorrections",
            "lsst.sal.MTM1M3.command_clearSlewFlag",
        ]

        # keeps track of which plots have been made on a given day
        self.plotsMade: dict[str, set] = {"MountMotionAnalysis": set(), "M1M3HardpointAnalysis": set()}

    def resetPlotsMade(self) -> None:
        """Reset the tracking of made plots for day-rollover."""
        self.plotsMade = {k: set() for k in self.plotsMade}

    def runMountMotionAnalysis(self, event: TMAEvent) -> None:
        # get the data separately so we can take some min/max on it etc
        dayObs = event.dayObs
        prePadding = self.slewPrePadding if event.type.name == "SLEWING" else self.trackPrePadding
        postPadding = self.slewPostPadding if event.type.name == "SLEWING" else self.trackPostPadding
        azimuthData, elevationData = getAzimuthElevationDataForEvent(
            self.client, event, prePadding=prePadding, postPadding=postPadding
        )

        clippedAz = clipDataToEvent(azimuthData, event)
        clippedEl = clipDataToEvent(elevationData, event)

        md = {}
        azStart = None
        azStop = None
        azMove = None

        elStart = None
        elStop = None
        elMove = None
        maxElTorque = None
        maxAzTorque = None

        if len(clippedAz) > 0:
            azStart = clippedAz.iloc[0]["actualPosition"]
            azStop = clippedAz.iloc[-1]["actualPosition"]
            azMove = azStop - azStart
            # key=abs gets the item with the largest absolute value but
            # keeps the sign so we don't deal with min/max depending on
            # the direction of the move etc
            maxAzTorque = max(clippedAz["actualTorque"], key=abs)

        if len(clippedEl) > 0:
            elStart = clippedEl.iloc[0]["actualPosition"]
            elStop = clippedEl.iloc[-1]["actualPosition"]
            elMove = elStop - elStart
            maxElTorque = max(clippedEl["actualTorque"], key=abs)

        # values could be None by design, for when there is no data
        # in the clipped dataframes, i.e. from the event window exactly
        md["Azimuth start"] = azStart
        md["Elevation start"] = elStart
        md["Azimuth move"] = azMove
        md["Elevation move"] = elMove
        md["Azimuth stop"] = azStop
        md["Elevation stop"] = elStop
        md["Largest azimuth torque"] = maxAzTorque
        md["Largest elevation torque"] = maxElTorque

        rowData = {event.seqNum: md}
        writeMetadataShard(self.shardsDirectory, event.dayObs, rowData)

        commands = getCommandsDuringEvent(
            self.client,
            event,
            self.commandsToPlot,
            prePadding=prePadding,
            postPadding=postPadding,
            doLog=False,
        )
        if not all([time is None for time in commands.values()]):
            rowData = {event.seqNum: {"Has commands?": "✅"}}
            writeMetadataShard(self.shardsDirectory, event.dayObs, rowData)

        metadataWriter = partial(writeMetadataShard, path=self.shardsDirectory)

        plotEvent(
            self.client,
            event,
            fig=self.figure,
            prePadding=prePadding,
            postPadding=postPadding,
            commands=commands,
            azimuthData=azimuthData,
            elevationData=elevationData,
            doFilterResiduals=True,
            metadataWriter=metadataWriter,
        )

        plotName = "tma_mount_motion_profile"
        filename = self._getSaveFilename(plotName, dayObs, event)
        self.figure.savefig(filename)
        self.s3Uploader.uploadPerSeqNumPlot(
            instrument="tma", plotName="mount", dayObs=event.dayObs, seqNum=event.seqNum, filename=filename
        )

    def runM1M3HardpointAnalysis(self, event: TMAEvent) -> None:
        m1m3ICSHPMaxForces = {}
        m1m3ICSHPMeanForces = {}

        md = {}  # blank out the previous md as it has already been written
        try:
            m1m3IcsResult = M1M3ICSAnalysis(
                event,
                self.client,
                log=self.log,
            )
        except ValueError:  # control flow error raise when the ICS is off
            return None

        # package all the items we want into dicts
        m1m3ICSHPMaxForces = {
            "measuredForceMax0": m1m3IcsResult.stats.measuredForceMax0,
            "measuredForceMax1": m1m3IcsResult.stats.measuredForceMax1,
            "measuredForceMax2": m1m3IcsResult.stats.measuredForceMax2,
            "measuredForceMax3": m1m3IcsResult.stats.measuredForceMax3,
            "measuredForceMax4": m1m3IcsResult.stats.measuredForceMax4,
            "measuredForceMax5": m1m3IcsResult.stats.measuredForceMax5,
        }
        m1m3ICSHPMeanForces = {
            "measuredForceMean0": m1m3IcsResult.stats.measuredForceMean0,
            "measuredForceMean1": m1m3IcsResult.stats.measuredForceMean1,
            "measuredForceMean2": m1m3IcsResult.stats.measuredForceMean2,
            "measuredForceMean3": m1m3IcsResult.stats.measuredForceMean3,
            "measuredForceMean4": m1m3IcsResult.stats.measuredForceMean4,
            "measuredForceMean5": m1m3IcsResult.stats.measuredForceMean5,
        }

        # do the max of the absolute values of the forces
        md["M1M3 ICS Hardpoint AbsMax-Max Force"] = max(m1m3ICSHPMaxForces.values(), key=abs)
        md["M1M3 ICS Hardpoint AbsMax-Mean Force"] = max(m1m3ICSHPMeanForces.values(), key=abs)

        # then repackage as strings with 1 dp for display
        m1m3ICSHPMaxForces = {k: f"{v:.1f}" for k, v in m1m3ICSHPMaxForces.items()}
        m1m3ICSHPMeanForces = {k: f"{v:.1f}" for k, v in m1m3ICSHPMeanForces.items()}

        md["M1M3 ICS Hardpoint Max Forces"] = m1m3ICSHPMaxForces  # dict
        md["M1M3 ICS Hardpoint Mean Forces"] = m1m3ICSHPMeanForces  # dict

        # must set string value in dict only after doing the max of the values
        m1m3ICSHPMaxForces["DISPLAY_VALUE"] = "📖" if m1m3ICSHPMaxForces else ""
        m1m3ICSHPMeanForces["DISPLAY_VALUE"] = "📖" if m1m3ICSHPMeanForces else ""

        rowData = {event.seqNum: md}
        writeMetadataShard(self.shardsDirectory, event.dayObs, rowData)

        plotName = "tma_m1m3_hardpoint_profile"
        filename = self._getSaveFilename(plotName, event.dayObs, event)

        commands = getCommandsDuringEvent(self.client, event, self.hardpointCommandsToPlot, doLog=False)

        # TODO: fix how typing is done in the M1M3 package now that we mypy
        plot_hp_measured_data(m1m3IcsResult, fig=self.figure, commands=commands, log=self.log)  # type: ignore
        self.figure.savefig(filename)
        self.s3Uploader.uploadPerSeqNumPlot(
            instrument="tma",
            plotName="m1m3_hardpoint",
            dayObs=event.dayObs,
            seqNum=event.seqNum,
            filename=filename,
        )

    def processDay(self, dayObs: int) -> None:
        """ """
        events = self.eventMaker.getEvents(dayObs)

        # check if every event seqNum is in both the M1M3HardpointAnalysis and
        # MountMotionAnalysis sets, and if not, return immediately
        if all([event.seqNum in self.plotsMade["MountMotionAnalysis"] for event in events]) and all(
            [event.seqNum in self.plotsMade["M1M3HardpointAnalysis"] for event in events]
        ):
            self.log.info(f"No new events found for {dayObs} (currently {len(events)} events).")
            return

        for event in events:
            assert event.dayObs == dayObs

            nMountMotionPlots = len(self.plotsMade["MountMotionAnalysis"])
            nM1M3HardpointPlots = len(self.plotsMade["M1M3HardpointAnalysis"])
            # the interesting phrasing in the message is because these plots
            # don't necessarily exist, due either to failures or M1M3 analyses
            # only being valid for some events so this is to make it clear
            # they've been processed.
            self.log.info(
                f"Found {len(events)} events for {dayObs=} of which "
                f"{nMountMotionPlots} have been mount-motion plotted and "
                f"{nM1M3HardpointPlots} have been M1M3-hardpoint-analysed plots."
            )

            # kind of worrying that this clear _is_ needed out here, but is
            # _not_ needed inside each of the plotting parts... maybe either
            # remove this or add it to the other parts?
            self.log.info(f"Plotting event {event.seqNum}")
            self.figure.clear()
            ax = self.figure.gca()
            ax.clear()

            newEvent = (
                event.seqNum not in self.plotsMade["MountMotionAnalysis"]
                or event.seqNum not in self.plotsMade["M1M3HardpointAnalysis"]
            )

            rowData: dict[int, dict[str, float | str]] = {}
            data: dict[int, dict[str, float | str]] = {}
            if event.seqNum not in self.plotsMade["MountMotionAnalysis"]:
                try:
                    self.runMountMotionAnalysis(event)  # writes its own shard
                except Exception as e:
                    data = {event.seqNum: {"Plotting failed?": "😔"}}
                    rowData.update(data)
                    self.log.exception(f"Failed to plot event {event.seqNum}")
                    raiseIf(self.doRaise, e, self.log)
                finally:  # don't retry plotting on failure
                    self.plotsMade["MountMotionAnalysis"].add(event.seqNum)

            if event.seqNum not in self.plotsMade["M1M3HardpointAnalysis"]:
                try:
                    self.runM1M3HardpointAnalysis(event)  # writes its own shard
                except Exception as e:
                    data = {event.seqNum: {"ICS processing error?": "😔"}}
                    rowData.update(data)
                    self.log.exception(f"Failed to plot event {event.seqNum}")
                    raiseIf(self.doRaise, e, self.log)
                finally:  # don't retry plotting on failure
                    self.plotsMade["M1M3HardpointAnalysis"].add(event.seqNum)

            if newEvent:
                eventData = self.eventToMetadataRow(event)
                rowData.update(eventData)
                writeMetadataShard(self.shardsDirectory, event.dayObs, rowData)

        return

    def eventToMetadataRow(self, event: TMAEvent) -> dict[int, dict[str, float | str]]:
        rowData: dict[str, float | str] = {}
        seqNum = event.seqNum
        rowData["Seq. No."] = event.seqNum
        rowData["Event version number"] = event.version
        rowData["Event type"] = event.type.name
        rowData["End reason"] = event.endReason.name
        rowData["Duration"] = event.duration
        rowData["Time UTC"] = event.begin.isot
        return {seqNum: rowData}

    def _getSaveFilename(self, plotName: str, dayObs: int, event: TMAEvent) -> str:
        filename = f"{plotName}_{dayObs}_{event.seqNum:06}.png"
        filename = os.path.join(self.locationConfig.plotPath, filename)
        return filename

    def run(self) -> None:
        """Run continuously, updating the plots and uploading the shards."""
        dayObs = getCurrentDayObs_int()
        while True:
            try:
                if hasDayRolledOver(dayObs):
                    dayObs = getCurrentDayObs_int()
                    self.resetPlotsMade()

                # TODO: need to work out a better way of dealing with pod
                # restarts. At present this will just remake everything.
                self.processDay(dayObs)
                self.mergeShardsAndUpload()  # updates all shards everywhere

                sleep(self.cadenceSeconds)

            except Exception as e:
                raiseIf(self.doRaise, e, self.log)


class AllNightAnimator:

    # The time between scans for new images to animate
    cadenceSeconds = 1

    def __init__(self, *, locationConfig: LocationConfig, instrument: str, doRaise: bool = False) -> None:
        self.locationConfig = locationConfig
        self.pngPath: Path = Path()
        self.doRaise = doRaise
        self.instrument = instrument
        self.log = _LOG.getChild("")

    def animateDir(self, pngPath: Path) -> None:
        outputFile = pngPath / "movie.mp4"

        self.pngsToMp4(pngPath.as_posix(), outputFile.as_posix(), 10)
        self.log.info(f"Uploading new {self.instrument} movie")

        # self.s3Uploader.uploadMovie(self.instrument, self.dayObs, filename)
        return

    def pngsToMp4(self, orderedPngDir: str, outfile: str, framerate: float, verbose: bool = False) -> None:
        """Create the movie with ffmpeg, from files."""
        # NOTE: the order of ffmpeg arguments *REALLY MATTERS*.
        # Reorder them at your own peril!
        pathPattern = f'"{os.path.join(orderedPngDir, "*.png")}"'
        if verbose:
            ffmpeg_verbose = "info"
        else:
            ffmpeg_verbose = "error"
        cmd = [
            "ffmpeg",
            "-v",
            ffmpeg_verbose,
            "-f",
            "image2",
            "-y",
            "-pattern_type glob",
            "-framerate",
            f"{framerate}",
            "-i",
            pathPattern,
            "-vcodec",
            "libx264",
            "-b:v",
            "20000k",
            "-profile:v",
            "main",
            "-pix_fmt",
            "yuv420p",
            "-threads",
            "10",
            "-r",
            f"{framerate}",
            os.path.join(outfile),
        ]

        subprocess.check_call(r" ".join(cmd), shell=True)

    def reset(self) -> None:
        return

    def run(self) -> None:
        """Run continuously, updating the plots and uploading the shards."""
        dayObs = getCurrentDayObs_int()
        lastAnimatedCount = 0
        while True:
            try:
                if hasDayRolledOver(dayObs):
                    dayObs = getCurrentDayObs_int()
                    lastAnimatedCount = 0

                # TODO DM-49948: currently this pattern is hard-coded in the
                # mosaic plotting code - to be made importable
                pngPath = Path(self.locationConfig.plotPath) / self.instrument / str(dayObs)
                nFiles = len(glob((pngPath / "*.png").as_posix()))
                if nFiles > lastAnimatedCount:
                    self.log.info(f"Creating new movie with {nFiles} frames")
                    self.animateDir(pngPath)

                sleep(self.cadenceSeconds)

            except Exception as e:
                raiseIf(self.doRaise, e, self.log)
