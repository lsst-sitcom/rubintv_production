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

__all__ = [
    "DonutLauncher",
    "PsfAzElPlotter",
    "FocalPlaneFWHMPlotter",
    "FocusSweepAnalysis",
    "RadialPlotter",
]

import logging
import subprocess
import tempfile
import threading
from time import sleep, time
from typing import TYPE_CHECKING

from matplotlib.figure import Figure

from lsst.daf.butler import DatasetNotFoundError, EmptyQueryResultError
from lsst.summit.extras.plotting.focusSweep import (
    collectSweepData,
    fitSweepParabola,
    inferSweepVariable,
    plotSweepParabola,
)
from lsst.summit.extras.plotting.fwhmFocalPlane import getFwhmValues, makeFocalPlaneFWHMPlot
from lsst.summit.extras.plotting.psfPlotting import (
    makeAzElPlot,
    makeFigureAndAxes,
    makeTableFromSourceCatalogs,
)
from lsst.summit.utils import ConsDbClient
from lsst.summit.utils.efdUtils import makeEfdClient
from lsst.summit.utils.plotRadialAnalysis import makePanel
from lsst.summit.utils.utils import getCameraFromInstrumentName, getDetectorIds
from lsst.utils.plotting.figures import make_figure

from .redisUtils import RedisHelper, _extractExposureIds
from .uploaders import MultiUploader
from .utils import (
    getCiPlotNameFromRecord,
    getRubinTvInstrumentName,
    managedTempFile,
    writeExpRecordMetadataShard,
    writeMetadataShard,
)

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, DimensionRecord

    from .utils import LocationConfig


class DonutLauncher:
    """The DonutLauncher, for automatically launching donut processing.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler object used for data access.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    inputCollection : `str`
        The name of the input collection.
    outputCollection : `str`
        The name of the output collection.
    instrument : `str`
        The instrument.
    queueName : `str`
        The name of the redis queue to consume from.
    allowMissingDependencies : `bool`, optional
        Can the class be instantiated when there are missing dependencies?
    """

    def __init__(
        self,
        *,
        butler,
        locationConfig,
        inputCollection,
        outputCollection,
        instrument,
        queueName,
        metadataShardPath,
        allowMissingDependencies=False,
    ):
        self.butler = butler
        self.locationConfig = locationConfig
        self.inputCollection = inputCollection
        self.outputCollection = outputCollection
        self.queueName = queueName
        self.metadataShardPath = metadataShardPath
        self.allowMissingDependencies = allowMissingDependencies

        self.instrument = instrument
        self.pipelineFile = locationConfig.getAosPipelineFile(instrument)
        self.repo = locationConfig.comCamButlerPath.replace("/butler.yaml", "")
        self.log = logging.getLogger("lsst.rubintv.production.DonutLauncher")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)
        self.checkSetup()
        self.numCoresToUse = 9

        self.runningProcesses = {}  # dict of running processes keyed by PID
        self.lock = threading.Lock()

    def checkSetup(self):
        try:
            import batoid  # noqa: F401
            import danish  # noqa: F401

            import lsst.donut.viz as donutViz  # noqa: F401
            import lsst.ts.wep as tsWep  # noqa: F401
        except ImportError:
            if self.allowMissingDependencies:
                pass
            else:
                raise RuntimeError("Missing dependencies - can't launch donut pipelines like this")

    def _run_command(self, command):
        """Run a command as a subprocess.

        Runs the specified command as a subprocess, storing the process on the
        class in a thread-safe manner. It logs the start time, process ID
        (PID), and waits for the command to complete. If the command fails
        (return code is non-zero), it logs an error. Finally, it logs the
        completion time and duration of the command execution.

        Parameters
        ----------
        command : `str`
            The command to be executed.
        """
        start_time = time()
        process = subprocess.Popen(command, shell=False)
        with self.lock:
            self.runningProcesses[process.pid] = process
        self.log.info(f"Process started with PID {process.pid}")
        retcode = process.wait()
        end_time = time()
        duration = end_time - start_time
        with self.lock:
            self.runningProcesses.pop(process.pid)
        if retcode != 0:
            self.log.error(f"Command failed with return code {retcode}")
        self.log.info(f"Command completed in {duration:.2f} seconds with return code {retcode}")

    def launchDonutProcessing(self, exposureBytes, doRegister=False):
        """Launches the donut processing for a pair of donut exposures.

        Parameters:
        -----------
        exposureBytes : bytes
            The byte representation of the donut exposures, from redis.
        doRegister : bool, optional
            Add --register-dataset-types on the command line?

        Notes:
        ------
        This method extracts the exposure IDs from the given byte
        representation and launches the donut processing for the pair of donut
        exposures. If the instrument is "LSSTComCamSim", it adjusts the
        exposure IDs by adding 5000000000000 to account for the way this is
        recorded in the butler. The command is executed in a separate thread,
        and recorded as being in progress on the class.
        """
        exposureIds = _extractExposureIds(exposureBytes, self.instrument)
        if len(exposureIds) != 2:
            raise ValueError(f"Expected two exposureIds, got {exposureIds}")
        expId1, expId2 = exposureIds
        self.log.info(f"Received donut pair: {expId1, expId2}")

        # TODO: reduce this sleep a bit once you know how long this needs, or
        # write a function to poll. Better would be to write a blocking
        # WaitForExpRecord function in redisHelper, and then flag that as
        # existing in the same place as their picked up and fanned out
        sleep(10)
        for expId in exposureIds:
            (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId={"exposure": expId})
            writeExpRecordMetadataShard(expRecord, self.metadataShardPath)

        self.log.info(f"Launching donut processing for donut pair: {expId1, expId2}")
        query = f"exposure in ({expId1},{expId2}) and instrument='{self.instrument}'"
        command = [
            # stop black messing this section up
            # fmt: off
            # TODO: DM-45436 break this down into three commands to save
            # hammering the butler. May well be moot though, as noted on the
            # ticket.
            "pipetask", "run",
            "-j", str(self.numCoresToUse),
            "-b", self.repo,
            "-i", self.inputCollection,
            "-o", self.outputCollection,
            "-p", self.pipelineFile,
            "-d", query,
            "--rebase",
            # remove the --register addition eventually
            "--register-dataset-types",
            # fmt: on
        ]
        if doRegister:
            command.append("--register-dataset-types")

        self.log.info(f"Launching with command line: {' '.join(command)}")
        threading.Thread(target=self._run_command, args=(command,)).start()

        # now that we've launched, add the FOCUSZ value to the table on RubinTV
        for expId in exposureIds:
            md = self.butler.get("raw.metadata", exposure=expId, detector=0)
            (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId={"exposure": expId})

            focus = md.get("FOCUSZ", "MISSING VALUE")
            mdDict = {expRecord.seq_num: {"Focus Z": focus}}
            writeMetadataShard(self.metadataShardPath, expRecord.day_obs, mdDict)

    def run(self):
        """Start the event loop, listening for data and launching processing.

        This method continuously checks for exposure pairs in the queue and
        launches the donut processing for each pair. It also logs the status of
        running processes at regular intervals.
        """
        lastLogTime = time()
        logInterval = 10

        while True:
            exposurePairBytes = self.redisHelper.redis.lpop(self.queueName)
            if exposurePairBytes is not None:
                self.launchDonutProcessing(exposurePairBytes)
            else:
                sleep(0.5)

            currentTime = time()
            if currentTime - lastLogTime >= logInterval:
                with self.lock:
                    nRunning = len(self.runningProcesses)
                if nRunning > 0:
                    self.log.info(f"Currently running {nRunning} processes each with -j {self.numCoresToUse}")
                else:
                    self.log.info(f"Waiting for donut exposure arrival at {self.queueName}")
                lastLogTime = currentTime


class PsfAzElPlotter:
    """The PsfAzElPlotter, for automatically plotting PSF shape in Az/El.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler object used for data access.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    instrument : `str`
        The instrument.
    queueName : `str`
        The name of the redis queue to consume from.
    """

    def __init__(
        self,
        *,
        butler: Butler,
        locationConfig: LocationConfig,
        instrument: str,
        queueName: str,
    ) -> None:
        self.butler = butler
        self.locationConfig = locationConfig
        self.instrument = instrument
        self.queueName = queueName

        self.instrument = instrument
        self.camera = getCameraFromInstrumentName(self.instrument)
        self.log = logging.getLogger("lsst.rubintv.production.aos.PsfAzElPlotter")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)
        self.s3Uploader = MultiUploader()
        self.fig, self.axes = makeFigureAndAxes()

    def makePlot(self, visitId: int) -> None:
        """Make the PSF plot for the given visit ID.

        Makes the plot by getting the available data from the butler, saves it
        to a temporary file, and uploads it to RubinTV.

        Parameters
        ----------
        visitId : `int`
            The visit ID for which to make the plot.
        """
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId={"visit": visitId})
        detectorIds = getDetectorIds(self.instrument)
        srcDict = {}
        for detectorId in detectorIds:
            try:
                srcDict[detectorId] = self.butler.get(
                    "single_visit_star_footprints", visit=visitId, detector=detectorId
                )
            except DatasetNotFoundError:
                pass

        visitInfo = None
        for detectorId in detectorIds:
            try:
                visitInfo = self.butler.get(
                    "preliminary_visit_image.visitInfo", visit=visitId, detector=detectorId
                )
                break
            except DatasetNotFoundError:
                pass
        if visitInfo is None:
            self.log.error(f"Could not find visitInfo for visitId {visitId}")
            return

        table = makeTableFromSourceCatalogs(srcDict, visitInfo)

        # TODO: DM-45437 Use a context manager here and everywhere
        tempFilename = tempfile.mktemp(suffix=".png")
        self.fig.clf()
        self.axes = self.fig.subplots(nrows=2, ncols=3)
        makeAzElPlot(self.fig, self.axes, table, self.camera, saveAs=tempFilename)

        self.s3Uploader.uploadPerSeqNumPlot(
            instrument=getRubinTvInstrumentName(self.instrument),
            plotName="psf_shape_azel",
            dayObs=expRecord.day_obs,
            seqNum=expRecord.seq_num,
            filename=tempFilename,
        )

    def run(self) -> None:
        """Start the event loop, listening for data and launching plotting."""
        while True:
            visitIdBytes = self.redisHelper.redis.lpop(self.queueName)
            if visitIdBytes is not None:
                visitId = int(visitIdBytes.decode("utf-8"))
                self.log.info(f"Making for PsfAzEl plot for visitId {visitId}")
                self.makePlot(visitId)
            else:
                sleep(0.5)


class FocalPlaneFWHMPlotter:
    """The FocalPlaneFWHMPlotter, for automatically plotting FWHM
    in Focal Plane.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler object used for data access.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    instrument : `str`
        The instrument.
    queueName : `str`
        The name of the redis queue to consume from.
    """

    def __init__(
        self,
        *,
        butler: Butler,
        locationConfig: LocationConfig,
        instrument: str,
        queueName: str,
    ) -> None:
        self.butler = butler
        self.locationConfig = locationConfig
        self.instrument = instrument
        self.queueName = queueName
        self.instrument = instrument
        self.camera = getCameraFromInstrumentName(self.instrument)
        self.log = logging.getLogger("lsst.rubintv.production.aos.FocalPlaneFWHMPlotter")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)
        self.s3Uploader = MultiUploader()

    def plotAndUpload(self, visitRecord: DimensionRecord) -> None:
        """Make the FWHM Focal Plane plot for the given visit ID.

        Makes the plot by getting the available data from the butler, saves it
        to a temporary file, and uploads it to RubinTV.

        Parameters
        ----------
        visitId : `int`
            The visit ID for which to make the plot.
        """
        visitSummary = None
        try:
            # might not be the best query here
            visitSummary = self.butler.get("visitSummary", visit=visitRecord.id)
        except DatasetNotFoundError:
            pass

        if visitSummary is None:
            self.log.error(f"Could not find visitInfo for visitId {visitRecord.id}")
            return

        fwhmValues, detectorIds = getFwhmValues(visitSummary)

        ciName = getCiPlotNameFromRecord(self.locationConfig, visitRecord, "fwhm_focal_plane")
        with managedTempFile(suffix=".png", ciOutputName=ciName) as tempFile:
            fig = make_figure(figsize=(12, 9))
            axes = fig.subplots(nrows=1, ncols=1)
            makeFocalPlaneFWHMPlot(fig, axes, fwhmValues, detectorIds, self.camera, saveAs=tempFile)
            self.s3Uploader.uploadPerSeqNumPlot(
                instrument=getRubinTvInstrumentName(self.instrument),
                plotName="fwhm_focal_plane",
                dayObs=visitRecord.day_obs,
                seqNum=visitRecord.seq_num,
                filename=tempFile,
            )

    def run(self) -> None:
        """Start the event loop, listening for data and launching plotting."""
        while True:
            expRecord = self.redisHelper.getExpRecordFromQueue(self.queueName)
            if expRecord is not None:
                t0 = time()
                self.log.info(f"Making for FWHMFocalPlane plot for visitId {expRecord.id}")
                self.plotAndUpload(expRecord)
                t1 = time()
                self.log.info(f"Finished making FWHMFocalPlane plot in {(t1 - t0):.2f}s for {expRecord.id}")
            else:
                sleep(0.5)


class FocusSweepAnalysis:
    """The FocusSweepAnalysis, for automatically plotting focus sweep data.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler object used for data access.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    queueName : `str`
        The name of the redis queue to consume from.
    instrument : `str`
        The instrument.
    metadataShardPath : `str`
        The path to write metadata shards to.
    """

    def __init__(
        self,
        *,
        butler: Butler,
        locationConfig: LocationConfig,
        queueName: str,
        instrument: str,
        metadataShardPath: str,
    ):
        self.butler = butler
        self.locationConfig = locationConfig
        self.queueName = queueName
        self.metadataShardPath = metadataShardPath

        self.instrument = instrument
        self.camera = getCameraFromInstrumentName(self.instrument)
        self.log = logging.getLogger("lsst.rubintv.production.aos.PsfAzElPlotter")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)
        self.s3Uploader = MultiUploader()
        self.consDbClient = ConsDbClient("http://consdb-pq.consdb:8080/consdb")
        self.efdClient = makeEfdClient()
        self.fig = Figure(figsize=(12, 9))
        self.fig, self.axes = makeFigureAndAxes()

    def makePlot(self, visitIds) -> None:
        """Extract the exposure IDs from the byte string.

        Parameters
        ----------
        visitId : `int`
            The byte string containing the exposure IDs.

        Returns
        -------
        expIds : `list` of `int`
            A list of two exposure IDs extracted from the byte string.

        Raises
        ------
        ValueError
            If the number of exposure IDs extracted is not equal to 2.
        """
        visitIds = sorted(visitIds)
        lastVisit = visitIds[-1]

        # blocking call which waits for RA to announce that visit level info
        # is in consDB.
        self.log.info(f"Waiting for PSF measurements for last image {lastVisit}")
        self.redisHelper.waitForResultInConsdDb(
            self.instrument, f"cdb_{self.instrument.lower()}.visit1_quicklook", lastVisit, timeout=90
        )
        self.log.info(f"Finished waiting for PSF measurements for last image {lastVisit}")

        records = []
        for visitId in visitIds:
            (record,) = self.butler.registry.queryDimensionRecords("exposure", dataId={"visit": visitId})
            records.append(record)
        lastRecord = records[-1]  # this is the one the plot is "for" on RubinTV
        writeExpRecordMetadataShard(lastRecord, self.metadataShardPath)

        data = collectSweepData(records, self.consDbClient, self.efdClient)
        varName = inferSweepVariable(data)
        fit = fitSweepParabola(data, varName)

        self.fig.clf()
        axes = self.fig.subplots(nrows=3, ncols=4)

        tempFilename = tempfile.mktemp(suffix=".png")
        plotSweepParabola(data, varName, fit, saveAs=tempFilename, figAxes=(self.fig, axes))

        self.s3Uploader.uploadPerSeqNumPlot(
            instrument=getRubinTvInstrumentName(self.instrument) + "_aos",
            plotName="focus_sweep",
            dayObs=lastRecord.day_obs,
            seqNum=lastRecord.seq_num,
            filename=tempFilename,
        )

    def run(self) -> None:
        """Start the event loop, listening for data and launching plotting."""
        while True:
            visitIdsBytes = self.redisHelper.redis.lpop(self.queueName)
            if visitIdsBytes is not None:
                visitIds = _extractExposureIds(visitIdsBytes, self.instrument)
                self.log.info(f"Making for focus sweep plots for visitIds: {visitIds}")
                self.makePlot(visitIds)
            else:
                sleep(0.5)


class RadialPlotter:
    """The Radial plotter, for making the radial analysus plots.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler object used for data access.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    instrument : `str`
        The instrument.
    queueName : `str`
        The name of the redis queue to consume from.
    """

    def __init__(
        self,
        *,
        butler: Butler,
        locationConfig: LocationConfig,
        instrument: str,
        queueName: str,
    ) -> None:
        self.butler = butler
        self.locationConfig = locationConfig
        self.instrument = instrument
        self.queueName = queueName

        self.instrument = instrument
        self.camera = getCameraFromInstrumentName(self.instrument)
        self.log = logging.getLogger("lsst.rubintv.production.aos.RadialPlotter")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)
        self.s3Uploader = MultiUploader()

    def plotAndUpload(self, expRecord: DimensionRecord) -> None:

        sat_col_ref = "calib_psf_used"
        try:
            imgRefs = self.butler.query_datasets("preliminary_visit_image", data_id=expRecord.dataId)
            srcRefs = self.butler.query_datasets("single_visit_star_footprints", data_id=expRecord.dataId)
        except EmptyQueryResultError:
            self.log.error(f"No data found for {expRecord.dataId}")
            return

        imgDict = {
            self.camera[dr.dataId["detector"]].getName(): self.butler.get(dr)
            for dr in imgRefs
            if "S11" in self.camera[dr.dataId["detector"]].getName()
        }
        srcDict = {
            self.camera[dr.dataId["detector"]].getName(): self.butler.get(dr)
            for dr in srcRefs
            if "S11" in self.camera[dr.dataId["detector"]].getName()
        }
        srcDict = {key: tab.asAstropy()[(tab[sat_col_ref])].to_pandas() for key, tab in srcDict.items()}
        fig = makePanel(imgDict, srcDict, instrument="LSSTCam", figsize=(15, 15), onlyS11=True)

        ciName = getCiPlotNameFromRecord(self.locationConfig, expRecord, "imexam")
        with managedTempFile(suffix=".png", ciOutputName=ciName) as tempFile:
            fig.savefig(tempFile, bbox_inches="tight")
            self.s3Uploader.uploadPerSeqNumPlot(
                instrument=getRubinTvInstrumentName(self.instrument),
                plotName="imexam",
                dayObs=expRecord.day_obs,
                seqNum=expRecord.seq_num,
                filename=tempFile,
            )

    def run(self) -> None:
        """Start the event loop, listening for data and launching plotting."""
        while True:
            expRecord = self.redisHelper.getExpRecordFromQueue(self.queueName)
            if expRecord is not None:
                t0 = time()
                self.log.info(f"Making for radial plot for {expRecord.id}")
                self.plotAndUpload(expRecord)
                t1 = time()
                self.log.info(f"Finished making radial plot in {(t1 - t0):.2f}s for {expRecord.id}")
            else:
                sleep(0.5)
