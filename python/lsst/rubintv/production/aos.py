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

__all__ = [
    "DonutLauncher",
    "PsfAzElPlotter",
]

import logging
import subprocess
import tempfile
import threading
from time import sleep, time

import numpy as np
from astropy.table import vstack
from matplotlib.figure import Figure

from lsst.daf.butler import DatasetNotFoundError
from lsst.geom import radians
from lsst.summit.extras.plotting.focusSweep import (
    collectSweepData,
    fitSweepParabola,
    inferSweepVariable,
    plotSweepParabola,
)
from lsst.summit.extras.plotting.psfPlotting import extendTable, makeAzElPlot, makeFigureAndAxes
from lsst.summit.utils.efdUtils import makeEfdClient
from lsst.summit.utils.utils import ConsDbClient, getCameraFromInstrumentName, getDetectorIds

from .redisUtils import RedisHelper
from .uploaders import MultiUploader


def _extractExposureIds(exposureBytes):
    """Extract the exposure IDs from the byte string.

    Parameters
    ----------
    exposureBytes : `bytes`
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
    exposureIds = exposureBytes.decode("utf-8").split(",")
    exposureIds = [int(v) for v in exposureIds]
    return exposureIds


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
    pipelineFile : `str`
        The path to the pipeline file to run.
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
        pipelineFile,
        queueName,
        allowMissingDependencies=False,
    ):
        self.butler = butler
        self.locationConfig = locationConfig
        self.inputCollection = inputCollection
        self.outputCollection = outputCollection
        self.pipelineFile = pipelineFile
        self.queueName = queueName
        self.allowMissingDependencies = allowMissingDependencies

        self.instrument = "LSSTComCamSim"
        self.repo = locationConfig.comCamButlerPath.replace("/butler.yaml", "")
        self.log = logging.getLogger("lsst.rubintv.production.DonutLauncher")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)
        self.checkSetup()
        self.numCoresToUse = 9

        self.runningProcesses = []
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
            self.runningProcesses.append(process)
        self.log.info(f"Process started with PID {process.pid}")
        retcode = process.wait()
        end_time = time()
        duration = end_time - start_time
        with self.lock:
            self.runningProcesses.remove(process)
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
        exposureIds = _extractExposureIds(exposureBytes)
        if len(exposureIds) != 2:
            raise ValueError(f"Expected two exposureIds, got {exposureIds}")
        expId1, expId2 = exposureIds

        if self.instrument == "LSSTComCamSim":
            # simulated exp ids are in the year 702X so add this manually, as
            # OCS doesn't know about the fact the butler will add this on. This
            # is only true for LSSTComCamSim though.
            self.log.info("Adding 50000000000000 to exposure ids to adjust for simulated LSSTComCamSim data")
            expId1 += 5000000000000
            expId2 += 5000000000000
        self.log.info(f"Launching donut processing for donut pair: {expId1, expId2}")
        query = f"exposure in ({expId1},{expId2}) and instrument='{self.instrument}'"
        command = [
            # stop black messing this section up
            # fmt: off
            "pipetask", "run",
            "-j", str(self.numCoresToUse),
            "-b", self.repo,
            "-i", self.inputCollection,
            "-o", self.outputCollection,
            "-p", self.pipelineFile,
            "-d", query,
            # fmt: on
        ]
        if doRegister:
            command.append("--register-dataset-types")

        self.log.info(f"Launching with command line: {' '.join(command)}")
        threading.Thread(target=self._run_command, args=(command,)).start()

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
    """

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler object used for data access.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    queueName : `str`
        The name of the redis queue to consume from.
    """

    def __init__(
        self,
        *,
        butler,
        locationConfig,
        queueName,
    ):
        self.butler = butler
        self.locationConfig = locationConfig
        self.queueName = queueName

        self.instrument = "LSSTComCamSim"
        self.camera = getCameraFromInstrumentName(self.instrument)
        self.log = logging.getLogger("lsst.rubintv.production.aos.PsfAzElPlotter")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)
        self.uploader = MultiUploader()
        self.fig, self.axes = makeFigureAndAxes()

    def makePlot(self, visitId):
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
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId={"visit": visitId})
        detectorIds = getDetectorIds(self.instrument)
        icSrcDict = {}
        for detectorId in detectorIds:
            try:
                icSrcDict[detectorId] = self.butler.get("icSrc", visit=visitId, detector=detectorId)
            except DatasetNotFoundError:
                pass

        visitInfo = None
        for detectorId in detectorIds:
            try:
                visitInfo = self.butler.get("calexp.visitInfo", visit=visitId, detector=detectorId)
                break
            except DatasetNotFoundError:
                pass
        if visitInfo is None:
            self.log.error(f"Could not find visitInfo for visitId {visitId}")
            return

        table = self.makeTableFromIcSrc(icSrcDict, visitInfo)

        tempFilename = tempfile.mktemp(suffix=".png")
        self.fig.clf()
        self.axes = self.fig.subplots(nrows=2, ncols=2)
        makeAzElPlot(self.fig, self.axes, table, self.camera, tempFilename)

        self.uploader.uploadPerSeqNumPlot(
            instrument="comcam_sim",
            plotName="psf_shape_azel",
            dayObs=expRecord.day_obs,
            seqNum=expRecord.seq_num,
            filename=tempFilename,
        )

    def makeTableFromIcSrc(self, icSrcs, visitInfo):
        tables = []

        for detectorNum, icSrc in icSrcs.items():
            icSrc = icSrc.asAstropy()
            icSrc = icSrc[icSrc["calib_psf_candidate"]]
            icSrc["detector"] = detectorNum
            tables.append(icSrc)

        table = vstack(tables)
        # Add shape columns
        table["Ixx"] = table["slot_Shape_xx"] * (0.2) ** 2
        table["Ixy"] = table["slot_Shape_xy"] * (0.2) ** 2
        table["Iyy"] = table["slot_Shape_yy"] * (0.2) ** 2
        table["T"] = table["Ixx"] + table["Iyy"]
        table["e1"] = (table["Ixx"] - table["Iyy"]) / table["T"]
        table["e2"] = 2 * table["Ixy"] / table["T"]
        table["e"] = np.hypot(table["e1"], table["e2"])
        table["x"] = table["base_FPPosition_x"]
        table["y"] = table["base_FPPosition_y"]

        table.meta["rotTelPos"] = (
            visitInfo.boresightParAngle - visitInfo.boresightRotAngle - (np.pi / 2 * radians)
        ).asRadians()
        table.meta["rotSkyPos"] = visitInfo.boresightRotAngle.asRadians()

        rtp = table.meta["rotTelPos"]
        srtp, crtp = np.sin(rtp), np.cos(rtp)
        aaRot = (
            np.array([[crtp, srtp], [-srtp, crtp]]) @ np.array([[0, 1], [1, 0]]) @ np.array([[-1, 0], [0, 1]])
        )
        table = extendTable(table, aaRot, "aa")
        table.meta["aaRot"] = aaRot

        rsp = table.meta["rotSkyPos"]
        srsp, crsp = np.sin(rsp), np.cos(rsp)
        nwRot = np.array([[crsp, -srsp], [srsp, crsp]])
        table = extendTable(table, nwRot, "nw")
        table.meta["nwRot"] = nwRot

        return table

    def run(self):
        """Start the event loop, listening for data and launching plotting."""
        while True:
            visitIdBytes = self.redisHelper.redis.lpop(self.queueName)
            if visitIdBytes is not None:
                visitId = int(visitIdBytes.decode("utf-8"))
                self.log.info(f"Making for PsfAzEl plot for visitId {visitId}")
                self.makePlot(visitId)
            else:
                sleep(0.5)


class FocusSweepAnalysis:
    """

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler object used for data access.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    queueName : `str`
        The name of the redis queue to consume from.
    """

    def __init__(
        self,
        *,
        butler,
        locationConfig,
        queueName,
    ):
        self.butler = butler
        self.locationConfig = locationConfig
        self.queueName = queueName

        self.instrument = "LSSTComCamSim"
        self.camera = getCameraFromInstrumentName(self.instrument)
        self.log = logging.getLogger("lsst.rubintv.production.aos.PsfAzElPlotter")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)
        self.uploader = MultiUploader()
        self.consDbClient = ConsDbClient("http://consdb-pq.consdb:8080/consdb")
        self.efdClient = makeEfdClient()
        self.fig = Figure(figsize=(12, 9))
        self.fig, self.axes = makeFigureAndAxes()

    def makePlot(self, visitIds):
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
        self.redisHelper.waitForResultInConsdDb(
            self.instrument, "cdb_lsstcomcamsim.visit1_quicklook", lastVisit, timeout=600
        )

        records = []
        for visitId in visitIds:
            (record,) = self.butler.registry.queryDimensionRecords("exposure", dataId={"visit": visitId})
            records.append(record)
        lastRecord = records[-1]  # this is the one the plot is "for" on RubinTV

        data = collectSweepData(records, self.consDbClient, self.efdClient)
        varName = inferSweepVariable(data)
        fit = fitSweepParabola(data, varName)

        self.fig.clf()
        axes = self.fig.subplots(nrows=3, ncols=4)

        tempFilename = tempfile.mktemp(suffix=".png")
        plotSweepParabola(data, varName, fit, saveAs=tempFilename, figAxes=(self.fig, axes))

        self.uploader.uploadPerSeqNumPlot(
            instrument="comcam_sim_aos",
            plotName="focus_sweep",
            dayObs=lastRecord.day_obs,
            seqNum=lastRecord.seq_num,
            filename=tempFilename,
        )

    def run(self):
        """Start the event loop, listening for data and launching plotting."""
        while True:
            visitIdsBytes = self.redisHelper.redis.lpop(self.queueName)
            if visitIdsBytes is not None:
                visitIds = _extractExposureIds(visitIdsBytes)
                self.log.info(f"Making for focus sweep plots for visitIds: {visitIds}")
                self.makePlot(visitIds)
            else:
                sleep(0.5)
