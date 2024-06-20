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

__all__ = ["DonutLauncher"]

import logging
import subprocess
import threading
from time import sleep, time

from .redisUtils import RedisHelper


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

    def _extractExposureIds(self, exposureBytes):
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
        if len(exposureIds) != 2:
            raise ValueError(f"Expected two exposureIds, got {exposureIds}")
        return exposureIds

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
        expId1, expId2 = self._extractExposureIds(exposureBytes)
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
