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
import signal
import sys

import lsst.daf.butler as dafButler
from lsst.rubintv.production.pipelineRunning import SingleCorePipelineRunner
from lsst.rubintv.production.utils import getAutomaticLocationConfig, getDoRaise
from lsst.summit.utils.utils import setupLogging


instrument = "LSSTComCamSim"
_log = logging.getLogger()


class SignalHandler:
    def __init__(self, core_runner: SingleCorePipelineRunner):
        self._core_runner = core_runner

    def handler(self, signum, frame):
        _log.info(f"Received signal {signum}")
        if signum == signal.SIGTERM:
            _log.info("Stopping core runner")
            self._core_runner.stop()


def main(workerNum: int):
    setupLogging()
    queueName = f"NIGHTLYROLLUP-WORKER-{workerNum:02}"
    print(f"Running nightly rollup worker {workerNum}, queueName={queueName}")

    locationConfig = getAutomaticLocationConfig()
    butler = dafButler.Butler(
        locationConfig.comCamButlerPath,
        collections=[
            "LSSTComCamSim/defaults",
        ],
        writeable=True,
    )

    rollupRunner = SingleCorePipelineRunner(
        butler=butler,
        locationConfig=locationConfig,
        instrument=instrument,
        pipeline=locationConfig.sfmPipelineFile,
        step="nightlyRollup",
        awaitsDataProduct=None,
        doRaise=getDoRaise(),
        queueName=queueName,
    )
    handler_instance = SignalHandler(rollupRunner)
    signal.signal(signal.SIGTERM, handler_instance.handler)
    rollupRunner.run()
    sys.exit(0)


if __name__ == '__main__':
    workerName = os.getenv("WORKER_NAME")  # when using statefulSets
    if workerName:
        workerNum = workerName.split("-")[-1]
        print(f"Found WORKER_NAME={workerName} in the env, derived {workerNum=} from that")
    else:
        # here for *forward* compatibility for next Kubernetes release
        workerNum = os.getenv("WORKER_NUMBER")  
        print(f"Found WORKER_NUMBER={workerNum} in the env")
        if not workerNum:
            if len(sys.argv) < 2:
                print("Must supply worker number either as WORKER_NUMBER env var or as a"
                      " command line argument")
                sys.exit(1)
            workerNum = sys.argv[2]
    workerNum = int(workerNum)
    main(workerNum)
