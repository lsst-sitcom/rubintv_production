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
from lsst.rubintv.production.pipelineRunning import SingleCorePipelineRunner
import lsst.daf.butler as dafButler
from lsst.rubintv.production.utils import LocationConfig
from lsst.summit.utils.utils import setupLogging

instrument = 'LSSTComCamSim'

setupLogging()

workerName = os.getenv("WORKER_NAME")  # when using statefulSets
if workerName:
    workerNum = int(workerName.split("-")[-1])
    print(f'Found WORKER_NAME={workerName} in the env, derived {workerNum=} from that')
else:
    workerNum = os.getenv("WORKER_NUMBER")  # here for *forward* compatibility for next Kubernetes release
    print(f'Found WORKER_NUMBER={workerNum} in the env')
    if not workerNum:
        if len(sys.argv) < 2:
            print("Must supply worker number either as WORKER_NUMBER env var or as a command line argument")
            sys.exit(1)
        workerNum = int(sys.argv[1])

workerNum = int(workerNum)

detectorNum = workerNum % 9
detectorDepth = workerNum//9
queueName = f"SFM-WORKER-{detectorNum:02}-{detectorDepth:02}"
print(f"Running raw processor for worker {workerNum}, queueName={queueName}")

location = 'summit'
locationConfig = LocationConfig(location)
butler = dafButler.Butler(
    locationConfig.comCamButlerPath,
    collections=[
        'LSSTComCamSim/defaults',
    ],
    writeable=True
)

sfmRunner = SingleCorePipelineRunner(
    butler=butler,
    locationConfig=locationConfig,
    instrument=instrument,
    pipeline=locationConfig.sfmPipelineFile,
    step='step1',
    awaitsDataProduct='raw',
    doRaise=True,
    queueName=queueName
)
sfmRunner.run()