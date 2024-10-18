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

import lsst.daf.butler as dafButler
from lsst.rubintv.production.pipelineRunning import SingleCorePipelineRunner
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.utils import getAutomaticLocationConfig, getDoRaise
from lsst.summit.utils.utils import setupLogging

setupLogging()
instrument = "LSSTCam"

workerName = os.getenv("WORKER_NAME")  # when using statefulSets
if workerName:
    workerNum = int(workerName.split("-")[-1])
    print(f"Found WORKER_NAME={workerName} in the env, derived {workerNum=} from that")
else:
    workerNum = os.getenv("WORKER_NUMBER")  # here for *forward* compatibility for next Kubernetes release
    print(f"Found WORKER_NUMBER={workerNum} in the env")
    if not workerNum:
        if len(sys.argv) < 3:
            print("Must supply worker number either as WORKER_NUMBER env var or as a command line argument")
            sys.exit(1)
        workerNum = int(sys.argv[2])

workerNum = int(workerNum)

detectorNum = workerNum % 189
detectorDepth = workerNum // 189

locationConfig = getAutomaticLocationConfig()
podDetails = PodDetails(
    instrument=instrument, podFlavor=PodFlavor.MOSAIC_WORKER, detectorNumber=detectorNum, detectorDepth=0
)
print(
    f"Running {podDetails.instrument} {podDetails.podFlavor.name} at {locationConfig.location},"
    f"consuming from {podDetails.queueName}..."
)

locationConfig = getAutomaticLocationConfig()
butler = dafButler.Butler(  # type: ignore
    locationConfig.lsstCamButlerPath,
    collections=[
        "LSSTCam/defaults",
    ],
    writeable=True,
)

sfmRunner = SingleCorePipelineRunner(
    butler=butler,
    locationConfig=locationConfig,
    instrument=instrument,
    pipeline=locationConfig.getSfmPipelineFile(instrument),
    step="step1",
    awaitsDataProduct="raw",
    podDetails=podDetails,
    doRaise=getDoRaise(),
)
sfmRunner.run()
