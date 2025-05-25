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

from lsst.daf.butler import Butler
from lsst.rubintv.production.pipelineRunning import SingleCorePipelineRunner
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.utils import getAutomaticLocationConfig, getDoRaise, getPodWorkerNumber
from lsst.summit.utils.utils import setupLogging

setupLogging()
instrument = "LSSTComCam"
locationConfig = getAutomaticLocationConfig()

workerNum = getPodWorkerNumber()
detectorNum = workerNum % 9
detectorDepth = workerNum // 9
podDetails = PodDetails(
    instrument=instrument, podFlavor=PodFlavor.AOS_WORKER, detectorNumber=detectorNum, depth=detectorDepth
)
print(
    f"Running {podDetails.instrument} {podDetails.podFlavor.name} at {locationConfig.location},"
    f"consuming from {podDetails.queueName}..."
)

locationConfig = getAutomaticLocationConfig()
butler = Butler.from_config(
    locationConfig.comCamButlerPath,
    instrument=instrument,
    collections=[
        f"{instrument}/defaults",
        locationConfig.getOutputChain(instrument),
    ],
    writeable=True,
)

sfmRunner = SingleCorePipelineRunner(
    butler=butler,
    locationConfig=locationConfig,
    instrument=instrument,
    step="step1a",
    awaitsDataProduct="raw",
    podDetails=podDetails,
    doRaise=getDoRaise(),
)
sfmRunner.run()
