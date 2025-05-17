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
from time import sleep

from lsst.daf.butler import Butler
from lsst.rubintv.production.pipelineRunning import SingleCorePipelineRunner
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.utils import getAutomaticLocationConfig, getDoRaise, getPodWorkerNumber
from lsst.summit.utils.utils import setupLogging

setupLogging()
log = logging.getLogger(__name__)
instrument = "LSSTCam"

workerNum = getPodWorkerNumber()
detectorNum = 0  # no detectors, this set doesn't have detector affinity
detectorDepth = workerNum  # flat set like step1b workers, so depth is workerNum

sleepDuration = float(os.getenv("BUTLER_ROLLOUT_PAUSE", 0)) * workerNum
log.info(
    f"Sleeping worker {workerNum} (det-{detectorNum}-{detectorDepth}) for {sleepDuration}s ease postgres load"
)
sleep(sleepDuration)
log.info(f"Worker {workerNum} (det-{detectorNum}-{detectorDepth}) starting up...")

locationConfig = getAutomaticLocationConfig()
podDetails = PodDetails(
    instrument=instrument, podFlavor=PodFlavor.BACKLOG_WORKER, detectorNumber=None, depth=detectorDepth
)
log.info(
    f"Running {podDetails.instrument} {podDetails.podFlavor.name} at {locationConfig.location},"
    f"consuming from {podDetails.queueName}..."
)

locationConfig = getAutomaticLocationConfig()
butler = Butler.from_config(
    locationConfig.lsstCamButlerPath,
    collections=[
        f"{instrument}/defaults",
        locationConfig.getOutputChain(instrument),
    ],
    writeable=True,
)

runner = SingleCorePipelineRunner(
    butler=butler,
    locationConfig=locationConfig,
    instrument=instrument,
    step="step1a",
    awaitsDataProduct="raw",
    podDetails=podDetails,
    doRaise=getDoRaise(),
)
runner.run()
