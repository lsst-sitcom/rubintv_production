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
from lsst.rubintv.production.oneOffProcessing import OneOffProcessor
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.utils import getAutomaticLocationConfig, getDoRaise
from lsst.summit.utils.utils import setupLogging

setupLogging()
instrument = "LSSTCam"

workerNum = 0

locationConfig = getAutomaticLocationConfig()

# The detectorNumber to be processed is defined in the init of the
# OneOffProcessor class below. However, because this is a one-per-instrument
# pod type, the detector number defined in the podDetails is therefore None,
# despite the fact that this will actually operate on a specific detector.
podDetails = PodDetails(
    instrument=instrument, podFlavor=PodFlavor.ONE_OFF_POSTISR_WORKER, detectorNumber=None, depth=workerNum
)
print(
    f"Running {podDetails.instrument} {podDetails.podFlavor.name} at {locationConfig.location},"
    f"consuming from {podDetails.queueName}..."
)

butler = Butler.from_config(
    locationConfig.lsstCamButlerPath,
    instrument=instrument,
    collections=[
        f"{instrument}/defaults",
        locationConfig.getOutputChain(instrument),
    ],
    writeable=True,
)

metadataDirectory = locationConfig.lsstCamMetadataPath
shardsDirectory = locationConfig.lsstCamMetadataShardPath

oneOffProcessor = OneOffProcessor(
    butler=butler,
    locationConfig=locationConfig,
    instrument=instrument,
    podDetails=podDetails,
    detectorNumber=94,  # this is the fallback value, the dynamic one comes from redis
    shardsDirectory=shardsDirectory,
    processingStage="post_isr_image",
    doRaise=getDoRaise(),
)
oneOffProcessor.run()
