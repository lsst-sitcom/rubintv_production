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

import lsst.daf.butler as dafButler
from lsst.rubintv.production.oneOffProcessing import OneOffProcessor
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.utils import getAutomaticLocationConfig, getDoRaise
from lsst.summit.utils.utils import setupLogging

setupLogging()
instrument = "LSSTComCam"

workerNum = 0

locationConfig = getAutomaticLocationConfig()
# detectorNumber = None becuase this is a per-instrument type pod
# detector number below is the actual CCD which will be processed, which
# should not be controlled by k8s or the head node, and therefore goes there
podDetails = PodDetails(
    instrument=instrument, podFlavor=PodFlavor.ONE_OFF_WORKER, detectorNumber=None, depth=workerNum
)
print(
    f"Running {podDetails.instrument} {podDetails.podFlavor.name} at {locationConfig.location},"
    f"consuming from {podDetails.queueName}..."
)

butler = dafButler.Butler(  # type: ignore
    locationConfig.comCamButlerPath,
    collections=[
        f"{instrument}/defaults",
        f"{instrument}/quickLook",  # accesses the outputs
    ],
    writeable=True,
)

metadataDirectory = locationConfig.comCamMetadataPath
shardsDirectory = locationConfig.comCamMetadataShardPath

oneOffProcessor = OneOffProcessor(
    butler=butler,
    locationConfig=locationConfig,
    instrument=instrument,
    podDetails=podDetails,
    detectorNumber=4,  # central CCD for ComCam
    shardsDirectory=shardsDirectory,
    doRaise=getDoRaise(),
)
oneOffProcessor.run()