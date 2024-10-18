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
from lsst.rubintv.production.aos import DonutLauncher
from lsst.rubintv.production.utils import getAutomaticLocationConfig
from lsst.summit.utils.utils import setupLogging

setupLogging()
instrument = "LSSTComCamSim"

locationConfig = getAutomaticLocationConfig()
if locationConfig.location not in ["summit", "tts", "bts"]:
    msg = (
        "This script is only intended to be run on summit-like locations -"
        " the signals from OCS for visit pairs go straight to the redis database and aren't"
        " accessible at USDF or elsewhere"
    )
    raise RuntimeError(msg)

butler = dafButler.Butler(
    locationConfig.comCamButlerPath,
    collections=[
        f"{instrument}/defaults",
    ],
    instrument=instrument,
    writeable=True,
)
print(f"Running donut launcher at {locationConfig.location}")

inputCollection = f"{instrument}/defaults"
outputCollection = "u/saluser/ra_wep_testing3"
queueName = f"{instrument}-FROM-OCS_DONUTPAIR"
donutLauncher = DonutLauncher(  # XXX still needs type annotations and to move to using podDetails
    butler=butler,
    locationConfig=locationConfig,
    inputCollection=inputCollection,
    outputCollection=outputCollection,
    instrument=instrument,
    queueName=queueName,
    metadataShardPath=locationConfig.comCamSimAosMetadataShardPath,
)
donutLauncher.run()
