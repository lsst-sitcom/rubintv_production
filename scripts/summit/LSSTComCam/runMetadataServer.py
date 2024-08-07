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

import sys

from lsst.rubintv.production.metadataServers import TimedMetadataServer
from lsst.rubintv.production.utils import LocationConfig, checkRubinTvExternalPackages, getDoRaise
from lsst.summit.utils.utils import setupLogging

setupLogging()
checkRubinTvExternalPackages()

location = "summit" if len(sys.argv) < 2 else sys.argv[1]
locationConfig = LocationConfig(location)
print(f"Running ComCam metadata server at {location}...")

metadataDirectory = locationConfig.comCamMetadataPath
shardsDirectory = locationConfig.comCamMetadataShardPath
channelName = "comcam_metadata"

ts8MetadataServer = TimedMetadataServer(
    locationConfig=locationConfig,
    metadataDirectory=metadataDirectory,
    shardsDirectory=shardsDirectory,
    channelName=channelName,
    doRaise=getDoRaise(),
)
ts8MetadataServer.run()
