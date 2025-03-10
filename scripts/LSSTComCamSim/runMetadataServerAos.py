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

from lsst.rubintv.production.timedServices import TimedMetadataServer
from lsst.rubintv.production.utils import checkRubinTvExternalPackages, getAutomaticLocationConfig, getDoRaise
from lsst.summit.utils.utils import setupLogging

setupLogging()
checkRubinTvExternalPackages()

locationConfig = getAutomaticLocationConfig()
print(f"Running ComCamSim AOS metadata server at {locationConfig.location}...")

metadataDirectory = locationConfig.comCamSimAosMetadataPath
shardsDirectory = locationConfig.comCamSimAosMetadataShardPath
channelName = "comcam_sim_aos_metadata"

metadataServer = TimedMetadataServer(
    locationConfig=locationConfig,
    metadataDirectory=metadataDirectory,
    shardsDirectory=shardsDirectory,
    channelName=channelName,
    doRaise=getDoRaise(),
)
metadataServer.run()
