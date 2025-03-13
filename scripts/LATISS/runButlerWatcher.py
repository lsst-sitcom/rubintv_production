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
from lsst.rubintv.production import ButlerWatcher
from lsst.rubintv.production.utils import getAutomaticLocationConfig, getDoRaise, writeDimensionUniverseFile
from lsst.summit.utils.utils import setupLogging

setupLogging()
instrument = "LATISS"
locationConfig = getAutomaticLocationConfig()
print(f"Running {instrument} butler watcher at {locationConfig.location}...")

butler = Butler.from_config(locationConfig.auxtelButlerPath, collections=["LATISS/raw/all"])
writeDimensionUniverseFile(butler, locationConfig)  # all summit repos need to update at the same time!
butlerWatcher = ButlerWatcher(
    butler=butler,
    locationConfig=locationConfig,
    instrument=instrument,
    dataProducts="raw",
    doRaise=getDoRaise(),
)
butlerWatcher.run()
