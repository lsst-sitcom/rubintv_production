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

from lsst.rubintv.production.rubinTv import StarTrackerChannel
from lsst.rubintv.production.utils import checkRubinTvExternalPackages, getSiteConfig
from lsst.summit.utils.utils import setupLogging

config = getSiteConfig()
rootDataPath = config.get('starTrackerDataPath')
metadataRoot = config.get('starTrackerMetadataRoot')
outputRoot = config.get('starTrackerOutputRoot')

setupLogging()
checkRubinTvExternalPackages()
print('Running wide star tracker channel...')
starTracker = StarTrackerChannel(rootDataPath=rootDataPath,
                                 metadataRoot=metadataRoot,
                                 outputRoot=outputRoot,
                                 wide=True)
starTracker.run()
