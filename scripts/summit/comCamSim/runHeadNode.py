
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
import lsst.daf.butler as dafButler
from lsst.rubintv.production.processingControl import HeadProcessController
from lsst.rubintv.production.utils import LocationConfig
from lsst.summit.utils.utils import setupLogging

instrument = 'LSSTComCamSim'

setupLogging()
location = 'summit' if len(sys.argv) < 2 else sys.argv[1]
locationConfig = LocationConfig(location)
print(f'Running {instrument} head node at {location}...')


butler = dafButler.Butler(
    locationConfig.comCamButlerPath,
    instrument=instrument,
    collections=[
        'LSSTComCamSim/defaults',
    ],
    writeable=True  # needed for defineVisits
)

pipelineFile = '$DRP_PIPE_DIR/pipelines/LSSTComCamSim/quickLook-ops-rehearsal-3.yaml'
controller = HeadProcessController(butler, instrument, locationConfig, pipelineFile)
controller.run()