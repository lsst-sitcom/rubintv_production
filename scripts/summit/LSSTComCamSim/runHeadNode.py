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
import signal
import sys

import lsst.daf.butler as dafButler
from lsst.rubintv.production.processingControl import HeadProcessController
from lsst.rubintv.production.utils import LocationConfig
from lsst.summit.utils.utils import setupLogging

instrument = "LSSTComCamSim"
_log = logging.getLogger()


class SignalHandler:
    def __init__(self, core_runner: HeadProcessController):
        self._core_runner = core_runner

    def handler(self, signum, frame):
        _log.info(f"Received signal {signum}")
        if signum == signal.SIGTERM:
            _log.info("Stopping core runner")
            self._core_runner.stop()


def main(location: str = "summit"):
    setupLogging()
    locationConfig = LocationConfig(location)
    print(f"Running {instrument} head node at {location}...")

    butler = dafButler.Butler(
        locationConfig.comCamButlerPath,
        instrument=instrument,
        collections=[
            "LSSTComCamSim/defaults",
        ],
        writeable=True,  # needed for defineVisits
    )

    controller = HeadProcessController(
        butler=butler,
        instrument=instrument,
        locationConfig=locationConfig,
        pipelineFile=locationConfig.sfmPipelineFile,
    )
    handler_instance = SignalHandler(controller)
    signal.signal(signal.SIGTERM, handler_instance.handler)
    controller.run()


if __name__ == '__main__':
    location = "summit" if len(sys.argv) < 2 else sys.argv[1]
    main(location)
