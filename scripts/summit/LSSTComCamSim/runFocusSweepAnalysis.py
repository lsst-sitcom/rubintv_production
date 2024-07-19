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

import lsst.daf.butler as dafButler
from lsst.rubintv.production.aos import FocusSweepAnalysis
from lsst.rubintv.production.utils import getAutomaticLocationConfig
from lsst.summit.utils.utils import setupLogging


instrument = "LSSTComCamSim"
_log = logging.getLogger()


class SignalHandler:
    def __init__(self, core_runner: FocusSweepAnalysis):
        self._core_runner = core_runner

    def handler(self, signum, frame):
        _log.info(f"Received signal {signum}")
        if signum == signal.SIGTERM:
            _log.info("Stopping core runner")
            self._core_runner.stop()


def main():
    setupLogging()

    locationConfig = getAutomaticLocationConfig()
    butler = dafButler.Butler(
        locationConfig.comCamButlerPath,
        collections=[
            "LSSTComCamSim/defaults",
            "LSSTComCamSim/quickLook",
        ],
    )
    print(f"Running focus sweep plotter at {locationConfig.location}")

    queueName = "LSSTComCamSim-FROM-OCS_FOCUSSWEEP"
    focusSweepAnalyzer = FocusSweepAnalysis(
        butler=butler,
        locationConfig=locationConfig,
        queueName=queueName,
        metadataShardPath=locationConfig.comCamSimAosMetadataShardPath,
    )
    handler_instance = SignalHandler(focusSweepAnalyzer)
    signal.signal(signal.SIGTERM, handler_instance.handler)
    focusSweepAnalyzer.run()


if __name__ == '__main__':
    main()
