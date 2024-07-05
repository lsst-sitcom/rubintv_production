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
from lsst.rubintv.production.slac.newPlotting import Plotter
from lsst.rubintv.production.utils import LocationConfig, getDoRaise
from lsst.summit.utils.utils import setupLogging


def main(location: str = "summit"):
    setupLogging()

    queueName = "MOSAIC-WORKER-00"
    print(f"Running LSSTComCamSim plotter at {location}, consuming from {queueName}")

    locationConfig = LocationConfig(location)
    butler = dafButler.Butler(locationConfig.comCamButlerPath, collections=["LSSTComCamSim/raw/all"])
    plotter = Plotter(
        butler=butler,
        locationConfig=locationConfig,
        instrument="LSSTComCamSim",
        queueName=queueName,
        doRaise=getDoRaise(),
    )

    plotter.run()


if __name__ == '__main__':
    location = "summit" if len(sys.argv) < 2 else sys.argv[1]
    main(location)
