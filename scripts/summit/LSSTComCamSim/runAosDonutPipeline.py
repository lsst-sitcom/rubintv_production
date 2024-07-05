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

instrument = "LSSTComCamSim"


def main():
    locationConfig = getAutomaticLocationConfig()
    butler = dafButler.Butler(
        locationConfig.comCamButlerPath,
        collections=[
            "LSSTComCamSim/defaults",
        ],
        instrument=instrument,
        writeable=True,
    )
    print(f"Running donut launcher at {locationConfig.location}")

    inputCollection = "LSSTComCamSim/defaults"
    outputCollection = "u/saluser/ra_wep_testing3"
    pipelineFile = "/project/rubintv/temp/donut_pipeline.yaml"
    queueName = "LSSTComCamSim-FROM-OCS_DONUTPAIR"
    donutLauncher = DonutLauncher(
        butler=butler,
        locationConfig=locationConfig,
        inputCollection=inputCollection,
        outputCollection=outputCollection,
        pipelineFile=pipelineFile,
        queueName=queueName,
        metadataShardPath=locationConfig.comCamSimAosMetadataShardPath,
    )
    donutLauncher.run()


if __name__ == '__main__':
    main()
