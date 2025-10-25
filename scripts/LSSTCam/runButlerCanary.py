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
import time

from lsst.daf.butler import Butler, DataCoordinate
from lsst.rubintv.production.utils import getAutomaticLocationConfig
from lsst.summit.utils.utils import setupLogging

setupLogging()

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

log.info("Starting butlerCanary")

locationConfig = getAutomaticLocationConfig()
repo = locationConfig.lsstCamButlerPath
butler = Butler.from_config(
    repo, collections=["LSSTCam/defaults"], instrument="LSSTCam"
)

while True:

    with butler.query() as query:
        _x = query.expression_factory
        (exposure_dataId,) = query.data_ids(["exposure"]).order_by(_x.exposure.timespan.end.desc).limit(1)

    log.info(f"Using exposure {exposure_dataId}")

    for detector in range(189):
        dataId = DataCoordinate.standardize(exposure_dataId, detector=detector)
        start = time.time()
        raw = butler.get("raw", dataId=dataId)
        end = time.time()
        duration = end - start
        log.info(f"Butler get duration: {duration} from repo {repo}")

        del raw

        time.sleep(60)
