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
from lsst.daf.butler import DataCoordinate
import time
import itertools
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                   format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("butlerCanary")

log.info("Starting butlerCanary")

b = dafButler.Butler("embargo", collections="LSSTCam/defaults")

while True:

    query = b.registry.queryDimensionRecords("exposure", datasets=["raw"])
    records = list(query.order_by("-exposure.timespan.end").limit(1))
    exposure_dataId = records[0].dataId
    log.info(f"Using exposure {exposure_dataId}")

    for detector in range(189):
        dataId = DataCoordinate.standardize(exposure_dataId, detector=detector)
        start = time.time()
        raw = b.get("raw", dataId=dataId)
        end = time.time()
        duration = end - start
        log.info(f"Butler get duration: {duration}")

        del raw

        time.sleep(60)

