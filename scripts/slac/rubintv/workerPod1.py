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
import os
from time import sleep

REPO_LOCATION = os.getenv("DAF_BUTLER_REPOSITORY_INDEX", "/sdf/group/rubin/repo/ir2/butler.yaml")
REPO_COLLECTIONS = os.getenv("DAF_BUTLER_COLLECTIONS", "LSSTCam/raw/all,LSSTCam/calib").split("/")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")


log = logging.getLogger("lsst.rubintv.production.frontendWorkerPod1")
msg = "Pod init successful"
print(msg)
log.info(msg)

while True:
    try:
        import numpy as np

        import lsst.afw.image as afwImage

        data = np.zeros((10, 10), dtype=np.float32)
        img = afwImage.ImageF(data)
        msg = "Create an afwImage - stack import successful"
        print(msg)
        log.info(msg)
    except Exception as e:
        log.exception(f"Failed to create an afwImage: {e}")

    try:
        from lsst.daf.butler import Butler

        butler = Butler(REPO_LOCATION, collections=REPO_COLLECTIONS)
        msg = "Created a full camera butler - repo mount sucessful"
        print(msg)
        log.info(msg)
    except Exception as e:
        log.exception(f"Failed to create the butler: {e}")

    try:
        import lsst.summit.utils.butlerUtils as butlerUtils

        butler = butlerUtils.makeDefaultLatissButler(embargo=True)
        msg = "Created an embargo LATISS butler - embargo credentials working"
        print(msg)
        log.info(msg)
    except Exception as e:
        log.exception(f"Failed to create LATISS embargo butler: {e}")

    try:
        from lsst.daf.butler import DimensionRecord

        where = "exposure.day_obs>=20230101 AND exposure.seq_num=1"
        records = butler.registry.queryDimensionRecords("exposure", where=where, datasets="raw")
        record = list(records)[0]
        assert isinstance(record, DimensionRecord)
        msg = "Successfully accessed an exposure record - butler is really working"
        print(msg)
        log.info(msg)
    except Exception as e:
        log.exception(f"Failed to get a dimension record from the butler: {e}")

    try:
        import redis

        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
        r.ping()
        msg = "Successfully spoke to redis!"
        print(msg)
        log.info(msg)
    except Exception as e:
        log.exception(f"Failed to connect to redis: {e}")

    sleep(10)  # don't hammer things if we're flailing
