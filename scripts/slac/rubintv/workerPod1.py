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
from time import sleep

REPO_LOCATION = '/sdf/group/rubin/repo/ir2/butler.yaml'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

log = logging.getLogger('lsst.rubintv.production.frontendWorkerPod1')
log.info('Pod init successful')

while True:
    try:
        import lsst.afw.image as afwImage
        import numpy as np
        data = np.zeros((10, 10), dtype=np.float32)
        img = afwImage.ImageF(data)
        log.info('Create an afwImage - stack import successful')
    except Exception as e:
        log.exception(f'Failed to create an afwImage: {e}')

    try:
        from lsst.daf.butler import Butler
        butler = Butler('/sdf/group/rubin/repo/ir2/butler.yaml',
                        collections=['LSSTCam/raw/all', 'LSSTCam/calib'])
        log.info('Created a full camera butler - repo mount sucessful')
    except Exception as e:
        log.exception(f'Failed to create the butler: {e}')

    try:
        from lsst.daf.butler import DimensionRecord
        where = "exposure.day_obs>=20230101 AND exposure.seq_num=1"
        records = butler.registry.queryDimensionRecords('exposure', where=where, datasets='raw')
        record = list(records)[0]
        assert isinstance(record, DimensionRecord)
        log.info('Successfully accessed an exposure record - butler is really working')
    except Exception as e:
        log.exception(f'Failed to get a dimension record from the butler: {e}')

    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.ping()
        log.info('Successfully spoke to redis!')
    except Exception as e:
        log.exception(f'Failed to connect to redis: {e}')

    sleep(10)  # don't hammer things if we're flailing
