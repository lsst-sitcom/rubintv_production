
import lsst.daf.butler as dafButler
from lsst.daf.butler import DataCoordinate
import logging
import time
import itertools
import sys

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

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

