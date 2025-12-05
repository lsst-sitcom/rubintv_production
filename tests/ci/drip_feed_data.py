# import sys
import time

t0 = time.time()

from lsst.daf.butler import Butler, DimensionRecord  # noqa: E402
from lsst.rubintv.production.redisUtils import RedisHelper  # noqa: E402
from lsst.rubintv.production.utils import getAutomaticLocationConfig  # noqa: E402

print(f"Imports took {(time.time() - t0):.2f} seconds")
t0 = time.time()


instrument = "LSSTCam"

locationConfig = getAutomaticLocationConfig()
butler = Butler.from_config(
    locationConfig.lsstCamButlerPath,
    instrument=instrument,
    collections=[
        f"{instrument}/defaults",
    ],
)

redisHelper = RedisHelper(butler, locationConfig)

# 226 - in focus, goes to SFM, expect a preliminary_visit_image mosaic etc.
# 227 - FAM CWFS image, goes as a FAM pair, but to the SFM pods
# 228 - FAM CWFS image, goes as a FAM pair, but to the SFM pods
# CWFS goes to AOS pods
# 437 - a bias, to test cpVerify pipelines and mosaicing

where = (
    "exposure.day_obs=20251115 AND exposure.seq_num in (226..228,436)"
    f" AND instrument='{instrument}'"  # on sky!
)
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
assert len(records) == 4, f"Expected 4 records, got {len(records)}"

for record in records:
    assert isinstance(record, DimensionRecord)
    redisHelper.pushNewExposureToHeadNode(record)  # let the SFM go first
    redisHelper.pushToButlerWatcherList(instrument, record)

t1 = time.time()
print(f"Butler init and query took {(time.time() - t0):.2f} seconds")

time.sleep(2)  # make sure the head node has done the dispatch of the SFM image

print("Pushing pair announcement signal to redis (simulating OCS signal)")
redisHelper.redis.rpush("LSSTCam-FROM-OCS_DONUTPAIR", "2025111500227,2025111500228")

# do LATISS with the same drip-feeder
instrument = "LATISS"
locationConfig = getAutomaticLocationConfig()
butler = Butler.from_config(
    locationConfig.auxtelButlerPath,
    collections=[
        f"{instrument}/defaults",
    ],
)

where = f"exposure.day_obs=20240813 AND exposure.seq_num=632 AND instrument='{instrument}'"  # on sky!
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
assert len(records) == 1, f"Expected 1 LATISS record, got {len(records)}"
redisHelper.pushNewExposureToHeadNode(records[0])
redisHelper.pushToButlerWatcherList(instrument, records[0])
