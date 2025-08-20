# import sys
import time

t0 = time.time()

from lsst.daf.butler import Butler, DimensionRecord  # noqa: E402
from lsst.rubintv.production.payloads import Payload  # noqa: E402
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor  # noqa: E402
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

# 86 - FAM CWFS image, goes as a FAM pair, but to the SFM pods
# 87 - FAM CWFS image, goes as a FAM pair, but to the SFM pods
# 88 - in focus, goes to SFM, expect a preliminary_visit_image mosaic etc.
# CWFS goes to AOS pods
# 311 - a bias, to test cpVerify pipelines and mosaicing

where = (
    "exposure.day_obs=20250415 AND exposure.seq_num in (86..88,311)"
    f" AND instrument='{instrument}'"  # on sky!
)
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
assert len(records) == 4, f"Expected 1 record, got {len(records)}"

for record in records:
    assert isinstance(record, DimensionRecord)
    redisHelper.pushNewExposureToHeadNode(record)  # let the SFM go first
    redisHelper.pushToButlerWatcherList(instrument, record)

t1 = time.time()
print(f"Butler init and query took {(time.time() - t0):.2f} seconds")

# assert isinstance(toPush, DimensionRecord)
# print(f"Pushing expId={toPush.id} for {toPush.instrument} for processing")
# # this is what the butlerWatcher does for each new record
# redisHelper.pushNewExposureToHeadNode(toPush)  # let the SFM go first
# redisHelper.pushToButlerWatcherList(instrument, toPush)

time.sleep(2)  # make sure the head node has done the dispatch of the SFM image

print("Pushing pair announcement signal to redis (simulating OCS signal)")
redisHelper.redis.rpush("LSSTCam-FROM-OCS_DONUTPAIR", "2025041500086,2025041500087")

# now send a single trigger manually to the guider pod
where = f"exposure.day_obs=20250629 AND exposure.seq_num=340 AND instrument='{instrument}'"
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
assert len(records) == 1, f"Expected a record for the guiders, got {len(records)}"
podDetails = PodDetails(
    instrument=instrument, podFlavor=PodFlavor.GUIDER_WORKER, detectorNumber=None, depth=0
)
payload = Payload([records[0].dataId], b"", "", who="")
redisHelper.enqueuePayload(payload, podDetails)

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
