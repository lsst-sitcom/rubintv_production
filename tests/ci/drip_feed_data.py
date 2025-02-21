# import sys
import time

t0 = time.time()

import lsst.daf.butler as dafButler  # noqa: E402
from lsst.rubintv.production.redisUtils import RedisHelper  # noqa: E402
from lsst.rubintv.production.utils import getAutomaticLocationConfig  # noqa: E402

print(f"Imports took {(time.time() - t0):.2f} seconds")
t0 = time.time()


instrument = "LSSTComCam"

locationConfig = getAutomaticLocationConfig()
butler = dafButler.Butler(
    locationConfig.comCamButlerPath,
    collections=[
        "LSSTComCamSim/defaults",
    ],
)

redisHelper = RedisHelper(butler, locationConfig)

where = (
    "exposure.day_obs=20241102 AND exposure.seq_num in (170..172)"
    f" AND instrument='{instrument}'"  # on sky!
)
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
assert len(records) == 3, f"Expected 3 record, got {len(records)}"

t1 = time.time()
print(f"Butler init and query took {(time.time() - t0):.2f} seconds")

for record in records:  # XXX remove the slice!
    assert isinstance(record, dafButler.DimensionRecord)
    print(f"Pushing expId={record.id} for {record.instrument} for processing")
    # this is what the butlerWatcher does for each new record
    redisHelper.pushNewExposureToHeadNode(record)
    redisHelper.pushToButlerWatcherList(instrument, record)

print("Pushing pair announcement signal to redis (simulating OCS signal)")
redisHelper.redis.rpush("LSSTComCam-FROM-OCS_DONUTPAIR", "2024110200171,2024110200172")
