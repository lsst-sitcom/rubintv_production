# import sys
import time

t0 = time.time()

import lsst.daf.butler as dafButler  # noqa: E402
from lsst.rubintv.production.redisUtils import RedisHelper  # noqa: E402
from lsst.rubintv.production.utils import getAutomaticLocationConfig  # noqa: E402

print(f"Imports took {(time.time()-t0):.2f} seconds")
t0 = time.time()


instrument = "LSSTComCamSim"

locationConfig = getAutomaticLocationConfig()
butler = dafButler.Butler(
    locationConfig.comCamButlerPath,
    collections=[
        "LSSTComCamSim/defaults",
    ],
)

redisHelper = RedisHelper(butler, locationConfig)

where = f"exposure.day_obs=20240613 AND exposure.seq_num=17 AND instrument='{instrument}'"  # OR4
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
assert len(records) == 1, f"Expected 1 record, got {len(records)}"
record = records[0]

t1 = time.time()
print(f"Butler init and query took {(time.time()-t0):.2f} seconds")

assert isinstance(record, dafButler.DimensionRecord)
print(f"Pushing expId={record.id} for {record.instrument} for processing")

# this is what the butlerWatcher does for each new record
redisHelper.pushNewExposureToHeadNode(record)
redisHelper.pushToButlerWatcherList(instrument, record)
