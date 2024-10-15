# import sys
import time

t0 = time.time()

import lsst.daf.butler as dafButler  # noqa: E402
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor  # noqa: E402
from lsst.rubintv.production.redisUtils import RedisHelper  # noqa: E402
from lsst.rubintv.production.utils import getAutomaticLocationConfig  # noqa: E402

print(f"Imports took {(time.time()-t0):.2f} seconds")
t0 = time.time()


instrument = "LSSTComCamSim"
WORKER_ONLINE_TIMEOUT = 60
HEAD_NODE_ONLINE_TIMEOUT = 60
HEAD_NODE_POST_INIT_DELAY = 30

locationConfig = getAutomaticLocationConfig()
butler = dafButler.Butler(
    locationConfig.comCamButlerPath,
    instrument=instrument,
    collections=[
        "LSSTComCamSim/raw/all",
    ],
)

redisHelper = RedisHelper(butler, locationConfig)

where = f"exposure.day_obs=20240724 AND exposure.seq_num IN (2..3) AND instrument='{instrument}'"  # OR4
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
records = sorted(records, key=lambda x: (x.day_obs, x.seq_num))
assert len(records) == 2, f"Expected 2 record, got {len(records)}"

t1 = time.time()
print(f"Butler init and query took {(time.time()-t0):.2f} seconds")

t0 = time.time()
nWorkersOnline = 0
while nWorkersOnline < 18 and time.time() - t0 < WORKER_ONLINE_TIMEOUT:
    nWorkersOnline = len(redisHelper.getAllWorkers(instrument, podFlavor=PodFlavor.SFM_WORKER))
    time.sleep(1)

if nWorkersOnline < 18:
    print(f"Workers never came online within timeout of {WORKER_ONLINE_TIMEOUT}")
    raise RuntimeError("Workers never came online within timeout")

print(f"Waiting for workers to come online took {(time.time()-t0):.2f} seconds")

headPod = PodDetails(instrument, PodFlavor.HEAD_NODE, None, None)
t0 = time.time()
while not redisHelper.confirmRunning(headPod) and time.time() - t0 < HEAD_NODE_ONLINE_TIMEOUT:
    time.sleep(1)
    print(f"Waiting for {headPod.queueName} to come online...")
if not redisHelper.confirmRunning(headPod):
    raise RuntimeError(f"Head node never came online within timeout of {HEAD_NODE_ONLINE_TIMEOUT}")

print(f"Head node is online, sleeping for {HEAD_NODE_POST_INIT_DELAY}")
time.sleep(HEAD_NODE_POST_INIT_DELAY)

assert isinstance(records[0], dafButler.DimensionRecord)
for record in records:
    print(f"Pushing expId={record.id} for {record.instrument} for processing")
    # this is what the butlerWatcher does for each new record
    redisHelper.pushNewExposureToHeadNode(record)
    redisHelper.pushToButlerWatcherList(instrument, record)
    time.sleep(5)  # exposures NEVER closer than readout time - this fixes and edge case

time.sleep(30)
print("Pushing pair announcement signal to redis (simulating OCS signal)")
redisHelper.redis.rpush("LSSTComCamSim-FROM-OCS_DONUTPAIR", "7024072400001,7024072400003")
