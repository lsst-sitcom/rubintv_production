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
from lsst.daf.butler import Butler
from astropy.time import Time
import numpy as np
import datetime
import asyncio

from lsst.summit.utils.utils import setupLogging
from lsst.rubintv.production.utils import getAutomaticLocationConfig
from lsst.summit.utils.efdUtils import makeEfdClient

setupLogging()

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def seqnum_from_dataId(dataId):
    if 'exposure' in dataId:
        return dataId['exposure'] % 10000
    else:
        return dataId['visit'] % 10000


async def find_missing_visits(butler, client, dataset_type="post_isr_image", observation_types=None):

    efd_res = await client.influx_client.query(
        """SELECT imageController, imageDate, imageNumber, timestampEndOfReadout """
        """FROM "lsst.sal.MTCamera.logevent_endReadout" """
        """WHERE time > (now() - 15m)  AND imageController = 'O' """
        """order by time desc """)

    if len(efd_res) == 0:
        return 0, [], []

    day_obs = efd_res['imageDate'].iloc[0]
    first_seqnum = np.min(efd_res['imageNumber'])
    butler_datasets = butler.query_datasets(dataset_type,
                                            where=f"day_obs = {day_obs} AND seq_num >= {first_seqnum} "
                                            "AND instrument='LSSTCam' and detector=94", explain=False)

    butler_seqnums = [seqnum_from_dataId(ref.dataId) for ref in butler_datasets]

    ok_missing_seqnums = set()

    # If we are watching post_isr_image, we only care if acq or
    # science images are missing, since cwfs are not expected to be processed.
    if observation_types and len(efd_res) > 0:
        exposure_list = ",".join(f"{day_obs}{seq_num:05d}" for seq_num in set(efd_res['imageNumber']))
        where_clause = (f"instrument='LSSTCam' AND exposure in ({exposure_list}) "
                        f"AND NOT exposure.observation_type in ({observation_types})")
        ok_exp_records = butler.query_dimension_records("exposure", where=where_clause, explain=False)
        ok_missing_seqnums = set(rec.seq_num for rec in ok_exp_records)
        if len(ok_missing_seqnums) > 0:
            log.debug(f"Ok to miss seqnums {ok_missing_seqnums}")

    missing_seqnums = set(efd_res['imageNumber']) - ok_missing_seqnums - set(butler_seqnums)
    missing_visits = [int(f"{day_obs}{seq_num:05d}") for seq_num in missing_seqnums]

    missing_times = []
    for missing_seqnum in missing_seqnums:
        match = efd_res[efd_res['imageNumber'] == missing_seqnum]
        time_delta = (Time(datetime.datetime.now(datetime.UTC), scale="utc")
                      - Time(match['timestampEndOfReadout'].iloc[0], format="unix_tai"))
        missing_times.append(float(time_delta.to_value('s')))

    return day_obs, missing_visits, missing_times


async def main(butler, dataset_configs):

    # This needs to be inside the async event loop or else it throws an error.
    efd_client = makeEfdClient()

    seen_visits = {dataset: set() for dataset, _ in dataset_configs}
    current_dayobs = "0"
    while True:
        for dataset, threshold_seconds in dataset_configs:

            # For any dataset types other than raws, we only care
            # about acq and science images.
            observation_types = "" if dataset == "raw" else "'acq','science'"
            day_obs, alerted_visits, times = await find_missing_visits(butler, efd_client,
                                                                       dataset_type=dataset,
                                                                       observation_types=observation_types)
            if int(day_obs) > 0 and day_obs != current_dayobs:
                log.info(f"Rolling over to day_obs {day_obs}")
                for key in seen_visits.keys():
                    seen_visits[key] = set()
                current_dayobs = day_obs

            for visit, time_delta in zip(alerted_visits, times):
                if visit in seen_visits[dataset]:
                    continue

                if time_delta > threshold_seconds:
                    log.warning(f"visit {visit} missing {dataset} dataset for {time_delta:.0f} seconds")
                    seen_visits[dataset].add(visit)

        # TODO: Configure with env var.
        await asyncio.sleep(120)

if __name__ == '__main__':

    log.info("Starting runTransferAlarm")

    locationConfig = getAutomaticLocationConfig()
    repo = locationConfig.lsstCamButlerPath
    butler = Butler.from_config(
        repo, collections=["LSSTCam/runs/nightlyValidation", "LSSTCam/defaults"], instrument="LSSTCam"
    )

    # TODO: load these from an env var.
    dataset_configs = [("raw", 120), ("post_isr_image", 240)]

    for dataset, threshold_seconds in dataset_configs:
        log.info(f"Dataset {dataset} configured with timeout {threshold_seconds} seconds")

    asyncio.run(main(butler, dataset_configs))
