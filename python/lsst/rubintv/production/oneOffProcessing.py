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
from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

import lsst.daf.butler as dafButler
from lsst.afw.geom import ellipses
from lsst.pipe.tasks.peekExposure import PeekExposureTask, PeekExposureTaskConfig

from .baseChannels import BaseButlerChannel
from .redisUtils import RedisHelper
from .utils import writeMetadataShard

if TYPE_CHECKING:
    from lsst.afw.image import Exposure
    from lsst.daf.butler import Butler, DataCoordinate

    from .payloads import Payload
    from .podDefinition import PodDetails
    from .utils import LocationConfig

__all__ = [
    "OneOffProcessor",
]


class OneOffProcessor(BaseButlerChannel):
    """XXX docs

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    butler : `lsst.daf.butler.Butler`
        The butler to use.
    instrument : `str`
        The instrument name.
    pipeline : `str`
        The path to the pipeline yaml file.
    step : `str`
        The step of the pipeline to run with this worker.
    awaitsDataProduct : `str`
        The data product that this runner needs in order to run. Should be set
        to `"raw"` for step1 runners and `None` for all other ones, as their
        triggering is dealt with by the head node. TODO: See if this can be
        removed entirely, because this inconsistency is a bit weird.
    queueName : `str`
        The queue that the worker should consume from.
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(
        self,
        locationConfig: LocationConfig,
        butler: Butler,
        instrument: str,
        podDetails: PodDetails,
        detectorNumber: int,
        shardsDirectory: str,
        *,
        doRaise=False,
    ):
        super().__init__(
            locationConfig=locationConfig,
            instrument=instrument,
            butler=butler,
            watcherType="redis",
            # TODO: DM-43764 this shouldn't be necessary on the
            # base class after this ticket, I think.
            detectors=None,
            dataProduct="postISRCCD",
            # TODO: DM-43764 should also be able to fix needing
            # channelName when tidying up the base class. Needed
            # in some contexts but not all. Maybe default it to
            # ''?
            channelName="",
            podDetails=podDetails,
            doRaise=doRaise,
            addUploader=False,  # pipeline running pods don't upload directly
        )
        self.instrument: str = instrument
        self.butler: Butler = butler
        self.podDetails: PodDetails = podDetails
        self.detector = detectorNumber
        self.shardsDirectory = shardsDirectory

        peekConfig = PeekExposureTaskConfig()
        self.peekTask = PeekExposureTask(config=peekConfig)

        self.log.info(f"Pipeline running configured to consume from {self.podDetails.queueName}")

        self.redisHelper: RedisHelper = RedisHelper(butler, self.locationConfig)

    def writeFocusZ(self, exp: Exposure, dayObs: int, seqNum: int) -> None:
        expMetadata = exp.getMetadata().toDict()
        focus = expMetadata.get("FOCUSZ", None)
        if focus is not None:
            focus = float(focus)
            md = {seqNum: {"Focus Z": f"{focus:.3f}"}}
        else:
            md = {seqNum: {"Focus Z": "MISSING VALUE!"}}
        writeMetadataShard(self.shardsDirectory, dayObs, md)

    def calcPsfAndWrite(self, exp: Exposure, dayObs: int, seqNum: int) -> None:
        try:
            result = self.peekTask.run(exp)

            shape = result.psfEquatorialShape
            fwhm = np.nan
            if shape is not None:
                SIGMA2FWHM = np.sqrt(8 * np.log(2))
                ellipse = ellipses.SeparableDistortionDeterminantRadius(shape)
                fwhm = SIGMA2FWHM * ellipse.getDeterminantRadius()

            md = {seqNum: {"PSF FWHM (central CCD, robust measure)": f"{fwhm:.3f}"}}
            writeMetadataShard(self.shardsDirectory, dayObs, md)
            self.log.info(f"Wrote measured PSF for {dayObs}-{seqNum} det={exp.detector.getId()}: {fwhm:.3f}")
        except Exception as e:
            self.log.error(f"Failed to calculate PSF for {dayObs}-{seqNum} det={exp.detector.getId()}: {e}")
            if self.doRaise:
                raise
            return

    def callback(self, payload: Payload) -> None:
        dataId: DataCoordinate = payload.dataId

        dataId = dafButler.DataCoordinate.standardize(dataId, detector=self.detector)

        self.log.info(f"Waiting for postISRCCD for {dataId}")
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
        postISR = self._waitForDataProduct(dataId, gettingButler=self.butler, timeout=60)
        if postISR is None:
            self.log.warning(f"Failed to get postISRCCD for {dataId}")
            return

        self.log.info(f"Writing focus Z for {dataId}")
        self.writeFocusZ(postISR, expRecord.day_obs, expRecord.seq_num)

        self.log.info(f"Calculating PSF for {dataId}")
        self.calcPsfAndWrite(postISR, expRecord.day_obs, expRecord.seq_num)
