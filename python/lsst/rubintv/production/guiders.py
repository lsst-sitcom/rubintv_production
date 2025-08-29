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

__all__ = [
    "GuiderWorker",
]

import logging
import os
from functools import partial
from time import monotonic, sleep
from typing import TYPE_CHECKING, Callable

from lsst.summit.utils.guiders.metrics import GuiderMetricsBuilder
from lsst.summit.utils.guiders.plotting import GuiderPlotter
from lsst.summit.utils.guiders.reading import GuiderReader
from lsst.summit.utils.guiders.tracking import GuiderStarTracker
from lsst.summit.utils.utils import getCameraFromInstrumentName

from .baseChannels import BaseButlerChannel
from .utils import (
    LocationConfig,
    getRubinTvInstrumentName,
    logDuration,
    makePlotFile,
    writeExpRecordMetadataShard,
    writeMetadataShard,
)

if TYPE_CHECKING:
    from pandas import DataFrame

    from lsst.daf.butler import Butler, DimensionRecord

    from .payloads import Payload
    from .podDefinition import PodDetails


KEY_MAP: dict[str, str] = {
    "n_stars": "Number of tracked stars",
    "n_measurements": "Number of tracked stars measurements",
    "fraction_possible_measurements": "Possible measurement fraction",
    "exptime": "Guider exposure time",
    "az_drift_slope": "Az drift (arcsec total)",
    "az_slope_significance": "Az drift significance (sigma)",
    "az_drift_trend_rmse": "Az RMS (detrended)",
    "az_drift_global_std": "Az drift standard deviation",
    "alt_drift_slope": "Alt drift (arcsec total)",
    "alt_slope_significance": "Alt drift significance (sigma)",
    "alt_drift_trend_rmse": "Alt RMS (detrended)",
    "alt_drift_global_std": "Alt drift standard deviation",
    "rotator_slope": "Rotator drift (arcsec total)",
    "rotator_slope_significance": "Rotator drift significance (sigma)",
    "rotator_trend_rmse": "Rotator RMS (detrended)",
    "rotator_global_std": "Rotator drift standard deviation",
    "mag_slope": "Magnitude drift per exposure",
    "mag_slope_significance": "Significance of the magnitude drift (sigma)",
    "mag_trend_rmse": "Magnitude RMS (detrended)",
    "mag_global_std": "Magnitude standard deviation",
    "psf_intercept": "PSF FWHM at zero stamp index",
    "psf_slope": "PSF FWHM drift per exposure",
    "psf_slope_significance": "Significance of the PSF drift (sigma)",
    "psf_trend_rmse": "PSF RMS (detrended)",
    "psf_global_std": "PSF standard deviation",
}


def waitForIngest(nExpected: int, timeout: float, expRecord: DimensionRecord, butler: Butler) -> int:
    """
    Wait for the expected number of guider_raw datasets to be ingested for the
    given record.

    TODO: replace this function by using the CachingLimitedButler on the
    GuiderWorker once that has been upgraded to support dimensions, so that it
    can cache all 8 guider raws at once.

    Parameters
    ----------
    nExpected : `int`
        Expected number of datasets to be present.
    timeout : `float`
        Maximum time to wait in seconds.
    expRecord : `DimensionRecord`
        The exposure or visit record whose dataId is used to query the
        datasets.
    butler : `Butler`
        The Butler instance to query.

    """
    cadence = 0.25
    startTime = monotonic()

    while True:
        nIngested = len(butler.query_datasets("guider_raw", data_id=expRecord.dataId, explain=False))
        if nIngested >= nExpected:
            return nIngested

        if monotonic() - startTime >= timeout:
            log = logging.getLogger("lsst.rubintv.production.guiders")
            log.warning(
                f"Timed out waiting for ingest of {nExpected} guider_raws (got {nIngested}) for "
                f"dataId={expRecord.dataId} after {timeout:.1f}s"
            )
            return nIngested

        sleep(cadence)


class GuiderWorker(BaseButlerChannel):
    def __init__(
        self,
        locationConfig: LocationConfig,
        butler: Butler,
        instrument: str,
        podDetails: PodDetails,
        *,
        doRaise=False,
    ) -> None:
        super().__init__(
            locationConfig=locationConfig,
            butler=butler,
            # TODO: DM-43764 this shouldn't be necessary on the
            # base class after this ticket, I think.
            detectors=None,  # unused
            dataProduct=None,  # unused
            # TODO: DM-43764 should also be able to fix needing
            # channelName when tidying up the base class. Needed
            # in some contexts but not all. Maybe default it to
            # ''?
            channelName="",  # unused
            podDetails=podDetails,
            doRaise=doRaise,
            addUploader=True,
        )
        assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
        assert self.podDetails is not None  # XXX why is this necessary? Fix mypy better!
        self.log.info(f"Guider worker running, consuming from {self.podDetails.queueName}")
        self.shardsDirectory = locationConfig.guiderShardsDirectory
        self.instrument = instrument  # why isn't this being set in the base class?!
        self.reader = GuiderReader(self.butler, view="dvcs")
        camera = getCameraFromInstrumentName(self.instrument)
        self.detectorNames: tuple[str, ...] = (
            "R00_SG0",
            "R00_SG1",
            "R04_SG0",
            "R04_SG1",
            "R40_SG0",
            "R40_SG1",
            "R44_SG0",
            "R44_SG1",
        )
        self.detectorIds: tuple[int, ...] = tuple([camera[d].getId() for d in self.detectorNames])

    def getRubinTvTableEntries(self, metrics: DataFrame) -> dict[str, str]:
        """Map the metrics to the RubinTV table entry names.

        Parameters
        ----------
        metrics : `pandas.DataFrame`
            DataFrame containing the metrics.

        Returns
        -------
        rubinTVtableItems : `dict` [`str`, `str`]
            Dictionary mapping the RubinTV table entry names to their values.
        """
        rubinTVtableItems: dict[str, str] = {}
        for key, value in KEY_MAP.items():
            if key in metrics:
                rubinTVtableItems[value] = f"{metrics[key].values[0]}"
            else:
                self.log.warning(f"Key {key} not found in metrics DataFrame columns")

        return rubinTVtableItems

    def makeAnimations(self, plotter: GuiderPlotter, dayObs: int, seqNum: int, uploadPlot: Callable) -> None:
        with logDuration(self.log, "Making the full frame movie"):
            plotName = "full_movie"
            plotFile = makePlotFile(self.locationConfig, self.instrument, dayObs, seqNum, plotName, "mp4")
            plotter.makeAnimation(cutoutSize=-1, saveAs=plotFile, plo=70, phi=99)
            if os.path.exists(plotFile):
                uploadPlot(plotName=plotName, filename=plotFile)

        with logDuration(self.log, "Making the star cutout movie"):
            plotName = "star_movie"
            plotFile = makePlotFile(self.locationConfig, self.instrument, dayObs, seqNum, plotName, "mp4")
            plotter.makeAnimation(cutoutSize=20, saveAs=plotFile, plo=50, phi=98, fps=10)
            if os.path.exists(plotFile):
                uploadPlot(plotName=plotName, filename=plotFile)

    def makeStripPlots(self, plotter: GuiderPlotter, dayObs: int, seqNum: int, uploadPlot: Callable) -> None:
        with logDuration(self.log, "Making the centroid alt/az plot"):
            plotName = "centroid_alt_az"
            plotFile = makePlotFile(self.locationConfig, self.instrument, dayObs, seqNum, plotName, "jpg")
            plotter.stripPlot(saveAs=plotFile)
            if os.path.exists(plotFile):
                uploadPlot(plotName=plotName, filename=plotFile)

        with logDuration(self.log, "Making the flux strip plot"):
            plotName = "flux_trend"
            plotFile = makePlotFile(self.locationConfig, self.instrument, dayObs, seqNum, plotName, "jpg")
            plotter.stripPlot(plotType="flux", saveAs=plotFile)
            if os.path.exists(plotFile):
                uploadPlot(plotName=plotName, filename=plotFile)

        with logDuration(self.log, "Making the psf strip plot"):
            plotName = "psf_trend"
            plotFile = makePlotFile(self.locationConfig, self.instrument, dayObs, seqNum, plotName, "jpg")
            plotter.stripPlot(plotType="psf", saveAs=plotFile)
            if os.path.exists(plotFile):
                uploadPlot(plotName=plotName, filename=plotFile)

    def callback(self, payload: Payload) -> None:
        """Callback function to be called when a new exposure is available."""
        dataId = payload.dataIds[0]
        record: DimensionRecord | None = None
        if "exposure" in dataId.dimensions:
            record = dataId.records["exposure"]
            assert record is not None, f"Failed to find exposure record in exposure-dimension dataId {dataId}"
            if not record.can_see_sky:  # can_see_sky only on exposure records, all visits should be on-sky
                self.log.info(f"Skipping {dataId=} as it's not on sky")
                return

        elif "visit" in dataId.dimensions:
            record = dataId.records["visit"]

        assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
        assert record is not None, f"Failed to find exposure or visit record in {dataId=}"

        dayObs: int = record.day_obs
        seqNum: int = record.seq_num

        nIngested = waitForIngest(len(self.detectorIds), 30, record, self.butler)
        if nIngested == 0:
            self.log.warning(f"No guider raws ingested for {dataId=}, skipping")
            return

        # Write the expRecord metadata shard after waiting for ingest and
        # confirming a non-zero number of guiders images were taken.
        writeExpRecordMetadataShard(record, self.shardsDirectory)

        uploadPlot = partial(
            self.s3Uploader.uploadPerSeqNumPlot,
            instrument=getRubinTvInstrumentName(self.instrument) + "_guider",
            dayObs=dayObs,
            seqNum=seqNum,
        )

        self.log.info(f"Processing guider data for {dayObs=} {seqNum=}")

        with logDuration(self.log, "Loading guider data"):
            guiderData = self.reader.get(dayObs=record.day_obs, seqNum=record.seq_num)

        with logDuration(self.log, "Creating star catalog from guider data"):
            starTracker = GuiderStarTracker(guiderData)
            stars = starTracker.trackGuiderStars()

        plotter = GuiderPlotter(guiderData, stars)

        self.makeAnimations(plotter, dayObs, seqNum, uploadPlot)

        if not stars.empty:
            self.makeStripPlots(plotter, dayObs, seqNum, uploadPlot)
        else:
            self.log.warning("No stars were tracked, skipping strip plots")

        rubinTVtableItems: dict[str, str | dict[str, str]] = {}
        rubinTVtableItems["Exposure time"] = record.exposure_time
        rubinTVtableItems["Image type"] = record.observation_type
        rubinTVtableItems["Target"] = record.target_name

        metricsBuilder = GuiderMetricsBuilder(stars, guiderData.nMissingStamps)
        metrics = metricsBuilder.buildMetrics(guiderData.expid)
        rubinTVtableItems.update(self.getRubinTvTableEntries(metrics))

        md = {record.seq_num: rubinTVtableItems}
        writeMetadataShard(self.shardsDirectory, record.day_obs, md)
