# This file is part of rubintv_production.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__all__ = [
    "CCD_VISIT_MAPPING",
    "VISIT_MIN_MED_MAX_MAPPING",
    "VISIT_MIN_MED_MAX_TOTAL_MAPPING",
    "ConsDBPopulator",
]

import itertools
from typing import Callable, cast

import numpy as np
from requests import HTTPError

from lsst.afw.image import ExposureSummaryStats  # type: ignore
from lsst.afw.table import ExposureCatalog  # type: ignore
from lsst.daf.butler import Butler, DimensionRecord
from lsst.obs.lsst import Latiss, LsstCam, LsstComCam  # type: ignore
from lsst.summit.utils import ConsDbClient
from lsst.summit.utils.utils import computeCcdExposureId

CCD_VISIT_MAPPING = {
    "astromOffsetMean": "astrom_offset_mean",
    "astromOffsetStd": "astrom_offset_std",
    "maxDistToNearestPsf": "max_dist_to_nearest_psf",
    "meanVar": "mean_var",
    "nPsfStar": "n_psf_star",
    "psfSigma": "psf_sigma",
    "psfStarDeltaE1Median": "psf_star_delta_e1_median",
    "psfStarDeltaE1Scatter": "psf_star_delta_e1_scatter",
    "psfStarDeltaE2Median": "psf_star_delta_e2_median",
    "psfStarDeltaE2Scatter": "psf_star_delta_e2_scatter",
    "psfStarDeltaSizeMedian": "psf_star_delta_size_median",
    "psfStarDeltaSizeScatter": "psf_star_delta_size_scatter",
    "psfStarScaledDeltaSizeScatter": "psf_star_scaled_delta_size_scatter",
    "psfTraceRadiusDelta": "psf_trace_radius_delta",
    "skyBg": "sky_bg",
    "skyNoise": "sky_noise",
    "zenithDistance": "zenith_distance",
    "zeroPoint": "zero_point",
}


VISIT_MIN_MED_MAX_MAPPING = {
    "effTime": "eff_time",
    "effTimePsfSigmaScale": "eff_time_psf_sigma_scale",
    "effTimeSkyBgScale": "eff_time_sky_bg_scale",
    "effTimeZeroPointScale": "eff_time_zero_point_scale",
    "astromOffsetMean": "astrom_offset_mean",
    "astromOffsetStd": "astrom_offset_std",
    "maxDistToNearestPsf": "max_dist_to_nearest_psf",
    "meanVar": "mean_var",
    "psfArea": "psf_area",
    "psfIxx": "psf_ixx",
    "psfIyy": "psf_iyy",
    "psfIxy": "psf_ixy",
    "psfSigma": "psf_sigma",
    "psfStarDeltaE1Median": "psf_star_delta_e1_median",
    "psfStarDeltaE2Median": "psf_star_delta_e2_median",
    "psfStarDeltaE1Scatter": "psf_star_delta_e1_scatter",
    "psfStarDeltaE2Scatter": "psf_star_delta_e2_scatter",
    "psfStarDeltaSizeMedian": "psf_star_delta_size_median",
    "psfStarDeltaSizeScatter": "psf_star_delta_size_scatter",
    "psfTraceRadiusDelta": "psf_trace_radius_delta",
    "psfStarScaledDeltaSizeScatter": "psf_star_scaled_delta_size_scatter",
    "skyNoise": "sky_noise",
    "skyBg": "sky_bg",
    "zeroPoint": "zero_point",
}


VISIT_MIN_MED_MAX_TOTAL_MAPPING = {
    "nPsfStar": "n_psf_star",
}


def _getDetectorIds(instrumentName: str) -> list[int]:
    _instrument = instrumentName.lower()

    match _instrument:
        case "lsstcam":
            camera = LsstCam.getCamera()
        case instrument if instrument in ("lsstcomcam", "lsstcomcamsim"):
            camera = LsstComCam.getCamera()
        case "latiss":
            camera = Latiss.getCamera()
        case _:
            raise ValueError(f"Unsupported instrument: {instrumentName}")
    return [detector.getId() for detector in camera]


class ConsDBPopulator:
    def __init__(self, client: ConsDbClient) -> None:
        self.client = client

    def _createExposureRow(self, expRecord: DimensionRecord, allowUpdate: bool = False) -> None:
        """Create a row for the exp in the cdb_<instrument>.exposure table.

        This is expected to always be populated by observatory systems, and is
        therefore not a user-facing method.
        """
        exposureValues: dict[str, str | int] = {
            "exposure_id": expRecord.id,
            "exposure_name": expRecord.obs_id,
            "controller": expRecord.obs_id.split("_")[1],
            "day_obs": expRecord.day_obs,
            "seq_num": expRecord.seq_num,
        }

        try:
            self.client.insert(
                instrument=expRecord.instrument,
                table=f"cdb_{expRecord.instrument.lower()}.exposure",
                obs_id=expRecord.id,
                values=exposureValues,
                allow_update=allowUpdate,
            )
        except HTTPError as e:
            print(e.response.json())
            raise RuntimeError from e

    def _createCcdExposureRows(
        self, expRecord: DimensionRecord, detectorNum: int | None = None, allowUpdate: bool = False
    ) -> None:
        """Create rows in all the relevant ccdexposure tables for the exp.

        This is expected to always be populated by observatory systems, and is
        therefore not a user-facing method.

        Parameters
        ----------
        expRecord : `DimensionRecord`
            The exposure record to populate the rows for.
        detectorNum : `int`, optional
            The detector number to populate the rows for. If ``None``, all
            detectors for the instrument are populated.
        allowUpdate : `bool`, optional
            Allow updating existing rows in the tables. Default is ``False``
        """
        if detectorNum is None:
            detectorNums = _getDetectorIds(expRecord.instrument)
        else:
            detectorNums = [detectorNum]

        for detNum in detectorNums:
            obsId = computeCcdExposureId(expRecord.instrument, expRecord.id, detNum)
            try:
                self.client.insert(
                    instrument=expRecord.instrument,
                    table=f"cdb_{expRecord.instrument.lower()}.ccdexposure",
                    obs_id=obsId,
                    values={"detector": detNum, "exposure_id": expRecord.id},
                    allow_update=allowUpdate,
                )
            except HTTPError as e:
                print(e.response.json())
                raise RuntimeError from e

    def populateCcdVisitRowWithButler(
        self,
        butler: Butler,
        expRecord: DimensionRecord,
        detectorNum: int,
        allowUpdate: bool = False,
    ):
        summaryStats = butler.get("calexp.summaryStats", visit=expRecord.id, detector=detectorNum)
        self.populateCcdVisitRow(expRecord, detectorNum, summaryStats, allowUpdate=allowUpdate)

    def populateCcdVisitRow(
        self,
        expRecord: DimensionRecord,
        detectorNum: int,
        summaryStats: ExposureSummaryStats,
        allowUpdate: bool = False,
    ) -> None:
        obsId = computeCcdExposureId(expRecord.instrument, expRecord.id, detectorNum)
        values = {value: getattr(summaryStats, key) for key, value in CCD_VISIT_MAPPING.items()}

        try:
            self.client.insert(
                instrument=expRecord.instrument,
                table=f"cdb_{expRecord.instrument.lower()}.ccdvisit1_quicklook",
                obs_id=obsId,
                values=values,
                allow_update=allowUpdate,
            )
        except HTTPError as e:
            print(e.response.json())
            raise RuntimeError from e

    def populateAllCcdVisitRowsWithButler(
        self, butler: Butler, expRecord: DimensionRecord, createRows: bool = False, allowUpdate: bool = False
    ) -> None:
        if createRows:
            self._createExposureRow(expRecord, allowUpdate=allowUpdate)
            self._createCcdExposureRows(expRecord, allowUpdate=allowUpdate)
            print(f"Populated tables for exposure and ccdexposure for {expRecord.instrument}+{expRecord.id})")

        detectorNums = _getDetectorIds(expRecord.instrument)
        for detectorNum in detectorNums:
            self.populateCcdVisitRowWithButler(butler, expRecord, detectorNum, allowUpdate=allowUpdate)

    def populateVisitRowWithButler(
        self, butler: Butler, expRecord: DimensionRecord, allowUpdate: bool = False
    ) -> None:
        visitSummary = butler.get("visitSummary", visit=expRecord.id)
        instrument = expRecord.instrument
        self.populateVisitRow(visitSummary, instrument, allowUpdate=allowUpdate)

    def populateVisitRow(
        self, visitSummary: ExposureCatalog, instrument: str, allowUpdate: bool = False
    ) -> None:
        schema = self.client.schema("lsstcomcamsim", "visit1_quicklook")
        schema = cast(dict[str, tuple[str, str]], schema)
        typeMapping: dict[str, str] = {k: v[0] for k, v in schema.items()}

        def changeType(key: str) -> Callable[[int | float], int | float]:
            dbType = typeMapping[key]
            if dbType in ("BIGINT", "INTEGER"):
                return int
            elif dbType == "DOUBLE PRECISION":
                return float
            else:
                raise ValueError(f"Got unknown database type {dbType}")

        visitSummary = visitSummary.asAstropy()
        visits = visitSummary["visit"]
        visit = visits[0]
        assert all(v == visit for v in visits)  # this has to be true, but let's be careful
        visit = int(visit)  # must be python into not np.int64

        values: dict[str, int | float] = {}
        for summaryKey, consDbKeyNoSuffix in itertools.chain(
            VISIT_MIN_MED_MAX_MAPPING.items(),
            VISIT_MIN_MED_MAX_TOTAL_MAPPING.items(),
        ):
            for suffix in ["_min", "_max", "_median"]:
                consDbKey = consDbKeyNoSuffix + suffix
                typeFunc = changeType(consDbKey)
                values[consDbKey] = typeFunc(np.nanmin(visitSummary[summaryKey]))
                values[consDbKey] = typeFunc(np.nanmax(visitSummary[summaryKey]))
                values[consDbKey] = typeFunc(np.nanmedian(visitSummary[summaryKey]))

        for summaryKey, consDbKey in VISIT_MIN_MED_MAX_TOTAL_MAPPING.items():
            typeFunc = changeType(consDbKey + "_total")
            values[consDbKey + "_total"] = typeFunc(np.nansum(visitSummary[summaryKey]))

        nInputs = max([len(visitSummary[col]) for col in visitSummary.columns])
        minInputs = min([len(visitSummary[col]) for col in visitSummary.columns])
        if minInputs != nInputs:
            raise RuntimeError("visitSummary is jagged - this should be impossible")

        values["n_inputs"] = nInputs

        self.client.insert(
            instrument=instrument,
            table=f"cdb_{instrument.lower()}.visit1_quicklook",
            obs_id=visit,
            values=values,
            allow_update=allowUpdate,
        )
