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

from __future__ import annotations

__all__ = [
    "CCD_VISIT_MAPPING",
    "VISIT_MIN_MED_MAX_MAPPING",
    "VISIT_MIN_MED_MAX_TOTAL_MAPPING",
    "ConsDBPopulator",
]

import itertools
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Callable, cast

import numpy as np
from requests import HTTPError

from lsst.afw.image import ExposureSummaryStats  # type: ignore
from lsst.afw.table import ExposureCatalog  # type: ignore
from lsst.daf.butler import Butler, DimensionRecord
from lsst.summit.utils import ConsDbClient
from lsst.summit.utils.simonyi.mountAnalysis import MountErrors
from lsst.summit.utils.utils import computeCcdExposureId, getDetectorIds

from .redisUtils import RedisHelper

if TYPE_CHECKING:
    from .utils import LocationConfig

logger = logging.getLogger(__name__)

# The mapping from ExposureSummaryStats columns to consDB columns
CCD_VISIT_MAPPING = {
    "effTime": "eff_time",
    "effTimePsfSigmaScale": "eff_time_psf_sigma_scale",
    "effTimeSkyBgScale": "eff_time_sky_bg_scale",
    "effTimeZeroPointScale": "eff_time_zero_point_scale",
    "magLim": "stats_mag_lim",
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

# The mapping from ExposureCatalog columns to consDB columns where
# min/median/max are calculated
VISIT_MIN_MED_MAX_MAPPING = {
    "effTime": "eff_time",
    "effTimePsfSigmaScale": "eff_time_psf_sigma_scale",
    "effTimeSkyBgScale": "eff_time_sky_bg_scale",
    "effTimeZeroPointScale": "eff_time_zero_point_scale",
    "magLim": "stats_mag_lim",
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

# The mapping from ExposureCatalog columns to consDB columns where
# min/median/max are calculated as well as the total
VISIT_MIN_MED_MAX_TOTAL_MAPPING = {
    "nPsfStar": "n_psf_star",
}


def _removeNans(values: Mapping[str, float | int | str]) -> dict[str, float | int | str]:
    out: dict[str, float | int | str] = {}
    for k, v in values.items():
        if isinstance(v, (float, np.floating)) and np.isnan(v):
            continue
        out[k] = v
    return out


def changeType(key: str, typeMapping: dict[str, str]) -> Callable[[int | float], int | float]:
    dbType = typeMapping[key]
    if dbType in ("BIGINT", "INTEGER"):
        return int
    elif dbType == "DOUBLE PRECISION":
        return float
    else:
        raise ValueError(f"Got unknown database type {dbType}")


class ConsDBPopulator:
    def __init__(
        self, client: ConsDbClient, redisHelper: RedisHelper, locationConfig: LocationConfig
    ) -> None:
        self.client = client
        self.redisHelper = redisHelper
        self.locationConfig = locationConfig

    def _shouldInsert(self) -> bool:
        """Check whether inserts to consDB are allowed at the current location.

        Returns
        -------
        allowed : `bool`
            True if location is one of "summit", "bts", or "tts".
        """
        location = self.locationConfig.location
        if location is None:
            logger.warning("LocationConfig.location is None; skipping consDB insert.")
            return False
        return str(location).lower() in ("summit", "bts", "tts")

    def _insertIfAllowed(
        self,
        instrument: str,
        table: str,
        obsId: int | tuple[int, int],
        values: Mapping[str, int | float | str],
        allowUpdate: bool,
    ) -> bool:
        """
        Conditionally call self.client.insert() based on location.

        Parameters
        ----------
        instrument : `str`
            Instrument name for the consDB schema.
        table : `str`
            Table name within the instrument schema.
        obsId : `int` or `tuple[int, int]`
            The primary key used by consDB for the row (visit/exposure id or
            (day_obs, seq_num)).
        values : `dict[str, int | float | str]`
            Column values to write; NaN values are removed.
        allowUpdate : `bool`
            Whether to allow updates to existing rows.

        Returns
        -------
        inserted : `bool`
            ``True`` if an insert/update was attempted and succeeded; ``False``
            if skipped due to location.
        """
        if not self._shouldInsert():  # called here again for safety
            location = self.locationConfig.location
            logger.info(f"Skipping consDB insert at {location} for {instrument}.{table} for {obsId}")
            return False

        try:
            self.client.insert(
                instrument=instrument,
                table=table,
                obs_id=obsId,
                values=_removeNans(values),
                allow_update=allowUpdate,
            )
            return True
        except HTTPError as e:
            try:
                print(e.response.json())
            except Exception:
                logger.exception("HTTPError during consDB insert and response JSON parse failed.")
            raise RuntimeError from e

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

        self._insertIfAllowed(
            instrument=expRecord.instrument,
            table=f"cdb_{expRecord.instrument.lower()}.exposure",
            obsId=(expRecord.day_obs, expRecord.seq_num),
            values=exposureValues,
            allowUpdate=allowUpdate,
        )

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
            detectorNums = getDetectorIds(expRecord.instrument)
        else:
            detectorNums = [detectorNum]

        for detNum in detectorNums:
            obsId = computeCcdExposureId(expRecord.instrument, expRecord.id, detNum)
            self._insertIfAllowed(
                instrument=expRecord.instrument,
                table=f"cdb_{expRecord.instrument.lower()}.ccdexposure",
                obsId=obsId,
                values={"detector": detNum, "exposure_id": expRecord.id},
                allowUpdate=allowUpdate,
            )

    def populateCcdVisitRowWithButler(
        self,
        butler: Butler,
        expRecord: DimensionRecord,
        detectorNum: int,
        allowUpdate: bool = False,
    ) -> None:
        summaryStats = butler.get(
            "preliminary_visit_image.summaryStats", visit=expRecord.id, detector=detectorNum
        )
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
        table = f"cdb_{expRecord.instrument.lower()}.ccdvisit1_quicklook"

        if allowUpdate and "visit_id" not in values:
            values["visit_id"] = expRecord.id

        inserted = self._insertIfAllowed(
            instrument=expRecord.instrument,
            table=table,
            obsId=(expRecord.day_obs, expRecord.seq_num),
            values=values,
            allowUpdate=allowUpdate,
        )
        if inserted:
            self.redisHelper.announceResultInConsDb(expRecord.instrument, table, obsId)

    def populateCcdVisitRowZernikes(
        self,
        visitRecord: DimensionRecord,
        detectorNum: int,
        zernikeValues: dict[str, float],
        allowUpdate: bool = False,
    ) -> None:
        """Populate a row in the cdb_<instrument>.ccdvisit1_quicklook table
        with Zernike values.

        Parameters
        ----------
        visitRecord : `DimensionRecord`
            The visit record to populate the row for.
        detectorNum : `int`
            The detector number to populate the row for.
        zernikeValues : `dict[str, float]`
            A dictionary containing Zernike values to populate the row with,
            where keys are Zernike names and values are the corresponding float
            values. Names are as in the consDB schema, e.g. "z4", "z5", etc.
        allowUpdate : `bool`, optional
            Allow updating existing rows in the table.
        """
        obsId = computeCcdExposureId(visitRecord.instrument, visitRecord.id, detectorNum)
        table = f"cdb_{visitRecord.instrument.lower()}.ccdvisit1_quicklook"

        if allowUpdate and "visit_id" not in zernikeValues:
            zernikeValues["visit_id"] = visitRecord.id

        self._insertIfAllowed(
            instrument=visitRecord.instrument,
            table=table,
            obsId=obsId,
            values=zernikeValues,
            allowUpdate=allowUpdate,
        )

    def populateAllCcdVisitRowsWithButler(
        self, butler: Butler, expRecord: DimensionRecord, createRows: bool = False, allowUpdate: bool = False
    ) -> None:
        if createRows:
            self._createExposureRow(expRecord, allowUpdate=allowUpdate)
            self._createCcdExposureRows(expRecord, allowUpdate=allowUpdate)
            print(f"Populated tables for exposure and ccdexposure for {expRecord.instrument}+{expRecord.id})")

        detectorNums = getDetectorIds(expRecord.instrument)
        for detectorNum in detectorNums:
            self.populateCcdVisitRowWithButler(butler, expRecord, detectorNum, allowUpdate=allowUpdate)

    def populateVisitRowWithButler(
        self, butler: Butler, expRecord: DimensionRecord, allowUpdate: bool = False
    ) -> None:
        visitSummary = butler.get("preliminary_visit_summary", visit=expRecord.id)
        instrument = expRecord.instrument
        self.populateVisitRow(visitSummary, instrument, allowUpdate=allowUpdate)

    def populateVisitRow(
        self, visitSummary: ExposureCatalog, instrument: str, allowUpdate: bool = False
    ) -> None:
        if not self._shouldInsert():  # ugly but need to check this before accessing the schema
            location = self.locationConfig.location
            logger.info(f"Skipping consDB insert at {location} for {instrument}.visit1_quicklook")
            return

        schema = self.client.schema(instrument.lower(), "visit1_quicklook")
        schema = cast(dict[str, tuple[str, str]], schema)
        typeMapping: dict[str, str] = {k: v[0] for k, v in schema.items()}

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
            consDbKey = consDbKeyNoSuffix + "_min"
            typeFunc = changeType(consDbKey, typeMapping)
            values[consDbKey] = typeFunc(np.nanmin(visitSummary[summaryKey]))

            consDbKey = consDbKeyNoSuffix + "_max"
            typeFunc = changeType(consDbKey, typeMapping)
            values[consDbKey] = typeFunc(np.nanmax(visitSummary[summaryKey]))

            consDbKey = consDbKeyNoSuffix + "_median"
            typeFunc = changeType(consDbKey, typeMapping)
            values[consDbKey] = typeFunc(np.nanmedian(visitSummary[summaryKey]))

        for summaryKey, consDbKey in VISIT_MIN_MED_MAX_TOTAL_MAPPING.items():
            typeFunc = changeType(consDbKey + "_total", typeMapping)
            values[consDbKey + "_total"] = typeFunc(np.nansum(visitSummary[summaryKey]))

        nInputs = max([len(visitSummary[col]) for col in visitSummary.columns])
        minInputs = min([len(visitSummary[col]) for col in visitSummary.columns])
        if minInputs != nInputs:
            raise RuntimeError("preliminary_visit_summary is jagged - this should be impossible")

        values["n_inputs"] = nInputs
        table = f"cdb_{instrument.lower()}.visit1_quicklook"

        inserted = self._insertIfAllowed(
            instrument=instrument,
            table=table,
            obsId=visit,
            values=values,
            allowUpdate=allowUpdate,
        )
        if inserted:
            self.redisHelper.announceResultInConsDb(instrument, table, visit)

    def populateArbitrary(
        self,
        instrument: str,
        table: str,
        values: dict[str, int | float],
        dayObs: int,
        seqNum: int,
        allowUpdate: bool = False,
    ) -> None:
        """Populate an arbitrary consDB table for a given visit or exposure.

        Parameters
        ----------
        instrument : `str`
            The instrument name, used to resolve the schema namespace (e.g.,
            "LATISS" or "lsstcam", case-insensitive).
        table : `str`
            The table name within the instrument schema (e.g.,
            "visit1_quicklook").
        values : `dict[str, int | float | str]`
            Mapping of consDB column names to values to write. Values are
            coerced to the database column types using the table schema; NaN
            values are dropped.
        visitOrExposureId : `int`
            The visit or exposure identifier, i.e. the row in the table.
        allowUpdate : `bool`, optional
            If True, allow updating existing rows in the table. An error is
            raised if False and a value exists.
        """
        # validate before checking _shouldInsert() for better CI coverage
        if allowUpdate and "exposure" in table.lower() and "exposure_id" not in values:
            raise ValueError("When updating an exposure table, exposure_id must be in values")
        if allowUpdate and "visit" in table.lower() and "visit_id" not in values:
            raise ValueError("When updating a visit table, visit must be in values")

        if not self._shouldInsert():  # ugly but need to check this before accessing the schema
            location = self.locationConfig.location
            logger.info(f"Skipping consDB insert at {location} for {instrument}.visit1_quicklook")
            return

        schema = self.client.schema(instrument.lower(), table)
        schema = cast(dict[str, tuple[str, str]], schema)
        typeMapping: dict[str, str] = {k: v[0] for k, v in schema.items()}

        toSend: dict[str, int | float] = {}
        for consDbKey, value in values.items():
            if consDbKey not in typeMapping:
                raise ValueError(f"Key {consDbKey} not in consDB table {table}")

            typeFunc = changeType(consDbKey, typeMapping)
            toSend[consDbKey] = typeFunc(value)

        self._insertIfAllowed(
            instrument=instrument,
            table=table,
            obsId=(dayObs, seqNum),  # tuple is required when doing an update so always require it
            values=toSend,
            allowUpdate=allowUpdate,
        )

    def populateMountErrors(
        self,
        expRecord: DimensionRecord,
        mountErrors: dict[str, float] | MountErrors,
        instrument: str,
    ) -> None:
        values: dict[str, float] = {}
        if isinstance(mountErrors, MountErrors):  # LSSTCam code path
            # image impact measurements
            imageError = (mountErrors.imageAzRms**2 + mountErrors.imageElRms**2) ** 0.5
            values["mount_motion_image_degradation"] = imageError
            values["mount_motion_image_degradation_az"] = mountErrors.imageAzRms
            values["mount_motion_image_degradation_el"] = mountErrors.imageElRms
            values["mount_motion_image_degradation_rot"] = mountErrors.imageRotRms

            # raw axis jitter values
            mountError = (mountErrors.azRms**2 + mountErrors.elRms**2) ** 0.5
            values["mount_jitter_rms"] = mountError
            values["mount_jitter_rms_az"] = mountErrors.azRms
            values["mount_jitter_rms_el"] = mountErrors.elRms
            values["mount_jitter_rms_rot"] = mountErrors.rotRms
            values["mount_jitter_rms_cam_hexapod"] = mountErrors.camHexRms
            values["mount_jitter_rms_m2_hexapod"] = mountErrors.m2HexRms
        elif isinstance(mountErrors, dict):  # LATISS code path until unified
            image_az_rms = mountErrors["image_az_rms"]
            image_el_rms = mountErrors["image_el_rms"]
            imageError = (image_az_rms**2 + image_el_rms**2) ** 0.5

            values["mount_motion_image_degradation"] = imageError
            values["mount_motion_image_degradation_az"] = mountErrors["image_az_rms"]
            values["mount_motion_image_degradation_el"] = mountErrors["image_el_rms"]

            az_rms = mountErrors["az_rms"]
            el_rms = mountErrors["el_rms"]
            mountError = (az_rms**2 + el_rms**2) ** 0.5
            values["mount_jitter_rms"] = mountError
            values["mount_jitter_rms_az"] = mountErrors["az_rms"]
            values["mount_jitter_rms_el"] = mountErrors["el_rms"]
            values["mount_jitter_rms_rot"] = mountErrors["rot_rms"]
        else:
            raise TypeError(f"Expected MountErrors or dict, got {type(mountErrors)}")

        table = f"cdb_{instrument.lower()}.exposure_quicklook"

        if "exposure_id" not in values:
            values["exposure_id"] = expRecord.id

        self._insertIfAllowed(
            instrument=instrument,
            table=table,
            obs_id=expRecord.id,
            values=values,
            # this should always be an update as it's going in the exposure
            # table which will always already be populated
            allowUpdate=True,
        )
