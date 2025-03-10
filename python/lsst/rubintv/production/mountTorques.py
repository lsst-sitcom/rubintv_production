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

import asyncio
import time
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
import numpy as np
from astro_metadata_translator import ObservationInfo
from astropy.time import Time

from lsst.summit.utils.utils import dayObsIntToString

try:
    from lsst_efd_client import merge_packed_time_series as mpts
except ImportError:
    pass

if TYPE_CHECKING:
    from logging import Logger

    from lsst_efd_client import EfdClient
    from matplotlib.pyplot import Figure
    from numpy.typing import NDArray

    from lsst.daf.butler import Butler, DimensionRecord

__all__ = ["calculateMountErrors"]

NON_TRACKING_IMAGE_TYPES = [
    "BIAS",
    "FLAT",
]

AUXTEL_ANGLE_TO_EDGE_OF_FIELD_ARCSEC = 280.0
MOUNT_IMAGE_WARNING_LEVEL = 0.25  # this determines the colouring of the cells in the table, yellow for this
MOUNT_IMAGE_BAD_LEVEL = 0.4  # and red for this


def _getEfdData(client: EfdClient, dataSeries: str, startTime: Time, endTime: Time):
    """A synchronous warpper for geting the data from the EFD.

    This exists so that the top level functions don't all have to be async def.
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(client.select_time_series(dataSeries, ["*"], startTime.utc, endTime.utc))


def calculateMountErrors(
    expRecord: DimensionRecord,
    butler: Butler,
    client: EfdClient,
    figure: Figure | None,
    saveFilename: str,
    logger: Logger,
) -> dict[str, NDArray] | bool:
    """Queries EFD for a given exposure and calculates the RMS errors in the
    axes during the exposure, optionally plotting and saving the data.

    Returns ``False`` if the analysis fails or is skipped e.g. due to the
    expTime being too short.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The dimension record for the exposure for which to plot the mount
        torques.
    butler : `lsst.daf.butler.Butler`
        The butler to use to retrieve the image metadata.
    client : `lsst_efd_client.EfdClient`
        The EFD client to retrieve the mount torques.
    figure : `matplotlib.figure.Figure` or `None`
        A matplotlib figure to re-use if plotting. Necessary to pass this in to
        prevent an ever-growing figure count and the ensuing memory leak.
    saveFilename : `str`
        Full path and filename to save the plot to. If provided, a figure must
        be provided too.
    logger : `logging.Logger`
        The logger.

    Returns
    -------
    axisErrors : `dict` or `False`
        The RMS errors in the three axes and their image contributions:
        ``az_rms`` - The RMS azimuth error.
        ``el_rms`` - The RMS elevation error.
        ``rot_rms`` - The RMS rotator error.
        ``image_az_rms`` - The RMS azimuth error for the image.
        ``image_el_rms`` - The RMS elevation error for the image.
        ``image_rot_rms`` - The RMS rotator error for the image.
    """
    # lsst-efd-client is not a required import at the top here, but is
    # implicitly required as a client is passed into this function so is not
    # rechecked here.

    start = time.time()

    dayString = dayObsIntToString(expRecord.day_obs)
    seqNumString = str(expRecord.seq_num)
    dataIdString = f"{dayString} - seqNum {seqNumString}"

    imgType = expRecord.observation_type.upper()
    if imgType in NON_TRACKING_IMAGE_TYPES:
        logger.info(f"Skipping mount torques for non-tracking image type {imgType} for {dataIdString}")
        return False

    # only process these if they're the unusual tracking-darks used for closed
    # dome testing of the telescope.
    if imgType == "DARK":
        if expRecord.target_name == "slew_icrs":
            logger.info(f"Calculating mount torques for slewing dark image {dataIdString}")
        else:
            logger.info(f"Skipping mount torques for non-slewing dark for {dataIdString}")
            return False

    exptime = expRecord.exposure_time
    if exptime < 1.99:
        logger.info("Skipping sub 2s expsoure")
        return False

    tStart = expRecord.timespan.begin.tai.to_value("isot")
    tEnd = expRecord.timespan.end.tai.to_value("isot")
    elevation = 90 - expRecord.zenith_angle

    # TODO: DM-33859 remove this once it can be got from the expRecord
    md = butler.get("raw.metadata", expRecord.dataId, detector=0)
    obsInfo = ObservationInfo(md)
    azimuth = obsInfo.altaz_begin.az.value
    logger.debug(f"dataId={dataIdString}, imgType={imgType}, Times={tStart}, {tEnd}")

    end = time.time()
    elapsed = end - start
    logger.debug(f"Elapsed time for butler query = {elapsed}")

    start = time.time()
    # Time base in the EFD is still a big mess.  Although these times are in
    # UTC, it is necessary to tell the code they are in TAI. Then it is
    # necessary to tell the merge_packed_time_series to use UTC.
    # After doing all of this, there is still a 2 second offset,
    # which is discussed in JIRA ticket DM-29243, but not understood.

    t_start = Time(tStart, scale="tai")
    t_end = Time(tEnd, scale="tai")
    logger.debug(f"Tstart = {t_start.isot}, Tend = {t_end.isot}")

    mount_position = _getEfdData(client, "lsst.sal.ATMCS.mount_AzEl_Encoders", t_start, t_end)
    nasmyth_position = _getEfdData(client, "lsst.sal.ATMCS.mount_Nasmyth_Encoders", t_start, t_end)
    torques = _getEfdData(client, "lsst.sal.ATMCS.measuredTorque", t_start, t_end)
    logger.debug("Length of time series", len(mount_position))

    az = mpts(mount_position, "azimuthCalculatedAngle", stride=1)
    el = mpts(mount_position, "elevationCalculatedAngle", stride=1)
    rot = mpts(nasmyth_position, "nasmyth2CalculatedAngle", stride=1)
    az_torque_1 = mpts(torques, "azimuthMotor1Torque", stride=1)
    az_torque_2 = mpts(torques, "azimuthMotor2Torque", stride=1)
    el_torque = mpts(torques, "elevationMotorTorque", stride=1)
    rot_torque = mpts(torques, "nasmyth2MotorTorque", stride=1)

    end = time.time()
    elapsed = end - start
    logger.debug(f"Elapsed time to get the data = {elapsed}")
    start = time.time()

    # Calculate the tracking errors
    az_vals = np.array(az.values[:, 0])
    el_vals = np.array(el.values[:, 0])
    rot_vals = np.array(rot.values[:, 0])
    times = np.array(az.values[:, 1])
    # The fits are much better if the time variable
    # is centered in the interval
    fit_times = times - times[int(len(az.values[:, 1]) / 2)]
    logger.debug("Length of packed time series", len(az_vals))

    # Fit with a polynomial
    az_fit = np.polyfit(fit_times, az_vals, 4)
    el_fit = np.polyfit(fit_times, el_vals, 4)
    rot_fit = np.polyfit(fit_times, rot_vals, 2)
    az_model = np.polyval(az_fit, fit_times)
    el_model = np.polyval(el_fit, fit_times)
    rot_model = np.polyval(rot_fit, fit_times)

    # Errors in arcseconds
    az_error = (az_vals - az_model) * 3600
    el_error = (el_vals - el_model) * 3600
    rot_error = (rot_vals - rot_model) * 3600

    # Calculate RMS
    az_rms = np.sqrt(np.mean(az_error * az_error))
    el_rms = np.sqrt(np.mean(el_error * el_error))
    rot_rms = np.sqrt(np.mean(rot_error * rot_error))

    # Calculate Image impact RMS
    image_az_rms = az_rms * np.cos(el_vals[0] * np.pi / 180.0)
    image_el_rms = el_rms
    image_rot_rms = rot_rms * AUXTEL_ANGLE_TO_EDGE_OF_FIELD_ARCSEC * np.pi / 180.0 / 3600.0

    end = time.time()
    elapsed = end - start
    logger.debug(f"Elapsed time for error calculations = {elapsed}")
    start = time.time()
    if saveFilename is not None:
        assert figure is not None, "Must supply a figure if plotting"
        # Plotting
        figure.clear()
        title = f"Mount Tracking {dataIdString}, Azimuth = {azimuth:.1f}, Elevation = {elevation:.1f}"
        plt.suptitle(title, fontsize=18)
        # Azimuth axis
        plt.subplot(3, 3, 1)
        ax1 = az["azimuthCalculatedAngle"].plot(legend=True, color="red")
        ax1.set_title("Azimuth axis", fontsize=16)
        ax1.axvline(az.index[0], color="red", linestyle="--")
        ax1.set_xticks([])
        ax1.set_ylabel("Degrees")
        plt.subplot(3, 3, 4)
        plt.plot(fit_times, az_error, color="red")

        plt.title(
            f"Azimuth RMS error = {az_rms:.2f} arcseconds\n"
            f"  Image RMS error = {image_az_rms:.2f} arcseconds"
        )
        plt.ylim(-10.0, 10.0)
        plt.xticks([])
        plt.ylabel("Arcseconds")
        plt.subplot(3, 3, 7)
        ax7 = az_torque_1["azimuthMotor1Torque"].plot(legend=True, color="blue")
        ax7 = az_torque_2["azimuthMotor2Torque"].plot(legend=True, color="green")
        ax7.axvline(az.index[0], color="red", linestyle="--")
        ax7.set_ylabel("Torque (motor current in amps)")

        # Elevation axis
        plt.subplot(3, 3, 2)
        ax2 = el["elevationCalculatedAngle"].plot(legend=True, color="green")
        ax2.set_title("Elevation axis", fontsize=16)
        ax2.axvline(az.index[0], color="red", linestyle="--")
        ax2.set_xticks([])
        plt.subplot(3, 3, 5)
        plt.plot(fit_times, el_error, color="green")
        plt.title(
            f"Elevation RMS error = {el_rms:.2f} arcseconds\n"
            f"    Image RMS error = {image_el_rms:.2f} arcseconds"
        )
        plt.ylim(-10.0, 10.0)
        plt.xticks([])
        plt.subplot(3, 3, 8)
        ax8 = el_torque["elevationMotorTorque"].plot(legend=True, color="blue")
        ax8.axvline(az.index[0], color="red", linestyle="--")
        ax8.set_ylabel("Torque (motor current in amps)")

        # Nasmyth2 rotator axis
        plt.subplot(3, 3, 3)
        ax3 = rot["nasmyth2CalculatedAngle"].plot(legend=True, color="blue")
        ax3.set_title("Nasmyth2 axis", fontsize=16)
        ax3.axvline(az.index[0], color="red", linestyle="--")
        ax3.set_xticks([])
        plt.subplot(3, 3, 6)
        plt.plot(fit_times, rot_error, color="blue")
        plt.title(
            f"Nasmyth2 RMS error = {rot_rms:.2f} arcseconds\n"
            f"  Image RMS error <= {image_rot_rms:.2f} arcseconds"
        )
        plt.ylim(-10.0, 10.0)
        plt.xticks([])
        plt.subplot(3, 3, 9)
        ax9 = rot_torque["nasmyth2MotorTorque"].plot(legend=True, color="blue")
        ax9.axvline(az.index[0], color="red", linestyle="--")
        ax9.set_ylabel("Torque (motor current in amps)")
        plt.savefig(saveFilename)

        end = time.time()
        elapsed = end - start
        logger.debug(f"Elapsed time for plots = {elapsed}")

    return dict(
        az_rms=az_rms,
        el_rms=el_rms,
        rot_rms=rot_rms,
        image_az_rms=image_az_rms,
        image_el_rms=image_el_rms,
        image_rot_rms=image_rot_rms,
    )
