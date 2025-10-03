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

import batoid
import galsim
import numpy as np
import pandas as pd
from astropy.table import Table
from batoid_rubin import LSSTBuilder

from lsst.afw.cameraGeom import FIELD_ANGLE
from lsst.obs.lsst import LsstCam
from lsst.ts.ofc import OFCData, StateEstimator
from lsst.ts.ofc.utils.ofc_data_helpers import get_intrinsic_zernikes
from lsst.ts.wep.utils import convertZernikesToPsfWidth, makeDense

__all__ = [
    "makeDataframeFromZernikes",
    "extractWavefrontData",
    "estimateTelescopeState",
    "parseDofStr",
]


def makeDataframeFromZernikes(zernikeTable: Table, filterName: str) -> pd.DataFrame:
    """Convert a table of Zernike coefficients into a pandas DataFrame.

    Parameters
    ----------
    zernikeTable : `astropy.table.Table`
        Table containing Zernike coefficients.
    filterName : `str`
        Name of the filter used for the exposure.

    Returns
    -------
    df : `pandas.DataFrame`
        DataFrame with zernikes in OCS, detector name,
        rotated field angles and aos_fwhm.
    """
    ofcData = OFCData("lsst")
    ofcData.zn_selected = np.array(zernikeTable.meta["nollIndices"])
    rotationAngle = zernikeTable.meta["rotTelPos"]
    rotMat = np.array(
        [
            [np.cos(-rotationAngle), -np.sin(-rotationAngle)],
            [np.sin(-rotationAngle), np.cos(-rotationAngle)],
        ]
    )

    records = []
    for row in zernikeTable:
        zernikesCCS = makeDense(row["zk_CCS"], nollIndices=zernikeTable.meta["nollIndices"])
        zernikesOCS = makeDense(row["zk_OCS"], nollIndices=zernikeTable.meta["nollIndices"])
        zernikes = makeDense(row["zk_OCS"], nollIndices=zernikeTable.meta["nollIndices"])
        intrinsicZernikesSparse = get_intrinsic_zernikes(
            ofcData, filterName.split("_")[0].upper(), [row["detector"]], np.rad2deg(rotationAngle)
        ).squeeze()[ofcData.zn_idx]
        intrinsicZernikes = makeDense(intrinsicZernikesSparse, nollIndices=zernikeTable.meta["nollIndices"])
        zernikesDeviation = zernikes - intrinsicZernikes

        aosFwhm = 1.06 * np.log(1 + np.sqrt(np.sum(np.square(convertZernikesToPsfWidth(zernikesDeviation)))))
        fieldAngles = ofcData.sample_points[row["detector"]]
        rotatedFieldAngles = fieldAngles @ rotMat

        records.append(
            {
                "detector": row["detector"],
                "zernikesCCS": zernikesCCS,
                "zernikesOCS": zernikesOCS,
                "zernikesDeviation": zernikesDeviation,
                "fieldAngles": rotatedFieldAngles,
                "aosFwhm": aosFwhm,
            }
        )

    return pd.DataFrame(records), rotMat


def extractWavefrontData(
    wavefrontResults: pd.DataFrame,
    sourceTable: Table,
    rotMat: np.ndarray,
    zMin: int = 4,
    fieldRadius: float = 1.75,
    kMax: int = 3,
    pupilInner: float = 2.558,
    pupilOuter: float = 4.18,
) -> dict:
    """Extract Zernike coefficients and FWHM values for
    measured and interpolated data.

    Parameters
    ----------
    wavefrontResults : `pandas.DataFrame`
        DataFrame containing wavefront results with zernikes,
        field angles, and aos_fwhm.
    sourceTable : `astropy.table.Table`
        Table containing source catalog data.
    rotMat : `numpy.ndarray`
        Rotation matrix to convert angles.
    zMin : `int`, optional
        Minimum Zernike index to consider, by default 4.
    fieldRadius : `float`, optional
        Field radius, by default 1.75.
    kMax : `int`, optional
        Maximum Zernike field radial order to use for interpolation.
        By default 3.
    pupilInner : `float`, optional
        Inner pupil radius in meters, by default 2.558.
    pupilOuter : `float`, optional
        Outer pupil radius in meters, by default 4.18.

    Returns
    -------
    dict
        Dictionary with keys 'fieldAngles', 'zksMeasured',
        'zksInterpolated', 'rotatedPositions', 'fwhmMeasured',
        'fwhmInterpolated'.
    """
    # Get rotated positions of the center for each camera detector
    rotatedPositions = getCameraRotatedPositions(rotMat)

    # Retrieve data from Dataframe
    fwhmMeasured = np.vstack(wavefrontResults["aosFwhm"].to_numpy())
    fieldAngles = np.vstack(wavefrontResults["fieldAngles"].to_numpy())
    zernikes = np.vstack(wavefrontResults["zernikesDeviation"].to_numpy())
    zernikesPadded = np.zeros((zMin, zernikes.shape[1] + zMin))
    zernikesPadded[:, zMin : zernikes.shape[1] + zMin] = zernikes

    # Fit a double Zernike to the measured Zernikes with maximum
    # field order of kMax
    basis = galsim.zernike.zernikeBasis(kMax, fieldAngles[:, 0], fieldAngles[:, 1], R_outer=fieldRadius)
    doubleZernikeCoeffs, *_ = np.linalg.lstsq(basis.T, zernikesPadded, rcond=None)
    doubleZernikeCoeffs[0, :] = 0.0  # Need to zero out k=0 term which doesn't have any meaning
    doubleZernikeCoeffs[0, :zMin] = 0.0  # We are not interested in PTT, so we zero them out

    doubleZernikes = galsim.zernike.DoubleZernike(
        doubleZernikeCoeffs,
        uv_inner=0.0,
        uv_outer=fieldRadius,
        xy_inner=pupilInner,
        xy_outer=pupilOuter,
    )

    # Interpolate Zernikes at the rotated positions of the camera detectors
    zksInterpolated = np.zeros((len(rotatedPositions[:, 0]), zernikes.shape[1]))
    for idx in range(len(rotatedPositions[:, 0])):
        zksInterpolated[idx, :] = doubleZernikes(rotatedPositions[idx, 0], rotatedPositions[idx, 1]).coef

    # Compute FWHM based on the interpolated Zernikes at the source positions
    fwhmInterpolated = np.zeros(len(sourceTable["aa_x"]))
    for idx in range(len(sourceTable["aa_x"])):
        zks_vec = doubleZernikes(sourceTable["aa_x"][idx], -sourceTable["aa_y"][idx]).coef[4:]
        fwhmInterpolated[idx] = np.sqrt(np.sum(convertZernikesToPsfWidth(zks_vec) ** 2))

    return {
        "fieldAngles": fieldAngles,
        "zksMeasured": zernikesPadded,
        "zksInterpolated": zksInterpolated,
        "rotatedPositions": rotatedPositions,
        "fwhmMeasured": fwhmMeasured,
        "fwhmInterpolated": fwhmInterpolated,
    }


def estimateWavefrontDataFromDofs(
    dofState: np.ndarray,
    wavefrontResults: pd.DataFrame,
    sourceTable: Table,
    rotMat: np.ndarray,
    filterName: str,
    zMin: int = 4,
    fieldRadius: float = 1.75,
    kMax: int = 6,
    jMax: int = 28,
    obscuration: float = 0.61,
    pupilInner: float = 2.558,
    pupilOuter: float = 4.18,
    batoidFeaDir: str = "/home/gmegias/ts_aos_analysis/notebooks/WET/fea_legacy",
    batoidBendDir: str = "/home/gmegias/ts_aos_analysis/notebooks/WET/bend",
) -> dict:
    # Get rotated positions of the center for each camera detector
    rotatedPositions = getCameraRotatedPositions(rotMat)

    fwhmMeasured = wavefrontResults["aosFwhm"].to_numpy()
    fieldAngles = np.vstack(wavefrontResults["fieldAngles"].to_numpy())
    zernikes = np.vstack(wavefrontResults["zernikesDeviation"].to_numpy())
    zernikesPadded = np.zeros((zMin, zernikes.shape[1] + zMin))
    zernikesPadded[:, zMin : zernikes.shape[1] + zMin] = zernikes

    ofcData = OFCData("lsst")
    wavelength = ofcData.eff_wavelength[filterName.upper()]

    # Need to fix the signs and unit conversions to use batoid
    dof = -np.array(dofState)  # Make a copy
    dof[[3, 4, 8, 9]] *= 3600  # degrees => arcsec
    dof[[0, 1, 3, 5, 6, 8] + list(range(30, 50))] *= -1  # coordsys

    fiducial = batoid.Optic.fromYaml(f"LSST_{filterName}.yaml")
    telescope = (
        LSSTBuilder(
            fiducial,
            fea_dir=batoidFeaDir,
            bend_dir=batoidBendDir,
        )
        .with_aos_dof(dof)
        .build()
    )

    # Build double Zernike model for the perturbed
    # telescope and the fiducial one
    doubleZernikesPerturbed = (
        batoid.doubleZernike(
            telescope,
            field=np.deg2rad(fieldRadius),
            wavelength=wavelength * 1e-6,
            eps=obscuration,
            jmax=jMax,
            kmax=kMax,
        )
        * wavelength
    )

    doubleZernikesFiducial = (
        batoid.doubleZernike(
            fiducial,
            field=np.deg2rad(fieldRadius),
            wavelength=wavelength * 1e-6,
            eps=obscuration,
            jmax=jMax,
            kmax=kMax,
        )
        * wavelength
    )

    # Generate double zernikes from subtraction of the two
    # (perturbed - fiducial). The fiducial one is small compared
    # to the perturbed one, but not zero.
    doubleZernikeCoeffs = doubleZernikesPerturbed - doubleZernikesFiducial
    doubleZernikes = galsim.zernike.DoubleZernike(
        doubleZernikeCoeffs,
        uv_inner=0,
        uv_outer=fieldRadius,
        xy_inner=pupilInner,
        xy_outer=pupilOuter,
    )

    zksEstimated = np.zeros((fieldAngles.shape[0], jMax + 1))
    for idx in range(fieldAngles.shape[0]):
        zksEstimated[idx, :] = doubleZernikes(fieldAngles[idx, 0], fieldAngles[idx, 1]).coef

    # Interpolate Zernikes at the rotated positions of the camera detectors
    zksInterpolated = np.zeros((len(rotatedPositions[:, 0]), jMax + 1))
    for idx in range(len(rotatedPositions[:, 0])):
        zksInterpolated[idx, :] = doubleZernikes(rotatedPositions[idx, 0], rotatedPositions[idx, 1]).coef

    # Compute FWHM based on the interpolated Zernikes at the source positions
    fwhmInterpolated = np.zeros(len(sourceTable["aa_x"]))
    for idx in range(len(sourceTable["aa_x"])):
        zks_vec = doubleZernikes(sourceTable["aa_x"][idx], -sourceTable["aa_y"][idx]).coef[4:]
        fwhmInterpolated[idx] = np.sqrt(np.sum(convertZernikesToPsfWidth(zks_vec) ** 2))

    return {
        "detector": wavefrontResults["detector"].to_list(),
        "fieldAngles": fieldAngles,
        "zksEstimated": zksEstimated,
        "zksMeasured": zernikesPadded,
        "zksInterpolated": zksInterpolated,
        "rotatedPositions": rotatedPositions,
        "fwhmMeasured": fwhmMeasured,
        "fwhmInterpolated": fwhmInterpolated,
    }


def estimateTelescopeState(
    zernikeTable: Table,
    wavefrontResults: pd.DataFrame,
    configPath: str,
    filterName: str,
    useDof: str = "0-9,10-16,30-34",
    nKeep: int = 12,
) -> np.ndarray:
    """Estimate the telescope state from wavefront results.

    Parameters
    ----------
    zernikeTable : `astropy.table.Table`
        Table containing Zernike coefficients.
    wavefrontResults : `pandas.DataFrame`
        DataFrame containing wavefront results with zernikes,
        field angles, and aos_fwhm.
    filterName : `str`
        Name of the filter used for the exposure.
    configPath : `str`
        Path to the configuration directory for OFCData.
    useDof : `str`
        String representing integer ranges of degrees of freedom to use.
    nKeep : `int`
        Number of modes to keep in the state estimation.
    zMin : `int`
        Minimum Zernike index to consider.

    Returns
    -------
    numpy.ndarray
        Array representing the estimated telescope state.
    """
    if isinstance(useDof, str):
        newCompDofIdx = parseDofStr(useDof)
    else:
        raise ValueError("useDof must be a string representing integer ranges.")

    ofc_data = OFCData("lsst", config_dir=configPath)
    ofc_data.zn_selected = np.array(zernikeTable.meta["nollIndices"])
    ofc_data.comp_dof_idx = newCompDofIdx
    ofc_data.controller["truncation_index"] = nKeep
    state_estimator = StateEstimator(ofc_data)

    zernikesCCS = np.vstack(wavefrontResults["zernikesCCS"].to_numpy())
    detector_names = wavefrontResults["detector"].to_list()

    out = state_estimator.dof_state(
        filterName.split("_")[0].upper(),
        zernikesCCS,
        detector_names,
        np.rad2deg(zernikeTable.meta["rotTelPos"]),
    )

    dof_state = np.zeros(50)
    dof_state[ofc_data.dof_idx] = out
    return dof_state


def getCameraRotatedPositions(rotMat: np.ndarray) -> np.ndarray:
    """Get rotated x and y positions of the camera detectors.

    Parameters
    ----------
    rotMat : `numpy.ndarray`
        Rotation matrix to convert angles.

    Returns
    -------
    numpy.ndarray
        Array of rotated x and y positions.
    """
    camera = LsstCam().getCamera()
    x_positions = []
    y_positions = []
    for detector in camera:
        centers_deg = np.rad2deg(detector.getCenter(FIELD_ANGLE))
        x_grid, y_grid = np.meshgrid(centers_deg[0], centers_deg[1])

        x_positions.extend(x_grid.flatten())
        y_positions.extend(y_grid.flatten())
        rotated_positions = np.array([x_positions, y_positions]).T @ rotMat

    return rotated_positions


def parseDofStr(dofStr: str) -> dict[str, np.ndarray]:
    """Parse a string representation of integer ranges
    into a sorted list of integers.

    The input string may contain comma-separated integers
    and/or ranges of the form "start-end".
    For example:
        "0-4,10-14" -> [0, 1, 2, 3, 4, 10, 11, 12, 13, 14]
        "3,7,9-11"  -> [3, 7, 9, 10, 11]

    Parameters
    ----------
    dofStr : str
        A string containing integers and/or integer ranges,
        separated by commas.

    Returns
    -------
    dict
        A dictionary with boolean arrays indicating active DOFs

    Raises
    ------
    ValueError
        If the string cannot be parsed into integers or ranges of integers.
    """
    dofStr = dofStr.strip()
    useDof = []
    for part in dofStr.split(","):
        if "-" in part:
            start, end = [int(p) for p in part.split("-")]
            useDof.extend(range(start, end + 1))
        else:
            useDof.append(int(part))
    useDof = np.sort(useDof)

    newCompDofIdx = dict(
        m2HexPos=np.full(5, False, dtype=bool),  # M2 hexapod (0–4)
        camHexPos=np.full(5, False, dtype=bool),  # Camera hexapod (5–9)
        M1M3Bend=np.full(20, False, dtype=bool),  # M1M3 bending modes (10–29)
        M2Bend=np.full(20, False, dtype=bool),  # M2 bending modes (30–49)
    )

    # Mark active DOFs
    for idof in useDof:
        if idof < 5:
            # M2 hexapod (x, y, z, tip, tilt)
            newCompDofIdx["m2HexPos"][idof] = True
        elif 5 <= idof < 10:
            # Camera hexapod (x, y, z, tip, tilt)
            newCompDofIdx["camHexPos"][idof - 5] = True
        elif 10 <= idof < 30:
            # M1M3 bending modes (low-order figure control)
            # These modes correct deformations of the
            # primary/tertiary mirror
            newCompDofIdx["M1M3Bend"][idof - 10] = True
        elif 30 <= idof < 50:
            # M2 bending modes (low-order figure control)
            # These modes correct deformations of the secondary mirror
            newCompDofIdx["M2Bend"][idof - 30] = True
    return newCompDofIdx
