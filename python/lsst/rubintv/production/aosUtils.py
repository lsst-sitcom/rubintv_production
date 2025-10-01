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

import numpy as np
import pandas as pd
from astropy.table import Table

__all__ = [
    "makeDataframeFromZernikes",
    "extractWavefrontData",
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
    from lsst.ts.ofc import OFCData
    from lsst.ts.wep.utils import convertZernikesToPsfWidth, makeDense
    from lsst.ts.ofc.utils.ofc_data_helpers import get_intrinsic_zernikes

    ofcData = OFCData("lsst")
    ofcData.zn_selected = np.array(zernikeTable.meta['nollIndices'])
    rotationAngle = zernikeTable.meta["rotTelPos"]
    rotMat = np.array(
        [
            [np.cos(-rotationAngle), -np.sin(-rotationAngle)],
            [np.sin(-rotationAngle), np.cos(-rotationAngle)],
        ]
    )

    records = []
    for row in zernikeTable:
        zernikes = makeDense(row["zk_OCS"], nollIndices=zernikeTable.meta["nollIndices"])
        intrinsicZernikesSparse = get_intrinsic_zernikes(
            ofcData, filterName.split("_")[0].upper(), [row['detector']], rotationAngle
        ).squeeze()[ofcData.zn_idx]
        intrinsicZernikes = makeDense(intrinsicZernikesSparse, nollIndices=zernikeTable.meta['nollIndices'])
        zernikesDeviation = zernikes - intrinsicZernikes

        aosFwhm = 1.06 * np.log(1 + np.sqrt(np.sum(np.square(convertZernikesToPsfWidth(zernikesDeviation)))))
        fieldAngles = ofcData.sample_points[row["detector"]]
        rotatedFieldAngles = fieldAngles @ rotMat

        records.append(
            {
                "detector": row["detector"],
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
        Dictionary with keys 'zksMeasured', 'zksInterpolated',
        'rotatedPositions', 'fwhmMeasured', 'fwhmInterpolated'.
    """
    import galsim

    from lsst.ts.wep.utils import convertZernikesToPsfWidth

    rotatedPositions = getCameraRotatedPositions(rotMat)

    fwhmMeasured = np.vstack(wavefrontResults["aosFwhm"].to_numpy())
    fieldAngles = np.vstack(wavefrontResults["fieldAngles"].to_numpy())
    zernikes = np.vstack(wavefrontResults["zernikesDeviation"].to_numpy())
    zernikesPadded = np.zeros((zMin, zernikes.shape[1] + zMin))
    zernikesPadded[:, zMin : zernikes.shape[1] + zMin] = zernikes

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

    zksInterpolated = np.zeros((len(rotatedPositions[:, 0]), 29))
    for idx in range(len(rotatedPositions[:, 0])):
        zksInterpolated[idx, :] = doubleZernikes(rotatedPositions[idx, 0], rotatedPositions[idx, 1]).coef

    fwhmInterpolated = np.zeros(len(sourceTable["aa_x"]))
    for idx in range(len(sourceTable["aa_x"])):
        zks_vec = doubleZernikes(sourceTable["aa_x"][idx], -sourceTable["aa_y"][idx]).coef[4:]
        fwhmInterpolated[idx] = np.sqrt(np.sum(convertZernikesToPsfWidth(zks_vec) ** 2))

    return {
        "zksMeasured": zernikesPadded,
        "zksInterpolated": zksInterpolated,
        "rotatedPositions": rotatedPositions,
        "fwhmMeasured": fwhmMeasured,
        "fwhmInterpolated": fwhmInterpolated,
    }


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
    from lsst.afw.cameraGeom import FIELD_ANGLE
    from lsst.obs.lsst import LsstCam

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
