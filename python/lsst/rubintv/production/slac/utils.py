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

import time
from time import sleep
import logging

__all__ = ['fullAmpDictToPerCcdDicts',
           'getCamera',
           ]


def getCamera(butler, instrument):
    """Get the camera for the specified instrument.

    This utility only exists because of the need to supply the calib/unbounded
    collection, which is non-standard. If/when the repo surgery is done to
    chain this on to the standard calib collection, this util can be removed.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    instrument : `str`
        The instrument name, either 'LSST-TS8' or 'LSSTCam'.

    Returns
    -------
    camera : `lsst.afw.cameraGeom.Camera`
        The camera object.
    """
    match instrument:
        case 'LSSTCam':
            return butler.get('camera', collections=['LSSTCam/calib/unbounded'], instrument='LSSTCam')
        case 'LSST-TS8':
            return butler.get('camera', collections=['LSST-TS8/calib/unbounded'], instrument='LSST-TS8')
        case _:
            raise ValueError("This utility function is just for getting LSST-TS8 and LSSTCam instruments,"
                             f" got {instrument}")


def fullAmpDictToPerCcdDicts(fullDict):
    """Split a single, flat dict of per-amp values by detector.

    This function exists because some eo_pipe plotting utils expect the data
    in this format.

    For example, a single dict like:
    {'R12_S01_C05': 123,
     'R12_S02_C06': 234,
     ...
    }

    is turned into a dict of dicts, keyed by full detector name,
    with the values in a dict of amp values:
    {'R12_S01': {'C05': 123},
     'R12_S02': {'C06': 234},
     ...
    }

    Parameters
    ----------
    fullDict : `dict` [`str`, `float`]
        The flat input dict of per-amp values, keyed like 'R12_S01_C05': 123.4

    Returns
    -------
    perCcdDicts : `dict` [`str`, `dict` [`str`, `float`]]
        The per-ccd dict of dicts, keyed by full detector name, with the values
        keyed by amp name.
    """
    detectors = set(name.split('_C')[0] for name in fullDict.keys())
    dicts = {k: {} for k in detectors}
    for k, v in fullDict.items():
        detName, ampName = k.split('_C')
        dicts[detName]["C" + ampName] = v
    return dicts


def getDetectorsWithData(butler, expRecord, dataset):
    """Get a list of detector ids for which the specified dataset exists.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposureRecord containing the dataId.
    dataset : `str`
        The dataset type to find, e.g. 'raw'.

    Returns
    -------
    detectorsWithData : `list` [`int`]
        The list of detector numbers for which the dataset exists.
    """
    return sorted([det.id for det in butler.registry.queryDimensionRecords('detector',
                                                                           dataId=expRecord.dataId,
                                                                           datasets=dataset)])


def waitForDataProduct(butler, expRecord, dataset, detector, timeout, cadence=1, logger=None):
    """Wait for a data product to appear in the butler.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposureRecord containing the dataId.
    dataset : `str`
        The dataset type to find, e.g. 'raw'.
    detector : `int`
        The detector to find the dataset for.
    timeout : `float`
        The timeout in seconds.
    cadence : `float`, optional
        The cadence in seconds to check for the dataset.
    logger : `logging.Logger`, optional
        The logger, created if not supplied.

    Returns
    -------
    product : `object` or `None`
        The requested data product, or None if the timeout was reached.
    """
    # TODO: in January work with Jim in pair coding to use a limited butler:
    # https://github.com/lsst/daf_butler/blob/main/python/lsst/daf/butler/_limited_butler.py
    # by constructing a dRef here myself, which is determinisitc, this would
    # allow using a limited butler, and thus polling wouldn't be a problem
    # at all as that can just use the filesystem.

    startTime = time.time()
    while time.time() - startTime < timeout:
        try:
            product = butler.get(dataset, expRecord.dataId, detector=detector)
            return product
        except LookupError:
            sleep(cadence)

    if not logger:
        logger = logging.logger('lsst.rubintv.production.slac.utils.waitForDataProduct')
    logger.warning(f"Timed out waiting for {dataset} for {expRecord.dataId} on detector {detector}")
    return None
