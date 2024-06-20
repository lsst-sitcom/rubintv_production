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

import glob
import json
import logging
import os
import time

import numpy as np
from astropy.io import fits

from lsst.utils import getPackageDir

__all__ = [
    "fullAmpDictToPerCcdDicts",
    "getCamera",
    "getAmplifierRegions",
    "getGains",
    "gainsToPtcDataset",
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
        case "LSSTCam":
            return butler.get("camera", collections=["LSSTCam/calib/unbounded"], instrument="LSSTCam")
        case "LSST-TS8":
            return butler.get("camera", collections=["LSST-TS8/calib/unbounded"], instrument="LSST-TS8")
        case "LSSTComCam":
            return butler.get("camera", collections=["LSSTComCam/calib/unbounded"], instrument="LSSTComCam")
        case "LSSTComCamSim":
            return butler.get(
                "camera", collections=["LSSTComCamSim/calib/unbounded"], instrument="LSSTComCamSim"
            )
        case _:
            raise ValueError(
                "This utility function is just for getting LSST-TS8, LSSTCam, LSSTComCam and "
                f"LSSTComCamSim instruments, got {instrument}"
            )


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
    detectors = set(name.split("_C")[0] for name in fullDict.keys())
    dicts = {k: {} for k in detectors}
    for k, v in fullDict.items():
        detName, ampName = k.split("_C")
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
    return sorted(
        [
            det.id
            for det in butler.registry.queryDimensionRecords(
                "detector", dataId=expRecord.dataId, datasets=dataset
            )
        ]
    )


def waitForDataProduct(butler, expRecord, dataset, detector, timeout, cadence=1, logger=None):
    """Wait for a data product to appear in the butler.

    TODO: unify this with the _waitForDataProduct() function which the
    BaseButlerChannel has. This should be done as part of the same work which
    makes (or doesn't make) these SLAC channels and utils inherit from the base
    channels etc, once this part of the codebase has matured.

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
            time.sleep(cadence)

    if not logger:
        logger = logging.logger("lsst.rubintv.production.slac.utils.waitForDataProduct")
    logger.warning(f"Timed out waiting for {dataset} for {expRecord.dataId} on detector {detector}")
    return None


def getAmplifierRegions(raw):
    """Get a dictionary of the imaging, serial and parallel sections in readout
    order, i.e. with the readout node in the lower left.

    Parameters
    ----------
    raw : `lsst.afw.image.Exposure`
        The input exposure, unassembled.

    Returns
    -------
    regions : `dict` [`str`, `dict` [`str`, `numpy.ndarray`]]
        The imaging, serial and parallel sections, keyed by amp name.
    """
    imaging = {}
    serial = {}
    parallel = {}

    for amp in raw.detector:
        # for raw images, get the imaging section and the serial/parallel
        # overscan regions
        ampName = amp.getName()

        serialArray = raw[amp.getRawSerialOverscanBBox()].image.array
        parallelArray = raw[amp.getRawParallelOverscanBBox()].image.array
        imagingArray = raw[amp.getRawDataBBox()].image.array

        # if need to flip X or Y, do it
        if amp.getRawFlipX():
            serialArray = np.fliplr(serialArray)
            parallelArray = np.fliplr(parallelArray)
            imagingArray = np.fliplr(imagingArray)
        if amp.getRawFlipY():
            serialArray = np.flipud(serialArray)
            parallelArray = np.flipud(parallelArray)
            imagingArray = np.flipud(imagingArray)

        imaging[ampName] = imagingArray
        serial[ampName] = serialArray
        parallel[ampName] = parallelArray

    return imaging, serial, parallel


def getGains(instrument):
    """Get the nominal gains for a given instrument.

    Parameters
    ----------
    instrument : `str`
        The instrument name, e.g. 'LSSTCam'.

    Returns
    -------
    gains : `dict` [`str`, `dict` [`str`, `float`]]
        A dict of dicts, keyed by detector name, with each being a dict of
        amplifier gains, as floats, keyed by amplifier name.
    """
    match instrument:
        case "LSSTCam":
            return _getLsstCamGains()
        case "LSSTComCam":
            return _getLsstComCamGains()
        case "LSSTComCamSim":  # for now, give the real ComCam gains. I think they're actually the same anyway
            return _getLsstComCamGains()
        case "LSST-TS8":
            return _getTs8Gains()
        case _:
            raise ValueError(f"Unknown instrument {instrument}")


def _getLsstComCamGains():
    """Get the nominal ComCam gains.

    Returns
    -------
    gains : `dict` [`str`, `dict` [`str`, `float`]]
        A dict of dicts, keyed by detector name, with each being a dict of
        amplifier gains, as floats, keyed by amplifier name.
    """
    lsstComCamGainsDir = os.path.join(getPackageDir("rubintv_production"), "data", "LSSTComCam_gains")

    filename = os.path.join(lsstComCamGainsDir, "approx_comcam_ptc_20220217_from_craig_lage.json")
    adjustmentFactor = 0.79  # this is for approx_comcam_ptc_20220217_from_craig_lage.json only!
    # these gains are very approximate, and Craig thinks they need this
    # fudge-factor. Once we get some proper gains from a recent PTC the json
    # file can be updated and this factor removed.

    with open(filename) as f:
        gains = json.load(f)

    for ccdName, gainDict in gains.items():
        gains[ccdName] = {ampName: gain * adjustmentFactor for ampName, gain in gainDict.items()}
    return gains


def _getLsstCamGains():
    """Get the nominal LSSTCam gains.

    Returns
    -------
    gains : `dict` [`str`, `dict` [`str`, `float`]]
        A dict of dicts, keyed by detector name, with each being a dict of
        amplifier gains, as floats, keyed by amplifier name.
    """
    lsstCamGainDir = os.path.join(getPackageDir("rubintv_production"), "data", "LSSTCam_gains")

    filename = os.path.join(lsstCamGainDir, "curated_ptc_gains_run_13144_2022-02-10.json")

    with open(filename) as f:
        gains = json.load(f)
    return gains


def _getTs8Gains():
    """Get the gains for all the amps in TS8.

    Get the gains for each detector in TS8, and return them in a dict keyed by
    detector name (in the short S01 form without the leading raft part), with
    the values being dicts keyed by amp name.

    If camera folk wanted the gains to be more dynamically updateable, these
    gains could be converted into a real PtcDataset and ingested and the
    isrTask could get the real data product. For now though, this is a
    reasonable workaround.

    Returns
    -------
    raftGains : `dict` [`str`, `dict` [`str`, `float`]]
        The gains, keyed by detector's short name (S01 not R22_S01), with the
        values being dicts keyed by amp name.
    """
    DATASET_NAME = "7045D_eotest_results"

    # This is the order they're currently stored in in the fits file Yousuke
    # supplied. Unsure how stable this would be if were to update the files,
    # but for the 7045D dataset, this works correctly.
    ampNameOrder = [
        "C10",
        "C11",
        "C12",
        "C13",
        "C14",
        "C15",
        "C16",
        "C17",
        "C07",
        "C06",
        "C05",
        "C04",
        "C03",
        "C02",
        "C01",
        "C00",
    ]

    raftGains = {}
    ts8GainDir = os.path.join(getPackageDir("rubintv_production"), "data", "LSST-TS8_gains")
    files = glob.glob(os.path.join(ts8GainDir, f"*_{DATASET_NAME}.fits"))

    for filename in sorted(files):
        # Filenames are like R22_S20_7045D_eotest_results.fits
        # We take just the S01 type part because TS8 has only one raft, and
        # the downstream processing only has the detector names, they don't
        # know which raft they're on, so we key by just the lone detector name.
        detectorName = os.path.basename(filename).split("_")[1]

        with fits.open(filename) as f:
            gains = f[1].data["GAIN"]

        # gains are np.float32 and this raises in isr, so cast to python float
        gains = {ampName: float(gains[i]) for i, ampName in enumerate(ampNameOrder)}

        raftGains[detectorName] = gains

    return raftGains


def gainsToPtcDataset(gains):
    """Given a dict of dict of gains for a single CCD, make a fake PTC dataset.

    The gains as a single dict, keyed by amplifier name, with the gains as
    python floats, not numpy floats. This is the format that the isrTask
    requires.

    This is neeeded in order for there to be a .gain property on the dataset,
    as this is how the isrTask accesses them.

    Parameters
    ----------
    gains : `dict` [`str`, `float`]
        The gains for each amplifier in the detector.

    Returns
    -------
    fakePtcDataset : `FakePtcDataset`
        The fake PTC dataset, which quacks enough like a real one for the
        isrTask.
    """

    class FakePtcDataset:
        def __init__(self, gains):
            self.gains = gains

        @property
        def gain(self):
            return self.gains

        @property
        def ptcTurnoff(self):
            return {ampName: 2e19 for ampName in self.gains.keys()}

    return FakePtcDataset(gains)
