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

import glob
import logging
import os
import time
from typing import TYPE_CHECKING, Any

import matplotlib.colors as colors
import numpy as np
from matplotlib import cm
from mpl_toolkits.axes_grid1 import make_axes_locatable

import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.pipe.base as pipeBase
from lsst.afw.cameraGeom import utils as cgu
from lsst.afw.fits import FitsError
from lsst.summit.utils import getQuantiles
from lsst.utils.iteration import ensure_iterable

from ..utils import isFileWorldWritable

if TYPE_CHECKING:
    from logging import Logger

    from matplotlib.pyplot import Figure, Normalize

    from lsst.afw.cameraGeom import Camera, Detector
    from lsst.afw.display import Display
    from lsst.afw.image import Exposure, Image
    from lsst.daf.butler import Butler
    from lsst.pipe.base import DeferredDatasetRef, Struct


def getBinnedFilename(expId: int, instrument: str, detectorName: str, dataPath: str, binSize: int) -> str:
    """Get the full path and filename for a binned image.

    Parameters
    ----------
    expId : `int`
        The exposure id.
    instrument : `str`
        The instrument name, e.g. 'LSSTCam'.
    detectorName : `str`
        The detector name, e.g. 'R22_S11'.
    dataPath : `str`
        The path on disk to write to or find the pre-binned images.
    binSize : `int`
        The binning factor.
    """
    return os.path.join(dataPath, f"{expId}_{instrument}_{detectorName}_binned_{binSize}.fits")


def getBinnedImageFiles(path: str, instrument: str, expId: int | None = None) -> list[str]:
    """Get a list of the binned image files for a given instrument.

    Optionally filters to only return the matching expId if expId is
    supplied. If expId is not supplied, all binned images are returned.

    Parameters
    ----------
    path : `str`
        The path to search for binned images.
    instrument : `str`
        The instrument name, e.g. 'LSSTCam'.
    expId : `int`, optional
        The exposure ID to filter on.
    """
    expIdToUse = f"{expId}"
    if expId is None:
        expIdToUse = ""
    pattern = os.path.join(path, f"{expIdToUse}*{instrument}*binned*")
    binnedImages = glob.glob(pattern)
    return binnedImages


def getBinnedImageExpIds(path: str, instrument: str) -> list[int]:
    """Get a list of the exposure IDs for which binned images exist.

    Parameters
    ----------
    path : `str`
        The path to search for binned images.
    instrument : `str`
        The instrument name, e.g. 'LSSTCam'.

    Returns
    -------
    expIds : `list` [`int`]
        The list of exposure IDs.
    """
    binnedImages = getBinnedImageFiles(path, instrument)
    expIds = sorted(set([int(os.path.basename(f).split("_")[0]) for f in binnedImages]))
    return expIds


def writeBinnedImageFromDeferredRefs(
    deferredDatasetRefs: list[DeferredDatasetRef], outputPath: str, binSize: int
) -> None:
    """Write a binned image out for a single or list of deferredDatasetRefs.

    Parameters
    ----------
    deferredDatasetRefs : `lsst.daf.butler.DeferredDatasetRef` or
                          `list` [`lsst.daf.butler.DeferredDatasetRef`]
        The dataRef(s).
    outputPath : `str`
        The path on disk to write the binned images to.
    binSize : `int`
        The binning factor.
    """
    deferredDatasetRefs = list(ensure_iterable(deferredDatasetRefs))
    for dRef in deferredDatasetRefs:
        exp = dRef.get()
        instrument = dRef.dataId["instrument"]
        writeBinnedImage(exp, instrument=instrument, outputPath=outputPath, binSize=binSize)


def writeBinnedImage(exp: Exposure, instrument: str, outputPath: str, binSize: int) -> None:
    """Bin an image and write it to disk.

    The image is binned by ``binSize`` and written to ``outputPath`` according
    to the detector name and exposure id.

    Parameters
    ----------
    exp : `lsst.afw.image.Exposure`
        The exposure to bin.
    instrument : `str`
        The instrument name, e.g. 'LSSTCam'.
    outputPath : `str`
        The path on disk to write the binned image to.
    binSize : `int`
        The binning factor.

    Notes
    -----
    It would be easy to make this take images rather than exposures, if needed,
    it would just require the detector name and expId to be passed in.
    """
    if not isinstance(exp, afwImage.Exposure):
        raise ValueError(f"exp must be an Exposure, got {type(exp)}")
    binnedImage = afwMath.binImage(exp.image, binSize)  # turns the exp into an afwImage.Image

    expId = exp.visitInfo.id  # note this is *not* exp.info.id, as that has the detNum on the end!
    detName = exp.detector.getName()
    outFilename = getBinnedFilename(expId, instrument, detName, outputPath, binSize)
    binnedImage.writeFits(outFilename)

    if not isFileWorldWritable(outFilename):
        os.chmod(outFilename, 0o777)


def readBinnedImage(
    expId: int,
    instrument: str,
    detectorName: str,
    dataPath: str,
    binSize: int,
    deleteAfterReading: bool,
    logger: Logger | None = None,
) -> Image:
    """Read a pre-binned image in from disk.

    Parameters
    ----------
    expId : `int`
        The exposure id.
    instrument : `str`
        The instrument name, e.g. 'LSSTCam'.
    detectorName : `str`
        The detector name, e.g. 'R22_S11'.
    dataPath : `str`
        The path on disk to find the pre-binned images.
    binSize : `int`
        The binning factor.
    deleteAfterReading : `bool`
        Whether to delete the file after reading it.
    logger : `logging.Logger`, optional
        The logger to use.

    Returns
    -------
    image : `lsst.afw.image.ImageF`
        The binned image.
    """
    filename = getBinnedFilename(expId, instrument, detectorName, dataPath, binSize)
    image = afwImage.ImageF(filename)
    if deleteAfterReading:
        try:
            os.remove(filename)
        except Exception:
            if logger is None:
                logger = logging.getLogger(__name__)
            logger.exception(f"Could not delete {filename}")
    return image


class PreBinnedImageSource:
    """An ImageSource for use in afw.cameraGeom.utils.showCamera

    Reads in pre-binned images from disk. Obviously, they must have already
    been created elsewhere, and with the correct binning factor.

    Parameters
    ----------
    expId : `int`
        The exposure id.
    instrument : `str`
        The instrument name, e.g. 'LSSTCam'.
    dataPath : `str`
        The path to the written files on disk.
    binSize : `int`
        The bin size.
    """

    isTrimmed = True  # required attribute camGeom.utils.showCamera(imageSource)
    background = np.nan  # required attribute camGeom.utils.showCamera(imageSource)

    def __init__(
        self, expId: int, instrument: str, dataPath: str, binSize: int, deleteAfterReading: bool
    ) -> None:
        self.expId = expId
        self.instrument = instrument
        self.dataPath = dataPath
        self.binSize = binSize
        self.deleteAfterReading = deleteAfterReading

    def getCcdImage(
        self, det: Detector, imageFactory: Any, binSize: int, *args, **kwargs
    ) -> tuple[Image, Detector]:
        """Call signature is required by camGeom.utils.showCamera(imageSource),
        but we don't use the arguments, e.g. imageFactory.
        """
        assert binSize == self.binSize
        detName = det.getName()
        binnedImage = readBinnedImage(
            expId=self.expId,
            instrument=self.instrument,
            detectorName=detName,
            dataPath=self.dataPath,
            binSize=binSize,
            deleteAfterReading=self.deleteAfterReading,
        )
        return afwMath.rotateImageBy90(binnedImage, det.getOrientation().getNQuarter()), det


def makeMosaic(
    deferredDatasetRefs: list[DeferredDatasetRef],
    camera: Camera,
    binSize: int,
    dataPath: str,
    timeout: float,
    nExpected: int,
    deleteIfComplete: bool,
    deleteRegardless: bool,
    logger: Logger | None = None,
) -> Struct:
    """Make a binned mosaic image from a list of deferredDatasetRefs.

    The binsize must match the binning used to write the images to disk
    upstream. This is controlled by ``LocationConfig.binning``.

    Parameters
    ----------
    deferredDatasetRefs : `list` of `lsst.daf.butler.DeferredDatasetRef`
        List of deferredDatasetRefs to make the mosaic from.
    camera : `lsst.afw.cameraGeom.Camera`
        The camera model, used for quick lookup of the detectors.
    binSize : `int`
        The binning factor.
    dataPath : `str`
        The path on disk to find the binned images.
    timeout : `float`
        The maximum time to wait for the images to land.
    nExpected : `int`
        The number of CCDs expected in the mosaic.
    deleteIfComplete : `bool`, optional
        If True, delete the binned image files if the number of expected files
        is the number which was found.
    deleteRegardless : `bool`, optional
        If True, delete the binned images regardless of how many are found.
    logger : `logging.Logger`, optional
        The logger, created if not provided.
    deleteAfterReading : `bool`
        Whether to delete the binned images after reading them.

    Returns
    -------
    result : `lsst.pipe.base.Struct`
        A pipeBase struct containing the ``output_mosaic`` as an
        `lsst.afw.image.Image`, or `None` if the mosaic could not be made.

    Notes
    -----
    Tricks used for speed:
        Pulling the detector names from a butler.get(component='detector')
        takes ~8s for 189 CCDs. Using the dRef.dataId['detector'] and getting
        the name from the camera is ~instant.

        Create an ImageSource which reads the pre-binned image straight from
        disk.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    instrument = camera.getName()

    detectorNameList = []
    expIds = set()

    for dRef in deferredDatasetRefs:
        detNum = dRef.dataId["detector"]
        expIds.add(dRef.dataId["exposure"])  # to check they all match
        detName = camera[detNum].getName()
        detectorNameList.append(detName)

    if len(expIds) != 1:
        raise ValueError(f"Expected only one exposure, got {expIds}!")
    expId = expIds.pop()

    # initially, deleteAfterReading *MUST* be False, because unless all the
    # files are there immediately, we will end up chewing holes in the mosaic
    # just by waiting for them in a loop!
    imageSource = PreBinnedImageSource(expId, instrument, dataPath, binSize=binSize, deleteAfterReading=False)

    success = False
    firstWarn = True
    waitTime = -0.000001  # start at minus 1 microsec as an easy fix for the first loop for timeouts of zero
    startTime = time.time()
    output_mosaic = None
    while (not success) and (waitTime < timeout):
        try:
            # keep trying while we wait for data to finish landing
            # the call to showCamera is extremely fast so no harm in keeping
            # trying.
            output_mosaic = cgu.showCamera(
                camera, imageSource=imageSource, detectorNameList=detectorNameList, binSize=binSize
            )
            success = True
        except (FileNotFoundError, FitsError):
            if firstWarn:
                logger.warning(
                    f"Failed to find one or more files for mosaic of {expId},"
                    f" waiting a maximum of {timeout} seconds for data to arrive."
                )
                firstWarn = False
            waitTime = time.time() - startTime
            time.sleep(0.5)
            continue

    if success and deleteIfComplete and (len(detectorNameList) == nExpected):
        # Remaking the image just to delete the files is pretty gross, but it's
        # very fast and the only simple way of deleting all the files
        imageSource = PreBinnedImageSource(
            expId, instrument, dataPath, binSize=binSize, deleteAfterReading=True
        )
        output_mosaic = cgu.showCamera(
            camera, imageSource=imageSource, detectorNameList=detectorNameList, binSize=binSize
        )

    if not success:
        # we're *not* complete, so remake the image source with the delete
        # option only set if we're deleting regardless
        imageSource = PreBinnedImageSource(
            expId, instrument, dataPath, binSize=binSize, deleteAfterReading=deleteRegardless
        )

        # make what you can based on what actually did arrive on disk
        logger.warning(
            f"Failed to find one or more files for mosaic of {expId},"
            f" making what is possible, based on the files found after timeout."
        )
        detectorNameList = _getDetectorNamesWithData(expId, camera, dataPath, binSize)

        if len(detectorNameList) == 0:
            logger.warning(f"Found {len(detectorNameList)} binned detector images, so no mosaic can be made.")
            return pipeBase.Struct(output_mosaic=None)

        logger.info(f"Making mosaic with {len(detectorNameList)} detectors")
        output_mosaic = cgu.showCamera(
            camera, imageSource=imageSource, detectorNameList=detectorNameList, binSize=binSize
        )

    return pipeBase.Struct(output_mosaic=output_mosaic)


def _getDetectorNamesWithData(expId: int, camera: Camera, dataPath: str, binSize: int) -> list[str]:
    """Check for existing binned image files and return the detector names
    for those with data.

    Parameters
    ----------
    expId : `int`
        The exposure id.
    camera : `lsst.afw.cameraGeom.Camera`
        The camera.
    dataPath : `str`
        The path to the binned images.
    binSize : `int`
        The binning factor.

    Returns
    -------
    existingNames : `list` of `str`
        The detector names for which binned images exist.
    """
    instrument = camera.getName()
    detNames = [det.getName() for det in camera]
    existingNames = [
        detName
        for detName in detNames
        if os.path.exists(getBinnedFilename(expId, instrument, detName, dataPath, binSize))
    ]
    return existingNames


def plotFocalPlaneMosaic(
    butler: Butler,
    figureOrDisplay: Figure | Display,
    expId: int,
    camera: Camera,
    binSize: int,
    dataPath: str,
    savePlotAs: str,
    nExpected: int,
    stretch: str,
    timeout: float,
    deleteIfComplete: bool = True,
    deleteRegardless: bool = False,
    logger: Logger | None = None,
) -> Image | None:
    """Save a full focal plane binned mosaic image for a given expId.

    The binned images must have been created upstream with the correct binning
    factor, as this uses a PreBinnedImageSource.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    figureOrDisplay : `matplotlib.figure.Figure` or `afwDisplay.Display`
        The figure to plot on, or the display to use.
    expId : `int`
        The exposure id.
    camera : `lsst.afw.cameraGeom.Camera`
        The camera.
    binSize : `int`
        The binning factor.
    dataPath : `str`
        The path to the binned images.
    savePlotAs : `str`
        The filename to save the plot as.
    nExpected : `int`
        The number of CCDs expected in the mosaic.
    stretch : `str`
        The scaling option for the plot.
    timeout : `float`
        The maximum time to wait for the images to land.
    deleteIfComplete : `bool`, optional
        If True, delete the binned image files if the number of expected files
        is the number which was found.
    deleteRegardless : `bool`, optional
        If True, delete the binned images regardless of how many are found.
    logger : `logging.Logger`, optional
        The logger, created if not provided.

    Returns
    -------
    mosaic : `lsst.afw.image.Image`
        The mosaiced image.
    """
    if not logger:
        logger = logging.getLogger("lsst.rubintv.production.slac.mosaicing.plotFocalPlaneMosaic")

    where = "exposure=expId"
    # we hardcode "raw" here the per-CCD binned images are written out
    # by the isrRunners to the dataPath, so we are not looking for butler-
    # written postISRCCDs.
    dRefs = list(butler.registry.queryDatasets("raw", where=where, bind={"expId": expId}))

    logger.info(f"Found {len(dRefs)} dRefs for {expId}")
    # sleazy part - if the raw exists then the binned image will get written
    # by the isrRunners. This fact is utilized by the PreBinnedImageSource.
    deferredDrefs = [butler.getDeferred(d) for d in dRefs]

    mosaic = makeMosaic(
        deferredDrefs,
        camera,
        binSize,
        dataPath,
        timeout,
        nExpected=nExpected,
        deleteIfComplete=deleteIfComplete,
        deleteRegardless=deleteRegardless,
        logger=logger,
    ).output_mosaic
    if mosaic is None:
        logger.warning(f"Failed to make mosaic for {expId}")
        return None
    logger.info(f"Made mosaic image for {expId}")
    _plotFpMosaic(mosaic, scalingOption=stretch, figureOrDisplay=figureOrDisplay, saveAs=savePlotAs)
    logger.info(f"Saved mosaic image for {expId} to {savePlotAs}")
    return mosaic


def _plotFpMosaic(
    im: Image, figureOrDisplay: Figure | Display, scalingOption: str = "CCS", saveAs: str = ""
) -> Figure | Display:
    """Plot the focal plane mosaic, optionally saving as a png.

    Parameters
    ----------
    im : `lsst.afw.image.Image`
        The focal plane mosaiced image to render.
    figureOrDisplay : `matplotlib.figure.Figure` or `afwDisplay.Display`
        The figure to plot on.
    scalingOption : `str`, optional
        The scaling option for the plot.
    saveAs : `str`, optional
        The filename to save the plot as.
    """
    useAfwDisplay = scalingOption == "zscale"

    if not useAfwDisplay:  # figureOrDisplay is a matplotlib figure
        if not isinstance(figureOrDisplay, Figure):
            raise ValueError(
                f"Wrong type of figure/display provided {type(figureOrDisplay)}"
                f" for given stretch option {scalingOption}"
            )
        data = im.array
        ax = figureOrDisplay.gca()
        ax.clear()
        # XXX why is this type ignore necessary? Can I fix this?
        cmap = cm.gray  # type: ignore
        norm: Normalize
        match scalingOption:
            case "asinh":

                def _forward(x):
                    return np.arcsinh(x)

                def _inverse(x):
                    return np.sinh(x)

                norm = colors.FuncNorm((_forward, _inverse))

            case "CCS":  # The CCS-style scaling
                quantiles = getQuantiles(im.array, cmap.N)
                norm = colors.BoundaryNorm(quantiles, cmap.N)

            case _:
                raise ValueError(f"Unknown plot scaling option {scalingOption}")
        im = ax.imshow(data, norm=norm, interpolation="None", cmap=cmap, origin="lower")
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("right", size="5%", pad=0.05)
        figureOrDisplay.colorbar(im, cax=cax)

        figureOrDisplay.tight_layout()
        if saveAs:
            figureOrDisplay.savefig(saveAs)
    else:  # figureOrDisplay is an afwDisplay
        if not isinstance(figureOrDisplay, Display):
            raise ValueError(
                f"Wrong type of figure/display provided {type(figureOrDisplay)}"
                f" for given stretch option {scalingOption}"
            )
        figureOrDisplay.scale("asinh", "zscale")
        figureOrDisplay.image(im)
        figureOrDisplay._impl._figure.tight_layout()
        # see if there is something better than this for titles
        # display._impl._figure.axes[0].set_title('title')

        if saveAs:
            figureOrDisplay._impl.savefig(saveAs, dpi=300)

    return figureOrDisplay
