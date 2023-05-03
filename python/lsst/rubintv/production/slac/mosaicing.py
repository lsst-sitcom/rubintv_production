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

import logging
import time
from lsst.utils.iteration import ensure_iterable
import lsst.afw.math as afwMath
from lsst.afw.cameraGeom import utils as cgu
import lsst.pipe.base as pipeBase
from lsst.afw.fits import FitsError
from lsst.summit.utils import getQuantiles

import os
import lsst.afw.image as afwImage
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from matplotlib import cm

import numpy as np
from mpl_toolkits.axes_grid1 import make_axes_locatable


def getBinnedFilename(expId, detectorName, dataPath, binSize):
    """Get the full path and filename for a binned image.

    Parameters
    ----------
    expId : `int`
        The exposure id.
    detectorName : `str`
        The detector name, e.g. 'R22_S11'.
    dataPath : `str`
        The path on disk to write to or find the pre-binned images.
    binSize : `int`
        The binning factor.
    """
    return os.path.join(dataPath, f'{expId}_{detectorName}_binned_{binSize}.fits')


def writeBinnedImageFromDeferredRefs(deferredDatasetRefs, outputPath, binSize):
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
    deferredDatasetRefs = ensure_iterable(deferredDatasetRefs)
    for dRef in deferredDatasetRefs:
        exp = dRef.get()
        writeBinnedImage(exp, outputPath, binSize)


def writeBinnedImage(exp, outputPath, binSize):
    """Bin an image and write it to disk.

    The image is binned by ``binSize`` and written to ``outputPath`` according
    to the detector name and exposure id.

    Parameters
    ----------
    exp : `lsst.afw.image.Exposure`
        The exposure to bin.
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
    outFilename = getBinnedFilename(expId, detName, outputPath, binSize)
    binnedImage.writeFits(outFilename)


def readBinnedImage(expId, detectorName, dataPath, binSize, deleteAfterReading, logger=None):
    """Read a pre-binned image in from disk.

    Parameters
    ----------
    expId : `int`
        The exposure id.
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
    filename = getBinnedFilename(expId, detectorName, dataPath, binSize)
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
    dataPath : `str`
        The path to the written files on disk.
    binSize : `int`
        The bin size.
    """
    isTrimmed = True  # required attribute camGeom.utils.showCamera(imageSource)
    background = np.nan  # required attribute camGeom.utils.showCamera(imageSource)

    def __init__(self, expId, dataPath, binSize, deleteAfterReading):
        self.expId = expId
        self.dataPath = dataPath
        self.binSize = binSize
        self.deleteAfterReading = deleteAfterReading

    def getCcdImage(self, det, imageFactory, binSize, *args, **kwargs):
        """Call signature is required by camGeom.utils.showCamera(imageSource),
        but we don't use the arguments, e.g. imageFactory.
        """
        assert binSize == self.binSize
        detName = det.getName()
        binnedImage = readBinnedImage(self.expId, detName, self.dataPath, binSize,
                                      deleteAfterReading=self.deleteAfterReading)
        return afwMath.rotateImageBy90(binnedImage, det.getOrientation().getNQuarter()), det


def makeMosaic(deferredDatasetRefs, camera, binSize, dataPath, timeout, deleteAfterReading, logger=None):
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

    detectorNameList = []
    expIds = set()

    for dRef in deferredDatasetRefs:
        detNum = dRef.dataId['detector']
        expIds.add(dRef.dataId['exposure'])  # to check they all match
        detName = camera[detNum].getName()
        detectorNameList.append(detName)

    if len(expIds) != 1:
        raise ValueError(f"Expected only one exposure, got {expIds}!")
    expId = expIds.pop()

    imageSource = PreBinnedImageSource(expId, dataPath, binSize=binSize,
                                       deleteAfterReading=deleteAfterReading)

    success = False
    firstWarn = True
    waitTime = 0
    startTime = time.time()
    while (not success) and (waitTime < timeout):
        try:
            # keep trying while we wait for data to finish landing
            # the call to showCamera is extremely fast so no harm in keeping
            # trying.
            output_mosaic = cgu.showCamera(camera,
                                           imageSource=imageSource,
                                           detectorNameList=detectorNameList,
                                           binSize=binSize)
            success = True
        except (FileNotFoundError, FitsError):
            if firstWarn:
                logger.warning(f"Failed to find one or more files for mosaic of {expId},"
                               f" waiting a maximum of {timeout} seconds for data to arrive.")
                firstWarn = False
            waitTime = time.time() - startTime
            time.sleep(0.5)
            continue

    if not success:
        # make what you can based on what actually did arrive on disk
        logger.warning(f"Failed to find one or more files for mosaic of {expId},"
                       f" making what is possible, based on the files found after timeout.")
        detectorNameList = _getDetectorNamesWithData(expId, camera, dataPath, binSize)

        if len(detectorNameList) == 0:
            logger.warning(f"Found {len(detectorNameList)} binned detector images, so no mosaic can be made.")
            return pipeBase.Struct(output_mosaic=None)

        logger.info(f"Making mosiac with {len(detectorNameList)} detectors")
        output_mosaic = cgu.showCamera(camera,
                                       imageSource=imageSource,
                                       detectorNameList=detectorNameList,
                                       binSize=binSize)

    return pipeBase.Struct(output_mosaic=output_mosaic)


def _getDetectorNamesWithData(expId, camera, dataPath, binSize):
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
    detNames = [det.getName() for det in camera]
    existingNames = [detName for detName in detNames if
                     os.path.exists(getBinnedFilename(expId, detName, dataPath, binSize))]
    return existingNames


def plotFocalPlaneMosaic(butler, expId, camera, binSize, dataPath, savePlotAs, doDeleteFiles,
                         timeout=5, logger=None):
    """Save a full focal plane binned mosaic image for a given expId.

    The binned images must have been created upstream with the correct binning
    factor, as this uses a PreBinnedImageSource.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
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
    doDeleteFiles : `bool`
        If True, delete the binned images after making the mosaic.
    timeout : `float`
        The maximum time to wait for the images to land.
    logger : `logging.Logger`, optional
        The logger, created if not provided.

    Returns
    -------
    existingNames : `list` of `str`
        The detector names for which binned images exist.
    """
    if not logger:
        logger = logging.getLogger('lsst.rubintv.production.slac.mosaicing.plotFocalPlaneMosaic')

    where = 'exposure=expId'
    # we hardcode "raw" here the per-CCD binned images are written out
    # by the isrRunners to the dataPath, so we are not looking for butler-
    # written postISRCCDs.
    dRefs = list(butler.registry.queryDatasets('raw',
                                               where=where,
                                               bind={'expId': expId}
                                               ))

    logger.info(f"Found {len(dRefs)} dRefs for {expId}")
    # sleazy part - if the raw exists then the binned image will get written
    # by the isrRunners. This fact is utilized by the PreBinnedImageSource.
    deferredDrefs = [butler.getDirectDeferred(d) for d in dRefs]

    mosaic = makeMosaic(deferredDrefs, camera, binSize, dataPath, timeout,
                        deleteAfterReading=doDeleteFiles).output_mosaic
    if mosaic is None:
        logger.warning(f"Failed to make mosaic for {expId}")
        return
    logger.info(f"Made mosaic image for {expId}")
    _plotFpMosaic(mosaic, saveAs=savePlotAs)
    logger.info(f"Saved mosaic image for {expId} to {savePlotAs}")


def _plotFpMosaic(im, scalingOption='CCS', saveAs=''):
    """Plot the focal plane mosaic, optionally saving as a png.

    Parameters
    ----------
    im : `lsst.afw.image.Image`
        The focal plane mosaiced image to render.
    saveAs : `str`, optional
        The filename to save the plot as.
    """
    data = im.array
    plt.figure(figsize=(16, 16))
    ax = plt.gca()

    cmap = cm.gray
    match scalingOption:
        case "default":
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

    im = plt.imshow(data, norm=norm, interpolation='None', cmap=cmap, origin='lower')

    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    plt.colorbar(im, cax=cax)

    plt.tight_layout()
    if saveAs:
        plt.savefig(saveAs)
