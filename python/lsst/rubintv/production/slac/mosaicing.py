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
import glob
from dataclasses import dataclass
from scipy.optimize import curve_fit
from scipy.ndimage import center_of_mass

from lsst.utils.iteration import ensure_iterable
import lsst.afw.math as afwMath
import lsst.afw.detection as afwDetect
import lsst.afw.geom
from lsst.geom import Box2I, Extent2I
from lsst.afw.cameraGeom import utils as cgu
from lsst.afw.cameraGeom import FOCAL_PLANE, PIXELS
import lsst.pipe.base as pipeBase
from lsst.afw.fits import FitsError
from lsst.summit.utils import getQuantiles

import os
import lsst.afw.image as afwImage
import matplotlib.colors as colors
from matplotlib import cm
import matplotlib.pyplot as plt
from matplotlib.pyplot import subplot_mosaic
from matplotlib.colors import LogNorm
import matplotlib.patches as patches

import numpy as np
from mpl_toolkits.axes_grid1 import make_axes_locatable
from ..utils import isFileWorldWritable

__all__ = (
    'getBinnedFilename',
    'getBinnedImageFiles',
    'getBinnedImageExpIds',
    'writeBinnedImageFromDeferredRefs',
    'writeBinnedImage',
    'readBinnedImage',
    'PreBinnedImageSource',
    'makeMosaic',
    'plotFocalPlaneMosaic',
    'getMosaicImage',
    'GausFitParameters',
    'SpotInfo',
    'analyzeCcobSpotImage',
    'plotCcobSpotInfo',
    'getDetectorForBinnedImageLocation',
)


def getBinnedFilename(expId, instrument, detectorName, dataPath, binSize):
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
    return os.path.join(dataPath, f'{expId}_{instrument}_{detectorName}_binned_{binSize}.fits')


def getBinnedImageFiles(path, instrument, expId=None):
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
    if expId is None:
        expId = ''
    pattern = os.path.join(path, f'{expId}*{instrument}*binned*')
    binnedImages = glob.glob(pattern)
    return binnedImages


def getBinnedImageExpIds(path, instrument):
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
    expIds = sorted(set([int(os.path.basename(f).split('_')[0]) for f in binnedImages]))
    return expIds


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
        instrument = dRef.dataId['instrument']
        writeBinnedImage(exp, instrument=instrument, outputPath=outputPath, binSize=binSize)


def writeBinnedImage(exp, instrument, outputPath, binSize):
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


def readBinnedImage(expId, instrument, detectorName, dataPath, binSize, deleteAfterReading, logger=None):
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

    def __init__(self, expId, instrument, dataPath, binSize, deleteAfterReading):
        self.expId = expId
        self.instrument = instrument
        self.dataPath = dataPath
        self.binSize = binSize
        self.deleteAfterReading = deleteAfterReading

    def getCcdImage(self, det, imageFactory, binSize, *args, **kwargs):
        """Call signature is required by camGeom.utils.showCamera(imageSource),
        but we don't use the arguments, e.g. imageFactory.
        """
        assert binSize == self.binSize
        detName = det.getName()
        binnedImage = readBinnedImage(expId=self.expId,
                                      instrument=self.instrument,
                                      detectorName=detName,
                                      dataPath=self.dataPath,
                                      binSize=binSize,
                                      deleteAfterReading=self.deleteAfterReading)
        return afwMath.rotateImageBy90(binnedImage, det.getOrientation().getNQuarter()), det


def makeMosaic(deferredDatasetRefs,
               camera,
               binSize,
               dataPath,
               timeout,
               nExpected,
               deleteIfComplete,
               deleteRegardless,
               logger=None):
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
        detNum = dRef.dataId['detector']
        expIds.add(dRef.dataId['exposure'])  # to check they all match
        detName = camera[detNum].getName()
        detectorNameList.append(detName)

    if len(expIds) != 1:
        raise ValueError(f"Expected only one exposure, got {expIds}!")
    expId = expIds.pop()

    # initially, deleteAfterReading *MUST* be False, because unless all the
    # files are there immediately, we will end up chewing holes in the mosaic
    # just by waiting for them in a loop!
    imageSource = PreBinnedImageSource(expId, instrument, dataPath, binSize=binSize,
                                       deleteAfterReading=False)

    success = False
    firstWarn = True
    waitTime = -0.000001  # start at minus 1 microsec as an easy fix for the first loop for timeouts of zero
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

    if success and deleteIfComplete and (len(detectorNameList) == nExpected):
        # Remaking the image just to delete the files is pretty gross, but it's
        # very fast and the only simple way of deleting all the files
        imageSource = PreBinnedImageSource(expId, instrument, dataPath, binSize=binSize,
                                           deleteAfterReading=True)
        output_mosaic = cgu.showCamera(camera,
                                       imageSource=imageSource,
                                       detectorNameList=detectorNameList,
                                       binSize=binSize)

    if not success:
        # we're *not* complete, so remake the image source with the delete
        # option only set if we're deleting regardless
        imageSource = PreBinnedImageSource(expId, instrument, dataPath, binSize=binSize,
                                           deleteAfterReading=deleteRegardless)

        # make what you can based on what actually did arrive on disk
        logger.warning(f"Failed to find one or more files for mosaic of {expId},"
                       f" making what is possible, based on the files found after timeout.")
        detectorNameList = _getDetectorNamesWithData(expId, camera, dataPath, binSize)

        if len(detectorNameList) == 0:
            logger.warning(f"Found {len(detectorNameList)} binned detector images, so no mosaic can be made.")
            return pipeBase.Struct(output_mosaic=None)

        logger.info(f"Making mosaic with {len(detectorNameList)} detectors")
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
    instrument = camera.getName()
    detNames = [det.getName() for det in camera]
    existingNames = [detName for detName in detNames if
                     os.path.exists(getBinnedFilename(expId, instrument, detName, dataPath, binSize))]
    return existingNames


def plotFocalPlaneMosaic(butler,
                         figure,
                         expId,
                         camera,
                         binSize,
                         dataPath,
                         savePlotAs,
                         nExpected,
                         timeout,
                         deleteIfComplete=True,
                         deleteRegardless=False,
                         logger=None):
    """Save a full focal plane binned mosaic image for a given expId.

    The binned images must have been created upstream with the correct binning
    factor, as this uses a PreBinnedImageSource.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    figure : `matplotlib.figure.Figure`
        The figure to plot on.
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
    existingNames : `list` of `str`
        The detector names for which binned images exist.
    """
    if not logger:
        logger = logging.getLogger('lsst.rubintv.production.slac.mosaicing.plotFocalPlaneMosaic')

    mosaic = getMosaicImage(butler=butler,
                            expId=expId,
                            camera=camera,
                            binSize=binSize,
                            dataPath=dataPath,
                            nExpected=nExpected,
                            timeout=timeout,
                            deleteIfComplete=deleteIfComplete,
                            deleteRegardless=deleteRegardless,
                            logger=logger)

    if mosaic is None:
        logger.warning(f"Failed to make mosaic for {expId}")
        return
    logger.info(f"Made mosaic image for {expId}")
    _plotFpMosaic(mosaic, fig=figure, saveAs=savePlotAs)
    logger.info(f"Saved mosaic image for {expId} to {savePlotAs}")


def getMosaicImage(butler,
                   expId,
                   camera,
                   binSize,
                   dataPath,
                   nExpected,
                   timeout,
                   deleteIfComplete=True,
                   deleteRegardless=False,
                   logger=None):
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
    nExpected : `int`
        The number of CCDs expected in the mosaic.
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
        The binned mosaiced image.
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
    deferredDrefs = [butler.getDeferred(d) for d in dRefs]

    mosaic = makeMosaic(deferredDrefs,
                        camera,
                        binSize,
                        dataPath,
                        timeout,
                        nExpected=nExpected,
                        deleteIfComplete=deleteIfComplete,
                        deleteRegardless=deleteRegardless,
                        logger=logger
                        ).output_mosaic
    if mosaic is None:
        logger.warning(f"Failed to make mosaic for {expId}")

    return mosaic


def _plotFpMosaic(im, fig, scalingOption='CCS', saveAs=''):
    """Plot the focal plane mosaic, optionally saving as a png.

    Parameters
    ----------
    im : `lsst.afw.image.Image`
        The focal plane mosaiced image to render.
    fig : `matplotlib.figure.Figure`
        The figure to plot on.
    saveAs : `str`, optional
        The filename to save the plot as.
    """
    data = im.array
    ax = fig.gca()
    ax.clear()

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

    im = ax.imshow(data, norm=norm, interpolation='None', cmap=cmap, origin='lower')

    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    fig.colorbar(im, cax=cax)

    fig.tight_layout()
    if saveAs:
        fig.savefig(saveAs)


@dataclass(slots=True, kw_only=True, frozen=True)
class GausFitParameters:
    goodFit: bool
    mean: float
    sigma: float
    amplitude: float


@dataclass(slots=True, kw_only=True, frozen=True)
class SpotInfo:
    flux: float
    centerOfMass: tuple
    footprint: lsst.afw.detection.Footprint
    xFitPars: GausFitParameters
    yFitPars: GausFitParameters


def gauss(x, a, x0, sigma):
    return a*np.exp(-(x-x0)**2/(2*sigma**2))


def analyzeCcobSpotImage(image, binning, threshold=100, nPixMin=3000, logger=None):
    """Calculate the spot flux, position and shape for a CCOB narrow beam spot.

    The spot is assumed to be the brightest object in the image, and the
    position is calculated from the center of mass of the footprint. The
    shape is calculated by fitting a Gaussian to the x and y slices through
    the center of the footprint.

    Parameters
    ----------
    image : `lsst.afw.image.Image`
        The mosaiced image.
    binning : `int`
        The binning factor.
    threshold : `float`
        The threshold in counts (which will be in electrons if the gain was
        applied when creating the image, and in ADU if not).
    nPixMin : `int`
        The minimum number of pixels in a footprint.
    logger : `logging.Logger`, optional
        The logger, created if needed and not provided.

    Returns
    -------
    spotInfo : `lsst.rubintv.production.slac.mosaicing.SpotInfo`
        The spot information, or ``None`` if no spot was found.
    """
    maskedImage = afwImage.MaskedImageF(image)  # detection has to have a masked image

    threshold = afwDetect.Threshold(threshold, afwDetect.Threshold.VALUE)
    footPrintSet = afwDetect.FootprintSet(maskedImage, threshold, "DETECTED", nPixMin)
    footprints = footPrintSet.getFootprints()
    nFootprints = len(footprints)

    if nFootprints == 0:
        logger.warning(f'Found no footprints in exposure with {threshold=} and {nPixMin=}')
        return

    footprint = footprints[0]  # assume we have one
    if nFootprints > 1:  # but if we have more, find the brightest
        logger.info(f'Found {nFootprints} footprints in exposure with {threshold=} and {nPixMin=},'
                    ' selecting the brightest')
        footprints = sorted(footprints, key=lambda fp: fp.computeFluxFromImage(image), reverse=True)
        footprint = footprints[0]
    flux = footprint.computeFluxFromImage(image)

    cutoutCenterOfMass = center_of_mass(image[footprint.getBBox()].array)
    xy0 = footprint.getBBox().getBegin()
    centerOfMass = (cutoutCenterOfMass[0] + xy0[0], cutoutCenterOfMass[1] + xy0[1])

    bbox = footprint.getBBox()
    xSlice = image[bbox].array[:, bbox.getWidth()//2]
    ySlice = image[bbox].array[bbox.getWidth()//2, :]

    fits = []
    bounds = ((0, 0, 0), (np.inf, np.inf, np.inf))
    for data in (xSlice, ySlice):
        peakPos = len(data)//2
        width = 25
        amplitude = np.max(data)

        try:
            pars, _ = curve_fit(gauss, np.arange(len(data)), data, [amplitude, peakPos, width], bounds=bounds)
            fits.append(GausFitParameters(goodFit=True,
                                          mean=pars[1],
                                          sigma=np.abs(pars[2]),
                                          amplitude=np.abs(pars[0])))
        except RuntimeError:
            fits.append(GausFitParameters(goodFit=False,
                                          mean=np.nan,
                                          sigma=np.nan,
                                          amplitude=np.nan))

    spotInfo = SpotInfo(
        flux=flux * binning**2,
        centerOfMass=centerOfMass,
        footprint=footprint,
        xFitPars=fits[0],
        yFitPars=fits[1],
    )
    return spotInfo


def plotCcobSpotInfo(image, spotInfo, boxSizeMin=150, fig=None, saveAs='', logger=None):
    """Plot the analyzed CCOB spot profile.

    Parameters
    ----------
    image : `lsst.afw.image.Image`
        The mosaiced image.
    spotInfo : `lsst.rubintv.production.slac.mosaicing.SpotInfo`
        The spot information.
    saveAs : `str`
        The filename to save the plot as.
    logger : `logging.Logger`, optional
        The logger, created if needed and not provided.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        The figure.
    """
    flux = spotInfo.flux
    center = spotInfo.centerOfMass
    footprint = spotInfo.footprint
    fpBbox = footprint.getBBox()
    spotArea = footprint.getArea()
    approx_radius = np.sqrt(spotArea/np.pi)

    # make a square box of at least the min size, centered on the original bbox
    size = max(boxSizeMin, fpBbox.width, fpBbox.height)
    extent = Extent2I(size, size)
    bbox = Box2I.makeCenteredBox(fpBbox.getCenter(), extent)
    bbox = bbox.clippedTo(image.getBBox())  # ensure we never overrun the image array

    if fig is None:
        if logger is None:
            logger = logging.getLogger(__name__)
        logger.warning("Making new matplotlib figure - if this is in a loop you're going to have a bad time."
                       " Pass in a figure with fig = plt.figure(figsize=(15, 10)) to avoid this warning.")
        fig, axs = subplot_mosaic(
            """
            AAB
            AAC
            """,
            figsize=(20, 10)
        )
    else:
        axs = fig.subplot_mosaic(
            """
            AAB
            AAC
            """
        )

    norm = LogNorm(vmin=1, vmax=1e4)  # same for both plots
    aspect = 'equal'
    fitAlpha = 0.5

    # main image plot
    axs["A"].imshow(image.array,
                    norm=norm,
                    aspect=aspect,
                    origin="lower")
    rect = patches.Rectangle((bbox.getMinX(), bbox.getMinY()),
                             bbox.getWidth(),
                             bbox.getHeight(),
                             linewidth=1,
                             edgecolor='r',
                             facecolor='none')
    axs["A"].add_patch(rect)
    text = (f'Total electrons in spot: {flux/1e6:.1f} Me-\n'
            f'Center of spot (binned coords): x={center[0]:.0f}, y={center[1]:.0f}\n'
            f'Spot area: {spotArea:.0f} binned px^2\n'
            f'Approx radius: {approx_radius:.1f} binned px\n'
            f'x fit width: {spotInfo.xFitPars.sigma:.1f} binned px sigma\n'
            f'y fit width: {spotInfo.yFitPars.sigma:.1f} binned px sigma')
    axs["A"].annotate(text,
                      xy=(bbox.getMaxX(), bbox.getMaxY()),
                      xycoords='data',
                      xytext=(10, 10),
                      textcoords='offset points',
                      bbox=dict(boxstyle="square,pad=0.3", fc="lightblue", ec="steelblue", lw=2))
    axs["A"].set_title("Assembled image with spot details")

    # spot zoom-in plot
    axs["B"].set_title("CCOB spot close-up")
    axRef = axs["B"].imshow(image[bbox].array,
                            norm=norm,
                            aspect=aspect,
                            origin="lower")
    divider = make_axes_locatable(axs["B"])
    cax = divider.append_axes("right", size="5%", pad=0.05)
    _ = plt.colorbar(axRef, cax=cax)

    # xy profile plot and fitting
    xSlice = image[fpBbox].array[:, fpBbox.getWidth()//2]
    ySlice = image[fpBbox].array[fpBbox.getWidth()//2, :]
    xs = np.arange(len(xSlice))
    ys = np.arange(len(ySlice))

    axs["C"].plot(xSlice, c='r', ls='-', label="X profile", )
    if spotInfo.xFitPars.goodFit:
        xFitline = gauss(xs, spotInfo.xFitPars.amplitude, spotInfo.xFitPars.mean, spotInfo.xFitPars.sigma)
        axs["C"].plot(xs, xFitline, c='r', ls='--', alpha=fitAlpha, label="X-profile Gaussian fit",)

    axs["C"].plot(ySlice, c='b', ls='-', label="Y profile")
    if spotInfo.yFitPars.goodFit:
        yFitline = gauss(ys, spotInfo.yFitPars.amplitude, spotInfo.yFitPars.mean, spotInfo.yFitPars.sigma)
        axs["C"].plot(ys, yFitline, c='b', ls='--', alpha=fitAlpha, label="Y-profile Gaussian fit")

    axs["C"].legend()
    axs["C"].set_title("X/Y slices of the spot profile")
    if spotInfo.xFitPars.goodFit or spotInfo.yFitPars.goodFit:
        axs["C"].set_ylim(100, 1.1*max((max(yFitline), max(xFitline))))
    plt.tight_layout()
    plt.show()
    if saveAs:
        fig.savefig(saveAs)

    return fig


def getDetectorForBinnedImageLocation(image, location, binning, camera, logger=None):
    """Get the detector which contains a point in a binned image.

    Binned mosaic images contain chip gaps, and so working out which detector
    a given location falls on is not trivial. This function uses the camera
    model to work out which detector a given location falls on.

    Parameters
    ----------
    image : `lsst.afw.image.Image`
        The image on which the location was found.
    location : `tuple` of `float`
        The location in the binned mosaic image to get the detector for.
    binning : `int`
        The binning factor for the binned mosaiced image.
    camera : `lsst.afw.cameraGeom.Camera`
        The camera model, containing the detectors.
    logger : `logging.Logger`, optional
        The logger for warning if the chip isn't found. Created if needed and
        not supplied.

    Returns
    -------
    detector : `lsst.afw.cameraGeom.Detector`
        The detector on which the location falls, or ``None`` if not found (for
        example if the location is outside of the focal plane, or falls in a
        chip gap).
    """
    plateScale = 1/100  # mm/pixel
    camCenter = image.getBBox().getCenter()
    # need to offset to the camera center for the transform to work as that is
    # centered at (0, 0), i.e. with the left half of the camera at negative x
    centroidToUse = ((location[0]-camCenter[0])*binning*plateScale,
                     (location[1]-camCenter[1])*binning*plateScale)

    for det in camera:
        xy = det.getTransform(FOCAL_PLANE, PIXELS).getMapping().applyForward(centroidToUse)
        if det.getBBox().contains(xy[0], xy[1]):
            return det

    if logger is None:
        logger = logging.getLogger(__name__)
    logger.warning(f"Could not find detector for {location=}")
    return None
