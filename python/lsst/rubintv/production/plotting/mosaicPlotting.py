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

__all__ = ["Plotter"]

import logging
from typing import TYPE_CHECKING, Any

import lsst.afw.display as afwDisplay
from lsst.summit.utils.utils import getCameraFromInstrumentName
from lsst.utils.plotting.figures import make_figure

from ..redisUtils import RedisHelper
from ..uploaders import MultiUploader
from ..utils import LocationConfig, makeFocalPlaneTitle, makePlotFile
from ..watchers import RedisWatcher
from .mosaicing import plotFocalPlaneMosaic

if TYPE_CHECKING:
    from logging import Logger

    from lsst.afw.cameraGeom import Camera
    from lsst.daf.butler import Butler, DimensionRecord

    from ..payloads import Payload
    from ..podDefinition import PodDetails


_LOG = logging.getLogger(__name__)


class Plotter:
    """Channel for producing the plots for the cleanroom on RubinTV.

    This will make plots for whatever it can find, and if the input data forms
    a complete set across the focal plane (taking into account partial
    readouts), deletes the input data, both to tidy up after itself, and to
    signal that this was completely processed and nothing is left to do.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration.
    instrument : `str`
        The instrument.
    doRaise : `bool`
        If True, raise exceptions instead of logging them.
    """

    def __init__(
        self,
        butler: Butler,
        locationConfig: LocationConfig,
        instrument: str,
        podDetails: PodDetails,
        doRaise=False,
    ) -> None:
        self.locationConfig: LocationConfig = locationConfig
        self.butler: Butler = butler
        self.camera: Camera = getCameraFromInstrumentName(instrument)
        self.instrument: str = instrument
        self.s3Uploader: MultiUploader = MultiUploader()
        self.log: Logger = _LOG.getChild(f"plotter_{self.instrument}")
        # currently watching for binnedImage as this is made last
        self.watcher: RedisWatcher = RedisWatcher(
            butler=butler,
            locationConfig=locationConfig,
            podDetails=podDetails,
        )
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)
        self.afwDisplay = afwDisplay.getDisplay(backend="matplotlib", figsize=(20, 20))
        self.doRaise = doRaise
        self.STALE_AGE_SECONDS = 45  # in seconds

    def plotFocalPlane(self, expRecord: DimensionRecord, dataProduct: str) -> str:
        """Create a binned mosaic of the full focal plane as a png.

        The binning factor is controlled via the locationConfig.binning
        property.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        dataProduct : `str`
            The data product to use for the plot, either `'post_isr_image'` or
            `'preliminary_visit_image'`.

        Returns
        -------
        filename : `str`
            The filename the plot was saved to, or "" if the plot failed.
        """
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        nExpected = len(self.redisHelper.getExpectedDetectors(self.instrument, expRecord.id, who="ISR"))

        stretch = "CCS"
        displayToUse: Any = None
        plotName = "unknown"
        match dataProduct:
            case "post_isr_image":
                stretch = "CCS"
                displayToUse = make_figure(figsize=(12, 12))
                plotName = "focal_plane_mosaic"
            case "preliminary_visit_image":
                stretch = "zscale"
                displayToUse = self.afwDisplay
                # this name is used by RubinTV internally - do not change
                # without both changing the frontend code and also renaming
                # every item with this name in the buckets at all locations
                plotName = "calexp_mosaic"
            case _:
                raise ValueError(f"Unknown data product: {dataProduct}")

        saveFile = makePlotFile(self.locationConfig, self.instrument, dayObs, seqNum, plotName, "jpg")
        title = makeFocalPlaneTitle(expRecord)

        image = plotFocalPlaneMosaic(
            butler=self.butler,
            figureOrDisplay=displayToUse,
            dayObs=dayObs,
            seqNum=seqNum,
            camera=self.camera,
            binSize=self.locationConfig.binning,
            dataProduct=dataProduct,
            savePlotAs=saveFile,
            nExpected=nExpected,
            title=title,
            deleteIfComplete=True,
            stretch=stretch,
            locationConfig=self.locationConfig,
        )
        if image is not None:
            self.log.info(f"Wrote focal plane plot for {expRecord.dataId} to {saveFile}")
            return saveFile
        else:
            self.log.warning(f"Failed to make plot for {expRecord.dataId}")
            return ""

    @staticmethod
    def getInstrumentChannelName(instrument: str) -> str:
        """Get the instrument channel name for the current instrument.

        This is the plot prefix to use for upload.

        Parameters
        ----------
        instrument : `str`
            The instrument name, e.g. 'LSSTCam'.

        Returns
        -------
        channel : `str`
            The channel prefix name.
        """
        # TODO: remove this whole method once RubinTV v2 uses real instrument
        # names
        match instrument:
            case "LSST-TS8":
                return "ts8"
            case "LSSTComCam":
                return "comcam"
            case "LSSTComCamSim":
                return "comcam_sim"
            case "LSSTCam":
                return "lsstcam"
            case _:
                raise ValueError(f"Unknown instrument {instrument}")

    def callback(self, payload: Payload) -> None:
        """Method called on each new expRecord as it is found in the repo.

        Note: the callback is used elsewhere to reprocess old data, so the
        default doX kwargs are all set to False, but are overrided to True in
        this class' run() method, such that replotting code sets what it *does*
        want to True, rather than having to know to set everything it *doesn't*
        want to False. This might feel a little counterintuitive here, but it
        makes the replotting code much more natural.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        doPlotMosaic : `bool`
            If True, plot and upload the focal plane mosaic.
        doPlotNoises : `bool`
            If True, plot and upload the per-amplifier noise map.
        timeout : `float`
            How to wait for data products to land before giving up and plotting
            what we have.
        """
        dataId = payload.dataIds[0]
        assert len(payload.dataIds) == 1, "Expected only one dataId - plotting doesn't support multiple"
        dataProduct = payload.run  # TODO: this really needs improving
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
        self.log.info(f"Making plots for {expRecord.dataId}")
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        instPrefix = self.getInstrumentChannelName(self.instrument)

        plotName = None
        match dataProduct:
            case "preliminary_visit_image":
                # this name is used by RubinTV internally - do not change
                # without both changing the frontend code and also renaming
                # every item with this name in the buckets at all locations
                plotName = "calexp_mosaic"
            case "post_isr_image":
                plotName = "focal_plane_mosaic"

        focalPlaneFile = self.plotFocalPlane(expRecord, dataProduct)
        if focalPlaneFile:  # only upload on plot success
            self.s3Uploader.uploadPerSeqNumPlot(
                instrument=instPrefix,
                plotName=plotName,
                dayObs=dayObs,
                seqNum=seqNum,
                filename=focalPlaneFile,
            )

    def run(self) -> None:
        """Run continuously, calling the callback method with the latest
        expRecord.
        """
        self.watcher.run(self.callback)
