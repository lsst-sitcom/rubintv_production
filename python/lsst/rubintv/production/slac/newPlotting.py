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
import os

import matplotlib.pyplot as plt

from ..uploaders import MultiUploader, Uploader
from ..watchers import RedisWatcher
from .mosaicing import plotFocalPlaneMosaic
from .utils import getCamera, getNumExpectedItems

_LOG = logging.getLogger(__name__)


class Plotter:
    """Channel for producing the plots for the cleanroom on RubinTV.

    This will make plots for whatever it can find, and if the input data forms
    a complete set across the focal plane (taking into account partial
    readouts), deletes the input data, both to tidy up after itself, and to
    signal that this was completely processed and nothing is left to do.

    The Replotter class, which inherits from this one, will replot anything
    that it finds to be complete later on, motivating this to leave any
    incomplete data, so that other processes can make complete plots once their
    input processing has finished.

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

    def __init__(self, butler, locationConfig, instrument, queueName, doRaise=False):
        self.locationConfig = locationConfig
        self.butler = butler
        self.camera = getCamera(self.butler, instrument)
        self.instrument = instrument
        self.uploader = Uploader(self.locationConfig.bucketName)
        self.s3Uploader = MultiUploader()
        self.log = _LOG.getChild(f"plotter_{self.instrument}")
        # currently watching for binnedImage as this is made last
        self.watcher = RedisWatcher(
            butler=butler,
            locationConfig=locationConfig,
            queueName=queueName,
        )
        self.fig = plt.figure(figsize=(12, 12))
        self.doRaise = doRaise
        self.STALE_AGE_SECONDS = 45  # in seconds

    def plotFocalPlane(self, expRecord, dataProduct, timeout):
        """Create a binned mosaic of the full focal plane as a png.

        The binning factor is controlled via the locationConfig.binning
        property.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        dataProduct : `str`
            The data product to use for the plot, either `'postISRCCD'` or
            `'calexp'`.
        timeout : `int`
            The timeout for waiting for the data to be complete.

        Returns
        -------
        filename : `str`
            The filename the plot was saved to.
        """
        expId = expRecord.id
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num

        nExpected = getNumExpectedItems(expRecord, self.log)

        self.fig.clear()

        datapath = None
        match dataProduct:
            case "postISRCCD":
                datapath = self.locationConfig.calculatedDataPath
            case "calexp":
                datapath = self.locationConfig.binnedCalexpPath

        plotName = f"{dataProduct}Mosaic_dayObs_{dayObs}_seqNum_{seqNum}.png"
        saveFile = os.path.join(self.locationConfig.plotPath, plotName)

        plotFocalPlaneMosaic(
            butler=self.butler,
            figure=self.fig,
            expId=expId,
            camera=self.camera,
            binSize=self.locationConfig.binning,
            dataPath=datapath,
            savePlotAs=saveFile,
            nExpected=nExpected,
            timeout=timeout,
            logger=self.log,
        )
        self.log.info(f"Wrote focal plane plot for {expRecord.dataId} to {saveFile}")
        return saveFile

    @staticmethod
    def getInstrumentChannelName(instrument):
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
                return "slac_lsstcam"
            case _:
                raise ValueError(f"Unknown instrument {instrument}")

    def callback(self, payload):
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
        dataId = payload.dataId
        dataProduct = payload.run  # TODO: this really needs improving
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
        self.log.info(f"Making plots for {expRecord.dataId}")
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        instPrefix = self.getInstrumentChannelName(self.instrument)

        plotName = None
        match dataProduct:
            case "postISRCCD":
                plotName = "focal_plane_mosaic"
            case "calexp":
                plotName = "calexp_mosaic"

        focalPlaneFile = self.plotFocalPlane(expRecord, dataProduct, timeout=0)
        if focalPlaneFile:  # only upload on plot success
            self.s3Uploader.uploadPerSeqNumPlot(
                instrument=instPrefix,
                plotName=plotName,
                dayObs=dayObs,
                seqNum=seqNum,
                filename=focalPlaneFile,
            )

    def run(self):
        """Run continuously, calling the callback method with the latest
        expRecord.
        """
        self.watcher.run(self.callback)
