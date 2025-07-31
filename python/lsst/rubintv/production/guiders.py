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

__all__ = [
    "GuiderWorker",
]

from typing import TYPE_CHECKING

from lsst.rubintv.production.baseChannels import BaseButlerChannel

# TODO Change these back to relative imports
from lsst.rubintv.production.utils import LocationConfig, logDuration, makePlotFile, writeMetadataShard
from lsst.summit.utils.guiders.detection import GuiderStarTracker
from lsst.summit.utils.guiders.plotting import GuiderPlotter
from lsst.summit.utils.guiders.reading import GuiderReader
from lsst.utils.plotting.figures import make_figure

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, DimensionRecord

    from .payloads import Payload
    from .podDefinition import PodDetails


class GuiderWorker(BaseButlerChannel):
    def __init__(
        self,
        locationConfig: LocationConfig,
        butler: Butler,
        instrument: str,
        podDetails: PodDetails,
        *,
        doRaise=False,
    ) -> None:
        super().__init__(
            locationConfig=locationConfig,
            butler=butler,
            # TODO: DM-43764 this shouldn't be necessary on the
            # base class after this ticket, I think.
            detectors=None,  # unused
            dataProduct=None,  # unused
            # TODO: DM-43764 should also be able to fix needing
            # channelName when tidying up the base class. Needed
            # in some contexts but not all. Maybe default it to
            # ''?
            channelName="",  # unused
            podDetails=podDetails,
            doRaise=doRaise,
            addUploader=True,
        )
        assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
        assert self.podDetails is not None  # XXX why is this necessary? Fix mypy better!
        self.log.info(f"Guider worker running, consuming from {self.podDetails.queueName}")
        self.shardsDirectory = locationConfig.guiderShardsDirectory
        self.instrument = instrument  # why isn't this being set in the base class?!
        self.reader = GuiderReader(self.butler, view="dvcs")

    def callback(self, payload: Payload) -> None:
        """Callback function to be called when a new exposure is available."""
        dataId = payload.dataIds[0]
        record: DimensionRecord | None = None
        if "exposure" in dataId.dimensions:
            record = dataId.records["exposure"]
        elif "visit" in dataId.dimensions:
            record = dataId.records["visit"]

        if record is None:
            raise RuntimeError(f"Failed to find exposure or visit record in {dataId=}")

        with logDuration(self.log, "Loading guider data"):
            guiderData = self.reader.get(dayObs=record.day_obs, seqNum=record.seq_num)

        with logDuration(self.log, "Creating star catalog from guider data"):
            starTracker = GuiderStarTracker(
                guiderData, psf_fwhm=6.0, min_snr=20, max_ellipticity=0.15, edge_margin=40
            )
            stars = starTracker.track_guider_stars(ref_catalog=None)

        with logDuration(self.log, "Making the star mosaic"):
            plotter = GuiderPlotter(stars, guiderData, isIsr=True)
            _ = plotter.star_mosaic(stamp_num=-1, cutout_size=30, plo=50, phi=98)

        with logDuration(self.log, "Making the strip plot"):
            plotter.strip_plot()

        with logDuration(self.log, "Making the movie"):
            plotter.make_gif(n_stamp_max=50, cutout_size=14, plo=50, phi=98)

        rubinTVtableItems: dict[str, str | dict[str, str]] = {}
        rubinTVtableItems["Exposure time"] = record.exposure_time
        rubinTVtableItems["Image type"] = record.observation_type
        rubinTVtableItems["Target"] = record.target_name

        plotName = "???"
        plotFile = makePlotFile(
            self.locationConfig, self.instrument, record.day_obs, record.seq_num, plotName, "jpg"
        )
        # fig.tight_layout()
        # fig.savefig(plotFile)
        # assert self.s3Uploader is not None  # XXX why is this necessary? Fix mypy better!
        # self.s3Uploader.uploadPerSeqNumPlot(
        #     instrument="ra_performance",
        #     plotName=plotName,
        #     dayObs=record.day_obs,
        #     seqNum=record.seq_num,
        #     filename=plotFile,
        # )

        md = {record.seq_num: rubinTVtableItems}
        writeMetadataShard(self.shardsDirectory, record.day_obs, md)
