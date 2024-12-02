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
    "SoarUploader",
]

import logging
import tempfile
from time import sleep

import matplotlib.pyplot as plt
from astropy.time import Time

from lsst.rubintv.production.uploaders import MultiUploader
from lsst.rubintv.production.utils import getRubinTvInstrumentName, raiseIf
from lsst.summit.extras.soarSeeing import SoarSeeingMonitor
from lsst.summit.utils.utils import getCurrentDayObs_int


class SoarUploader:
    def __init__(self, doRaise: bool) -> None:
        self.soar = SoarSeeingMonitor()
        self.instrument = "LSSTComCam"  # change to LSSTCam when we switch instruments
        self.doRaise = doRaise
        self.figure = plt.figure(figsize=(18, 10))
        self.uploader = MultiUploader()
        self._lastUpdated = Time.now()
        self.log = logging.getLogger(__name__)

    def newData(self) -> bool:
        return self._lastUpdated != self.soar.getMostRecentTimestamp()

    def run(self) -> None:
        while True:
            dayObs = getCurrentDayObs_int()
            try:
                if not self.newData():
                    sleep(15)
                    continue

                self._lastUpdated = self.soar.getMostRecentTimestamp()
                self.figure = self.soar.plotSeeingForDayObs(dayObs, addMostRecentBox=False, fig=self.figure)
                with tempfile.NamedTemporaryFile(suffix=".png") as f:
                    self.log.info(f"Uploading SOAR seeing to night report for {dayObs}")
                    self.figure.savefig(f.name)
                    self.uploader.uploadNightReportData(
                        getRubinTvInstrumentName(self.instrument), dayObs, f.name, "SoarSeeingMonitor"
                    )
                    self.log.info("Done")
                    self.figure.clear()

            except Exception as e:
                logging.error(f"Error: {e}")
                raiseIf(self.doRaise, e, self.log)
