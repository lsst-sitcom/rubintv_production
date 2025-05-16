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

import logging
import re
import shutil
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING

from lsst.summit.utils.efdUtils import offsetDayObs
from lsst.summit.utils.utils import getCurrentDayObs_int

from .highLevelTools import deleteAllSkyStills, deleteNonFinalAllSkyMovies, syncBuckets
from .uploaders import MultiUploader
from .utils import hasDayRolledOver, raiseIf

if TYPE_CHECKING:
    from lsst.rubintv.production.utils import LocationConfig


__all__ = ["TempFileCleaner"]

_LOG = logging.getLogger(__name__)


class TempFileCleaner:
    """ """

    def __init__(self, locationConfig: LocationConfig, doRaise: bool = False) -> None:
        self.log = _LOG.getChild("TempFileCleaner")
        self.doRaise = doRaise

        # TODO: probably move these to yaml when we do the LocationConfig
        # refactor
        self.dirsToDelete = {
            "LATISSPlots": Path(locationConfig.plotPath) / "LATISS",
            "LSSTCamPlots": Path(locationConfig.plotPath) / "LSSTCam",
            "postIsrBinnedImages": Path(locationConfig.calculatedDataPath),
            "visitBinnedImages": Path(locationConfig.binnedVisitImagePath),
        }
        self.keepDays = 2  # 2 means curent dayObs and the day before

    def deleteDirectories(self) -> None:
        currentDayObs = getCurrentDayObs_int()
        deleteBefore = offsetDayObs(currentDayObs, -self.keepDays)

        for locationName, dirPath in self.dirsToDelete.items():
            self.log.info(f"Deleting old data from subdirectories in {dirPath}:")
            subDir = None
            try:
                subDirs = dirPath.iterdir()
                for subDir in subDirs:
                    if not subDir.is_dir():  # don't touch regular files
                        continue

                    dirName = subDir.name  # only delete dayObs type dirs
                    if not re.match(r"^2\d{7}$", dirName):
                        continue  # Skip if not in YYYYMMDD format and starting with a 2

                    day = int(dirName)
                    if day <= deleteBefore:
                        self.log.info(f"Deleting old data from {subDir}")
                        shutil.rmtree(subDir)
                    else:
                        self.log.info(f"Keeping {subDir} as it's not old enough yet")

            except Exception as e:
                msg = f"Error processing removing data from {subDir}: {e}"
                raiseIf(self.doRaise, e, self.log, msg)

    def cleanupBuckets(self) -> None:
        # reinit the MultiUploader each time rather than holding one on the
        # class in case of connection problems
        mu = MultiUploader()

        self.log.info("Deleting stale local all sky stills")
        deleteAllSkyStills(mu.localUploader._s3Bucket)

        self.log.info("Deleting stale remote all sky stills")
        deleteAllSkyStills(mu.remoteUploader._s3Bucket)

        self.log.info("Deleting local non-final movies")
        deleteNonFinalAllSkyMovies(mu.localUploader._s3Bucket)

        self.log.info("Deleting remote non-final movies")
        deleteNonFinalAllSkyMovies(mu.remoteUploader._s3Bucket)

        self.log.info("Syncing remote bucket to local bucket's contents")
        syncBuckets(mu)  # always do the deletion before running the sync
        self.log.info("Finished bucket cleanup")

    def runEndOfDay(self) -> None:
        self.deleteDirectories()
        self.cleanupBuckets()
        self.log.info("Finished daily cleanup")

    def run(self) -> None:
        """Run forever, deleting all old dayObs format directories in target
        directories.
        """
        self.runEndOfDay()  # always run once on startup

        currentDayObs = getCurrentDayObs_int()
        while True:
            if hasDayRolledOver(currentDayObs):
                self.log.info("Day has rolled over, running cleanup")
                currentDayObs = getCurrentDayObs_int()
                self.runEndOfDay()

            sleep(60)
