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

from lsst.daf.butler import Butler
from lsst.summit.utils.dateTime import getCurrentDayObsInt, offsetDayObs

from .highLevelTools import deleteAllSkyStills, deleteNonFinalAllSkyMovies, syncBuckets
from .resources import getBasePath, getSubDirs, rmtree
from .uploaders import MultiUploader
from .utils import hasDayRolledOver, raiseIf

if TYPE_CHECKING:
    from lsst.rubintv.production.utils import LocationConfig


__all__ = ["TempFileCleaner"]

_LOG = logging.getLogger(__name__)


class TempFileCleaner:
    """Clean up all temporary files and directories created by RA."""

    def __init__(self, locationConfig: LocationConfig, doRaise: bool = False) -> None:
        self.log = _LOG.getChild("TempFileCleaner")
        self.doRaise = doRaise
        self.locationConfig = locationConfig

        # TODO: probably move these to yaml when we do the LocationConfig
        # refactor
        self.nfsDirsToDelete = {
            "LATISSPlots": Path(locationConfig.plotPath) / "LATISS",
            "LSSTCamPlots": Path(locationConfig.plotPath) / "LSSTCam",
        }
        self.s3DirsToDelete = ("binnedImages/",)  # NB: must end in the trailing backslash
        self.keepDaysS3temp = 2  # 2 means curent dayObs and the day before
        self.keepDaysPixelProducts = 14  # keep the last two weeks for pixel products for now

        self.butler = Butler.from_config(
            locationConfig.lsstCamButlerPath,
            instrument="LSSTCam",
            collections=[
                "LSSTCam/defaults",
                locationConfig.getOutputChain("LSSTCam"),
            ],
            writeable=True,
        )

    def deletePixelProducts(self) -> None:
        """Delete old pixel data products for LSSTCam."""
        site = self.locationConfig.location
        if site.lower() not in ("summit", "bts", "tts"):
            self.log.info(f"Pixel products are only deleted at summit/BTS/TTS sites, not {site}, skipping")
            return

        currentDayObs = getCurrentDayObsInt()
        deleteBefore = offsetDayObs(currentDayObs, -self.keepDaysPixelProducts)

        where = f"exposure.day_obs<={deleteBefore} AND instrument='LSSTCam'"
        for product in [
            "post_isr_image",
        ]:
            self.log.info(f"Querying for {product}s to delete before {deleteBefore}...")
            allDRefs = self.butler.query_datasets(
                product,
                where=where,
                limit=1_000_000_000,
                collections=self.locationConfig.getOutputChain("LSSTCam"),
                explain=False,  # sometimes there's nothing and this is expected
            )
            days = sorted(set(int(d.dataId["day_obs"]) for d in allDRefs))
            self.log.info(f"Found {len(allDRefs)} {product}s across {len(days)} days to delete")
            dayMap: dict[int, list] = {d: [] for d in days}
            for d in allDRefs:
                dayMap[int(d.dataId["day_obs"])].append(d)

            total = 0
            for dayObs, refs in dayMap.items():
                self.log.info(f"Removing {len(refs)} {product}s for {dayObs=}...")
                self.butler.pruneDatasets(
                    refs,
                    disassociate=True,
                    unstore=True,
                    purge=True,
                )
                total += len(refs)
                self.log.info(f"Deletion for {product} {100 * (total / len(allDRefs)):.1f}% complete")

    def deleteDirectories(self) -> None:
        """Delete all specified NFS directories that are older than
        `keepDaysS3temp` days.
        """
        currentDayObs = getCurrentDayObsInt()
        deleteBefore = offsetDayObs(currentDayObs, -self.keepDaysS3temp)

        for locationName, dirPath in self.nfsDirsToDelete.items():
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

    def deleteS3Directories(self) -> None:
        """Delete all specified S3 directories that are older than
        `keepDaysS3temp` days.
        """
        currentDayObs = getCurrentDayObsInt()
        deleteBefore = offsetDayObs(currentDayObs, -self.keepDaysS3temp)

        basePath = getBasePath(self.locationConfig)
        for locationName in self.s3DirsToDelete:
            fullDirName = basePath.join(locationName)

            self.log.info(f"Deleting old data from subdirectories in {fullDirName}:")
            subDir = None
            subDirs = getSubDirs(fullDirName)
            try:
                for subDir in subDirs:
                    fullSubDir = fullDirName.join(subDir)
                    if not fullSubDir.isdir():  # don't touch regular files
                        continue

                    # only delete dayObs type dirs
                    if not re.match(r"^2\d{7}/?$", subDir):  # Allow optional trailing slash
                        continue  # Skip if not in YYYYMMDD format and starting with a 2

                    if subDir.endswith("/"):
                        subDir = subDir[:-1]

                    day = int(subDir)
                    if day <= deleteBefore:
                        self.log.info(f"Deleting old data from {fullSubDir}")
                        rmtree(fullSubDir)
                    else:
                        self.log.info(f"Keeping {fullSubDir} as it's not old enough yet")

            except Exception as e:
                msg = f"Error processing removing data from {subDir}: {e}"
                raiseIf(self.doRaise, e, self.log, msg)

    def cleanupBuckets(self) -> None:
        """Delete stale S3 files and sync local and remote buckets.

        Delete any stale all sky stills and non-final movies from the buckets
        and sync the local bucket's objects to the remote.
        """
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
        syncBuckets(mu, self.locationConfig)  # always do the deletion before running the sync
        self.log.info("Finished bucket cleanup")

    def runEndOfDay(self) -> None:
        """Run all the functions at the end of the day to clean up."""
        self.deletePixelProducts()
        self.deleteDirectories()
        self.deleteS3Directories()
        self.cleanupBuckets()
        self.log.info("Finished daily cleanup")

    def run(self) -> None:
        """Run forever, deleting all old dayObs format directories in target
        directories.
        """
        self.runEndOfDay()  # always run once on startup

        currentDayObs = getCurrentDayObsInt()
        while True:
            if hasDayRolledOver(currentDayObs):
                self.log.info("Day has rolled over, running cleanup")
                currentDayObs = getCurrentDayObsInt()
                self.runEndOfDay()

            sleep(60)
