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

from lsst.daf.butler import (
    Butler,
    DimensionConfig,
    DimensionRecord,
    DimensionUniverse,
    MissingCollectionError,
)
from lsst.rubintv.production.processingControl import PIPELINE_NAMES
from lsst.rubintv.production.utils import safeJsonOpen

__all__ = ("getSampleExpRecord", "getUserRunCollectionName", "removeUserRunCollection")

_LOG = logging.getLogger(__name__)


def getSampleExpRecord() -> DimensionRecord:
    """Get a sample exposure record for testing purposes."""
    dirname = os.path.dirname(__file__)
    expRecordFilename = os.path.join(dirname, "data", "sampleExpRecord.json")
    dimensionUniverseFile = os.path.join(dirname, "data", "butlerDimensionUniverse.json")
    expRecordJson = safeJsonOpen(expRecordFilename)
    duJson = safeJsonOpen(dimensionUniverseFile)
    universe = DimensionUniverse(DimensionConfig(duJson))
    expRecord = DimensionRecord.from_json(expRecordJson, universe=universe)
    return expRecord


def getUserRunCollectionName(pipelineName: str) -> str:
    """Get the user RUN collection name for use in CI and unit testing."""
    if pipelineName not in PIPELINE_NAMES:
        raise ValueError(f"Unknown pipeline name: {pipelineName}")

    username = os.getenv("USER", None)
    if username is None:
        raise RuntimeError("USER environment variable is not set")
    return f"u/{username}/RAPID_ANALYSIS_CI/{pipelineName}"


def removeUserRunCollection(butler: Butler, pipelineName: str) -> None:
    """Get the user RUN collection name for use in CI and unit testing."""
    runCollectionName = getUserRunCollectionName(pipelineName)
    try:
        butler.removeRuns([runCollectionName])
    except MissingCollectionError:
        _LOG.info(f"Collection {runCollectionName} does not exist, nothing to remove")
