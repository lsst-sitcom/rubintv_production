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
import os
from typing import TYPE_CHECKING

from lsst.resources import ResourcePath
from lsst.summit.utils.utils import getSite

if TYPE_CHECKING:
    from lsst.rubintv.production.utils import LocationConfig


__all__ = ["getBasePath"]

ENDPOINTS = {
    "summit": "https://s3.rubintv.cp.lsst.org",
    "base": "https://s3.rubintv.ls.lsst.org",
    "tucson": "https://s3.rubintv.tu.lsst.org",
    "rubin-devl": "https://sdfembs3.sdf.slac.stanford.edu",
    "usdf-k8s": "https://sdfembs3.sdf.slac.stanford.edu",
}

PROFILE_NAMES = {
    "summit": "summit-data-summit",
    "base": "base-data-base",
    "tucson": "tucson-data-tucson",
    "rubin-devl": "rubin-rubintv-data-usdf-embargo",
    "usdf-k8s": "rubin-rubintv-data-usdf-embargo",
}

BUCKET_NAMES = {
    "summit": "rubintv",
    "base": "rubintv",
    "tucson": "rubintv",
    "rubin-devl": "rubin-rubintv-data-usdf",
    "usdf-k8s": "rubin-rubintv-data-usdf",
}


def getBasePath(locationConfig: LocationConfig, suffix: str = "") -> ResourcePath:
    """Get the base resource path for the rubintv_production package.

    Parameters
    ----------
    suffix : `str`
        A suffix to append to the base path. This should be a relative path
        that will be appended to the base path.

    Returns
    -------
    ResourcePath
        The resource path for the rubintv_production package.
    """
    site = getSite()

    # XXX this almost certainly isn't good enough / won't work in many places
    os.environ["S3_ENDPOINT_URL"] = ENDPOINTS[site]

    profileName = PROFILE_NAMES[site]
    bucketName = BUCKET_NAMES[site]

    if suffix and not suffix.endswith("/"):
        suffix += "/"

    base = f"s3://{profileName}@{bucketName}/{locationConfig.scratchPath}/{suffix}"
    return ResourcePath(base)


def listDir(resourcePath: ResourcePath) -> list[ResourcePath]:
    """List the contents of a directory in the resource path.

    Parameters
    ----------
    resourcePath : `ResourcePath`
        The resource path to list.

    Returns
    -------
    list[str]
        A list of the contents of the directory.
    """
    if not resourcePath.isdir():
        raise ValueError(f"{resourcePath} is not (necessarily) a directory: got {resourcePath.isdir()=}")

    paths = []
    for dirPath, dirNames, fileNames in resourcePath.walk():
        for fileName in fileNames:
            paths.append(dirPath.join(fileName))

    return paths


def getSubDirs(resourcePath: ResourcePath) -> list[str]:
    """Get the subdirectories of a directory in the resource path.

    Parameters
    ----------
    resourcePath : `ResourcePath`
        The resource path to list.

    Returns
    -------
    list[ResourcePath]
        A list of the subdirectories in the directory.
    """
    if not resourcePath.isdir():
        raise ValueError(f"{resourcePath} is not (necessarily) a directory: got {resourcePath.isdir()=}")

    for dirPath, dirNames, fileNames in resourcePath.walk():
        return dirNames  # Return just the first level of subdirectories
    return []  # If no subdirectories, return an empty list


def rmtree(resourcePath: ResourcePath, raiseOnError: bool = False) -> None:
    """Remove a directory and all its contents.

    Parameters
    ----------
    resourcePath : `ResourcePath`
        The resource path to remove.
    """
    log = logging.getLogger(__name__)
    resources = listDir(resourcePath)
    result = ResourcePath.mremove(resources)
    for resource, mbulkResult in result.items():
        if not mbulkResult.success:
            if raiseOnError:
                raise RuntimeError(f"Failed to remove {resource}: {mbulkResult.exception}")
            else:
                log.warning(f"Failed to remove {resource}: {mbulkResult.exception}")
