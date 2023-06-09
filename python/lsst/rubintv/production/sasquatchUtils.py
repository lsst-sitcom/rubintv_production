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

import json
import logging
import os
import re
import requests
from astropy.time import Time

from lsst.utils import getPackageDir

# note that this is a sub-utils package, and so cannot ever import from .utils

__all__ = ["sendMetadataToSasquatch"]


def _getDefaultValue(dataType):
    """
    Return a default value following Sasquatch convention.

    These types are ignored for now: Avro boolean, bytes, null.

    Parameters
    ----------
    dataType : `str`
        The Avro data type to return a value for.
    """
    if dataType in ("int", "long", "float", "double"):
        return 0
    elif dataType == "string":
        return ""
    else:
        raise ValueError(f"Unsupported type {dataType}")


def makeSasquatchPayload(md, namespace, topic, schemaFile, logger=None):
    """
    Compose a payload in the right format suitable to send to Sasquatch.

    Parameters
    ----------
    md : `dict`
        The metadata to make a sasquatch payload.
    namespace : `str`
        lsst.rubintv
    topic : `str`
        The full qualified name of the Kafka topic is namespace.topic
    schemaFile : `str`
        A file path to read the schema from.

    Returns
    -------
    payload : `dict`
        A payload in a format ready for Sasquatch Kafka REST Proxy.
        Schema is included.
        TODO: consider using value_schema_id instead.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

    logger.debug("original md %s", md)

    with open(schemaFile) as f:
        schema = json.load(f)

    # Sasquatch field names cannot have space, hyphen, etc.
    # also, all lower case is preferred in Sasquatch field names
    schema = {re.sub(r"[^a-zA-Z0-9]", "_", key).lower(): schema[key] for key in schema}
    md = {re.sub(r"[^a-zA-Z0-9]", "_", key).lower(): md[key] for key in md}
    # field names have to start with [A-Za-z_]
    md = {re.sub(r"^([0-9])", r"__\1", key): md[key] for key in md}

    avroSchema = []
    for key in schema:
        avroSchema.append({"name": key, "type": schema[key]})

    try:
        timestamp = (
            Time.strptime(
                str(md["dayobs"]) + md["tai"], "%Y%m%d%H:%M:%S", scale="tai"
            ).unix
            * 1_000_000
        )
    except KeyError:
        logger.debug("Use current time as timestamp")
        timestamp = Time.now().unix * 1_000_000

    md_value = dict()
    # Need a value for every item in schema
    # TODO: type checking and converting?
    for key in schema.keys():
        if key == "timestamp":
            md_value[key] = timestamp
            continue
        try:
            # value can be: None, empty list, and?
            if md[key]:
                md_value[key] = md[key]
                continue
        except KeyError:
            logger.debug("Want %s but not in md. Filling in a value", key)
        md_value[key] = _getDefaultValue(schema[key])

    # Fields in the input md but not in the schema might be wanted. Warn so
    # to later decide whether to add them to the schema.
    for key in md.keys() - schema.keys():
        # Ignore those like _PSF; they're for colouring cells on the front end.
        if not key.startswith("_"):
            logger.warn("Missing %s in Sasquatch Avro schema", key)

    z = dict(namespace=namespace, type="record", name=topic, fields=avroSchema)

    payload = {"value_schema": json.dumps(z), "records": [{"value": md_value}]}
    return payload


def sendMetadataToSasquatch(
    mdDict,
    instrument="LATISS",
    namespace="lsst.rubintv",
    componentString="debug1",
    logger=None,
    dryrun=False,
):
    """Send a piece of metadata to Sasquatch.

    Parameters
    ----------
    mdDict : `dict` of `dict`
        The metadata items to write, as a dict of dicts. Each key in the main
        dict should be a sequence number. Each value is a dict of values for
        that seqNum, as {'measurement_name': value}.
        (intentionally make this the same as writeMetadataShard)
    instrument : `str`
        This is used to load the corresponding schema and
        to form the full Kafka topic string.
    namespace : `str`
    componentString : `str`
    logger : `logging.Logger`, optional
        The logger, created if not supplied.
    dryrun : `bool`
        If False, prepare the md payload but not actually send.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

    packageDir = getPackageDir("rubintv_production")
    try:
        schemaFile = os.path.join(packageDir, "config", instrument, "md_schema.json")
    except FileNotFoundError:
        logger.warning(f"Schema not set for {instrument}; skip Sasquatch uploading.")

    topic = ".".join((componentString, instrument))
    url = (
        "https://usdf-rsp-dev.slac.stanford.edu/sasquatch-rest-proxy/topics/"
        + namespace
        + "."
        + topic
    )
    headers = {
        "Content-Type": "application/vnd.kafka.avro.v2+json",
        "Accept": "application/vnd.kafka.v2+json",
    }

    logger.debug("Sending md to topic %s.%s", namespace, topic)
    for md in mdDict.values():
        payload = makeSasquatchPayload(
            md,
            namespace,
            topic,
            schemaFile,
            logger=logger,
        )
        logger.debug("Sasquatch payload: %s", payload)
        if dryrun:
            continue
        try:
            response = requests.request("POST", url, json=payload, headers=headers)
        except Exception:
            logger.exception("Expections in sending metadata to Sasquatch")
            continue

        if response.ok:
            logger.info("Sent. Response : %s", response.text)
        else:
            logger.error("Failed in sending, response : %s", response.text)
