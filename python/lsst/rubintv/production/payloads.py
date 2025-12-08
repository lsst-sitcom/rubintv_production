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

import base64
import io
import json
from dataclasses import dataclass
from typing import Any, Self

from lsst.daf.butler import Butler, DataCoordinate
from lsst.pipe.base import PipelineGraph

__all__ = [
    "pipelineGraphToBytes",
    "pipelineGraphFromBytes",
    "Payload",
    "RestartPayload",
    "isRestartPayload",
    "getDetectorId",
]

RESTART_SIGNAL = "__RESTART_SIGNAL__"


def pipelineGraphToBytes(pipelineGraph: PipelineGraph) -> bytes:
    """
    Convert a pipelineGraph to bytes.

    Upstream this to pipe_base after OR3.
    """
    with io.BytesIO() as f:
        pipelineGraph._write_stream(f)
        return f.getvalue()


def pipelineGraphFromBytes(pipelineGraphBytes: bytes) -> PipelineGraph:
    """
    Get a pipelineGraph from bytes.

    Upstream this to pipe_base after OR3 as a PipelineGraph classmethod.
    """
    with io.BytesIO(pipelineGraphBytes) as f:
        return PipelineGraph._read_stream(f)  # to be public soon


def isRestartPayload(payload: Payload) -> bool:
    """Check if the payload is a restart signal.

    Note that this function should be used, and *not* `isinstance`, because
    the payload is deserialized as a `Payload` instance, not a `RestartPayload`
    instance.
    """
    return payload.run == RESTART_SIGNAL or payload.who == RESTART_SIGNAL


def getDetectorId(payload: Payload) -> int | None:
    """Get the detector ID from the payload.

    Parameters
    ----------
    payload : `Payload`
        The payload to get the detector ID from.

    Returns
    -------
    detectorId : `int`
        The detector ID, or None if there is no detector ID in the payload.
    """
    if "detector" in payload.dataId:
        return int(payload.dataId["detector"])
    return None


@dataclass(frozen=True)
class Payload:
    """
    A dataclass representing a payload.

    These go in minimal, but come out full, by using the butler.
    """

    dataId: DataCoordinate
    pipelineGraphBytes: bytes
    run: str
    who: str
    specialMessage: str = ""

    @classmethod
    def from_json(
        cls,
        json_str: str,
        butler: Butler,
    ) -> Self:
        json_dict = json.loads(json_str)
        dataId = butler.registry.expandDataId(json_dict["dataId"])

        pipelineGraphBytes = base64.b64decode(json_dict["pipelineGraphBytes"].encode())
        return cls(
            dataId=dataId,
            pipelineGraphBytes=pipelineGraphBytes,
            run=json_dict["run"],
            who=json_dict["who"],
            specialMessage=json_dict.get("specialMessage", ""),
        )

    def to_json(self) -> str:
        json_dict: dict[str, Any] = {
            "pipelineGraphBytes": base64.b64encode(self.pipelineGraphBytes).decode(),
            "run": self.run,
            "who": self.who,
            "specialMessage": self.specialMessage,
        }
        json_dict["dataId"] = dict(self.dataId.required)
        return json.dumps(json_dict)

    def __repr__(self):
        return (
            f"Payload(dataId={[self.dataId]}, run={self.run}, who={self.who},"
            " pipelineGraphBytes=<the bytes>)"
        )


class RestartPayload(Payload):
    """The restart payload.

    Enqueue this payload on a worker so that it restarts without terminating
    any work in progress.

    Note: do not check for this via `isinstance` because it will be
    deserialised just like other payloads, and will end up as a `Payload`
    instance. This class just exists to make instantiation clearer.
    """

    def __init__(self) -> None:
        super().__init__(
            dataId=DataCoordinate(),  # type: ignore[abstract]
            # these are all unused, but set them to something to be clear if
            # they end up elsewhere somehow. Can repurose them later if needed.
            pipelineGraphBytes=b"{RESTART_SIGNAL}",
            run=f"{RESTART_SIGNAL}",
            who=f"{RESTART_SIGNAL}",
            specialMessage="RESTARTING",
        )
