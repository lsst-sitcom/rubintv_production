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
from dataclasses import asdict, dataclass
from typing import Any, Self

from lsst.daf.butler import Butler, DataCoordinate
from lsst.pipe.base import PipelineGraph

__all__ = [
    "pipelineGraphToBytes",
    "pipelineGraphFromBytes",
    "Payload",
    "PayloadResult",
    "RestartPayload",
    "RESTART_SIGNAL",
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


@dataclass(frozen=True)
class Payload:
    """
    A dataclass representing a payload.

    These go in minimal, but come out full, by using the butler.
    """

    dataIds: list[DataCoordinate]
    pipelineGraphBytes: bytes
    run: str
    who: str

    @classmethod
    def from_json(
        cls,
        json_str: str,
        butler: Butler,
    ) -> Self:
        json_dict = json.loads(json_str)
        dataIds = []
        for dataId in json_dict["dataIds"]:
            dataIds.append(butler.registry.expandDataId(dataId))

        pipelineGraphBytes = base64.b64decode(json_dict["pipelineGraphBytes"].encode())
        return cls(
            dataIds=dataIds, pipelineGraphBytes=pipelineGraphBytes, run=json_dict["run"], who=json_dict["who"]
        )

    def to_json(self) -> str:
        json_dict: dict[str, Any] = {
            "pipelineGraphBytes": base64.b64encode(self.pipelineGraphBytes).decode(),
            "run": self.run,
            "who": self.who,
        }
        json_dict["dataIds"] = []
        for dataId in self.dataIds:
            json_dict["dataIds"].append(dict(dataId.required))
        return json.dumps(json_dict)

    def __repr__(self):
        return (
            f"Payload(dataIds={[d for d in self.dataIds]}, run={self.run}, who={self.who},"
            " pipelineGraphBytes=<the bytes>)"
        )


@dataclass(frozen=True)
class PayloadResult:
    """
    A dataclass representing a payload result, composed of a Payload.
    """

    payload: Payload
    startTime: float
    endTime: float
    splitTimings: dict
    success: bool
    message: str

    @classmethod
    def from_json(
        cls,
        json_str: str,
        butler: Butler,
    ) -> Self:
        json_dict = json.loads(json_str)

        # Validate JSON keys
        allowed_keys = {"payload", "startTime", "endTime", "splitTimings", "success", "message"}
        unexpected_keys = set(json_dict.keys()) - allowed_keys
        if unexpected_keys:
            raise TypeError(f"Unexpected keys in JSON: {unexpected_keys}")

        # Extract the payload section for Payload.from_json
        payload_dict = json_dict["payload"]
        payload_json = json.dumps(payload_dict)
        payload = Payload.from_json(payload_json, butler)

        return cls(
            payload=payload,
            startTime=json_dict["startTime"],
            endTime=json_dict["endTime"],
            splitTimings=json_dict["splitTimings"],
            success=json_dict["success"],
            message=json_dict["message"],
        )

    def to_json(self) -> str:
        json_dict = asdict(self)
        json_dict["payload"] = json.loads(self.payload.to_json())
        return json.dumps(json_dict)


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
            dataIds=[],
            # these are all unused, but set them to something to be clear if
            # they end up elsewhere somehow. Can repurose them later if needed.
            pipelineGraphBytes=b"{RESTART_SIGNAL}",
            run=f"{RESTART_SIGNAL}",
            who=f"{RESTART_SIGNAL}",
        )
