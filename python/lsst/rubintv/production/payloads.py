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
]


def pipelineGraphToBytes(pipelineGraph) -> bytes:
    """
    Convert a pipelineGraph to bytes.

    Upstream this to pipe_base after OR3.
    """
    with io.BytesIO() as f:
        pipelineGraph._write_stream(f)
        return f.getvalue()


def pipelineGraphFromBytes(pipelineGraphBytes):
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
        return cls(dataIds=dataIds, pipelineGraphBytes=pipelineGraphBytes, run=json_dict["run"])

    def to_json(self) -> str:
        json_dict: dict[str, Any] = {
            "pipelineGraphBytes": base64.b64encode(self.pipelineGraphBytes).decode(),
            "run": self.run,
        }
        json_dict["dataIds"] = []
        for dataId in self.dataIds:
            json_dict["dataIds"].append(dict(dataId.mapping))
        return json.dumps(json_dict)

    def __repr__(self):
        return f"Payload(dataIds={[d for d in self.dataIds]}, run={self.run}, pipelineGraphBytes=<the bytes>)"


@dataclass(frozen=True)
class PayloadResult(Payload):
    """
    A dataclass representing a payload result.
    """

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
        instance = super().from_json(json_str, butler)
        json_dict = json.loads(json_str)
        return cls(
            dataIds=instance.dataIds,
            pipelineGraphBytes=instance.pipelineGraphBytes,
            run=instance.run,
            startTime=json_dict["startTime"],
            endTime=json_dict["endTime"],
            splitTimings=json_dict["splitTimings"],
            success=json_dict["success"],
            message=json_dict["message"],
        )

    def to_json(self) -> str:
        json_dict = asdict(self)
        json_dict["pipelineGraphBytes"] = base64.b64encode(self.pipelineGraphBytes).decode()
        json_dict["dataIds"] = []
        for dataId in self.dataIds:
            json_dict["dataIds"].append(dict(dataId.mapping))
        return json.dumps(json_dict)
