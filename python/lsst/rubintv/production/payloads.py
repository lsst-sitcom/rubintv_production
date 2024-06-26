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

from lsst.daf.butler import Butler, DataCoordinate
from lsst.pipe.base import PipelineGraph

__all__ = [
    "pipelineGraphToBytes",
    "pipelineGraphFromBytes",
    "Payload",
    "PayloadResult",
]


def pipelineGraphToBytes(pipelineGraph):
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

    dataId: DataCoordinate
    pipelineGraphBytes: bytes
    run: str

    @classmethod
    def from_json(
        cls,
        json_str: str,
        butler: Butler,
    ) -> Payload:
        json_dict = json.loads(json_str)
        dataId = butler.registry.expandDataId(json_dict["dataId"])
        pipelineGraphBytes = base64.b64decode(json_dict["pipelineGraphBytes"].encode())
        return cls(dataId=dataId, pipelineGraphBytes=pipelineGraphBytes, run=json_dict["run"])

    def to_json(self) -> str:
        json_dict = {
            "dataId": dict(self.dataId.mapping),
            "pipelineGraphBytes": base64.b64encode(self.pipelineGraphBytes).decode(),
            "run": self.run,
        }
        return json.dumps(json_dict)

    def __repr__(self):
        return f"Payload(dataId={self.dataId}, run={self.run}, pipelineGraphBytes=<the bytes>)"


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
