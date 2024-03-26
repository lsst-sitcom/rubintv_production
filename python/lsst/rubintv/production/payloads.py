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
from dataclasses import dataclass
import json

from lsst.daf.butler import DataCoordinate, Butler
from lsst.rubintv.production.utils import expRecordFromJson


@dataclass(frozen=True)
class Payload:
    """
    A dataclass representing a payload.
    """
    dataId: DataCoordinate
    pipelineGraphBytes: bytes

    @classmethod
    def from_json(
        cls,
        json_str: str,
        butler: Butler,
    ) -> Payload:
        json_dict = json.loads(json_str)
        dataId = butler.registry.expandDataId(json_dict['dataId'])
        pipelineGraphBytes = base64.b64decode(json_dict['pipelineGraphBytes'].encode())
        return cls(dataId=dataId, pipelineGraphBytes=pipelineGraphBytes)

    def to_json(self) -> str:
        json_dict = {
            'dataId': dict(self.dataId.mapping),
            'pipelineGraphBytes': base64.b64encode(self.pipelineGraphBytes).decode()
        }
        return json.dumps(json_dict)

    def __repr__(self):
        return f"Payload(dataId={self.dataId}, pipelineGraphBytes=<the bytes>)"


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

    def __eq__(self, __value: object) -> bool:
        return (
            super().__eq__(__value)
            and self.startTime == __value.startTime
            and self.endTime == __value.endTime
            and self.splitTimings == __value.splitTimings
            and self.success == __value.success
            and self.message == __value.message
        )
