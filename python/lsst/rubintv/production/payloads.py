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

from dataclasses import dataclass
import json

from lsst.daf.butler.dimensions import DimensionRecord
from lsst.rubintv.production.utils import expRecordFromJson


@dataclass(frozen=True)
class Payload:
    """
    A dataclass representing a payload.
    """
    expRecord: DimensionRecord
    detector: int
    pipeline: str

    @classmethod
    def from_json(cls, json_str: str) -> 'Payload':
        json_dict = json.loads(json_str)
        expRecordJson = json_dict.pop('expRecord')  # must pop so it doesn't get passed to cls
        expRecord = expRecordFromJson(expRecordJson)
        return cls(expRecord=expRecord, **json_dict)

    def to_json(self) -> str:
        json_dict = self.__dict__.copy()  # need a copy in order to mutate expRecord item safely
        json_dict['expRecord'] = json_dict['expRecord'].to_simple().json()
        return json.dumps(json_dict)

    def __eq__(self, __value: object) -> bool:
        """Check that two payloads are equal.

        Note that internally, the expRecords are only compared on their dataId,
        not their full contents.
        """
        if isinstance(__value, Payload):
            return (
                self.expRecord == __value.expRecord
                and self.detector == __value.detector
                and self.pipeline == __value.pipeline
            )
        return False


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
