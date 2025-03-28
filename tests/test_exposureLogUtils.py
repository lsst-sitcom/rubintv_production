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

import unittest

import responses

import lsst.utils.tests
from lsst.rubintv.production.exposureLogUtils import getLogsForDayObs


class ExposurLogUtilsTestCase(lsst.utils.tests.TestCase):
    """A test case for exposure log utilities"""

    @responses.activate
    def test_getLogsForDayObs(self) -> None:
        mock_logs = [
            {
                "id": "0123",
                "obs_id": "AT_O_20230316_000520",
                "seq_num": 520,
                "instrument": "LATISS",
            },
            {
                "id": "9999",
                "obs_id": "AT_O_20230316_000521",
                "seq_num": 521,
                "instrument": "LATISS",
            },
            {
                "id": "9998",
                "obs_id": "AT_O_20230316_000521",
                "seq_num": 522,
                "instrument": "LSSTCam",
            },
        ]
        responses.add(
            responses.GET,
            "https://summit-lsp.lsst.codes/exposurelog/messages",
            json=mock_logs,
        )
        logs = getLogsForDayObs("LATISS", 20230316)
        self.assertEqual(logs, {520: mock_logs[0], 521: mock_logs[1]})


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
