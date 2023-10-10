# This file is part of summit_utils.
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

"""Test cases for utils."""
import logging
import unittest
from unittest.mock import patch, MagicMock
import lsst.utils.tests

from lsst.rubintv.production.utils import (isDayObsContiguous,
                                           sanitizeNans,
                                           hasDayRolledOver,
                                           )


class RubinTVUtilsTestCase(lsst.utils.tests.TestCase):
    """A test case RubinTV utility functions."""

    def test_isDayObsContiguous(self):
        dayObs = 20220930
        nextDay = 20221001  # next day in a different month
        differentDay = 20221005
        self.assertTrue(isDayObsContiguous(dayObs, nextDay))
        self.assertTrue(isDayObsContiguous(nextDay, dayObs))
        self.assertFalse(isDayObsContiguous(nextDay, differentDay))

    def test_sanitizeNans(self):
        self.assertEqual(sanitizeNans({'a': 1.0, 'b': float('nan')}), {'a': 1.0, 'b': None})
        self.assertEqual(sanitizeNans([1.0, float('nan')]), [1.0, None])
        self.assertIsNone(sanitizeNans(float('nan')))

        # test that a nested dictionary with nan values is sanitized
        nestedDict = {'a': 1.0, 'b': {'c': float('nan'), 'd': 2.0}}
        result = sanitizeNans(nestedDict)
        self.assertEqual(result['a'], 1.0)
        self.assertEqual(result['b'], {'c': None, 'd': 2.0})

        noneKeyedDict = {None: 1.0, 'b': {'c': float('nan'), 'd': 2.0}}
        self.assertEqual(sanitizeNans(noneKeyedDict), {None: 1.0, 'b': {'c': None, 'd': 2.0}})

    @patch("lsst.rubintv.production.utils.getCurrentDayObs_int")
    def test_has_day_rolled_over(self, mock_get_current_day_obs_int):
        logger = MagicMock(spec=logging.Logger)
        day_obs = 20200305
        mock_get_current_day_obs_int.return_value = day_obs + 1

        self.assertTrue(hasDayRolledOver(day_obs, logger=logger))

    @patch("lsst.rubintv.production.utils.getCurrentDayObs_int")
    def test_has_not_rolled_over(self, mock_get_current_day_obs_int):
        logger = MagicMock(spec=logging.Logger)
        day_obs = 20200305
        mock_get_current_day_obs_int.return_value = day_obs

        self.assertFalse(hasDayRolledOver(day_obs, logger=logger))
        logger.warning.assert_not_called()

    @patch("lsst.rubintv.production.utils.getCurrentDayObs_int")
    def test_has_rolled_over_unexpectedly(self, mock_get_current_day_obs_int):
        logger = MagicMock(spec=logging.Logger)
        day_obs = 20200305
        mock_get_current_day_obs_int.return_value = day_obs + 2

        self.assertTrue(hasDayRolledOver(day_obs, logger=logger))
        logger.warning.assert_called_once_with(f"Encountered non-linear time! dayObs supplied was {day_obs}"
                                               f" and now the current dayObs is {day_obs + 2}!")


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
