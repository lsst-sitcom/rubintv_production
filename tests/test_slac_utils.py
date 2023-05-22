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
import unittest
import lsst.utils.tests

from lsst.rubintv.production.slac.utils import getGains


class RubinTVSlacUtilsTestCase(lsst.utils.tests.TestCase):
    """A test case RubinTV SLAC utility functions."""

    def test_getGains(self):
        # check we have all the gains for TS8
        ts8Gains = getGains('LSST-TS8')
        self.assertEqual(len(ts8Gains), 9)
        for ccd, gains in ts8Gains.items():
            self.assertEqual(len(gains), 16)

        # check we have all the gains for ComCam
        comCamGains = getGains('LSSTComCam')
        self.assertEqual(len(comCamGains), 9)
        for ccd, gains in comCamGains.items():
            self.assertEqual(len(gains), 16)

        # check the TS8 gains and ComCam gains aren't the same
        for ccdName in ts8Gains.keys():
            ts8ChipGains = ts8Gains[ccdName]
            comCamChipGains = comCamGains[ccdName]
            self.assertEqual(ts8ChipGains.keys(), comCamChipGains.keys())  # amps have the same names
            self.assertTrue(all([t1 != t2 for t1, t2 in zip(ts8ChipGains.values(),
                                                            comCamChipGains.values())]))  # all gains differ
        self.assertNotEqual(ts8Gains, comCamGains)

        # check we have all the gains for the full camera
        lsstCamGains = getGains('LSSTCam')
        self.assertEqual(len(lsstCamGains), 205)
        for ccd, gains in lsstCamGains.items():
            nExpected = 16  # amps in an imaging CCD
            if ccd.split('_')[1].startswith('SW'):  # it's a wavefront sensor with 8 amps
                nExpected = 8
            self.assertEqual(len(gains), nExpected)

        # check it only works for known instruments
        with self.assertRaises(ValueError):
            getGains('BadInstrumentName')


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
