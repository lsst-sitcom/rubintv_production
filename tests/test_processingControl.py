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
from lsst.rubintv.production.processingControl import CameraControlConfig


class CamearaControlConfigTestCase(lsst.utils.tests.TestCase):

    def test_behavior(self):
        """Checks that everything is properly applied on top of everything
        else and there are no unintended toggles, so be careful if reordering
        or refactoring into separate functions. Runtime is negligible so it's
        not worth splitting.
        """
        nWavefront = 8
        nGuiders = 8
        nImaging = 189
        nFullPhase0 = 96
        nFullPhase1 = 93
        nRaftPhase0 = 108
        nRaftPhase1 = 81
        ne2v = 117
        nITL = 72

        camConfig = CameraControlConfig()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setWavefrontOn()
        self.assertEqual(nWavefront, camConfig.getNumEnabled())

        camConfig.setWavefrontOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setGuidersOn()
        self.assertEqual(nGuiders, camConfig.getNumEnabled())

        camConfig.setGuidersOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setGuidersOn()
        camConfig.setWavefrontOn()
        self.assertEqual(nWavefront + nGuiders, camConfig.getNumEnabled())

        camConfig.setGuidersOff()
        self.assertEqual(nWavefront, camConfig.getNumEnabled())

        camConfig.setAllOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setFullChequerboard(phase=0)
        self.assertEqual(nFullPhase0, camConfig.getNumEnabled())

        camConfig.setFullChequerboard(phase=1)
        self.assertEqual(nFullPhase1, camConfig.getNumEnabled())

        camConfig.setWavefrontOn()
        self.assertEqual(nFullPhase1 + nWavefront, camConfig.getNumEnabled())

        camConfig.setWavefrontOff()
        self.assertEqual(nFullPhase1, camConfig.getNumEnabled())

        camConfig.setAllImagingOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setAllImagingOn()
        self.assertEqual(nImaging, camConfig.getNumEnabled())

        camConfig.invertImagingSelection()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setE2Von()
        self.assertEqual(ne2v, camConfig.getNumEnabled())

        camConfig.invertImagingSelection()
        self.assertEqual(nITL, camConfig.getNumEnabled())

        camConfig.setWavefrontOn()
        self.assertEqual(nITL + nWavefront, camConfig.getNumEnabled())

        camConfig.setAllOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setAllImagingOn()
        self.assertEqual(nImaging, camConfig.getNumEnabled())

        camConfig.setAllOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setRaftChequerboard(phase=0)
        self.assertEqual(nRaftPhase0, camConfig.getNumEnabled())

        camConfig.setRaftChequerboard(phase=1)
        self.assertEqual(nRaftPhase1, camConfig.getNumEnabled())

        camConfig.setWavefrontOn()
        self.assertEqual(nRaftPhase1 + nWavefront, camConfig.getNumEnabled())

        camConfig.setGuidersOn()
        self.assertEqual(nRaftPhase1 + nWavefront + nGuiders, camConfig.getNumEnabled())

    def test_plot(self):
        camConfig = CameraControlConfig()
        camConfig.plotConfig()

        camConfig.setRaftChequerboard(phase=1)
        camConfig.plotConfig()

        camConfig.setWavefrontOn()
        camConfig.setRaftChequerboard(phase=1)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
