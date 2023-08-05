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


from lsst.analysis.tools.actions.plot import FocalPlaneGeometryPlot
from lsst.obs.lsst import LsstCam
import numpy as np


class CameraControlConfig:
    """Processing control for which CCDs will be processed.
    """
    def __init__(self):
        self.camera = LsstCam.getCamera()
        self._detectorStates = {det: False for det in self.camera}
        self._detectors = [det for det in self.camera]
        self._imaging = [det for det in self._detectors if self.isImaging(det)]
        self._guiders = [det for det in self._detectors if self.isGuider(det)]
        self._wavefronts = [det for det in self._detectors if self.isWavefront(det)]
        self.plot = FocalPlaneGeometryPlot()
        self.plotInfo = {"plotName": "test plot", "run": "no run",
                         "tableName": None, "bands": []}

    @staticmethod
    def isWavefront(detector):
        return detector.getPhysicalType() == 'ITL_WF'

    @staticmethod
    def isGuider(detector):
        return detector.getPhysicalType() == 'ITL_G'

    @staticmethod
    def isImaging(detector):
        return detector.getPhysicalType() in ['E2V', 'ITL']

    @staticmethod
    def _getRaftTuple(detector):
        rString = detector.getName().split('_')[0]
        return int(rString[1]), int(rString[2])

    @staticmethod
    def _getSensorTuple(detector):
        sString = detector.getName().split('_')[1]
        return int(sString[1]), int(sString[2])

    def _getFullLocationTuple(self, detector):
        """Get the (colNum, rowNum) of the detector wrt the full focal plane.

        0, 0 is the bottom left
        """
        raftX, raftY = self._getRaftTuple(detector)
        sensorX, sensorY = self._getSensorTuple(detector)
        col = (raftX * 3) + sensorX + 1
        row = (raftY * 3) + sensorY + 1
        return col, row

    def setWavefrontOn(self):
        for detector in self._wavefronts:
            self._detectorStates[detector] = True

    def setWavefrontOff(self):
        for detector in self._wavefronts:
            self._detectorStates[detector] = False

    def setGuidersOn(self):
        for detector in self._guiders:
            self._detectorStates[detector] = True

    def setGuidersOff(self):
        for detector in self._guiders:
            self._detectorStates[detector] = False

    def setFullChequerboard(self, phase=0):
        for detector in self._imaging:
            x, y = self._getFullLocationTuple(detector)
            self._detectorStates[detector] = ((x % 2) + (y % 2) + phase) % 2

    def setRaftChequerboard(self, phase=0):
        for detector in self._imaging:
            raftX, raftY = self._getRaftTuple(detector)
            self._detectorStates[detector] = ((raftX % 2) + (raftY % 2) + phase) % 2

    def setE2Von(self):
        for detector in self._imaging:
            if detector.getPhysicalType() == 'E2V':
                self._detectorStates[detector] = True

    def setITLon(self):
        for detector in self._imaging:
            if detector.getPhysicalType() == 'ITL':
                self._detectorStates[detector] = True

    def setAllOn(self):
        """Note: includes corners and guiders
        """
        for detector in self._detectors:
            self._detectorStates[detector] = True

    def setAllOff(self):
        """Note: includes corners and guiders
        """
        for detector in self._detectors:
            self._detectorStates[detector] = False

    def setAllImagingOn(self):
        for detector in self._imaging:
            self._detectorStates[detector] = True

    def setAllImagingOff(self):
        for detector in self._imaging:
            self._detectorStates[detector] = False

    def invertImagingSelection(self):
        for detector in self._imaging:
            self._detectorStates[detector] = not self._detectorStates[detector]

    def getNumEnabled(self):
        return sum(self._detectorStates.values())

    def getEnabledDetIds(self):
        return sorted([det.getId() for (det, state) in self._detectorStates.items() if state is True])

    def asPlotData(self):
        detNums = []
        ampNames = []
        x = []
        y = []
        z = []
        for detector, state in self._detectorStates.items():
            for amp in detector:
                detNums.append(detector.getId())
                ampNames.append(None)
                x.append(None)
                y.append(None)
                z.append(state)

        return {
            'detector': detNums,
            'amplifier': ampNames,
            'x': np.array(x),
            'y': np.array(y),
            'z': np.array(z)
        }

    def plotConfig(self):
        self.plot.level = 'detector'
        plot = self.plot.makePlot(self.asPlotData(), self.camera, self.plotInfo)
        return plot
