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

import numpy as np
import json
import enum
from datetime import timedelta

from lsst.analysis.tools.actions.plot import FocalPlaneGeometryPlot
from lsst.obs.lsst import LsstCam
from .redisUtils import RedisHelper


class WorkerProcessingMode(enum.IntEnum):
    """Defines the mode in which worker nodes process images.

    WAITING: The worker will process only the most recently taken image, and
        then will wait for new images to land, and will not process the backlog
        in the meantime.
    CONSUMING: The worker will always process the most recent image, but will
        will also process the backlog of images if no new images have landed
        during the last processing.
    MURDEROUS: The worker will process the most recent image, and will also
        work its way through the backlog of images, but if new images land
        while backlog images are bring processed, the worker will abandon the
        work in progress and switch to processing the newly-landed image. Only
        backlog images will be abandoned though - if the in-progress processing
        is for an image which came from the `current` stack then processing
        will not be abadoned. This is necessary, otherwise, if we can't keep up
        with the incoming images, we will never fully process a single image!
    """
    WAITING = 0
    CONSUMING = 1
    MURDEROUS = 2


class VisitProcessingMode(enum.IntEnum):
    CONSTANT = 0
    ALTERNATING = 1
    ALTERNATING_BY_TWOS = 2


class HeadProcessController:
    """The head node, which controls which pods process which images.

    Decides how and when each detector-visit is farmed out.

    Despite being the head node, the behaviour of this controller can be
    remotely controlled by a RemoteProcessController, for example to change the
    processing strategy from a notebook or from LOVE.
    """
    def __init__(self):
        self.redisHelper = RedisHelper(isHeadNode=True)
        self.focalPlane = CameraControlConfig()
        self.workerMode = WorkerProcessingMode.WAITING
        self.visitMode = VisitProcessingMode.CONSTANT

    def confirmRunning(self, instrument):
        self.redisHelper.redis.setex(f'butlerWatcher-{instrument}',
                                     timedelta(seconds=10),
                                     value=1)

    def executeRemoteCommands(self):
        commandList = self.redisHelper.getRemoteCommands()
        if commandList is None:
            return

        def getBottomComponent(obj, componentList):
            if len(componentList) == 0:
                return obj
            else:
                return getBottomComponent(getattr(obj, componentList[0]), componentList[1:])

        def parseCommand(command):
            getterParts = None
            setterPart = None
            if '=' in command:
                getterPart, setterPart = command.split('=')
                getterParts = getterPart.split('.')
            else:
                getterParts = command.split('.')
            return getterParts, setterPart

        for command in commandList:
            for method, kwargs in command.items():
                getterParts, setter = parseCommand(method)
                component = getBottomComponent(self, getterParts[:-1])
                functionName = getterParts[-1]
                if setter is not None:
                    component.__setattr__(functionName, eval(setter))
                else:
                    attr = getattr(component, functionName)
                    attr.__call__(**kwargs)

    def doFanout(self):
        expRecord = self.redisHelper.popDataId('raw')
        if expRecord is not None:
            for detector in self.focalPlane.getEnabledDetIds():
                self.redisHelper.enqueueCurrentWork(expRecord, detector)

    def run(self):
        while True:
            self.executeRemoteCommands()  # look for remote control commands here
            self.confirmRunning()  # push the expiry out 10s
            self.doFanout()


class RemoteProcessController:
    def __init__(self):
        self.redisHelper = RedisHelper()

    def sendCommand(self, method, **kwargs):
        """Execute the specified method on the head node with the specified
        kwargs.

        Note that all kwargs must be JSON serializable.
        """
        payload = {method: kwargs}
        self.redisHelper.redis.lpush('commands', json.dumps(payload))


class PodProcessController:
    def __init__(self, detectors):
        self.redisHelper = RedisHelper()

    def isHeadNodeRuning(self, instrument):
        isRunning = self.redisHelper.redis.get(f'butlerWatcher-{instrument}')
        return bool(isRunning)  # 0 and None both bool() to False

    # def run():


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
        # XXX would be nice if we could improve the spurious/nonsense plot info
        self.plotInfo = {"plotName": "test plot", "run": "no run",
                         "tableName": None, "bands": []}

    @staticmethod
    def isWavefront(detector):
        """Check if the detector is a wavefront sensor.

        Parameters
        ----------
        detector : `lsst.afw.cameraGeom.Detector`
            The detector.

        Returns
        -------
        isWavefront : `bool`
            `True` is the detector is a wavefront sensor, else `False`.
        """
        return detector.getPhysicalType() == 'ITL_WF'

    @staticmethod
    def isGuider(detector):
        """Check if the detector is a guider.

        Parameters
        ----------
        detector : `lsst.afw.cameraGeom.Detector`
            The detector.

        Returns
        -------
        isGuider : `bool`
            `True` is the detector is a guider sensor, else `False`.
        """
        return detector.getPhysicalType() == 'ITL_G'

    @staticmethod
    def isImaging(detector):
        """Check if the detector is an imaging sensor.

        Parameters
        ----------
        detector : `lsst.afw.cameraGeom.Detector`
            The detector.

        Returns
        -------
        isImaging : `bool`
            `True` is the detector is an imaging sensor, else `False`.
        """
        return detector.getPhysicalType() in ['E2V', 'ITL']

    @staticmethod
    def _getRaftTuple(detector):
        """Get the detector's raft x, y coordinates as integers.

        Numbers are zero-indexed, with (0, 0) being at the bottom left.

        Parameters
        ----------
        detector : `lsst.afw.cameraGeom.Detector`
            The detector.

        Returns
        -------
        x : `int`
            The raft's column number, zero-indexed.
        y : `int`
            The raft's row number, zero-indexed.
        """
        rString = detector.getName().split('_')[0]
        return int(rString[1]), int(rString[2])

    @staticmethod
    def _getSensorTuple(detector):
        """Get the detector's x, y coordinates as integers within the raft.

        Numbers are zero-indexed, with (0, 0) being at the bottom left.

        Parameters
        ----------
        detector : `lsst.afw.cameraGeom.Detector`
            The detector.

        Returns
        -------
        x : `int`
            The detectors's column number, zero-indexed within the raft.
        y : `int`
            The detectors's row number, zero-indexed within the raft.
        """
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
        """Turn all the wavefront sensors on.
        """
        for detector in self._wavefronts:
            self._detectorStates[detector] = True

    def setWavefrontOff(self):
        """Turn all the wavefront sensors off.
        """
        for detector in self._wavefronts:
            self._detectorStates[detector] = False

    def setGuidersOn(self):
        """Turn all the guider sensors on.
        """
        for detector in self._guiders:
            self._detectorStates[detector] = True

    def setGuidersOff(self):
        """Turn all the wavefront sensors off.
        """
        for detector in self._guiders:
            self._detectorStates[detector] = False

    def setFullChequerboard(self, phase=0):
        """Set a chequerboard pattern at the CCD level.

        Parameters
        ----------
        phase : `int`, optional
            Any integer is acceptable as it is applied mod-2, so even integers
            will get you one phase, and odd integers will give the other.
            Even-phase contains 96 detectors, odd-phase contains 93.
        """
        for detector in self._imaging:
            x, y = self._getFullLocationTuple(detector)
            self._detectorStates[detector] = ((x % 2) + (y % 2) + phase) % 2

    def setRaftChequerboard(self, phase=0):
        """Set a chequerboard pattern at the raft level.

        Parameters
        ----------
        phase : `int`, optional
            Any integer is acceptable as it is applied mod-2, so even integers
            will get you one phase, and odd integers will give the other. The
            even-phase contains 108 detectors (12 rafts), the odd-phase
            contains 81 (9 rafts).
        """
        for detector in self._imaging:
            raftX, raftY = self._getRaftTuple(detector)
            self._detectorStates[detector] = ((raftX % 2) + (raftY % 2) + phase) % 2

    def setE2Von(self):
        """Turn all e2v sensors on.
        """
        for detector in self._imaging:
            if detector.getPhysicalType() == 'E2V':
                self._detectorStates[detector] = True

    def setITLon(self):
        """Turn all ITL sensors on.
        """
        for detector in self._imaging:
            if detector.getPhysicalType() == 'ITL':
                self._detectorStates[detector] = True

    def setAllOn(self):
        """Turn all sensors on.

        Note that this includes wavefront sensors and guiders.
        """
        for detector in self._detectors:
            self._detectorStates[detector] = True

    def setAllOff(self):
        """Turn all sensors off.

        Note that this includes wavefront sensors and guiders.
        """
        for detector in self._detectors:
            self._detectorStates[detector] = False

    def setAllImagingOn(self):
        """Turn all imaging sensors on.
        """
        for detector in self._imaging:
            self._detectorStates[detector] = True

    def setAllImagingOff(self):
        """Turn all imaging sensors off.
        """
        for detector in self._imaging:
            self._detectorStates[detector] = False

    def invertImagingSelection(self):
        """Invert the selection of the imaging chips only.
        """
        for detector in self._imaging:
            self._detectorStates[detector] = not self._detectorStates[detector]

    def getNumEnabled(self):
        """Get the number of enabled sensors.

        Returns
        -------
        nEnabled : `int`
            The number of enabled CCDs.
        """
        return sum(self._detectorStates.values())

    def getEnabledDetIds(self):
        """Get the detectorIds of the enabled sensors.

        Returns
        -------
        enabled : `list` of `int`
            The detectorIds of the enabled CCDs.
        """
        return sorted([det.getId() for (det, state) in self._detectorStates.items() if state is True])

    def asPlotData(self):
        """Get the data in a form for rendering as a FocalPlaneGeometryPlot.

        Returns
        -------
        XXX Get this from analysis tools directly.
        """
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

    def plotConfig(self, saveAs=''):
        """Plot the current configuration.

        Parameters
        ----------
        saveAs : `str`, optional
            If specified, save the figure to this file.

        Returns
        -------
        fig : `matplotlib.figure.Figure`
            The plotted focal plane as a `Figure`.
        """
        self.plot.level = 'detector'
        plot = self.plot.makePlot(self.asPlotData(), self.camera, self.plotInfo)
        if saveAs:
            plot.savefig(saveAs)
        return plot
