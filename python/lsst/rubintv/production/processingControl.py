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
import logging
from ast import literal_eval
from time import sleep
import io

from lsst.analysis.tools.actions.plot import FocalPlaneGeometryPlot
from lsst.obs.lsst import LsstCam
from lsst.daf.butler import MissingCollectionError, CollectionType
from lsst.pipe.base import Pipeline
from lsst.ctrl.mpexec import TaskFactory
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


def prepRunCollection(butler, pipelineGraph, run):
    """This should only be run once with a particular combination of
    pipelinegraph and run.

    This writes the schemas (and the configs? to check). It does *not* write
    the software versions!
    """
    newRun = butler.registry.registerCollection(run, CollectionType.RUN)  # fine to always call this
    if not newRun:
        return newRun

    initRefs = {}
    taskFactory = TaskFactory()
    for taskDef, taskNode in zip(pipelineGraph._iter_task_defs(), pipelineGraph.tasks.values()):

        inputRefs = [butler.find_dataset(readEdge.dataset_type_name, collections=[run])
                     if readEdge.dataset_type_name not in readEdge.dataset_type_name else initRefs[readEdge.dataset_type_name]
                     for readEdge in taskNode.init.inputs.values()]
        task = taskFactory.makeTask(taskDef, butler, inputRefs)

        for writeEdge in taskNode.init.outputs.values():
            datasetTypeName = writeEdge.dataset_type_name
            initRefs[datasetTypeName] = butler.put(getattr(task, writeEdge.connection_name), datasetTypeName, run=run)


class HeadProcessController:
    """The head node, which controls which pods process which images.

    Decides how and when each detector-visit is farmed out.

    Despite being the head node, the behaviour of this controller can be
    remotely controlled by a RemoteController, for example to change the
    processing strategy from a notebook or from LOVE.
    """
    def __init__(self, outputChain=None, forceNewRun=False):
        self.instrument = 'LSSTCam'
        self.name = f'headNode-{self.instrument}'
        self.log = logging.getLogger('lsst.rubintv.production.processControl.HeadProcessController')
        self.redisHelper = RedisHelper(isHeadNode=True)
        self.focalPlaneControl = CameraControlConfig()
        self.workerMode = WorkerProcessingMode.WAITING
        self.visitMode = VisitProcessingMode.CONSTANT
        self.remoteController = RemoteController()
        self.nDispatched = 0

        self.sfmPipelineUri = '$DRP_PIPE_DIR/pipelines/LSSTComCamSim/RA-ops-rehearsal-3.yaml'
        self.sfmPipelineGraph = Pipeline.fromFile(self.sfmPipelineUri).to_graph(registry=self.butler.registry)
        with io.BytesIO() as f:
            self.sfmPipelineGraph._write_stream(f)
            self.sfmPipelineGraphBytes = f.getvalue()

        self.outputChain = f'{self.instument}/quickLook' if not outputChain else outputChain
        self.run = self.getLatestRun(forceNewRun=forceNewRun)

    def getLatestRun(self, forceNewRun):
        try:
            allRuns = self.butler.registry.getCollectionChain(self.outputChain)
        except MissingCollectionError:
            self.butler.registry.registerCollection(self.outputChain, CollectionType.CHAINED)
            allRuns = []

        if not allRuns:
            lastRun = f'{self.outputChain}/0'
            self.butler.registerCollection(lastRun, CollectionType.RUN)
            prepRunCollection(self.butler, self.sfmPipelineGraph, lastRun)
            self.butler.setCollectionChain(self.outputChain, [lastRun])
        elif forceNewRun:
            lastRun = int(allRuns[-1]) + 1
            self.butler.registerCollection(lastRun, CollectionType.RUN)
            prepRunCollection(self.butler, self.sfmPipelineGraph, lastRun)
            self.butler.setCollectionChain(self.outputChain, [lastRun] + list(allRuns))
        else:
            lastRun = allRuns[-1]
            # XXX check here if we need a new run, and if so, push it and prep
            # it
        return lastRun

    def checkIfNewRunNeeded(self):
        """Check if a new run is needed, and if so, create it and prep it.
        """
        pass

    def doFanout(self, expRecord):
        """Send the expRecord out for processing based on current selection.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The expRecord to process.
        """
        for detector in self.focalPlaneControl.getEnabledDetIds():
            self.redisHelper.enqueueCurrentWork(expRecord, detector)
        self.nDispatched += 1

    def getWork(self):
        return self.redisHelper.popDataId('raw')

    def repattern(self):
        """Apply the VisitProcessingMode to the focal plane sensor selection.
        """
        match self.visitMode:
            case VisitProcessingMode.CONSTANT:
                return
            case VisitProcessingMode.ALTERNATING:
                self.focalPlaneControl.invertImagingSelection()
            case VisitProcessingMode.ALTERNATING_BY_TWOS:
                if self.nDispatched % 2 == 0:
                    self.focalPlaneControl.invertImagingSelection()
            case _:
                raise ValueError(f'Unknown visit processing mode {self.visitMode=}')

    def run(self):
        while True:
            self.redisHelper.affirmRunning(self.name, 10)  # push the expiry out 10s
            self.remoteController.executeRemoteCommands(self)  # look for remote control commands here
            work = self.getWork()
            if work is None:
                sleep(0.1)
                continue

            self.doFanout(work)
            # note the repattern comes after the fanout so that any commands
            # executed are present for the next image to follow and only then
            # do we toggle
            self.repattern()


class RemoteController:
    def __init__(self):
        self.log = logging.getLogger('lsst.rubintv.production.processControl.RemoteController')
        self.redisHelper = RedisHelper()

    def sendCommand(self, method, **kwargs):
        """Execute the specified method on the head node with the specified
        kwargs.

        Note that all kwargs must be JSON serializable.
        """
        payload = {method: kwargs}
        self.redisHelper.redis.lpush('commands', json.dumps(payload))

    def executeRemoteCommands(self, parentClass):
        """Execute commands sent from a RemoteController or LOVE.

        Pops all remote commands from the stack and executes them as if they
        were calls to this class itself. Remote code can therefore do anything
        that this class itself can do, and furthermore, nothing that it cannot.

        Parameters
        ----------
        parentClass : `obj`
            The class which owns this ``RemoteController``, such that commands
            can be executed on the parent object itself, rather than only on
            the remote controller.
        """
        commandList = self.redisHelper.getRemoteCommands()
        if commandList is None:
            return

        def getBottomComponent(obj, componentList):
            """Get the bottom-most component of an object.

            Given a part of compound object, get the part which is being
            referred to, so for example, if passed
            `someClass.somePart.otherPart.componentToGet` then return
            `componentToGet` as an object, such that it can be called or set to
            things, as appropriate.

            Parameters
            ----------
            obj : `object`
                The object to get the component from.
            componentList : `list` of `str`
                The drill-down list, so from the example above, this would be
                ['somePart', 'otherPart', 'componentToGet']. Note it does not
                include the name of the class itself, i.e. 'self'.
            """
            if len(componentList) == 0:
                return obj
            else:
                return getBottomComponent(getattr(obj, componentList[0]), componentList[1:])

        def parseCommand(command):
            """Given a command, return the getting parts and the setting parts,
            if any.

            For example, 'focalPlane.setFullChequerboard' is just components to
            call, and would therefore return `['focalPlane',
            'setFullChequerboard'], None` and if the command were
            'workerMode=WorkerProcessingMode.CONSUMING' this would return
            `['workerMode'], ['WorkerProcessingMode.CONSUMING']`

            Parameters
            ----------
            command : `str`
                The command to parse.

            Returns
            -------
            getterParts : `list` of `str`
                List of components to get from `self`, such that the last item
                can be called.
            setterPart : `str`
                If a setter type command, what the component is being set to,
                such that it can be instantiated.
            """
            getterParts = None
            setterPart = None
            if '=' in command:
                getterPart, setterPart = command.split('=')
                getterParts = getterPart.split('.')
            else:
                getterParts = command.split('.')
            return getterParts, setterPart

        def safeEval(setterPart):
            """Ensure whatever we're being asked to instantiate is safe to.

            If a primative is passed, it's safely evaluated with literal_eval,
            otherwise, it's only instantiated if it's already an item in the
            global namespace, ensuring that arbitrary code execution cannot
            occur.

            Parameters
            ----------
            setterPart : `str`
                Whatever item we need to instantiate.

            Returns
            -------
            item : `obj`
                Whatever item was asked for.

            Raises
                ValueError if the item could not be safely evaluated.
            """
            try:
                # if we have a primative, get it simply and safely
                item = literal_eval(setterPart)
                return item
            except (SyntaxError, ValueError):  # anything non-primative will raise like this
                pass

            if setterPart.split('.')[0] in globals():
                # we're instantiating a known class, so it's safe
                item = eval(setterPart)
                return item
            raise ValueError(f'Will not execute arbitrary code - got {setterPart=}')

        # command list is a list of dict: dict with each dict only having a
        # single key, and the value being the kwargs, if any.
        for command in commandList:
            try:
                for method, kwargs in command.items():
                    getterParts, setter = parseCommand(method)
                    component = getBottomComponent(parentClass, getterParts[:-1])
                    functionName = getterParts[-1]
                    if setter is not None:
                        setItem = safeEval(setter)
                        component.__setattr__(functionName, setItem)
                    else:
                        attr = getattr(component, functionName)
                        attr.__call__(**kwargs)
            except Exception as e:
                self.log.exception(f"Failed to apply command {command}: {e}")
                return  # do not apply further commands as soon as one fails


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

    def setE2Voff(self):
        """Turn all e2v sensors off.
        """
        for detector in self._imaging:
            if detector.getPhysicalType() == 'E2V':
                self._detectorStates[detector] = False

    def setITLon(self):
        """Turn all ITL sensors on.
        """
        for detector in self._imaging:
            if detector.getPhysicalType() == 'ITL':
                self._detectorStates[detector] = True

    def setITLoff(self):
        """Turn all ITL sensors off.
        """
        for detector in self._imaging:
            if detector.getPhysicalType() == 'ITL':
                self._detectorStates[detector] = False

    def setFullFocalPlaneGuidersOn(self):
        """Turn all ITL sensors on.
        """
        for detector in self._imaging:
            sensorX, sensorY = self._getSensorTuple(detector)
            if sensorX <= 1 and sensorY <= 1:
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
