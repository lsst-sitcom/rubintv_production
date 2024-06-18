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

import enum
import json
import logging
from ast import literal_eval
from time import sleep
from typing import Iterable

import numpy as np

from lsst.analysis.tools.actions.plot import FocalPlaneGeometryPlot
from lsst.ctrl.mpexec import TaskFactory
from lsst.daf.butler import CollectionType, DataCoordinate, MissingCollectionError
from lsst.obs.base import DefineVisitsConfig, DefineVisitsTask
from lsst.obs.lsst import LsstCam
from lsst.pipe.base import Instrument, Pipeline, PipelineGraph
from lsst.utils.iteration import ensure_iterable
from lsst.utils.packages import Packages

from .payloads import Payload, pipelineGraphToBytes
from .redisUtils import RedisHelper
from .timing import BoxCarTimer
from .utils import getShardPath, writeExpRecordMetadataShard


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


def prepRunCollection(
    butler,
    pipelineGraphs: PipelineGraph | Iterable[PipelineGraph],
    run,
    packages: Packages,
):
    """This should only be run once with a particular combination of
    pipelinegraph and run.

    This writes the schemas (and the configs? to check). It does *not* write
    the software versions!

    Return
    ------
    created : `bool`
        Was a new run created? ``True`` if so, ``False`` if it already existed.
    """
    log = logging.getLogger("lsst.rubintv.production.processControl.prepRunCollection")
    newRun = butler.registry.registerCollection(run, CollectionType.RUN)  # fine to always call this
    if not newRun:
        raise RuntimeError(
            f"New {run=} already exists, so either there is a logic error in the head node"
            " init/getLatestRunAndPrep() or someone manually created collections with that"
            " prefix and didn't chain it to the output collection."
        )

    pipelineGraphs = list(ensure_iterable(pipelineGraphs))
    log.info(f"Prepping new run {run} with {len(pipelineGraphs)} pipelineGraphs")
    butler.put(packages, "packages", run=run)

    for pipelineGraph in pipelineGraphs:
        for datasetTypeNode in pipelineGraph.dataset_types.values():
            if pipelineGraph.producer_of(datasetTypeNode.name) is not None:
                butler.registry.registerDatasetType(datasetTypeNode.dataset_type)

        initRefs = {}
        taskFactory = TaskFactory()
        for taskDef, taskNode in zip(pipelineGraph._iter_task_defs(), pipelineGraph.tasks.values()):
            inputRefs = [
                (
                    butler.find_dataset(readEdge.dataset_type_name, collections=[run])
                    if readEdge.dataset_type_name not in readEdge.dataset_type_name
                    else initRefs[readEdge.dataset_type_name]
                )
                for readEdge in taskNode.init.inputs.values()
            ]
            task = taskFactory.makeTask(taskDef, butler, inputRefs)

            for writeEdge in taskNode.init.outputs.values():
                datasetTypeName = writeEdge.dataset_type_name
                initRefs[datasetTypeName] = butler.put(
                    getattr(task, writeEdge.connection_name),
                    datasetTypeName,
                    run=run,
                )


def defineVisit(butler, expRecord):
    """Define a visit in the registry, given an expRecord.

    Note that this takes about 9ms regardless of whether it exists, so it
    is no quicker to check than just run the define call.

    NB: butler must be writeable for this to work.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to define the visit for.
    """
    ids = list(butler.registry.queryDimensionRecords("visit", dataId=expRecord.dataId))
    if len(ids) == 0:  # only run if needed
        instr = Instrument.from_string(butler.registry.defaults.dataId["instrument"], butler.registry)
        config = DefineVisitsConfig()
        instr.applyConfigOverrides(DefineVisitsTask._DefaultName, config)

        task = DefineVisitsTask(config=config, butler=butler)

        task.run([{"exposure": expRecord.id}], collections=butler.collections)


def getVisitId(butler, expRecord):
    """Lookup visitId for an expRecord or dataId containing an exposureId
    or other uniquely identifying keys such as dayObs and seqNum.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record for which to get the visit id.

    Returns
    -------
    visitDataId : `int`
        The visitId, as an int.
    """
    expIdDict = {"exposure": expRecord.id}
    visitDataIds = butler.registry.queryDataIds(["visit"], dataId=expIdDict)
    visitDataIds = list(set(visitDataIds))
    if len(visitDataIds) == 1:
        visitDataId = visitDataIds[0]
        return visitDataId["visit"]
    else:
        log = logging.getLogger("lsst.rubintv.production.processControl.HeadProcessController")
        log.warning(
            f"Failed to find visitId for {expIdDict}, got {visitDataIds}. Do you need to run"
            " define-visits?"
        )
        return None


def getHeadNodeName(instrument):
    return f"headNode-{instrument}"


def getStep2aTriggerTask(pipelineFile):
    """Get the last task in a step1 which runs, to know when to trigger step2a.

    This is the task which is run when a decetor-exposure is complete, and
    which therefore means it's time to trigger the step2a processing if all
    quanta are complete.

    Parameters
    ----------
    pipelineFile : `str`
        The pipelineFile defining the pipeline. Hopefully we can use the real
        pipeline in the future and thus avoid the hard-coding of strings below.

    Returns
    -------
    taskName : `str`
        The task which triggers step2a processing.
    """
    if "nightly-validation" in pipelineFile:
        return "lsst.pipe.tasks.postprocess.TransformSourceTableTask"
    elif "quickLook" in pipelineFile:
        return "lsst.pipe.tasks.calibrate.CalibrateTask"
    else:
        raise ValueError(f"Unsure how to trigger step2a when {pipelineFile=}")


def getNightlyRollupTriggerTask(pipelineFile):
    """Get the last task in a step1 which runs, to know when to trigger step2a.

    This is the task which is run when a decetor-exposure is complete, and
    which therefore means it's time to trigger the step2a processing if all
    quanta are complete.

    Parameters
    ----------
    pipelineFile : `str`
        The pipelineFile defining the pipeline. Hopefully we can use the real
        pipeline in the future and thus avoid the hard-coding of strings below.

    Returns
    -------
    taskName : `str`
        The task which triggers step2a processing.
    """
    if "nightly-validation" in pipelineFile:
        return "lsst.analysis.tools.tasks.refCatSourceAnalysis.RefCatSourceAnalysisTask"
    elif "quickLook" in pipelineFile:
        return "lsst.pipe.tasks.postprocess.ConsolidateVisitSummaryTask"
    else:
        raise ValueError(f"Unsure how to trigger nightly rollup when {pipelineFile=}")


class HeadProcessController:
    """The head node, which controls which pods process which images.

    Decides how and when each detector-visit is farmed out.

    Despite being the head node, the behaviour of this controller can be
    remotely controlled by a RemoteController, for example to change the
    processing strategy from a notebook or from LOVE.
    """

    targetLoopDuration = 0.2  # in seconds, so 5Hz

    def __init__(self, butler, instrument, locationConfig, pipelineFile, outputChain=None, forceNewRun=False):
        self.butler = butler
        self.instrument = instrument
        self.locationConfig = locationConfig
        self._basePipeline = pipelineFile
        self.name = getHeadNodeName(instrument)
        self.log = logging.getLogger("lsst.rubintv.production.processControl.HeadProcessController")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig, isHeadNode=True)
        self.focalPlaneControl = CameraControlConfig()
        self.workerMode = WorkerProcessingMode.WAITING
        self.visitMode = VisitProcessingMode.CONSTANT
        self.remoteController = RemoteController(butler=butler, locationConfig=locationConfig)
        self.workTimer = BoxCarTimer(length=100)  # don't start here, the event loop starts the lap timer
        self.loopTimer = BoxCarTimer(length=100)  # don't start here, the event loop starts the lap timer
        self.nDispatched = 0
        self.nNightlyRollups = 0

        steps = ("step1", "step2a", "nightlyRollup")  # NB: these need to be in order for prepRunCollection!
        self.pipelineGraphUris = {}
        self.pipelineGraphs = {}
        self.pipelineGraphsBytes = {}

        for step in steps:
            stepStr = "#" + step
            self.pipelineGraphUris[step] = self._basePipeline + stepStr
            self.pipelineGraphs[step] = Pipeline.fromFile(self.pipelineGraphUris[step]).to_graph(
                registry=self.butler.registry
            )
            self.pipelineGraphsBytes[step] = pipelineGraphToBytes(self.pipelineGraphs[step])

        if outputChain is None:
            # allows it to be user specified, or use the default from the site
            # config, but e.g. slac_testing doesn't use the real quickLook
            # collection, but the k8s configs do.
            outputChain = locationConfig.getOutputChain(self.instrument)
        self.outputChain = outputChain

        self.outputRun = self.getLatestRunAndPrep(forceNewRun=forceNewRun)
        self.log.info(f"Head node ready. Data will be writen data to {self.outputRun}")

    def getLatestRunAndPrep(self, forceNewRun):
        packages = Packages.fromSystem()

        allRuns = []
        try:
            allRuns = self.butler.registry.getCollectionChain(self.outputChain)
        except MissingCollectionError:
            self.butler.registry.registerCollection(self.outputChain, CollectionType.CHAINED)
            lastRun = f"{self.outputChain}/0"
            prepRunCollection(self.butler, self.pipelineGraphs.values(), lastRun, packages)
            self.butler.registry.setCollectionChain(self.outputChain, [lastRun])
            self.log.info(f"Started brand new collection at {lastRun}")
            return lastRun

        allRunNums = [int(run.removeprefix(self.outputChain + "/")) for run in allRuns]
        lastRunNum = max(allRunNums)
        lastRun = f"{self.outputChain}/{lastRunNum}"

        if forceNewRun or self.checkIfNewRunNeeded(lastRun, packages):
            lastRunNum += 1
            lastRun = f"{self.outputChain}/{lastRunNum}"

            # prepRunCollection is called instead of registerCollection
            prepRunCollection(self.butler, self.pipelineGraphs.values(), lastRun, packages)
            self.butler.registry.setCollectionChain(self.outputChain, [lastRun] + list(allRuns))
            self.log.info(f"Started new run collection at {lastRun}")

        return lastRun

    def checkIfNewRunNeeded(self, lastRun, packages):
        """Check if a new run is needed, and if so, create it and prep it.

        Needed if the configs change, or if the software versions change, or if
        the pipelines changes, but that's mostly likely going to happen via
        config changes anyway.

        Note that this is safe for checking config versions so long as the
        configs only come from packages in git, so DRP_PIPE and obs_packages.
        The only way of this going wrong would be either running wiht -c on the
        command line, which isn't relevant here, or pushing straight to the
        head node from a notebook *and* using the same outputChain. As long as
        notebook users always set a manual outputChain and don't squat on
        quickLook this is sufficient.
        """
        oldPackages = self.butler.get("packages", collections=[lastRun])
        if packages.difference(oldPackages):  # checks if any of the versions are different
            return True
        return False

    def getFreeSFMWorkerQueue(self, detectorId):
        # TODO: this really should take all the detectorIds that we need a free
        # worker for and return them all at once so that we only have to call
        # redisHelper.getFreeWorkers() once but I'm too low on time right now.

        sfmWorkers = self.redisHelper.getFreeWorkers(workerType="SFM")
        sfmWorkers = sorted(sfmWorkers)  # the lowest number in the stack will be at the top alphabetically

        # get ones which match the detectorId. We'll make this smarter later.
        idMatchedWorkers = [queue for queue in sfmWorkers if f"-{detectorId:02}-" in queue]
        if idMatchedWorkers == []:
            # TODO: until we have a real backlog queue just put it on the last
            # worker in the stack.
            busyWorkers = self.redisHelper.getAllWorkers(workerType="SFM")
            idMatchedWorkers = [queue for queue in busyWorkers if f"-{detectorId:02}-" in queue]
            busyWorker = idMatchedWorkers[-1]
            self.log.warning(f"No free workers available for {detectorId=}, sending work to {busyWorker=}")
            return busyWorker
        return idMatchedWorkers[0]

    def getFreeGatherWorkerQueue(self, workerType):
        freeWorkers = self.redisHelper.getFreeWorkers(workerType=workerType)
        freeWorkers = sorted(freeWorkers)  # the lowest number in the stack will be at the top alphabetically
        if freeWorkers:
            return freeWorkers[0]

        # We have no free workers of this type, so send to a busy work and warn
        # TODO: until we have a real backlog queue just put it on the last
        # worker in the stack.
        busyWorkers = self.redisHelper.getAllWorkers(workerType=workerType)
        busyWorker = busyWorkers[-1]
        self.log.warning(f"No free workers available for {workerType=}, sending work to {busyWorker=}")
        return busyWorker

    def doStep1Fanout(self, expRecord):
        """Send the expRecord out for processing based on current selection.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The expRecord to process.
        """
        match self.instrument:
            case "LATISS":
                detectorIds = [0]
            case instrument if instrument in ("LSSTComCam", "LSSTComCamSim"):
                detectorIds = range(9)  # at least for OR3, always process all ComCam chips
            case "LSSTCom":
                detectorIds = self.focalPlaneControl.getEnabledDetIds()
            case _:
                raise ValueError(f"Unknown instrument {self.instrument=}")

        dataIds = {}
        for detectorId in detectorIds:
            dataIds[detectorId] = DataCoordinate.standardize(expRecord.dataId, detector=detectorId)

        self.log.info(
            f"Fanning {expRecord.instrument}-{expRecord.day_obs}-{expRecord.seq_num}"
            f" out to {len(detectorIds)} detectors."
        )

        for detectorId, dataId in dataIds.items():
            queueName = self.getFreeSFMWorkerQueue(detectorId)
            self.log.info(f"Sending {detectorId=} to {queueName} for {dataId}")
            payload = Payload(
                dataId=dataId, pipelineGraphBytes=self.pipelineGraphsBytes["step1"], run=self.outputRun
            )
            self.redisHelper.enqueuePayload(payload, queueName)

        self.nDispatched += 1  # required for the alternating by twos mode

    def getNewExposureAndDefineVisit(self):
        expRecord = self.redisHelper.getExposureForFanout(self.instrument)
        if expRecord is None:
            return

        # first time touching the new expRecord so run define visits

        # butler must be writeable for the task to run, but don't check here
        # and let the DefineVisitsTaskraise, because it is useful to be able to
        # run from a notebook with a normal butler when not needing to define
        # visits
        self.log.info(f"Defining visit (if needed) for {expRecord.id}")
        defineVisit(self.butler, expRecord)
        return expRecord

    def repattern(self):
        """Apply the VisitProcessingMode to the focal plane sensor
        selection.
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
                raise ValueError(f"Unknown visit processing mode {self.visitMode=}")

    def getNumExpected(self, instrument):
        if instrument in ("LSSTComCam", "LSSTComCamSim"):
            return 9
        raise NotImplementedError(f"Need to extend dispatch for non-single-raft instruments {instrument=}")

    def _dispatch2a(self, dataCoordinate):
        payload = Payload(
            dataId=dataCoordinate, pipelineGraphBytes=self.pipelineGraphsBytes["step2a"], run=self.outputRun
        )
        # caps for the queue name. Maybe should reconsider how that's dealt wit
        # post OR3
        queueName = self.getFreeGatherWorkerQueue("STEP2A")
        self.redisHelper.enqueuePayload(payload, queueName)

    def dispatchGatherSteps(self, triggeringTask, step, dispatchIncomplete=False):
        """Dispatch any gather steps as needed.

        Note that the return value is currently unused, but is planned to be
        built upon in the next few tickets.

        Returns
        -------
        dispatchedWork : `bool`
            Was anything sent out?
        """
        # allIds is all the incomplete or just-completed exp/visit ids for
        # gather processing. Completed ids are removed once the gather step
        # has been run.
        allIds = set(self.redisHelper.getIdsForTask(self.instrument, triggeringTask))
        completeIds = [
            _id
            for _id in allIds
            if self.redisHelper.getNumFinished(self.instrument, triggeringTask, _id)
            == self.getNumExpected(self.instrument)
        ]

        if len(allIds) == 0:
            return False

        for _id in allIds:
            isComplete = _id in completeIds
            dataCoord = DataCoordinate.standardize(
                instrument=self.instrument, exposure=_id, universe=self.butler.dimensions
            )
            if isComplete:
                self.log.info(f"Dispatching {step} with complete inputs for {dataCoord}")
                self._dispatch2a(dataCoord)
                self.redisHelper.removeTaskCounter(self.instrument, triggeringTask, _id)
            else:
                if dispatchIncomplete:
                    self.log.info(f"Dispatching incomplete {step} for {dataCoord}")
                    self._dispatch2a(dataCoord)
                    # NB do not remove the counter key here, as this will be
                    # redispatched once complete, and should only be removed
                    # then

        if completeIds or (dispatchIncomplete and allIds):
            return True  # we sent something out
        return False  # nothing was sent

    def dispatchRollupIfNecessary(self):
        """Check if we should do another rollup, and if so, dispatch it.

        Returns
        -------
        doRollup : `bool`
            Should we do another rollup?
        """
        numComplete = self.redisHelper.getNumVisitLevelFinished(self.instrument, "step2a")
        if numComplete > self.nNightlyRollups:
            self.log.info(
                f"Found {numComplete - self.nNightlyRollups} more completed step2a's - "
                " dispatching them for nightly rollup"
            )
            self.nNightlyRollups = numComplete
            self._dispatchNightlyRollup()
            return True
        return False

    def _dispatchNightlyRollup(self):
        dataId = {"instrument": self.instrument, "skymap": "ops_rehersal_prep_2k_v1"}
        dataCoord = DataCoordinate.standardize(dataId, universe=self.butler.dimensions)
        payload = Payload(dataCoord, self.pipelineGraphsBytes["step2a"], run=self.outputRun)
        queueName = self.getFreeGatherWorkerQueue("NIGHTLYROLLUP")
        self.redisHelper.enqueuePayload(payload, queueName)

    def dispatchFocalPlaneMosaics(self):
        """Dispatch the focal plane mosaic task.

        This will be dispatched to a worker which will then gather the
        individual CCD mosaics and make the full focal plane mosaic and upload
        to S3. At the moment, it will only work when everything is completed.
        """
        triggeringTasks = ("lsst.ip.isr.isrTask.IsrTask", "binnedCalexpCreation")
        dataProducts = ("postISRCCD", "calexp")

        for triggeringTask, dataProduct in zip(triggeringTasks, dataProducts):
            allIds = set(self.redisHelper.getIdsForTask(self.instrument, triggeringTask))
            completeIds = [
                _id
                for _id in allIds
                if self.redisHelper.getNumFinished(self.instrument, triggeringTask, _id)
                == self.getNumExpected(self.instrument)
            ]
            if not completeIds:
                continue

            self.log.info(f"Dispatching {len(completeIds)} complete focal {dataProduct} mosaics for creation")
            for expId in completeIds:
                dataId = {"exposure": expId, "instrument": self.instrument}
                dataCoord = DataCoordinate.standardize(dataId, universe=self.butler.dimensions)
                # TODO: this abuse of Payload really needs improving
                payload = Payload(dataCoord, bytes("".encode("utf-8")), dataProduct)
                queueName = self.getFreeGatherWorkerQueue("MOSAIC")
                self.redisHelper.enqueuePayload(payload, queueName)
                self.redisHelper.removeTaskCounter(self.instrument, triggeringTask, expId)

    def regulateLoopSpeed(self):
        """Attempt to regulate the loop speed to the target frequency.

        This will sleep for the appropriate amount of time if the loop is
        running quickly enough to require it, and will log a warning if the
        loop is running too slowly. The sleep time doesn't count towards the
        loop timings, that is only the time taken to actually perform the event
        loop's work.
        """
        self.loopTimer.lap()  # times the actual loop
        self.workTimer.lap()  # times the actual work done in the loop

        # XXX there is a minor bug here in what the logs say but it's not
        # serious enough for me to fix right now. I don't think it's affecting
        # things.
        if self.loopTimer.totalLaps % 100 == 0:  # +1 so we don't trigger this on the first loop
            loopSpeed = self.loopTimer.median(frequency=True)
            maxLoopTime = self.loopTimer.max(frequency=False)
            self.log.info(
                f"Event loop running at regulated speed of {loopSpeed:.2f}Hz with a max time of"
                f" {maxLoopTime:.2f}s for the last {len(self.loopTimer._buffer)} loops"
            )

            medianFreq = self.workTimer.mean(frequency=True)
            maxWorkTime = self.workTimer.mean(frequency=False)
            self.log.info(
                f"If unlimited, the event loop would run at {medianFreq:.2f}Hz, with a longest"
                f" workload of {maxWorkTime:.2f}s in the last {len(self.workTimer._buffer)} loops"
            )

        sleepPeriod = self.targetLoopDuration - self.loopTimer.lastLapTime()
        if sleepPeriod > 0:
            self.workTimer.pause()  # don't count the sleeping towards the loop time on work timer
            sleep(sleepPeriod)
            self.workTimer.resume()
        else:
            if sleepPeriod < -0.01:  # allow some noise
                lastLap = self.loopTimer.lastLapTime()
                lastWork = self.loopTimer.lastLapTime()
                self.log.warning(
                    f"Event loop running slow, last loop took {lastLap:.2f}s" f" with {lastWork:.2f}s of work"
                )

    def run(self):
        self.workTimer.start()  # times how long it actually takes to do the work
        self.loopTimer.start()  # checks the delivered loop performance
        while True:
            # affirmRunning should be longer than longest loop but no longer
            self.redisHelper.affirmRunning(self.name, 5)

            self.remoteController.executeRemoteCommands(self)  # look for remote control commands here
            expRecord = self.getNewExposureAndDefineVisit()
            if expRecord is not None:
                assert self.instrument == expRecord.instrument
                writeExpRecordMetadataShard(expRecord, getShardPath(self.locationConfig, expRecord))
                self.doStep1Fanout(expRecord)

            # for now, only dispatch to step2a once things are complete because
            # there is some subtlety in the dispatching incomplete things
            # because they will be dispatched again and again until they are
            # complete, and that will happen not when another completes, but at
            # the speed of this loop, which would be bad, so we need to deal
            # with tracking that and dispatching only if the number has gone up
            # *and* there are 2+ free workers, because it's not worth
            # re-dispatching for every single new CCD exposure which finishes.
            self.dispatchGatherSteps(
                triggeringTask=getStep2aTriggerTask(self._basePipeline),
                step="step2a",
                dispatchIncomplete=False,
            )

            self.dispatchFocalPlaneMosaics()

            self.dispatchRollupIfNecessary()

            # note the repattern comes after the fanout so that any commands
            # executed are present for the next image to follow and only then
            # do we toggle
            self.repattern()

            self.regulateLoopSpeed()


class RemoteController:
    def __init__(self, butler, locationConfig):
        self.log = logging.getLogger("lsst.rubintv.production.processControl.RemoteController")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)

    def sendCommand(self, method, **kwargs):
        """Execute the specified method on the head node with the specified
        kwargs.

        Note that all kwargs must be JSON serializable.
        """
        payload = {method: kwargs}
        self.redisHelper.redis.lpush("commands", json.dumps(payload))

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
            if "=" in command:
                getterPart, setterPart = command.split("=")
                getterParts = getterPart.split(".")
            else:
                getterParts = command.split(".")
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

            if setterPart.split(".")[0] in globals():
                # we're instantiating a known class, so it's safe
                item = eval(setterPart)
                return item
            raise ValueError(f"Will not execute arbitrary code - got {setterPart=}")

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


class CameraControlConfig:
    """Processing control for which CCDs will be processed."""

    def __init__(self):
        self.camera = LsstCam.getCamera()
        self._detectorStates = {det: False for det in self.camera}
        self._detectors = [det for det in self.camera]
        self._imaging = [det for det in self._detectors if self.isImaging(det)]
        self._guiders = [det for det in self._detectors if self.isGuider(det)]
        self._wavefronts = [det for det in self._detectors if self.isWavefront(det)]
        self._focalPlanePlot = FocalPlaneGeometryPlot()

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
        return detector.getPhysicalType() == "ITL_WF"

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
        return detector.getPhysicalType() == "ITL_G"

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
        return detector.getPhysicalType() in ["E2V", "ITL"]

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
        rString = detector.getName().split("_")[0]
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
        sString = detector.getName().split("_")[1]
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
        """Turn all the wavefront sensors on."""
        for detector in self._wavefronts:
            self._detectorStates[detector] = True

    def setWavefrontOff(self):
        """Turn all the wavefront sensors off."""
        for detector in self._wavefronts:
            self._detectorStates[detector] = False

    def setGuidersOn(self):
        """Turn all the guider sensors on."""
        for detector in self._guiders:
            self._detectorStates[detector] = True

    def setGuidersOff(self):
        """Turn all the wavefront sensors off."""
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
            self._detectorStates[detector] = bool(((x % 2) + (y % 2) + phase) % 2)

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
            self._detectorStates[detector] = bool(((raftX % 2) + (raftY % 2) + phase) % 2)

    def setE2Von(self):
        """Turn all e2v sensors on."""
        for detector in self._imaging:
            if detector.getPhysicalType() == "E2V":
                self._detectorStates[detector] = True

    def setE2Voff(self):
        """Turn all e2v sensors off."""
        for detector in self._imaging:
            if detector.getPhysicalType() == "E2V":
                self._detectorStates[detector] = False

    def setITLon(self):
        """Turn all ITL sensors on."""
        for detector in self._imaging:
            if detector.getPhysicalType() == "ITL":
                self._detectorStates[detector] = True

    def setITLoff(self):
        """Turn all ITL sensors off."""
        for detector in self._imaging:
            if detector.getPhysicalType() == "ITL":
                self._detectorStates[detector] = False

    def setFullFocalPlaneGuidersOn(self):
        """Turn all ITL sensors on."""
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
        """Turn all imaging sensors on."""
        for detector in self._imaging:
            self._detectorStates[detector] = True

    def setAllImagingOff(self):
        """Turn all imaging sensors off."""
        for detector in self._imaging:
            self._detectorStates[detector] = False

    def invertImagingSelection(self):
        """Invert the selection of the imaging chips only."""
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
        """Get the data in a form for rendering as a ``FocalPlaneGeometryPlot``

        Returns
        -------
        data : `dict`
            A dict with properties which match the pandas dataframe `data`
        which analysis_tools expects.
            The catalog to plot the points from. It is necessary for it to
            contain the following columns/keys:

            ``"detector"``
                The integer detector id for the points.
            ``"amplifier"``
                The string amplifier name for the points.
            ``"z"``
                The numerical value that will be combined via
                ``statistic`` to the binned value.
            ``"x"``
                Focal plane x position, optional.
            ``"y"``
                Focal plane y position, optional.
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
            "detector": detNums,
            "amplifier": ampNames,
            "x": np.array(x),
            "y": np.array(y),
            "z": np.array(z),
        }

    def plotConfig(self, saveAs=""):
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
        self._focalPlanePlot.level = "detector"
        plot = self._focalPlanePlot.makePlot(self.asPlotData(), self.camera, plotInfo=None)
        if saveAs:
            plot.savefig(saveAs)
        return plot
