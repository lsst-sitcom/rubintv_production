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

import enum
import json
import logging
import time
from ast import literal_eval
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Any, Iterable, Sequence, cast

import numpy as np

from lsst.analysis.tools.actions.plot import FocalPlaneGeometryPlot
from lsst.ctrl.mpexec import TaskFactory
from lsst.daf.butler import (
    Butler,
    CollectionType,
    DataCoordinate,
    DatasetNotFoundError,
    DatasetRef,
    DimensionRecord,
    MissingCollectionError,
    Registry,
)
from lsst.daf.butler.registry.interfaces import DatabaseConflictError  # TODO: DM-XXXXX fix this import
from lsst.obs.base import DefineVisitsConfig, DefineVisitsTask
from lsst.obs.lsst import LsstCam
from lsst.pipe.base import Instrument, Pipeline, PipelineGraph
from lsst.utils import getPackageDir
from lsst.utils.packages import Packages

from .payloads import Payload, pipelineGraphToBytes
from .podDefinition import PodDetails, PodFlavor
from .redisUtils import RedisHelper
from .timing import BoxCarTimer
from .utils import (
    LocationConfig,
    getShardPath,
    isWepImage,
    raiseIf,
    writeExpRecordMetadataShard,
    writeMetadataShard,
)

if TYPE_CHECKING:
    from numpy.typing import NDArray

    from lsst.afw.cameraGeom import Detector


class WorkerProcessingMode(enum.IntEnum):
    """Defines the mode in which worker nodes process images.

    WAITING: The worker will process only the most recently taken image, and
        then will wait for new images to land, and will not process the backlog
        in the meantime.
    CONSUMING: The worker will always process the most recent image, but also
        process the backlog of images if no new images have landed during
        the last processing.
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


def ensureRunCollection(
    butler: Butler,
    pipelineGraphs: Iterable[PipelineGraph],
    packages: Packages,
    outputChain: str,
    runNumber: int,
) -> str:
    """This should only be run once with a particular combination of
    pipelinegraph and run.

    This writes the schemas (and the configs? to check). It does *not* write
    the software versions!

    Return
    ------
    created : `bool`
        Was a new run created? ``True`` if so, ``False`` if it already existed.
    """
    log = logging.getLogger("lsst.rubintv.production.processControl.ensureRunCollection")

    while True:
        run = f"{outputChain}/{runNumber}"
        newRun = butler.registry.registerCollection(run, CollectionType.RUN)
        if not newRun:
            runNumber += 1
            log.warning(
                f"New {run=} already existed, previous init probably failed, incrementing"
                " run number automatically"
            )
        else:
            break  # success

    pipelineGraphs = list(pipelineGraphs)
    log.info(f"Prepping new run {run} with {len(pipelineGraphs)} pipelineGraphs")
    butler.put(packages, "packages", run=run)

    for pipelineGraph in pipelineGraphs:
        for datasetTypeNode in pipelineGraph.dataset_types.values():
            if pipelineGraph.producer_of(datasetTypeNode.name) is not None:
                butler.registry.registerDatasetType(datasetTypeNode.dataset_type)

        initRefs: dict[str, Any] = {}
        taskFactory = TaskFactory()
        for taskNode in pipelineGraph.tasks.values():
            inputRefs = cast(
                Iterable[DatasetRef] | None,
                [
                    (
                        butler.find_dataset(readEdge.dataset_type_name, collections=[run])
                        if readEdge.dataset_type_name not in readEdge.dataset_type_name
                        else initRefs[readEdge.dataset_type_name]
                    )
                    for readEdge in taskNode.init.inputs.values()
                ],
            )
            task = taskFactory.makeTask(taskNode, butler, inputRefs)

            for writeEdge in taskNode.init.outputs.values():
                datasetTypeName = writeEdge.dataset_type_name
                initRefs[datasetTypeName] = butler.put(
                    getattr(task, writeEdge.connection_name),
                    datasetTypeName,
                    run=run,
                )
    return run


def defineVisit(butler: Butler, expRecord: DimensionRecord) -> None:
    """Define a visit in the registry, given an expRecord.

    Only runs if the visit hasn't already been defined. Previously, it was
    thought to be fine to run repeatedly, but updates in the stack can cause
    slight differences in the calcualted region, which causes a ConflictError,
    so only run if we don't already have a visit id available.

    NB: butler must be writeable for this to work.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to define the visit for.
    """
    ids = list(butler.registry.queryDimensionRecords("visit", dataId=expRecord.dataId))
    if len(ids) < 1:  # only run if needed
        instrumentString = expRecord.instrument
        assert isinstance(instrumentString, str), f"Expected {instrumentString=} to be a string"
        instr = Instrument.from_string(instrumentString, butler.registry)
        config = DefineVisitsConfig()
        instr.applyConfigOverrides(DefineVisitsTask._DefaultName, config)

        task = DefineVisitsTask(config=config, butler=butler)
        try:
            task.run([{"exposure": expRecord.id}], collections=butler.collections)
        except DatabaseConflictError:
            log = logging.getLogger("lsst.rubintv.production.processControl.defineVisit")
            log.warning(
                f"Failed to define visit for {expRecord.id} due to a conflict error. This is likely"
                " due to a change in the stack causing a slight difference in the calculated region."
            )
            pass


def getVisitId(butler: Butler, expRecord: DimensionRecord) -> int | None:
    """Lookup visitId for an expRecord.

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
    visitDataIds = list(set(butler.registry.queryDataIds(["visit"], dataId=expIdDict)))
    if len(visitDataIds) == 1:
        visitDataId = visitDataIds[0]
        assert isinstance(
            visitDataId["visit"], int
        ), f"Expected visitDataId['visit'] to be an int, got {visitDataId=}"
        return visitDataId["visit"]
    else:
        log = logging.getLogger("lsst.rubintv.production.processControl.HeadProcessController")
        log.warning(
            f"Failed to find visitId for {expIdDict}, got {visitDataIds}. Do you need to run"
            " define-visits?"
        )
        return None


def getIsrConfigDict(graph: PipelineGraph) -> dict[str, str]:
    # TODO: DM-50003 Make this config dumping more robust
    isrTasks = [task for name, task in graph.tasks.items() if "isr" in name.lower()]
    if len(isrTasks) != 1:
        log = logging.getLogger("lsst.rubintv.production.processControl.getIsrConfigDict")
        log.warning(f"Found {len(isrTasks)} isr tasks in pipeline graph!")
        return {}
    isrTask = isrTasks[0]
    isrDict: dict[str, str] = {}
    config: Any = isrTask.config  # annotate as Any to save having to do all the type ignores
    isrDict["doDiffNonLinearCorrection"] = f"{config.doDiffNonLinearCorrection}"
    isrDict["doCorrectGains"] = f"{config.doCorrectGains}"
    isrDict["doSaturation"] = f"{config.doSaturation}"
    isrDict["doApplyGains"] = f"{config.doApplyGains}"
    isrDict["doCrosstalk"] = f"{config.doCrosstalk}"
    isrDict["doLinearize"] = f"{config.doLinearize}"
    isrDict["doDeferredCharge"] = f"{config.doDeferredCharge}"
    isrDict["doITLEdgeBleedMask"] = f"{config.doITLEdgeBleedMask}"
    isrDict["doITLSatSagMask"] = f"{config.doITLSatSagMask}"
    isrDict["doITLDipMask"] = f"{config.doITLDipMask}"
    isrDict["doBias"] = f"{config.doBias}"
    isrDict["doDark"] = f"{config.doDark}"
    isrDict["doDefect"] = f"{config.doDefect}"
    isrDict["doBrighterFatter"] = f"{config.doBrighterFatter}"
    isrDict["doFlat"] = f"{config.doFlat}"
    isrDict["doInterpolate"] = f"{config.doInterpolate}"
    isrDict["doAmpOffset"] = f"{config.doAmpOffset}"
    isrDict["ampOffset.doApplyAmpOffset"] = f"{config.ampOffset.doApplyAmpOffset}"
    return isrDict


def writeIsrConfigShard(expRecord: DimensionRecord, graph: PipelineGraph, shardDir: str) -> None:
    """Write the ISR config to a shard.

    This is used to check if the ISR config has changed, and if so, to
    create a new run. It should be called after the pipeline graph has been
    created, but before it is run.
    """
    isrDict = getIsrConfigDict(graph)
    isrDict["DISPLAY_VALUE"] = "📖"
    writeMetadataShard(shardDir, expRecord.day_obs, {expRecord.seq_num: {"ISR config": isrDict}})


def getNightlyRollupTriggerTask(pipelineFile: str) -> str:
    """Get the last task that runs in step2, to know when to trigger rollup.

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
    # TODO: See if this can be removed entirely now we have finished counters
    if "nightly-validation" in pipelineFile:
        return "lsst.analysis.tools.tasks.refCatSourceAnalysis.RefCatSourceAnalysisTask"
    elif "quickLook" in pipelineFile:
        return "lsst.pipe.tasks.postprocess.ConsolidateVisitSummaryTask"
    else:
        raise ValueError(f"Unsure how to trigger nightly rollup when {pipelineFile=}")


@dataclass
class PipelineComponents:
    """Details about a pipeline graph.

    Parameters
    ----------
    pipelineGraph : `lsst.pipe.base.PipelineGraph`
        The pipeline graph.
    pipelineGraphBytes : `bytes`
        The pipeline graph as bytes.
    pipelineGraphUri : `str`
        The URI of the pipeline graph, i.e. the filename#step.
    steps : `str`
        The steps of the pipeline without the file prepended.
    overrides : `list` [`tuple`], optional
        The config overrides to apply to the pipeline graph as a list of tuples
        of (label, key, value), passed to `Pipeline.addConfigOverride()`.
    """

    graphs: dict[str, PipelineGraph]
    graphBytes: dict[str, bytes]
    uris: dict[str, str]
    steps: list[str]
    pipelineFile: str

    def __init__(
        self,
        registry: Registry,
        pipelineFile: str,
        steps: list[str],
        overrides: list[tuple[str, str, str]] | None = None,
    ) -> None:
        self.uris: dict[str, str] = {}
        self.graphs: dict[str, PipelineGraph] = {}
        self.graphBytes: dict[str, bytes] = {}
        self.pipelineFile = pipelineFile

        for step in steps:
            self.uris[step] = pipelineFile + f"#{step}"
            pipeline = Pipeline.fromFile(self.uris[step])
            if overrides:
                for override in overrides:
                    if override[0] in pipeline.task_labels:
                        pipeline.addConfigOverride(*override)
            self.graphs[step] = pipeline.to_graph(registry=registry)
            self.graphBytes[step] = pipelineGraphToBytes(self.graphs[step])

        self.steps = steps


class HeadProcessController:
    """The head node, which controls which pods process which images.

    Decides how and when each detector-visit is farmed out.

    Despite being the head node, the behaviour of this controller can be
    remotely controlled by a RemoteController, for example to change the
    processing strategy from a notebook or from LOVE.
    """

    targetLoopDuration = 0.2  # in seconds, so 5Hz

    def __init__(
        self,
        butler: Butler,
        instrument: str,
        locationConfig: LocationConfig,
        outputChain: str | None = None,
        forceNewRun: bool = False,
        doRaise: bool = False,
    ) -> None:
        self.butler = butler
        self.instrument = instrument
        self.locationConfig = locationConfig
        self.log = logging.getLogger("lsst.rubintv.production.processControl.HeadProcessController")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig, isHeadNode=True)
        self.focalPlaneControl: CameraControlConfig | None = (
            CameraControlConfig() if instrument == "LSSTCam" else None
        )
        self.workerMode = WorkerProcessingMode.WAITING
        self.visitMode = VisitProcessingMode.CONSTANT
        self.remoteController = RemoteController(butler=butler, locationConfig=locationConfig)
        # don't start here, the event loop starts the lap timer
        self.workTimer = BoxCarTimer(length=100)
        self.loopTimer = BoxCarTimer(length=100)
        self.podDetails = PodDetails(
            instrument=instrument, podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=None
        )
        self.doRaise = doRaise
        self.nDispatched: int = 0
        self.nNightlyRollups: int = 0

        if self.focalPlaneControl is not None:
            if self.locationConfig.location == "bts":
                # five on a dice pattern in the middle, plus AOS chips
                self.focalPlaneControl.setWavefrontOn()
                self.focalPlaneControl.setRaftOn("R22")
                self.focalPlaneControl.setRaftOn("R33")
                self.focalPlaneControl.setRaftOn("R11")
                self.focalPlaneControl.setRaftOn("R13")
                self.focalPlaneControl.setRaftOn("R31")
            else:
                self.focalPlaneControl.setWavefrontOn()
                self.focalPlaneControl.setAllImagingOn()

        self.pipelines: dict[str, PipelineComponents] = {}
        self.buildPipelines()

        if outputChain is None:
            # allows it to be user specified, or use the default from the site
            # config, but e.g. slac_testing doesn't use the real quickLook
            # collection, but the k8s configs do.
            outputChain = locationConfig.getOutputChain(self.instrument)
        self.outputChain = outputChain

        self.outputRun = self.getLatestRunAndPrep(forceNewRun=forceNewRun)
        self.runningAos = True
        self.log.info(
            f"Head node ready and {'IS' if self.runningAos else 'NOT'} running AOS."
            f"Data will be writen data to {self.outputRun}"
        )

    def buildPipelines(self) -> None:
        """Build the pipeline graphs from the pipeline file.

        This is a separate method so that it can be called after the
        RemoteController has been set up, which is needed for the AOS pipeline.
        """
        sfmPipelineFile = self.locationConfig.getSfmPipelineFile(self.instrument)
        aosPipelineFile = self.locationConfig.getAosPipelineFile(self.instrument)

        cpVerifyDir = getPackageDir("cp_verify")
        biasFile = (Path(cpVerifyDir) / "pipelines" / self.instrument / "verifyBias.yaml").as_posix()
        darkFile = (Path(cpVerifyDir) / "pipelines" / self.instrument / "verifyDark.yaml").as_posix()
        flatFile = (Path(cpVerifyDir) / "pipelines" / self.instrument / "verifyFlat.yaml").as_posix()
        self.pipelines["BIAS"] = PipelineComponents(
            self.butler.registry,
            biasFile,
            ["verifyBiasIsr"],
            overrides=[("verifyBiasIsr", "connections.outputExposure", "postISRCCD")],
        )
        self.pipelines["DARK"] = PipelineComponents(
            self.butler.registry,
            darkFile,
            ["verifyDarkIsr"],
            overrides=[("verifyDarkIsr", "connections.outputExposure", "postISRCCD")],
        )
        self.pipelines["FLAT"] = PipelineComponents(
            self.butler.registry,
            flatFile,
            ["verifyFlatIsr"],
            overrides=[("verifyFlatIsr", "connections.outputExposure", "postISRCCD")],
        )

        self.pipelines["ISR"] = PipelineComponents(self.butler.registry, sfmPipelineFile, ["isr"])
        if self.instrument == "LATISS":
            self.pipelines["SFM"] = PipelineComponents(self.butler.registry, sfmPipelineFile, ["step1"])
        else:
            self.pipelines["SFM"] = PipelineComponents(
                self.butler.registry, sfmPipelineFile, ["step1", "step2a", "nightlyRollup"]
            )
            # TODO: see if this will matter that this component doesn't exist
            self.pipelines["AOS"] = PipelineComponents(
                self.butler.registry, aosPipelineFile, ["step1", "step2a"]
            )

        self.allGraphs: list[PipelineGraph] = []
        for pipeline in self.pipelines.values():
            self.allGraphs.extend(pipeline.graphs.values())

    def getLatestRunAndPrep(self, forceNewRun: bool) -> str:
        packages = Packages.fromSystem()

        allRuns: Sequence[str] = []

        try:
            allRuns = self.butler.registry.getCollectionChain(self.outputChain)
        except MissingCollectionError:
            # special case where this is a totally new CHAINED collection
            self.log.warning(f"Creating a new CHAINED collection from scratch at {self.outputChain}")
            self.butler.registry.registerCollection(self.outputChain, CollectionType.CHAINED)
            newCollection = ensureRunCollection(self.butler, self.allGraphs, packages, self.outputChain, 0)
            self.butler.registry.setCollectionChain(self.outputChain, [newCollection])
            self.log.info(f"Started brand new collection at {newCollection}")
            return newCollection

        allRunNums = [
            int(run.removeprefix(self.outputChain + "/")) for run in allRuns if self.outputChain in run
        ]
        lastRunNum = max(allRunNums) if allRunNums else 0
        latestRun = f"{self.outputChain}/{lastRunNum}"
        self.log.info(f"Latest run is {latestRun} at run number {lastRunNum}")

        if forceNewRun or self.checkIfNewRunNeeded(latestRun, packages):
            lastRunNum += 1
            self.log.info(f"New run being created for {self.outputChain}")
            # ensureRunCollection is called instead of registerCollection
            latestRun = ensureRunCollection(
                self.butler, self.allGraphs, packages, self.outputChain, lastRunNum
            )
            self.log.info(f"New run created at {latestRun}")
            self.butler.collections.prepend_chain(self.outputChain, latestRun)
            self.log.info(f"New run chained in as {[latestRun] + list(allRuns)}")

        return latestRun

    def checkIfNewRunNeeded(self, latestRun: str, packages: Packages) -> bool:
        """Check if a new run is needed, and if so, create it and prep it.

        Needed if the configs change, or if the software versions change, or if
        the pipelines changes, but that's mostly likely going to happen via
        config changes anyway.

        Note that this is safe for checking config versions so long as the
        configs only come from packages in git, so DRP_PIPE and obs_packages.
        The only way of this going wrong would be either running with -c on the
        command line, which isn't relevant here, or pushing straight to the
        head node from a notebook *and* using the same outputChain. As long as
        notebook users always set a manual outputChain and don't squat on
        quickLook this is sufficient.
        """
        try:
            oldPackages = self.butler.get("packages", collections=[latestRun])
        except (MissingCollectionError, DatasetNotFoundError):  # for bootstrapping a new collections
            return True
        if packages.difference(oldPackages):  # checks if any of the versions are different
            return True
        return False

    def getSingleWorker(self, instrument: str, podFlavor: PodFlavor) -> PodDetails | None:
        freeWorkers = self.redisHelper.getFreeWorkers(instrument=instrument, podFlavor=podFlavor)
        freeWorkers = sorted(freeWorkers)  # the lowest number in the stack will be at the top alphabetically
        if freeWorkers:
            return freeWorkers[0]

        # We have no free workers of this type, so send to a busy worker and
        # warn

        # TODO: until we have a real backlog queue just put it on the last
        # worker in the stack.
        busyWorkers = self.redisHelper.getAllWorkers(instrument=instrument, podFlavor=podFlavor)
        try:
            busyWorker = busyWorkers[-1]
            self.log.warning(f"No free workers available for {podFlavor=}, sending work to {busyWorker=}")
            return busyWorker
        except IndexError as e:
            raiseIf(self.doRaise, e, self.log)
            return None

    def doDetectorFanout(self, expRecord: DimensionRecord) -> None:
        """Send the expRecord out for processing based on current selection.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The expRecord to process.
        """
        # run isr only for calibs, otherwise run the appropriate step1
        targetPipelineBytes: bytes = b""

        imageType = expRecord.observation_type.lower()
        shardPath = getShardPath(self.locationConfig, expRecord)

        # TODO: DM-50003 Make this data-driven dispatch config instead of code
        match imageType:
            case "bias":
                self.log.info(f"Sending {expRecord.id} {imageType=} to for cp_verify style bias processing")
                targetPipelineBytes = self.pipelines["BIAS"].graphBytes["verifyBiasIsr"]
                targetPipelineGraph = self.pipelines["BIAS"].graphs["verifyBiasIsr"]
                who = "ISR"
            case "dark":
                self.log.info(f"Sending {expRecord.id} {imageType=} to for cp_verify style dark processing")
                targetPipelineBytes = self.pipelines["DARK"].graphBytes["verifyDarkIsr"]
                targetPipelineGraph = self.pipelines["DARK"].graphs["verifyDarkIsr"]
                who = "ISR"
            case "flat":
                self.log.info(f"Sending {expRecord.id} {imageType=}  to for cp_verify style flat processing")
                targetPipelineBytes = self.pipelines["FLAT"].graphBytes["verifyFlatIsr"]
                targetPipelineGraph = self.pipelines["FLAT"].graphs["verifyFlatIsr"]
                who = "ISR"
            case "unknown":
                self.log.info(f"Sending {expRecord.id} {imageType=} for full ISR processing")
                targetPipelineBytes = self.pipelines["ISR"].graphBytes["isr"]
                targetPipelineGraph = self.pipelines["ISR"].graphs["isr"]
                who = "ISR"
            case _:  # all non-calib, properly headered images
                self.log.info(f"Sending {expRecord.id} {imageType=} for full step1 SFM")
                targetPipelineBytes = self.pipelines["SFM"].graphBytes["step1"]
                targetPipelineGraph = self.pipelines["SFM"].graphs["step1"]
                who = "SFM"

        writeIsrConfigShard(expRecord, targetPipelineGraph, shardPath)

        detectorIds: list[int] = []
        nEnabled = None
        if self.focalPlaneControl is not None:  # only LSSTCam has a focalPlaneControl at present
            detectorIds = self.focalPlaneControl.getEnabledDetIds()
            nEnabled = len(detectorIds)
        else:
            results = set(self.butler.registry.queryDataIds(["detector"], instrument=self.instrument))
            detectorIds = sorted([item["detector"] for item in results])  # type: ignore

        self.log.info(
            f"Fanning {expRecord.instrument}-{expRecord.day_obs}-{expRecord.seq_num}"
            f" out to {len(detectorIds)} detectors {'' if nEnabled is None else f'of {nEnabled} enabled'}."
        )

        payloads: dict[int, Payload] = {}
        for detectorId in detectorIds:
            dataId = DataCoordinate.standardize(expRecord.dataId, detector=detectorId)
            payload = Payload(
                dataIds=[dataId],
                pipelineGraphBytes=targetPipelineBytes,
                run=self.outputRun,
                who=who,
            )
            payloads[detectorId] = payload

        self._dispatchPayloads(payloads, PodFlavor.SFM_WORKER)

    def doStep1FanoutAos(self, expRecords: list[DimensionRecord]) -> None:
        """Send the expRecord out for processing based on current selection.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The expRecord to process.
        """
        # just some basic sanity checking
        instruments = list(set([expRecord.instrument for expRecord in expRecords]))
        assert len(instruments) == 1, f"Expected all expRecords to have same instrument, got {instruments=}"
        instrument = instruments[0]
        assert instrument == self.instrument, "Expected expRecords to make this head node instrument"

        self.log.info(f"Sending {[r.id for r in expRecords]} to WEP pipeline")
        targetPipelineBytes = self.pipelines["AOS"].graphBytes["step1"]
        targetPipelineGraph = self.pipelines["AOS"].graphs["step1"]

        # deliberately do NOT set isAos=True here because we want this written
        # to the main table, not the AOS page on RubinTV
        for expRecord in expRecords:
            shardPath = getShardPath(self.locationConfig, expRecord)
            writeIsrConfigShard(expRecord, targetPipelineGraph, shardPath)

        # this block will be different when we're observing with LSSTCam the
        # triggering will be too, because it'll be for every image so at that
        # point consider making this part of the normal step1 fanout

        detectorIds = []
        results = list(set(self.butler.registry.queryDataIds(["detector"], instrument=self.instrument)))
        detectorIds = cast(list[int], sorted([item["detector"] for item in results]))

        dataIds: dict[int, list[DataCoordinate]] = {}
        for detectorId in detectorIds:
            dataIds[detectorId] = []
            for expRecord in expRecords:
                dataId = DataCoordinate.standardize(expRecord.dataId, detector=detectorId)
                dataIds[detectorId].append(dataId)

        payloads: dict[int, Payload] = {}
        for detectorId in detectorIds:
            payloads[detectorId] = Payload(
                dataIds=dataIds[detectorId],
                pipelineGraphBytes=targetPipelineBytes,
                run=self.outputRun,
                who="AOS",
            )

        dayObs = expRecords[0].day_obs
        self.log.info(
            f"Fanning out {instrument}-{dayObs}-{[r.seq_num for r in expRecords]} for"
            f" {len(detectorIds)} detectors"
        )

        self._dispatchPayloads(payloads, PodFlavor.AOS_WORKER)

    def _dispatchPayloads(self, payloads: dict[int, Payload], podFlavor: PodFlavor) -> None:
        """Distribute payloads to available workers based on detector IDs.

        Attempts to send payloads to workers. It first tries to match payloads
        with free workers that handle the same detector. If no matching free
        worker is available, it will try to send to a busy worker handling the
        same detector. If no worker (free or busy) exists for the detector, an
        exception is raised, as this means the cluster is misconfigured.

        Parameters
        ----------
        payloads : dict[int, Payload]
            Dictionary mapping detector IDs to payload objects to be processed.
        podFlavor : PodFlavor
            The pod flavor to use for worker selection.

        Raises
        ------
        RuntimeError
            If no workers (free or busy) are available for a specific detector.
        """
        freeWorkers = self.redisHelper.getFreeWorkers(instrument=self.instrument, podFlavor=podFlavor)
        freeWorkers = sorted(freeWorkers)  # the lowest number in the stack will be at the top alphabetically
        busyWorkers = self.redisHelper.getAllWorkers(instrument=self.instrument, podFlavor=podFlavor)
        busyWorkers = sorted(busyWorkers)

        # handle the just started up condition
        detectorWorkers = {w.detectorNumber for w in freeWorkers + busyWorkers}
        missingWorkers = [detId for detId in payloads if detId not in detectorWorkers]
        if missingWorkers:  # probably due to just restarting
            self.log.warning(f"No workers available for {podFlavor=} for detectors={missingWorkers}")
            if self.timeAlive < 60:
                # we've just been rebooted so give workers a chance to come up
                # and then retry. If we haven't just been rebooted, the rest of
                # this function will raise, and correctly so.
                sleep(30)
                self._dispatchPayloads(payloads, podFlavor)
                return

        for detectorId, payload in payloads.items():
            matchingFreeWorkers = [w for w in freeWorkers if w.detectorNumber == detectorId]
            if matchingFreeWorkers:
                worker = matchingFreeWorkers[0]
                self.log.info(f"Sending {detectorId=} to free worker {worker.queueName} for {payload.who}")
                self.redisHelper.enqueuePayload(payload, worker)
                continue

            else:
                # No free worker with matching detector, so look for busy one
                matchingBusyWorkers = [w for w in busyWorkers if w.detectorNumber == detectorId]
                if matchingBusyWorkers:
                    worker = matchingBusyWorkers[0]
                    self.log.warning(
                        f"No free workers available for {detectorId=},"
                        f" sending to busy worker {worker.queueName}"
                    )
                    self.redisHelper.enqueuePayload(payload, worker)
                    continue
                else:
                    # Consider changing this to a log.exception for production,
                    # but this should be a raise while we're configuring things
                    # for LSSTCam
                    raise RuntimeError(
                        f"No workers (not even busy ones) available for {detectorId=},",
                        f" cannot dispatch process for {payload.who}",
                    )

    def dispatchOneOffProcessing(self, expRecord: DimensionRecord, podFlavor: PodFlavor) -> None:
        """Send the expRecord out for processing based on current selection.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The expRecord to process.
        """
        instrument = expRecord.instrument
        idStr = f"{instrument}-{expRecord.day_obs}-{expRecord.seq_num}+{podFlavor}"

        self.log.info(f"Sending signal to one-off processor for {idStr}")

        worker = self.getSingleWorker(expRecord.instrument, podFlavor=podFlavor)
        if worker is None:
            self.log.error(f"No worker available for {podFlavor} for {idStr}")
            return

        # who value doesn't matter for one-off processing, maybe SFM instead?
        payload = Payload(dataIds=[expRecord.dataId], pipelineGraphBytes=b"", run="", who="ONE_OFF")
        self.redisHelper.enqueuePayload(payload, worker)

    def getNewExposureAndDefineVisit(self) -> DimensionRecord | None:
        expRecord = self.redisHelper.getExposureForFanout(self.instrument)
        if expRecord is None:
            return expRecord

        # first time touching the new expRecord so run define visits

        # butler must be writeable for the task to run, but don't check here
        # and let the DefineVisitsTask raise, because it is useful to be able
        # to run from a notebook with a normal butler when not needing to
        # define visits
        self.log.info(f"Defining visit (if needed) for {expRecord.id}")
        defineVisit(self.butler, expRecord)
        return expRecord

    def repattern(self) -> None:
        """Apply the VisitProcessingMode to the focal plane sensor
        selection.
        """
        assert self.focalPlaneControl is not None, "Only LSSTCam has a focalPlaneControl"
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

    def getNumExpected(self, instrument: str) -> int:
        if instrument in ("LSSTComCam", "LSSTComCamSim"):
            return 9
        elif instrument == "LSSTCam":
            # TODO: probably should redirect this to utils.py
            # getNumExpectedItems() soon, but that will need to be
            # site-dependent to properly work, and we also need Tony to start
            # writing out the expected sensors file too, before it's useful on
            # the summit.
            return 189
        raise ValueError(f"Unknown instrument {instrument=}")

    def dispatchGatherSteps(self, who: str) -> bool:
        """Dispatch any gather steps as needed.

        Note that the return value is currently unused, but is planned to be
        built upon in the next few tickets.

        Returns
        -------
        dispatchedWork : `bool`
            Was anything sent out?
        """
        if self.instrument == "LATISS":  # no gather type steps for single chip cameras
            return False

        assert who in ("SFM", "AOS"), f"Unknown pipeline {who=}"
        processedIds = self.redisHelper.getAllIdsForDetectorLevel(self.instrument, step="step1", who=who)

        if not processedIds:
            return False

        completeIds = []
        for idStr in processedIds:
            nFinished = self.redisHelper.getNumDetectorLevelFinished(self.instrument, "step1", who, idStr)
            if nFinished == self.getNumExpected(self.instrument):
                completeIds.append(idStr)

        if not completeIds:
            return False

        podFlavour = PodFlavor.STEP2A_AOS_WORKER if who == "AOS" else PodFlavor.STEP2A_WORKER

        self.log.debug(f"For {who}: Found {completeIds=} for step1 for {who}")

        for idStr in completeIds:
            # idStr is a list of visitIds as a string with a + separator if AOS
            # else idStr is a visit int as a string
            intIds = [int(_id) for _id in idStr.split("+")]
            self.log.debug(f"Found {len(intIds)} complete visits for {who} dispatch {intIds=}, {idStr}")
            dataCoords = [
                DataCoordinate.standardize(
                    instrument=self.instrument, visit=intId, universe=self.butler.dimensions
                )
                for intId in intIds
            ]

            payload = Payload(
                dataIds=dataCoords,
                pipelineGraphBytes=self.pipelines[who].graphBytes["step2a"],
                run=self.outputRun,
                who=who,
            )

            worker = self.getSingleWorker(self.instrument, podFlavour)
            if not worker:
                self.log.warning(f"No worker available for {who} step2a")
                return False
            self.log.info(f"Dispatching step2a for {who} with complete inputs: {dataCoords} to {worker}")
            self.redisHelper.enqueuePayload(payload, worker)
            self.log.debug(f"Removing step1 finished counter for {idStr=}")
            self.redisHelper.removeFinishedIdDetectorLevel(self.instrument, "step1", who, idStr)

            # never dispatch this incomplete because it relies on a specific
            # detector having finished. It might have failed, but that's OK
            # because the one-off processor will time out quickly.
            if who == "SFM":
                (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataCoords[0])
                self.dispatchOneOffProcessing(expRecord, PodFlavor.ONE_OFF_CALEXP_WORKER)
            if who == "AOS":
                for dataCoord in dataCoords:
                    (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataCoord)
                    self.dispatchOneOffProcessing(expRecord, PodFlavor.ONE_OFF_POSTISR_WORKER)

        return True  # we sent something out

    def dispatchRollupIfNecessary(self) -> bool:
        """Check if we should do another rollup, and if so, dispatch it.

        Returns
        -------
        doRollup : `bool`
            Did we do another rollup?
        """
        if self.instrument == "LATISS":
            # self.log.info("Consider making a one-off processor for
            # the night plots and dispatching it here")
            return False

        numComplete = self.redisHelper.getNumVisitLevelFinished(self.instrument, "step2a", who="SFM")
        if numComplete > self.nNightlyRollups:
            self.log.info(
                f"Found {numComplete - self.nNightlyRollups} more completed step2a's - "
                " dispatching them for nightly rollup"
            )
            self.nNightlyRollups = numComplete
            # TODO: DM-49947 try adding the current day_obs to this dataId
            dataId = {"instrument": self.instrument, "skymap": "lsst_cells_v1"}
            dataCoord = DataCoordinate.standardize(dataId, universe=self.butler.dimensions)
            payload = Payload(
                [dataCoord], self.pipelines["SFM"].graphBytes["nightlyRollup"], run=self.outputRun, who="SFM"
            )
            worker = self.getSingleWorker(self.instrument, PodFlavor.NIGHTLYROLLUP_WORKER)
            if worker is None:
                self.log.error("No free workers available for nightly rollup")
                return False
            self.redisHelper.enqueuePayload(payload, worker)
            return True
        return False

    def dispatchFocalPlaneMosaics(self) -> None:
        """Dispatch the focal plane mosaic task.

        This will be dispatched to a worker which will then gather the
        individual CCD mosaics and make the full focal plane mosaic and upload
        to S3. At the moment, it will only work when everything is completed.
        """
        if self.instrument == "LATISS":
            # single chip cameras aren't plotted as binned mosaics, so this
            # happens in a one-off-processor instead for all round ease.
            return

        triggeringTasks = ("lsst.ip.isr.isrTaskLSST.IsrTaskLSST", "binnedCalexpCreation")
        dataProducts = ("postISRCCD", "calexp")

        for triggeringTask, dataProduct in zip(triggeringTasks, dataProducts):
            allDataIds = set(self.redisHelper.getAllDataIdsForTask(self.instrument, triggeringTask))
            completeIds = [
                _id
                for _id in allDataIds
                if self.redisHelper.getNumTaskFinished(self.instrument, triggeringTask, _id)
                == self.getNumExpected(self.instrument)
            ]
            if not completeIds:
                continue

            idString = (
                f"{len(completeIds)} images: {completeIds}"
                if len(completeIds) > 1
                else f"expId={completeIds[0]}"
            )
            self.log.info(f"Dispatching complete {dataProduct} mosaic for {idString}")

            for dataId in completeIds:  # intExpId because mypy doesn't like reusing loop variables?!
                # TODO: this abuse of Payload really needs improving
                payload = Payload([dataId], b"", dataProduct, who="SFM")
                worker = self.getSingleWorker(self.instrument, PodFlavor.MOSAIC_WORKER)
                if worker is None:
                    self.log.error(f"No free workers available for {dataProduct} mosaic")
                    continue
                self.redisHelper.enqueuePayload(payload, worker)
                self.redisHelper.removeTaskCounter(self.instrument, triggeringTask, dataId)

    def regulateLoopSpeed(self) -> None:
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
        if self.loopTimer.totalLaps % 100 == 0:
            loopSpeed = self.loopTimer.median(frequency=True)
            maxLoopTime = self.loopTimer.max(frequency=False)
            self.log.debug(
                f"Event loop running at regulated speed of {loopSpeed:.2f}Hz with a max time of"
                f" {maxLoopTime:.2f}s for the last {len(self.loopTimer._buffer)} loops"
            )

            medianFreq = self.workTimer.mean(frequency=True)
            maxWorkTime = self.workTimer.max(frequency=False)
            self.log.debug(
                f"If unlimited, the event loop would run at {medianFreq:.2f}Hz, with a longest"
                f" workload of {maxWorkTime:.2f}s in the last {len(self.workTimer._buffer)} loops"
            )

        lastLap = self.loopTimer.lastLapTime()
        assert lastLap is not None, "Expected lastLap to be set"
        sleepPeriod = self.targetLoopDuration - lastLap
        if sleepPeriod > 0:
            self.workTimer.pause()  # don't count the sleeping towards the loop time on work timer
            sleep(sleepPeriod)
            self.workTimer.resume()
        else:
            if sleepPeriod < -0.05:  # allow some noise
                lastLap = self.loopTimer.lastLapTime()
                lastWork = self.workTimer.lastLapTime()
                self.log.warning(
                    f"Event loop running slow, last loop took {lastLap:.2f}s" f" with {lastWork:.2f}s of work"
                )

    @property
    def timeAlive(self) -> float:
        return time.time() - self.startTime

    def run(self) -> None:
        self.workTimer.start()  # times how long it actually takes to do the work
        self.loopTimer.start()  # checks the delivered loop performance
        self.startTime = time.time()
        while True:
            # affirmRunning should be longer than longest loop but no longer
            self.redisHelper.affirmRunning(self.podDetails, 5)
            self.remoteController.executeRemoteCommands(self)  # look for remote control commands here

            expRecord = self.getNewExposureAndDefineVisit()
            if expRecord is not None:
                assert self.instrument == expRecord.instrument
                self.dispatchOneOffProcessing(expRecord, podFlavor=PodFlavor.ONE_OFF_EXPRECORD_WORKER)
                writeExpRecordMetadataShard(expRecord, getShardPath(self.locationConfig, expRecord))
                if not isWepImage(expRecord) or self.instrument == "LATISS":  # process CWFS image on LATISS
                    self.doDetectorFanout(expRecord)
                    self.dispatchOneOffProcessing(expRecord, podFlavor=PodFlavor.ONE_OFF_POSTISR_WORKER)

            if self.runningAos:  # only consume this queue once we switch from DonutLauncher to this approach
                donutPair = self.redisHelper.checkForOcsDonutPair(self.instrument)
                if donutPair is not None:
                    self.log.info(f"Found a donut pair trigger for {donutPair}")
                    (record1,) = self.butler.registry.queryDimensionRecords("exposure", exposure=donutPair[0])
                    (record2,) = self.butler.registry.queryDimensionRecords("exposure", exposure=donutPair[1])
                    aosShardPath = getShardPath(self.locationConfig, record1, isAos=True)
                    writeExpRecordMetadataShard(record1, aosShardPath)
                    writeExpRecordMetadataShard(record2, aosShardPath)
                    # define visits here for the edge case that the donut pair
                    # signal arrives before the getNewExposureAndDefineVisit
                    # call in the main loop
                    defineVisit(self.butler, record1)
                    defineVisit(self.butler, record2)
                    self.doStep1FanoutAos([record1, record2])

            # for now, only dispatch to step2a once things are complete because
            # there is some subtlety in the dispatching incomplete things
            # because they will be dispatched again and again until they are
            # complete, and that will happen not when another completes, but at
            # the speed of this loop, which would be bad, so we need to deal
            # with tracking that and dispatching only if the number has gone up
            # *and* there are 2+ free workers, because it's not worth
            # re-dispatching for every single new CCD exposure which finishes.
            self.dispatchGatherSteps(who="SFM")

            self.dispatchGatherSteps(who="AOS")

            self.dispatchFocalPlaneMosaics()

            self.dispatchRollupIfNecessary()

            # note the repattern comes after the fanout so that any commands
            # executed are present for the next image to follow and only then
            # do we toggle
            if self.instrument == "LSSTCam":
                self.repattern()

            self.regulateLoopSpeed()


class RemoteController:
    # TODO: consider removing this completely. There has to be a simpler way
    # and this was basically a fun plane project
    def __init__(self, butler: Butler, locationConfig: LocationConfig) -> None:
        self.log = logging.getLogger("lsst.rubintv.production.processControl.RemoteController")
        self.redisHelper = RedisHelper(butler=butler, locationConfig=locationConfig)

    def sendCommand(self, method: str, **kwargs) -> None:
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

            For example, 'focalPlane.setFullCheckerboard' is just components to
            call, and would therefore return `['focalPlane',
            'setFullCheckerboard'], None` and if the command were
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

            If a primitive is passed, it's safely evaluated with literal_eval,
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

    # TODO: Make this camera agnostic if necessary.
    def __init__(self) -> None:
        self.camera = LsstCam.getCamera()
        self._detectorStates = {det: False for det in self.camera}
        self._detectors = [det for det in self.camera]
        self._imaging = [det for det in self._detectors if self.isImaging(det)]
        self._guiders = [det for det in self._detectors if self.isGuider(det)]
        self._wavefronts = [det for det in self._detectors if self.isWavefront(det)]
        # plotConfig = FocalPlaneGeometryPlotConfig()
        self._focalPlanePlot = FocalPlaneGeometryPlot()
        self._focalPlanePlot.showStats = False
        self._focalPlanePlot.plotMin = 0
        self._focalPlanePlot.plotMax = 1
        self.GUIDER_NUMS = tuple(det.getId() for det in self._guiders)
        self.CWFS_NUMS = tuple(det.getId() for det in self._wavefronts)

    @staticmethod
    def isWavefront(detector: Detector) -> bool:
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
    def isGuider(detector: Detector) -> bool:
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
    def isImaging(detector: Detector) -> bool:
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
    def _getRaftTuple(detector: Detector) -> tuple[int, int]:
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
    def _getSensorTuple(detector: Detector) -> tuple[int, int]:
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

    def _getFullLocationTuple(self, detector) -> tuple[int, int]:
        """Get the (colNum, rowNum) of the detector wrt the full focal plane.

        0, 0 is the bottom left
        """
        raftX, raftY = self._getRaftTuple(detector)
        sensorX, sensorY = self._getSensorTuple(detector)
        col = (raftX * 3) + sensorX + 1
        row = (raftY * 3) + sensorY + 1
        return col, row

    def setWavefrontOn(self) -> None:
        """Turn all the wavefront sensors on."""
        for detector in self._wavefronts:
            self._detectorStates[detector] = True

    def setWavefrontOff(self) -> None:
        """Turn all the wavefront sensors off."""
        for detector in self._wavefronts:
            self._detectorStates[detector] = False

    def setGuidersOn(self) -> None:
        """Turn all the guider sensors on."""
        for detector in self._guiders:
            self._detectorStates[detector] = True

    def setGuidersOff(self) -> None:
        """Turn all the wavefront sensors off."""
        for detector in self._guiders:
            self._detectorStates[detector] = False

    def setFullCheckerboard(self, phase: int = 0) -> None:
        """Set a checkerboard pattern at the CCD level.

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

    def setRaftCheckerboard(self, phase: int = 0) -> None:
        """Set a checkerboard pattern at the raft level.

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

    def setE2Von(self) -> None:
        """Turn all e2v sensors on."""
        for detector in self._imaging:
            if detector.getPhysicalType() == "E2V":
                self._detectorStates[detector] = True

    def setE2Voff(self) -> None:
        """Turn all e2v sensors off."""
        for detector in self._imaging:
            if detector.getPhysicalType() == "E2V":
                self._detectorStates[detector] = False

    def setITLon(self) -> None:
        """Turn all ITL sensors on."""
        for detector in self._imaging:
            if detector.getPhysicalType() == "ITL":
                self._detectorStates[detector] = True

    def setITLoff(self) -> None:
        """Turn all ITL sensors off."""
        for detector in self._imaging:
            if detector.getPhysicalType() == "ITL":
                self._detectorStates[detector] = False

    def setRaftOn(self, raftName: str) -> None:
        for detector in self._detectors:
            if detector.getName().startswith(raftName):
                self._detectorStates[detector] = True

    def setRaftOff(self, raftName: str) -> None:
        for detector in self._detectors:
            if detector.getName().startswith(raftName):
                self._detectorStates[detector] = False

    def setDetectorOn(self, detectorNumber: int) -> None:
        for detector in self._detectors:
            if detector.getId() == detectorNumber:
                self._detectorStates[detector] = True

    def setDetectorOff(self, detectorNumber: int) -> None:
        for detector in self._detectors:
            if detector.getId() == detectorNumber:
                self._detectorStates[detector] = False

    def setFullFocalPlaneGuidersOn(self) -> None:
        """Turn all ITL sensors on."""
        for detector in self._imaging:
            sensorX, sensorY = self._getSensorTuple(detector)
            if sensorX <= 1 and sensorY <= 1:
                self._detectorStates[detector] = True

    def setAllOn(self) -> None:
        """Turn all sensors on.

        Note that this includes wavefront sensors and guiders.
        """
        for detector in self._detectors:
            self._detectorStates[detector] = True

    def setAllOff(self) -> None:
        """Turn all sensors off.

        Note that this includes wavefront sensors and guiders.
        """
        for detector in self._detectors:
            self._detectorStates[detector] = False

    def setAllImagingOn(self) -> None:
        """Turn all imaging sensors on."""
        for detector in self._imaging:
            self._detectorStates[detector] = True

    def setAllImagingOff(self) -> None:
        """Turn all imaging sensors off."""
        for detector in self._imaging:
            self._detectorStates[detector] = False

    def invertImagingSelection(self) -> None:
        """Invert the selection of the imaging chips only."""
        for detector in self._imaging:
            self._detectorStates[detector] = not self._detectorStates[detector]

    def getNumEnabled(self) -> int:
        """Get the number of enabled sensors.

        Returns
        -------
        nEnabled : `int`
            The number of enabled CCDs.
        """
        return sum(self._detectorStates.values())

    def getEnabledDetIds(self) -> list[int]:
        """Get the detectorIds of the enabled sensors.

        Returns
        -------
        enabled : `list` of `int`
            The detectorIds of the enabled CCDs.
        """
        return sorted([det.getId() for (det, state) in self._detectorStates.items() if state is True])

    def asPlotData(self) -> dict[str, list[int] | list[None] | NDArray]:
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
        detNums: list[int] = []
        ampNames: list[None] = []
        x: list[None] = []
        y: list[None] = []
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

    def plotConfig(self, saveAs: str = "") -> FocalPlaneGeometryPlot:
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
