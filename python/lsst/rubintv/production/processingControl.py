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
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Any, Iterable, Sequence, cast

import numpy as np

from lsst.analysis.tools.actions.plot import FocalPlaneGeometryPlot
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
from lsst.pex.config.configurableField import ConfigurableInstance
from lsst.pipe.base import Instrument, Pipeline, PipelineGraph, TaskFactory
from lsst.utils import getPackageDir
from lsst.utils.packages import Packages

from .payloads import Payload, pipelineGraphToBytes
from .podDefinition import PodDetails, PodFlavor
from .redisUtils import RedisHelper
from .timing import BoxCarTimer
from .utils import (
    LocationConfig,
    getShardPath,
    isCalibration,
    isWepImage,
    raiseIf,
    writeExpRecordMetadataShard,
    writeMetadataShard,
)

if TYPE_CHECKING:
    from numpy.typing import NDArray

    from lsst.pipe.base import PipelineTaskConfig
    from lsst.pipe.base.pipeline_graph import TaskNode


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


def configToReadableDict(config: PipelineTaskConfig) -> dict[str, str]:
    """Convert a config to a readable dict.

    Currently just removes boring things like connections, but allows for easy
    extension as we see how other configs serialize.

    Parameters
    ----------
    config : `lsst.pipe.base.PipelineTaskConfig`
        The config to convert to a readable dict.

    Returns
    -------
    readable : `dict` [`str`, `str`]
        The config as a dict of strings.
    """
    SKIP = ["connections"]
    readable = {}
    for k, v in config.items():
        if k in SKIP:
            continue
        if isinstance(v, ConfigurableInstance):
            readable[k] = repr(v.value)
        else:
            readable[k] = repr(v)
    return readable


def writeIsrConfigShard(expRecord: DimensionRecord, graph: PipelineGraph, shardDir: str) -> None:
    """Write the ISR config to a shard.

    This is used to check if the ISR config has changed, and if so, to
    create a new run. It should be called after the pipeline graph has been
    created, but before it is run.
    """
    isrDict = getIsrConfigDict(graph)
    isrDict["DISPLAY_VALUE"] = "📖"
    writeMetadataShard(shardDir, expRecord.day_obs, {expRecord.seq_num: {"ISR config": isrDict}})


def writeAosConfigShards(
    expRecord: DimensionRecord, pipelineComponents: PipelineComponents, shardDir: str, pipelineName: str
) -> None:
    """Write all the requested the AOS tasks configs out the AOS page.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to process.
    pipelineComponents : `PipelineComponents`
        The pipeline components to use.
    shardDir : `str`
        The directory to write the shards to.
    """
    graph = pipelineComponents.graphs["step1a"]

    writeMetadataShard(shardDir, expRecord.day_obs, {expRecord.seq_num: {"Pipeline name": pipelineName}})

    czTasks = [task for name, task in graph.tasks.items() if "zern" in name.lower()]
    if czTasks:
        czTask = czTasks[0]
        readableConfig = configToReadableDict(czTask.config)
        readableConfig["DISPLAY_VALUE"] = "📖"
        writeMetadataShard(
            shardDir, expRecord.day_obs, {expRecord.seq_num: {"CalcZernikes config": readableConfig}}
        )

    generateDonutTasks = [task for name, task in graph.tasks.items() if "generatedonut" in name.lower()]
    if generateDonutTasks:
        generateDonutTask = generateDonutTasks[0]
        readableConfig = configToReadableDict(generateDonutTask.config)
        readableConfig["DISPLAY_VALUE"] = "📖"
        writeMetadataShard(
            shardDir,
            expRecord.day_obs,
            {expRecord.seq_num: {"GenerateDonutDirectDetectTask config": readableConfig}},
        )

    cutoutDonutTasks = [task for name, task in graph.tasks.items() if "cutoutdonutscwfs" in name.lower()]
    if cutoutDonutTasks:
        cutoutDonutTask = cutoutDonutTasks[0]
        readableConfig = configToReadableDict(cutoutDonutTask.config)
        readableConfig["DISPLAY_VALUE"] = "📖"
        writeMetadataShard(
            shardDir, expRecord.day_obs, {expRecord.seq_num: {"CutOutDonutsCwfsPair config": readableConfig}}
        )

    cutoutDonutScienceSensorTasks = [
        task for name, task in graph.tasks.items() if "cutoutdonutssciencesensorgroup" in name.lower()
    ]
    if cutoutDonutScienceSensorTasks:
        cutoutDonutScienceSensorTask = cutoutDonutScienceSensorTasks[0]
        readableConfig = configToReadableDict(cutoutDonutScienceSensorTask.config)
        readableConfig["DISPLAY_VALUE"] = "📖"
        writeMetadataShard(
            shardDir,
            expRecord.day_obs,
            {expRecord.seq_num: {"CutOutDonutsScienceSensorGroup config": readableConfig}},
        )


def getNightlyRollupTriggerTask(pipelineFile: str) -> str:
    """Get the last task that runs in step1b, to know when to trigger rollup.

    This is the task which is run when a decetor-exposure is complete, and
    which therefore means it's time to trigger the step1b processing if all
    quanta are complete.

    Parameters
    ----------
    pipelineFile : `str`
        The pipelineFile defining the pipeline. Hopefully we can use the real
        pipeline in the future and thus avoid the hard-coding of strings below.

    Returns
    -------
    taskName : `str`
        The task which triggers step1b processing.
    """
    # TODO: See if this can be removed entirely now we have finished counters
    if "nightly-validation" in pipelineFile:
        return "lsst.analysis.tools.tasks.refCatSourceAnalysis.RefCatSourceAnalysisTask"
    elif "quickLook" in pipelineFile:
        return "lsst.pipe.tasks.postprocess.ConsolidateVisitSummaryTask"
    else:
        raise ValueError(f"Unsure how to trigger nightly rollup when {pipelineFile=}")


def buildPipelines(
    instrument: str,
    locationConfig: LocationConfig,
    butler: Butler,
) -> tuple[list[PipelineGraph], dict[str, PipelineComponents]]:
    """Build the pipeline graphs from the pipeline file.

    Parameters
    ----------
    instrument : `str`
        The name of the instrument.
    locationConfig : `LocationConfig`
        The location config object.
    butler : `lsst.daf.butler.Butler`
        The butler object.

    Returns
    -------
    pipelineGraphs : `list` [`lsst.pipe.base.PipelineGraph`]
        The `PipelineGraph`s, as a list.
    pipelines : `dict` [`str`, `PipelineComponents`]
        The `PipelineComponent`s as a dict, keyed by the overall pipeline names
        e.g. "SFM", "ISR", "AOS" etc.
    """
    pipelines: dict[str, PipelineComponents] = {}
    sfmPipelineFile = locationConfig.getSfmPipelineFile(instrument)
    aosFileDanish = locationConfig.aosLSSTCamPipelineFileDanish
    aosFileTIE = locationConfig.aosLSSTCamPipelineFileTie
    aosFileDanishFam = locationConfig.aosLSSTCamFullArrayModePipelineFileDanish
    aosFileTIEFam = locationConfig.aosLSSTCamFullArrayModePipelineFileTie

    drpPipeDir = getPackageDir("drp_pipe")
    biasFile = (Path(drpPipeDir) / "pipelines" / instrument / "quickLookBias.yaml").as_posix()
    darkFile = (Path(drpPipeDir) / "pipelines" / instrument / "quickLookDark.yaml").as_posix()
    flatFile = (Path(drpPipeDir) / "pipelines" / instrument / "quickLookFlat.yaml").as_posix()

    pipelines["BIAS"] = PipelineComponents(butler.registry, biasFile, ["verifyBiasIsr"], ["step1a"])
    pipelines["DARK"] = PipelineComponents(butler.registry, darkFile, ["verifyDarkIsr"], ["step1a"])
    pipelines["FLAT"] = PipelineComponents(butler.registry, flatFile, ["verifyFlatIsr"], ["step1a"])
    pipelines["ISR"] = PipelineComponents(butler.registry, sfmPipelineFile, ["isr"], ["step1a"])

    if instrument == "LATISS":
        # TODO: unify SFM for LATISS and LSSTCam once LATISS has step1b working
        pipelines["SFM"] = PipelineComponents(
            butler.registry,
            sfmPipelineFile,
            ["step1a-single-visit-detectors", "step1b-single-visit-visits"],
            ["step1a", "step1b"],
        )
    else:
        # TODO: remove nightlyrollup
        pipelines["SFM"] = PipelineComponents(
            butler.registry,
            sfmPipelineFile,
            ["step1a-single-visit-detectors", "step1b-single-visit-visits", "step1d-single-visit-global"],
            ["step1a", "step1b", "nightlyRollup"],
        )
        # NOTE: there is no dict entry for LATISS for AOS as AOS runs
        # differently there. It might change in the future, but not soon.
        pipelines["AOS_DANISH"] = PipelineComponents(
            butler.registry, aosFileDanish, ["step1a-detectors", "step1b-visits"], ["step1a", "step1b"]
        )
        pipelines["AOS_TIE"] = PipelineComponents(
            butler.registry, aosFileTIE, ["step1a-detectors", "step1b-visits"], ["step1a", "step1b"]
        )

        pipelines["AOS_FAM_TIE"] = PipelineComponents(
            butler.registry, aosFileTIEFam, ["step1a-detectors", "step1b-visits"], ["step1a", "step1b"]
        )
        pipelines["AOS_FAM_DANISH"] = PipelineComponents(
            butler.registry, aosFileDanishFam, ["step1a-detectors", "step1b-visits"], ["step1a", "step1b"]
        )

    allGraphs: list[PipelineGraph] = []
    for pipeline in pipelines.values():
        allGraphs.extend(pipeline.graphs.values())

    return allGraphs, pipelines


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
    stepAliases: list[str]
    pipelineFile: str

    def __init__(
        self,
        registry: Registry,
        pipelineFile: str,
        steps: list[str],
        stepAliases: list[str],
        overrides: list[tuple[str, str, object]] | None = None,
    ) -> None:
        self.uris: dict[str, str] = {}
        self.graphs: dict[str, PipelineGraph] = {}
        self.graphBytes: dict[str, bytes] = {}
        self.pipelineFile = pipelineFile
        self.stepAliases = stepAliases

        if len(steps) != len(stepAliases):
            raise ValueError(
                f"Number of steps ({len(steps)}) does not match number of step names ({len(stepAliases)})"
            )

        for stepAlias, step in zip(stepAliases, steps):
            self.uris[stepAlias] = pipelineFile + f"#{step}"
            pipeline = Pipeline.fromFile(self.uris[stepAlias])

            if overrides:
                for override in overrides:
                    if override[0] in pipeline.task_labels:
                        pipeline.addConfigOverride(*override)
            self.graphs[stepAlias] = pipeline.to_graph(registry=registry)
            self.graphBytes[stepAlias] = pipelineGraphToBytes(self.graphs[stepAlias])

        self.steps = steps

    def getTasks(self) -> dict[str, TaskNode]:
        """Get the tasks in the pipeline graph.

        Returns
        -------
        tasks : `dict` [`str`, `PipelineGraph`]
            The tasks in the pipeline graph.
        """
        tasks: dict[str, TaskNode] = {}
        for stepAlias in self.stepAliases:
            tasks.update(self.graphs[stepAlias].tasks)
        return tasks


class HeadProcessController:
    """The head node, which controls which pods process which images.

    Decides how and when each detector-visit is farmed out.
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
        # don't start here, the event loop starts the lap timer
        self.workTimer = BoxCarTimer(length=100)
        self.loopTimer = BoxCarTimer(length=100)
        self.podDetails = PodDetails(
            instrument=instrument, podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=None
        )
        self.doRaise = doRaise
        self.nDispatched: int = 0
        self.nNightlyRollups: int = 0
        self.currentAosPipeline = "AOS_DANISH"  # uses the name of the self.pipelines key
        self.currentAosFamPipeline = "AOS_FAM_DANISH"  # ignored for ComCam

        if self.focalPlaneControl is not None:
            if self.locationConfig.location == "bts":
                # five on a dice pattern in the middle, plus AOS chips
                self.focalPlaneControl.setWavefrontOn()
                self.focalPlaneControl.setRaftOn("R22")
                self.focalPlaneControl.setRaftOn("R33")
                self.focalPlaneControl.setRaftOn("R11")
                self.focalPlaneControl.setRaftOn("R13")
                self.focalPlaneControl.setRaftOn("R31")
            if self.locationConfig.location == "usdf_testing":
                # For the CI suite - might be good to find a better way of
                # controlling this but this is fine for gettin it working
                self.focalPlaneControl.setWavefrontOn()
                self.focalPlaneControl.setRaftOn("R22")  # central raft
                self.focalPlaneControl.setRaftOn("R33")  # one more for luck because of 0, 1, inf.
            else:
                self.focalPlaneControl.setWavefrontOn()
                self.focalPlaneControl.setAllImagingOn()

            # set the current state of selected detectors in redis. Could
            # change this to resume from redis now, if we wanted to, but coming
            # up in a clean and predictable state is probably preferable for
            # now
            self.redisHelper.setDetectorsIgnoredByHeadNode(
                self.instrument, self.focalPlaneControl.getDisabledDetIds(excludeCwfs=True)
            )

        allGraphs, pipelines = buildPipelines(
            instrument=instrument,
            locationConfig=locationConfig,
            butler=butler,
        )
        self.allGraphs = allGraphs
        self.pipelines = pipelines

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

    def updateConfigsFromRubinTV(self) -> None:
        if self.instrument != "LSSTCam":
            # TODO: Only the LSSTCam head node should consume these, and this
            # is a solution, but it's not a very good one. Need frontend
            # changes to do better though
            return

        # do this first so that any other commands take effect upon restart
        if self.redisHelper.redis.getdel("RUBINTV_CONTROL_RESET_HEAD_NODE"):
            self.log.warning("Received reset command from RubinTV, restarting the head node...")
            sys.exit(0)

        _aosPipeline = self.redisHelper.redis.getdel("RUBINTV_CONTROL_AOS_PIPELINE")
        if _aosPipeline is not None:
            aosOverride = _aosPipeline.decode()
            if aosOverride != self.currentAosPipeline:
                # comes as 'tie' or 'danish'
                aosOverride = f"AOS_{aosOverride.upper()}"
                aosFamOverride = f"AOS_FAM_{aosOverride.upper()}"
                # Note: always keep currentAosPipeline and
                # currentAosFamPipeline as the same flavour
                self.currentAosPipeline = aosOverride
                self.currentAosFamPipeline = aosFamOverride
                self.log.info(f"Now running AOS: {self.currentAosPipeline}")
            else:
                self.log.info(f"Received new AOS pipeline: {aosOverride} = a no-op as it was already set")

        _processingMode = self.redisHelper.redis.getdel("RUBINTV_CONTROL_VISIT_PROCESSING_MODE")
        if _processingMode is not None:
            processingMode = _processingMode.decode()
            self.log.warning(f"Received new visit processing mode: {processingMode} but not implemented yet")

        _ccConfig = self.redisHelper.redis.getdel("RUBINTV_CONTROL_CHIP_SELECTION")
        if _ccConfig is not None:
            ccConfig = _ccConfig.decode()
            if self.focalPlaneControl is not None:
                self.log.info(f"Applying new chip selection config: {ccConfig}")
                self.focalPlaneControl.applyNamedPattern(ccConfig)
                self.log.info(f"{self.focalPlaneControl.getEnabledDetIds()} now enabled")
                self.redisHelper.setDetectorsIgnoredByHeadNode(
                    self.instrument, self.focalPlaneControl.getDisabledDetIds(excludeCwfs=True)
                )

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
            if len(busyWorkers) == 0:
                self.log.error(f"No free or busy workers available for {podFlavor=}, cannot dispatch work.")
                return None

            busyWorker = busyWorkers[-1]
            self.log.warning(f"No free workers available for {podFlavor=}, sending work to {busyWorker=}")
            return busyWorker
        except IndexError as e:
            raiseIf(self.doRaise, e, self.log, msg=f"No workers AT ALL for {podFlavor=}")
            return None

    def getPipelineConfig(self, expRecord: DimensionRecord) -> tuple[bytes, PipelineGraph, str]:
        """Get the pipeline config for the given expRecord.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record to process.

        Returns
        -------
        targetPipelineBytes : `bytes`
            The pipeline graph as bytes.
        targetPipelineGraph : `lsst.pipe.base.PipelineGraph`
            The pipeline graph.
        who : `str`
            Who this processing is for, either "ISR" or "SFM", "AOS"
        """

        # run isr only for calibs, otherwise run the appropriate step1a
        targetPipelineBytes: bytes = b""
        imageType = expRecord.observation_type.lower()

        # TODO: DM-50003 Make this data-driven dispatch config instead of code
        match imageType:
            case "bias":
                self.log.info(f"Sending {expRecord.id} {imageType=} to for cp_verify style bias processing")
                targetPipelineBytes = self.pipelines["BIAS"].graphBytes["step1a"]
                targetPipelineGraph = self.pipelines["BIAS"].graphs["step1a"]
                who = "ISR"
            case "dark":
                self.log.info(f"Sending {expRecord.id} {imageType=} to for cp_verify style dark processing")
                targetPipelineBytes = self.pipelines["DARK"].graphBytes["step1a"]
                targetPipelineGraph = self.pipelines["DARK"].graphs["step1a"]
                who = "ISR"
            case "flat":
                self.log.info(f"Sending {expRecord.id} {imageType=}  to for cp_verify style flat processing")
                targetPipelineBytes = self.pipelines["FLAT"].graphBytes["step1a"]
                targetPipelineGraph = self.pipelines["FLAT"].graphs["step1a"]
                who = "ISR"
            case "unknown":
                self.log.info(f"Sending {expRecord.id} {imageType=} for full ISR processing")
                targetPipelineBytes = self.pipelines["ISR"].graphBytes["step1a"]
                targetPipelineGraph = self.pipelines["ISR"].graphs["step1a"]
                who = "ISR"
            case _:  # all non-calib, properly headered images
                self.log.info(f"Sending {expRecord.id} {imageType=} for full step1a SFM")
                targetPipelineBytes = self.pipelines["SFM"].graphBytes["step1a"]
                targetPipelineGraph = self.pipelines["SFM"].graphs["step1a"]
                who = "SFM"

        return targetPipelineBytes, targetPipelineGraph, who

    def doAosFanout(self, expRecord: DimensionRecord) -> None:
        """Send the CWFS sensors out for AOS processing. LSSTCam only.

        Hard-codes always sending this to all 8 CWFS detectors (191, 192, 195,
        196, 199, 200, 203, 204).

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record to process.
        """
        if expRecord.instrument in ["LATISS", "LSSTComCam"]:
            return
        assert self.focalPlaneControl is not None  # just for mypy

        aosShardPath = getShardPath(self.locationConfig, expRecord, isAos=True)
        if not isCalibration(expRecord):
            targetPipelineBytes = self.pipelines[self.currentAosPipeline].graphBytes["step1a"]
            writeAosConfigShards(
                expRecord, self.pipelines[self.currentAosPipeline], aosShardPath, self.currentAosPipeline
            )
            who = "AOS"
        else:
            # send the detectors to the AOS workers for normal ISR processing
            targetPipelineBytes = self.pipelines["ISR"].graphBytes["step1a"]
            who = "ISR"

        detectorPairs = self.focalPlaneControl.getIntraExtraFocalPairs()

        payloads: dict[int, Payload] = {}
        for det1, det2 in detectorPairs:
            dataId1 = DataCoordinate.standardize(expRecord.dataId, detector=det1)
            dataId2 = DataCoordinate.standardize(expRecord.dataId, detector=det2)
            self.log.debug("AOS paired dispatch:")
            self.log.debug(f"{dataId1=}")
            self.log.debug(f"{dataId2=}")
            payload = Payload(
                dataIds=[dataId1, dataId2],
                pipelineGraphBytes=targetPipelineBytes,
                run=self.outputRun,
                who=who,
            )
            # TODO: indexing payloads by only det1 wastes half the worker pool
            # the way it's currently done. If this stays the way we do things,
            # fix this.
            payloads[det1] = payload

        # TODO: Again just key on the first of the pair for downstream - the
        # count needs to be the number of processings which will finish so
        # that's 4 not 8 and there's no need to compoundify them as they're
        # only counted in dispatchGatherSteps
        self.redisHelper.writeDetectorsToExpect(
            self.instrument, expRecord.id, list(d[0] for d in detectorPairs), "AOS"
        )
        # AOS is running ISR (for now, at least) so we need to write that we
        # expected the detectors from that processing too.
        cwfsDets = list(self.focalPlaneControl.CWFS_IDS)
        self.redisHelper.writeDetectorsToExpect(self.instrument, expRecord.id, cwfsDets, "ISR")
        self.redisHelper.recordAosPipelineConfig(self.instrument, expRecord.id, self.currentAosPipeline)

        # NOTE: probably want a segregated pool for AOS processing when we go
        # to dynamic allocation - SFM shouldn't be able to bog down the AOS
        # pool
        self._dispatchPayloads(payloads, PodFlavor.AOS_WORKER)

        # TODO: Consider whether this should move to the expRecord getting
        # function, or the event loop, or if this is OK. If this really does
        # fire for every image this is probably fine.
        writeExpRecordMetadataShard(expRecord, aosShardPath)

    def doDetectorFanout(self, expRecord: DimensionRecord) -> None:
        """Send the expRecord out for processing based on current selection.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The expRecord to process.
        """
        # AOS first
        self.doAosFanout(expRecord)

        # data driven section
        targetPipelineBytes, targetPipelineGraph, who = self.getPipelineConfig(expRecord)
        shardPath = getShardPath(self.locationConfig, expRecord)
        writeIsrConfigShard(expRecord, targetPipelineGraph, shardPath)  # all pipelines contain an ISR step

        detectorIds: list[int] = []
        nEnabled = None
        if self.focalPlaneControl is not None:  # only LSSTCam has a focalPlaneControl at present
            # excludeCwfs=True if we sent them to AOS
            detectorIds = self.focalPlaneControl.getEnabledDetIds(excludeCwfs=True)
            nEnabled = len(detectorIds)
        else:
            results = set(self.butler.registry.queryDataIds(["detector"], instrument=self.instrument))
            detectorIds = sorted([item["detector"] for item in results])  # type: ignore

        self.redisHelper.writeDetectorsToExpect(self.instrument, expRecord.id, detectorIds, "ISR")
        self.redisHelper.writeDetectorsToExpect(self.instrument, expRecord.id, detectorIds, who)

        namedPattern = self.focalPlaneControl.currentNamedPattern if self.focalPlaneControl else None
        self.log.info(
            f"Fanning {expRecord.instrument}-{expRecord.day_obs}-{expRecord.seq_num}"
            f" out to {len(detectorIds)} detectors {'' if nEnabled is None else f'of {nEnabled} enabled'} "
            f"{' with named pattern ' + namedPattern if namedPattern else ''}"
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

    def doDonutPairFanout(self) -> None:
        """Send the expRecord out for processing based on current selection.

        This is ONLY for full-focal-plane intra-focal, extra-focal pairs where
        the processing is triggered by OCS (vis OCPS sending a redis signal).

        It appropriates the entire cluster for the processing of these pairs,
        so uses the SFM worker pool, and the AOS pipeline graph.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The expRecord to process.
        """
        donutPair = self.redisHelper.checkForOcsDonutPair(self.instrument)
        if donutPair is None:
            return

        self.log.info(f"Found a donut pair trigger for {donutPair}")
        (record1,) = self.butler.registry.queryDimensionRecords("exposure", exposure=donutPair[0])
        (record2,) = self.butler.registry.queryDimensionRecords("exposure", exposure=donutPair[1])
        aosShardPath = getShardPath(self.locationConfig, record1, isAos=True)
        writeExpRecordMetadataShard(record1, aosShardPath)
        writeExpRecordMetadataShard(record2, aosShardPath)

        expRecords = [record1, record2]

        # define visits here for the edge case that the donut pair
        # signal arrives before the getNewExposureAndDefineVisit
        # call in the main loop
        defineVisit(self.butler, record1)
        defineVisit(self.butler, record2)

        # just some basic sanity checking
        instruments = list(set([expRecord.instrument for expRecord in expRecords]))
        assert len(instruments) == 1, f"Expected all expRecords to have same instrument, got {instruments=}"
        instrument = instruments[0]
        assert instrument == self.instrument, "Expected expRecords to make this head node instrument"

        self.log.info(f"Sending {[r.id for r in expRecords]} to {self.currentAosFamPipeline} pipeline")
        targetPipelineBytes = self.pipelines[self.currentAosFamPipeline].graphBytes["step1a"]
        targetPipelineGraph = self.pipelines[self.currentAosFamPipeline].graphs["step1a"]
        # record pipeline config via the first id for interoperability with
        # CWFS processing. This is just so the step1b dispatch knows what the
        # active pipeline was
        self.redisHelper.recordAosPipelineConfig(self.instrument, record1.id, self.currentAosFamPipeline)

        # deliberately do NOT set isAos=True here because we want this written
        # to the main table, not the AOS page on RubinTV
        for expRecord in expRecords:
            shardPath = getShardPath(self.locationConfig, expRecord)
            writeIsrConfigShard(expRecord, targetPipelineGraph, shardPath)

        assert self.focalPlaneControl is not None  # just for mypy
        # excludeCwfs because they get normal fanout to CWFS pipelines
        detectorIds = self.focalPlaneControl.getEnabledDetIds(excludeCwfs=True)

        identifier = "+".join(str(r.id) for r in expRecords)
        self.redisHelper.writeDetectorsToExpect(self.instrument, identifier, detectorIds, "AOS")
        for expRecord in expRecords:
            # send expect signal with who=ISR for single expIds as these are
            # for the focal plane mosaicing, and send above with who=AOS as
            # + joined above, as this is the real dispatch
            self.redisHelper.writeDetectorsToExpect(self.instrument, expRecord.id, detectorIds, "ISR")

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

        # SFM_WORKER set here deliberately as we're appropriating the entire
        # cluster for this processing.
        self._dispatchPayloads(payloads, PodFlavor.SFM_WORKER)

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

        sentToBusy = 0
        sentToFree = 0
        notDispatched = 0
        for detectorId, payload in payloads.items():
            matchingFreeWorkers = [w for w in freeWorkers if w.detectorNumber == detectorId]
            if matchingFreeWorkers:
                worker = matchingFreeWorkers[0]
                sentToFree += 1
                self.redisHelper.enqueuePayload(payload, worker)
                continue

            else:
                # No free worker with matching detector, so look for busy one
                matchingBusyWorkers = [w for w in busyWorkers if w.detectorNumber == detectorId]
                if matchingBusyWorkers:
                    worker = matchingBusyWorkers[0]
                    sentToBusy += 1
                    self.redisHelper.enqueuePayload(payload, worker)
                    continue
                else:
                    notDispatched += 1
                    self.log.error(
                        f"No workers (not even busy ones) available for {detectorId=},"
                        f" cannot dispatch process for {payload.who}",
                    )

        allWhos = {p.who for p in payloads.values()}
        whos = ",".join(sorted(allWhos))
        mixed = "" if len(allWhos) == 1 else "mixed "
        self.log.info(
            f"Sent {sentToFree} {mixed}payloads to free workers, {sentToBusy} to busy workers for {whos}"
        )
        if notDispatched:
            self.log.error(f"Failed to dispatch {notDispatched} {mixed}payloads for {whos}")

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

    def dispatchVisitImageMosaic(self, visitId: int) -> None:
        """Dispatch a preliminary_visit_image mosaic for the given visitId.

        Parameters
        ----------
        visitId : `int`
            The visit ID to dispatch the preliminary_visit_image mosaic for.
        """
        dataProduct = "preliminary_visit_image"
        self.log.info(f"Dispatching complete {dataProduct} mosaic for {visitId}")

        dataCoord = DataCoordinate.standardize(
            instrument=self.instrument, visit=visitId, universe=self.butler.dimensions
        )

        # TODO: this abuse of Payload really needs improving
        payload = Payload([dataCoord], b"", dataProduct, who="SFM")
        worker = self.getSingleWorker(self.instrument, PodFlavor.MOSAIC_WORKER)
        if worker is None:
            self.log.warning(f"No workers AT ALL for {dataProduct} mosaic - should be impossible, check k8s")
            return
        self.redisHelper.enqueuePayload(payload, worker)
        return

    def dispatchGatherSteps(self, who: str) -> bool:
        """Dispatch any gather steps as needed.

        Note that the return value is currently unused, but is planned to be
        built upon in the next few tickets.

        Returns
        -------
        dispatchedWork : `bool`
            Was anything sent out?
        """
        assert who in ("SFM", "AOS", "ISR"), f"Unknown pipeline {who=}"
        processedIds = self.redisHelper.getAllIdsForDetectorLevel(self.instrument, step="step1a", who=who)

        if not processedIds:
            return False

        completeIds: list[str] = []
        for idStr in processedIds:
            # idStr is "<int>" or "<int>+<int"> for AOS pairs
            nFinished = self.redisHelper.getNumDetectorLevelFinished(self.instrument, "step1a", who, idStr)
            nExpected = len(self.redisHelper.getExpectedDetectors(self.instrument, idStr, who))
            if nFinished >= nExpected:
                completeIds.append(idStr)
            if nFinished > nExpected:
                self.log.warning(
                    f"Found {nFinished} step1as finished for {idStr=}, but expected {nExpected} for {who=}"
                )

        if not completeIds:
            return False

        # let isr dispatch to the step1b workers anyway, they'll just drop
        # everything due to a lack of quanta
        podFlavour = PodFlavor.STEP1B_AOS_WORKER if who == "AOS" else PodFlavor.STEP1B_WORKER

        self.log.debug(f"For {who}: Found {completeIds=} for step1a for {who}")

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

            if who == "AOS":  # get the full AOS_XXX name for this exposure
                # Note: does not break the paired-processing because we always
                # record the config via the first id. Therefore always retrieve
                # the config for the first id.
                whoToUse = self.redisHelper.getAosPipelineConfig(self.instrument, intIds[0])
                # whoToUse takes values like "AOS_DANISH" or "AOS_FAM_TIE"
                if whoToUse is None:
                    self.log.warning(f"Failed to dispatch {who} for {idStr=}! This shouldn't happen")
                    continue
            else:
                whoToUse = who

            if self.pipelines[whoToUse].graphBytes.get("step1b") is not None:  # no step1b dispatch for ISR
                visitRecord = None
                try:  # not used, but checks whether this payload is even usable downstream
                    (visitRecord,) = self.butler.registry.queryDimensionRecords("visit", dataId=dataCoords[0])
                except ValueError:
                    # note: do not ``continue`` here, because there's other
                    # bits that still need to run later on - this is why we use
                    # a visitRecord=None sentinel instead
                    self.log.info(f"Skipping doomed step1b dispatch for {idStr=} due to lack of visit record")

                if visitRecord is not None:
                    payload = Payload(
                        dataIds=dataCoords,
                        pipelineGraphBytes=self.pipelines[whoToUse].graphBytes["step1b"],
                        run=self.outputRun,
                        who=who,
                    )
                    worker = self.getSingleWorker(self.instrument, podFlavour)
                    if not worker:
                        self.log.warning(f"No worker available for {who} step1b")
                        return False
                    self.log.info(
                        f"Dispatching step1b for {who} with complete inputs: {dataCoords} to {worker}"
                    )
                    self.redisHelper.enqueuePayload(payload, worker)
                    if who == "AOS":
                        intraId = visitRecord.id  # got from dataCoords[0] above so is intra
                        numZernikesFinished = self.redisHelper.getNumDetectorLevelFinished(
                            self.instrument, "step1a", who, idStr
                        )
                        self.redisHelper.sendZernikeCountToMTAOS(
                            self.instrument, intraId, numZernikesFinished
                        )

            self.log.debug(f"Removing step1a finished counter for {idStr=}")
            self.redisHelper.removeFinishedIdDetectorLevel(self.instrument, "step1a", who, idStr)

            # never dispatch this incomplete because it relies on a specific
            # detector having finished. It might have failed, but that's OK
            # because the one-off processor will time out quickly.
            if who in ["SFM", "ISR"]:
                # use exposure=dataCoords[0]["visit"] because we still want
                # to dispatch one-off post-isr processing for non-on-sky
                # images, and if you used dataId=dataCoords[0] that will fail
                # if the visit isn't defined.
                (expRecord,) = self.butler.registry.queryDimensionRecords(
                    "exposure", exposure=dataCoords[0]["visit"]
                )
                self.dispatchOneOffProcessing(expRecord, PodFlavor.ONE_OFF_POSTISR_WORKER)
                if self.instrument != "LATISS" and who != "ISR":
                    self.log.info(f"Dispatching the focal plane visit_image mosaic for {expRecord.id}")
                    # TODO: this should be visitId but that's OK for now
                    self.dispatchVisitImageMosaic(expRecord.id)

                    self.log.info(f"Sending {expRecord.id} for one-off visit image processing")
                    self.dispatchOneOffProcessing(expRecord, PodFlavor.ONE_OFF_VISITIMAGE_WORKER)
                    self.log.info(f"Sending {expRecord.id} for radial plot processing")
                    self.redisHelper.sendExpRecordToQueue(expRecord, f"{self.instrument}-RADIALPLOTTER")
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
        return False  # stop running rollups until we have some plots attached etc
        if self.instrument == "LATISS":
            # self.log.info("Consider making a one-off processor for
            # the night plots and dispatching it here")
            return False

        numComplete = self.redisHelper.getNumVisitLevelFinished(self.instrument, "step1b", who="SFM")
        if numComplete > self.nNightlyRollups:
            self.log.info(
                f"Found {numComplete - self.nNightlyRollups} more completed step1b's - "
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

    def dispatchPostIsrMosaic(self) -> None:
        """Dispatch the focal plane mosaic task.

        This will be dispatched to a worker which will then gather the
        individual CCD mosaics and make the full focal plane mosaic and upload
        to S3. At the moment, it will only work when everything is completed.
        """
        if self.instrument == "LATISS":
            # single chip cameras aren't plotted as binned mosaics, so this
            # happens in a one-off-processor instead for all round ease.
            return

        triggeringTask = "binnedIsrCreation"
        dataProduct = "post_isr_image"

        allDataIds = set(self.redisHelper.getAllDataIdsForTask(self.instrument, triggeringTask))

        completeIds = []
        for _id in allDataIds:
            nFinished = self.redisHelper.getNumTaskFinished(self.instrument, triggeringTask, _id)
            expOrVisitId = int(_id["exposure"]) if "exposure" in _id else int(_id["visit"])
            nExpected = len(self.redisHelper.getExpectedDetectors(self.instrument, expOrVisitId, who="ISR"))
            if nFinished >= nExpected:
                completeIds.append(_id)
            if nFinished > nExpected:
                msg = f"Found {nFinished=} for {triggeringTask} for {_id=}, but expected only {nExpected}"
                self.log.warning(msg)

        if not completeIds:
            return

        idString = (
            f"{len(completeIds)} images: {completeIds}" if len(completeIds) > 1 else f"expId={completeIds[0]}"
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

            (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dataId)
            self.dispatchOneOffProcessing(expRecord, PodFlavor.ONE_OFF_POSTISR_WORKER)

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

        # Get timing values once and reuse them
        lastLap = self.loopTimer.lastLapTime()
        lastWork = self.workTimer.lastLapTime()
        assert lastLap is not None, "Expected lastLap to be set"
        assert lastWork is not None, "Expected lastWork to be set"

        # Log statistics periodically
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

        sleepPeriod = self.targetLoopDuration - lastLap
        if sleepPeriod > 0:
            self.workTimer.pause()  # don't count the sleeping towards the loop time on work timer
            sleep(sleepPeriod)
            self.workTimer.resume()
        elif sleepPeriod < -0.3:  # allow some noise and only warn when we're severely slow
            self.log.warning(
                f"Event loop running slow, last loop took {lastLap:.2f}s with {lastWork:.2f}s of work"
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
            self.updateConfigsFromRubinTV()

            expRecord = self.getNewExposureAndDefineVisit()
            if expRecord is not None:
                assert self.instrument == expRecord.instrument
                self.dispatchOneOffProcessing(expRecord, podFlavor=PodFlavor.ONE_OFF_EXPRECORD_WORKER)
                writeExpRecordMetadataShard(expRecord, getShardPath(self.locationConfig, expRecord))
                if not isWepImage(expRecord) or self.instrument == "LATISS":  # process CWFS image on LATISS
                    self.doDetectorFanout(expRecord)

            try:
                self.doDonutPairFanout()  # checks for pair signal and dispatches
            except Exception as e:
                self.log.exception(f"Failed during of donut PAIR fanout: {e}")

            # for now, only dispatch to step1b once things are complete because
            # there is some subtlety in the dispatching incomplete things
            # because they will be dispatched again and again until they are
            # complete, and that will happen not when another completes, but at
            # the speed of this loop, which would be bad, so we need to deal
            # with tracking that and dispatching only if the number has gone up
            # *and* there are 2+ free workers, because it's not worth
            # re-dispatching for every single new CCD exposure which finishes.
            try:
                self.dispatchGatherSteps(who="SFM")
            except Exception as e:
                self.log.exception(f"Failed during dispatch of gather steps for SFM: {e}")

            try:
                self.dispatchGatherSteps(who="AOS")
            except Exception as e:
                self.log.warning(f"Failed during dispatch of gather steps for AOS: {e}")

            try:  # there's no real step1b for ISR, but the one-off postISR
                # trigger is in here, so this keeps the design more simple
                self.dispatchGatherSteps(who="ISR")
            except Exception as e:
                self.log.exception(f"Failed during dispatch of gather steps for ISR: {e}")

            try:
                self.dispatchPostIsrMosaic()
            except Exception as e:
                self.log.exception(f"Failed during dispatch of focal plane mosaics: {e}")

            try:
                self.dispatchRollupIfNecessary()
            except Exception as e:
                self.log.exception(f"Failed during dispatch nightly rollup: {e}")

            # note the repattern comes after the fanout so that any commands
            # executed are present for the next image to follow and only then
            # do we toggle
            if self.instrument == "LSSTCam":
                self.repattern()

            self.regulateLoopSpeed()


class CameraControlConfig:
    """Processing control for which CCDs will be processed."""

    # TODO: Make this camera agnostic if necessary.
    def __init__(self) -> None:
        self.log = logging.getLogger("lsst.rubintv.production.processControl.CameraControlConfig")
        self.camera = LsstCam.getCamera()
        self._detectorStates: dict[int, bool] = {det.getId(): False for det in self.camera}
        self._detectorIds: list[int] = [det.getId() for det in self.camera]
        self._imagingIds: list[int] = [detId for detId in self._detectorIds if self.isImaging(detId)]
        self._guiderIds: list[int] = [detId for detId in self._detectorIds if self.isGuider(detId)]
        self._wavefrontIds: list[int] = [detId for detId in self._detectorIds if self.isWavefront(detId)]
        # plotConfig = FocalPlaneGeometryPlotConfig()
        self._focalPlanePlot = FocalPlaneGeometryPlot()
        self._focalPlanePlot.showStats = False
        self._focalPlanePlot.plotMin = 0
        self._focalPlanePlot.plotMax = 1
        self.GUIDER_IDS: tuple[int, ...] = tuple(detId for detId in self._guiderIds)
        self.CWFS_IDS: tuple[int, ...] = tuple(detId for detId in self._wavefrontIds)
        self.INTRA_FOCAL_IDS = (192, 196, 200, 204)
        self.EXTRA_FOCAL_IDS = (191, 195, 199, 203)
        self.DIAGONAL_IDS = (90, 94, 98, 144, 148, 152, 36, 40, 44)
        self.DIAGONAL_IDS2 = (92, 94, 96, 132, 130, 128, 58, 56, 60)
        self.HORIZONTAL_IDS = (76, 75, 77, 85, 84, 86, 94, 93, 95, 103, 102, 104, 112, 111, 113)
        self.VERTICAL_IDS = (10, 13, 16, 46, 49, 52, 91, 94, 97, 136, 139, 142, 172, 175, 178)

        self.currentNamedPattern = ""

    def getIntraExtraFocalPairs(self) -> list[tuple[int, int]]:
        """Get the intra-focal and extra-focal pairs.

        Returns
        -------
        pairs : `list` of `tuple`
            List of tuples of the form (intra, extra) for each pair.
        """
        return list(zip(self.INTRA_FOCAL_IDS, self.EXTRA_FOCAL_IDS))

    def setDiagonalOn(self, other: bool = False) -> None:
        """Set the diagonal pattern on the focal plane.

        Parameters
        ----------
        other : `bool`, optional
            If True, set the diagonal2 pattern instead of the default diagonal.
            Default is False.

        """
        dets = self.DIAGONAL_IDS if not other else self.DIAGONAL_IDS2
        for det in dets:
            self.setDetectorOn(det)

    def setCardinalsOn(self, horizontal: bool = False) -> None:
        """Set the cardinal pattern on the focal plane.

        Parameters
        ----------
        other : `bool`, optional
            If True, set the cardinal2 pattern instead of the default cardinal.
            Default is False.

        """
        dets = self.HORIZONTAL_IDS if horizontal else self.VERTICAL_IDS
        for det in dets:
            self.setDetectorOn(det)

    def applyNamedPattern(self, pattern: str) -> None:
        """Apply a named pattern to the focal plane."""
        # strings from RubinTV frontend
        pattern = pattern.lower()
        match pattern:
            case "raft_checkerboard":
                self.setRaftCheckerboard()
            case "ccd_checkerboard":
                self.setFullCheckerboard()
            case "all":
                self.setAllImagingOn()
            case "5-on-a-die":
                self.setAllImagingOff()
                self.setRaftOn("R22")
                self.setRaftOn("R33")
                self.setRaftOn("R11")
                self.setRaftOn("R13")
                self.setRaftOn("R31")
            case "minimal":
                self.setAllImagingOff()
                self.setDiagonalOn()
                self.setDiagonalOn(other=True)
                self.setCardinalsOn()
                self.setCardinalsOn(horizontal=True)
            case "ultra-minimal":
                self.setAllImagingOff()
                self.setDiagonalOn()
                self.setDiagonalOn(other=True)
            case _:
                self.log.error(f"Tried and failed to apply pattern {pattern} - not a valid pattern")
                return  # don't hold the named pattern on fail
        self.currentNamedPattern = pattern

    def isWavefront(self, detectorId: int) -> bool:
        """Check if the detector is a wavefront sensor.

        Parameters
        ----------
        detectorId : `int`
            The detector id.

        Returns
        -------
        isWavefront : `bool`
            `True` is the detector is a wavefront sensor, else `False`.
        """
        return self.camera[detectorId].getPhysicalType() == "ITL_WF"

    def isGuider(self, detectorId: int) -> bool:
        """Check if the detector is a guider.

        Parameters
        ----------
        detectorId : `int`
            The detector id.

        Returns
        -------
        isGuider : `bool`
            `True` is the detector is a guider sensor, else `False`.
        """
        return self.camera[detectorId].getPhysicalType() == "ITL_G"

    def isImaging(self, detectorId: int) -> bool:
        """Check if the detector is an imaging sensor.

        Parameters
        ----------
        detectorId : `int`
            The detector id.

        Returns
        -------
        isImaging : `bool`
            `True` is the detector is an imaging sensor, else `False`.
        """
        return self.camera[detectorId].getPhysicalType() in ["E2V", "ITL"]

    def _getRaftTuple(self, detectorId: int) -> tuple[int, int]:
        """Get the detector's raft x, y coordinates as integers.

        Numbers are zero-indexed, with (0, 0) being at the bottom left.

        Parameters
        ----------
        detectorId : `int`
            The detector id.

        Returns
        -------
        x : `int`
            The raft's column number, zero-indexed.
        y : `int`
            The raft's row number, zero-indexed.
        """
        rString = self.camera[detectorId].getName().split("_")[0]
        return int(rString[1]), int(rString[2])

    def _getSensorTuple(self, detectorId: int) -> tuple[int, int]:
        """Get the detector's x, y coordinates as integers within the raft.

        Numbers are zero-indexed, with (0, 0) being at the bottom left.

        Parameters
        ----------
        detectorId : `int`
            The detector id.

        Returns
        -------
        x : `int`
            The detectors's column number, zero-indexed within the raft.
        y : `int`
            The detectors's row number, zero-indexed within the raft.
        """
        sString = self.camera[detectorId].getName().split("_")[1]
        return int(sString[1]), int(sString[2])

    def _getFullLocationTuple(self, detectorId: int) -> tuple[int, int]:
        """Get the (colNum, rowNum) of the detector wrt the full focal plane.

        0, 0 is the bottom left
        """
        raftX, raftY = self._getRaftTuple(detectorId)
        sensorX, sensorY = self._getSensorTuple(detectorId)
        col = (raftX * 3) + sensorX + 1
        row = (raftY * 3) + sensorY + 1
        return col, row

    def setWavefrontOn(self) -> None:
        """Turn all the wavefront sensors on."""
        for detectorId in self._wavefrontIds:
            self._detectorStates[detectorId] = True

    def setWavefrontOff(self) -> None:
        """Turn all the wavefront sensors off."""
        for detectorId in self._wavefrontIds:
            self._detectorStates[detectorId] = False

    def setGuidersOn(self) -> None:
        """Turn all the guider sensors on."""
        for detectorId in self._guiderIds:
            self._detectorStates[detectorId] = True

    def setGuidersOff(self) -> None:
        """Turn all the wavefront sensors off."""
        for detectorId in self._guiderIds:
            self._detectorStates[detectorId] = False

    def setFullCheckerboard(self, phase: int = 0) -> None:
        """Set a checkerboard pattern at the CCD level.

        Parameters
        ----------
        phase : `int`, optional
            Any integer is acceptable as it is applied mod-2, so even integers
            will get you one phase, and odd integers will give the other.
            Even-phase contains 96 detectors, odd-phase contains 93.
        """
        for detectorId in self._imagingIds:
            x, y = self._getFullLocationTuple(detectorId)
            self._detectorStates[detectorId] = bool(((x % 2) + (y % 2) + phase) % 2)

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
        for detectorId in self._imagingIds:
            raftX, raftY = self._getRaftTuple(detectorId)
            self._detectorStates[detectorId] = bool(((raftX % 2) + (raftY % 2) + phase) % 2)

    def setE2Von(self) -> None:
        """Turn all e2v sensors on."""
        for detectorId in self._imagingIds:
            if self.camera[detectorId].getPhysicalType() == "E2V":
                self._detectorStates[detectorId] = True

    def setE2Voff(self) -> None:
        """Turn all e2v sensors off."""
        for detectorId in self._imagingIds:
            if self.camera[detectorId].getPhysicalType() == "E2V":
                self._detectorStates[detectorId] = False

    def setITLon(self) -> None:
        """Turn all ITL sensors on."""
        for detectorId in self._imagingIds:
            if self.camera[detectorId].getPhysicalType() == "ITL":
                self._detectorStates[detectorId] = True

    def setITLoff(self) -> None:
        """Turn all ITL sensors off."""
        for detectorId in self._imagingIds:
            if self.camera[detectorId].getPhysicalType() == "ITL":
                self._detectorStates[detectorId] = False

    def setRaftOn(self, raftName: str) -> None:
        for detectorId in self._detectorIds:
            if self.camera[detectorId].getName().startswith(raftName):
                self._detectorStates[detectorId] = True

    def setRaftOff(self, raftName: str) -> None:
        for detectorId in self._detectorIds:
            if self.camera[detectorId].getName().startswith(raftName):
                self._detectorStates[detectorId] = False

    def setDetectorOn(self, detectorNumber: int) -> None:
        self._detectorStates[detectorNumber] = True

    def setDetectorOff(self, detectorNumber: int) -> None:
        self._detectorStates[detectorNumber] = False

    def setFullFocalPlaneGuidersOn(self) -> None:
        """Turn all the guiders on."""
        # XXX is this actually correct??
        for detectorId in self._imagingIds:
            sensorX, sensorY = self._getSensorTuple(detectorId)
            if sensorX <= 1 and sensorY <= 1:
                self._detectorStates[detectorId] = True

    def setAllOn(self) -> None:
        """Turn all sensors on.

        Note that this includes wavefront sensors and guiders.
        """
        for detectorId in self._detectorIds:
            self._detectorStates[detectorId] = True

    def setAllOff(self) -> None:
        """Turn all sensors off.

        Note that this includes wavefront sensors and guiders.
        """
        for detectorId in self._detectorIds:
            self._detectorStates[detectorId] = False

    def setAllImagingOn(self) -> None:
        """Turn all imaging sensors on."""
        for detectorId in self._imagingIds:
            self._detectorStates[detectorId] = True

    def setAllImagingOff(self) -> None:
        """Turn all imaging sensors off."""
        for detectorId in self._imagingIds:
            self._detectorStates[detectorId] = False

    def invertImagingSelection(self) -> None:
        """Invert the selection of the imaging chips only."""
        for detectorId in self._imagingIds:
            self._detectorStates[detectorId] = not self._detectorStates[detectorId]

    def getNumEnabled(self) -> int:
        """Get the number of enabled sensors.

        Returns
        -------
        nEnabled : `int`
            The number of enabled detectors.
        """
        return sum(self._detectorStates.values())

    def getEnabledDetIds(self, excludeCwfs=False) -> list[int]:
        """Get the detectorIds of the enabled sensors.

        Returns
        -------
        enabled : `list` of `int`
            The detectorIds of the enabled detectors.
        """
        enabled = sorted([detId for (detId, state) in self._detectorStates.items() if state is True])
        if excludeCwfs:
            enabled = [detId for detId in enabled if detId not in self.CWFS_IDS]
        return enabled

    def getDisabledDetIds(self, excludeCwfs: bool = False) -> list[int]:
        """Get the detectorIds of the disabled sensors.

        Parameters
        ----------
        excludeCwfs : `bool`, optional
            If ``True``, exclude the CWFS detectors from the list of disabled
            detectors.

        Returns
        -------
        disabled : `list` of `int`
            The detectorIds of the disabled detectors.
        """
        disabled = sorted([detId for (detId, state) in self._detectorStates.items() if state is False])
        if excludeCwfs:
            disabled = [detId for detId in disabled if detId not in self.CWFS_IDS]
        return disabled

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
        for detectorId, state in self._detectorStates.items():
            detector = self.camera[detectorId]
            for amp in detector:
                detNums.append(detectorId)
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
