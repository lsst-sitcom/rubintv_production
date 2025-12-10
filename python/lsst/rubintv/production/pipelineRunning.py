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

import datetime
import logging
import os
import time
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

import numpy as np
from astropy.time import Time
from galsim.zernike import zernikeRotMatrix

from lsst.daf.butler import Butler, DataCoordinate, DatasetIdGenEnum, DatasetRef, DimensionRecord, Quantum
from lsst.pipe.base import ExecutionResources, PipelineGraph, QuantumGraph, TaskFactory
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.caching_limited_butler import CachingLimitedButler
from lsst.pipe.base.single_quantum_executor import SingleQuantumExecutor
from lsst.pipe.base.trivial_quantum_graph_builder import TrivialQuantumGraphBuilder
from lsst.summit.utils import ConsDbClient, computeCcdExposureId
from lsst.summit.utils.efdUtils import getEfdData, makeEfdClient
from lsst.summit.utils.utils import getCameraFromInstrumentName
from lsst.ts.ofc import OFCData

from .baseChannels import BaseButlerChannel
from .consdbUtils import ConsDBPopulator
from .payloads import getDetectorId, pipelineGraphFromBytes
from .plotting.mosaicing import writeBinnedImage
from .processingControl import buildPipelines
from .redisUtils import RedisHelper
from .utils import (
    getExpIdOrVisitId,
    getExpRecordFromId,
    getShardPath,
    logDuration,
    raiseIf,
    writeMetadataShard,
)

if TYPE_CHECKING:
    from lsst_efd_client import EfdClient

    from lsst.afw.image import ExposureSummaryStats
    from lsst.pipe.base.graph.quantumNode import QuantumNode
    from lsst.pipe.base.quantum_graph_builder import QuantumGraphBuilder

    from .payloads import Payload
    from .podDefinition import PodDetails
    from .utils import LocationConfig


__all__ = [
    "SingleCorePipelineRunner",
]

# TODO: post OR3 maybe get this from the pipeline graph?
# TODO: post OR3 add something to allow us to trigger steps based on time or
# something that allows us to still run when a task fails. Could even maybe
# just be a finally block in the quantum execution code to allow us to count
# fails. Only downside there is that it won't be robust to OOM kills.

# record when these tasks finish per-quantum so we can trigger off the counts
TASK_ENDPOINTS_TO_TRACK = (
    "lsst.ip.isr.isrTask.IsrTask",  # for focal plane mosaics
    "lsst.pipe.tasks.calibrate.CalibrateTask",  # end of step1a for quickLook pipeline
    "lsst.pipe.tasks.postprocess.TransformSourceTableTask",  # end of step1a for nightly pipeline
    "lsst.pipe.tasks.postprocess.ConsolidateVisitSummaryTask",  # end of step1b for quickLook pipeline
    "lsst.analysis.tools.tasks.refCatSourceAnalysis.RefCatSourceAnalysisTask",  # end of step1b for nightly
)

NO_COPY_ON_CACHE: set = {"bias", "dark", "flat", "defects", "camera", "astrometry_camera"}
PSF_GRADIENT_WARNING = 0.65
PSF_GRADIENT_BAD = 0.85

INTRA_IDS = (192, 196, 200, 204)
EXTRA_IDS = (191, 195, 199, 203)


def makeCachingLimitedButler(butler: Butler, pipelineGraphs: list[PipelineGraph]) -> CachingLimitedButler:
    cachedOnGet = set()
    cachedOnPut = set()
    for pipelineGraph in pipelineGraphs:
        for name in pipelineGraph.dataset_types.keys():
            if pipelineGraph.consumers_of(name):
                if pipelineGraph.producer_of(name) is not None:
                    cachedOnPut.add(name)
                else:
                    cachedOnGet.add(name)

    noCopyOnCache = NO_COPY_ON_CACHE
    log = logging.getLogger("lsst.rubintv.production.pipelineRunning.makeCachingLimitedButler")
    log.info(f"Creating CachingLimitedButler with {cachedOnPut=}, {cachedOnGet=}, {noCopyOnCache=}")
    return CachingLimitedButler(butler, cachedOnPut, cachedOnGet, noCopyOnCache)


class SingleCorePipelineRunner(BaseButlerChannel):
    """Class for detector-parallel or single-core pipelines, e.g. SFM.

    Runs a pipeline using a CachingLimitedButler.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The locationConfig containing the path configs.
    butler : `lsst.daf.butler.Butler`
        The butler to use.
    instrument : `str`
        The instrument name.
    step : `str`
        The step of the pipeline to run with this worker.
    awaitsDataProduct : `str`
        The data product that this runner needs in order to run. Should be set
        to `"raw"` for step1a runners and `None` for all other ones, as their
        triggering is dealt with by the head node. TODO: See if this can be
        removed entirely, because this inconsistency is a bit weird.
    queueName : `str`
        The queue that the worker should consume from.
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(
        self,
        locationConfig: LocationConfig,
        butler: Butler,
        instrument: str,
        step: str,
        awaitsDataProduct: str | None,
        podDetails: PodDetails,
        *,
        doRaise=False,
    ):
        super().__init__(
            locationConfig=locationConfig,
            butler=butler,
            # TODO: DM-43764 this shouldn't be necessary on the
            # base class after this ticket, I think.
            detectors=None,
            dataProduct=awaitsDataProduct,
            # TODO: DM-43764 should also be able to fix needing
            # channelName when tidying up the base class. Needed
            # in some contexts but not all. Maybe default it to
            # ''?
            channelName="",
            podDetails=podDetails,
            doRaise=doRaise,
            addUploader=False,  # pipeline running pods don't upload directly
        )
        self.instrument = instrument
        self.butler = butler
        self.step = step

        allGraphs, pipelines = buildPipelines(
            instrument=instrument,
            locationConfig=locationConfig,
            butler=butler,
        )
        self.allGraphs = allGraphs
        self.pipelines = pipelines

        self.podDetails = podDetails

        self.runCollection: str = "uninitialized!"
        self.cachingButler = makeCachingLimitedButler(butler, self.allGraphs)
        self.log.info(f"Pipeline running configured to consume from {self.podDetails.queueName}")

        self.consdbClient = ConsDbClient("http://consdb-pq.consdb:8080/consdb")
        self.redisHelper = RedisHelper(butler, self.locationConfig)

        self.consDBPopulator = ConsDBPopulator(self.consdbClient, self.redisHelper, self.locationConfig)
        self.ofcData = OFCData("lsst")

    @cached_property
    def efdClient(self) -> EfdClient:
        """Create an EFD client as needed, but don't add automatically to all
        runners.

        Returns
        -------
        efdClient : `lsst.efd_client.EfdClient`
            The EFD client to use for this runner.
        """
        return makeEfdClient()

    def doProcessImage(self, dataId: DataCoordinate) -> bool:
        """Determine if we should skip this image.

        Should take responsibility for logging the reason for skipping.

        Parameters
        ----------
        dataId : `lsst.daf.butler.DataCoordinate`
            The data coordinate.

        Returns
        -------
        doProcess : `bool`
            True if the image should be processed, False if we should skip it.
        """
        # add any necessary data-driven logic here to choose if we process
        return True

    def getExtraDataCoordinate(self, payload: Payload) -> DataCoordinate | None:
        """Get the extra data coordinate (if any) for AOS pipelines.

        AOS pipelines are always dispatched via their extra-focal id. For the
        initial steps, and for normal images, group this with the corner pair.
        For FAM images, pair with the intra-focal image, rather than detector.
        For step1b, which doesn't have a detector, pair with nothing for normal
        images, with the intra-focal image for FAM images.

        Parameters
        ----------
        dataId : `lsst.daf.butler.DataCoordinate`
            The data coordinate.

        Returns
        -------
        extraDataId : `dict` [`str`, `str` or `int`]
            Any extra dataId components needed for this runner.
        """
        # TODO: need make this aware of whether we're running pair or unpaired
        # also need to make a sister-function called dropExtraDataCoordinate
        # for used in the main loop so we drop the intra-id for paired running.

        if payload.who != "AOS":  # keep things simple for other pipelines
            return None

        expId = getExpIdOrVisitId(payload.dataId)
        expRecord = getExpRecordFromId(expId, self.instrument, self.butler)
        isFamImage = expRecord.observation_type.lower() == "cwfs"
        if isFamImage:
            # For FAM images we want to pair with the previous visit,
            # regardless of whether we're step1a or step1b. Detector as-is
            previousVisit = expRecord.id - 1  # intra is always taken first, and we always key on extra
            extraCoordNoRecords = DataCoordinate.standardize(
                visit=previousVisit, universe=self.butler.dimensions, instrument=self.instrument
            )
            extraCoord = self.butler.registry.expandDataId(extraCoordNoRecords)
            return extraCoord

        # for non-FAM images, for step1a we pair with the intra-focal detector,
        # for step1b things are already visit-level so don't pair with anything
        detectorId = getDetectorId(payload)
        if detectorId is None:  # deals with non-FAM step1b
            return None

        if detectorId in EXTRA_IDS:  # intra-focal IDs hard-coded for now
            extraCoordNoRecords = DataCoordinate.standardize(
                visit=expRecord.id,
                detector=detectorId + 1,
                universe=self.butler.dimensions,
                instrument=self.instrument,
            )
            extraCoord = self.butler.registry.expandDataId(extraCoordNoRecords)
            return extraCoord

        assert False, "This should be unreachable, detectorId should be in EXTRA_IDS for AOS non-FAM images"

    def doDropQuantum(self, node: QuantumNode) -> bool:
        taskName = node.task_node.label
        dataCoord = node.quantum.dataId
        assert dataCoord is not None, "dataCoord is None, this shouldn't be possible in RA"  # for mypy
        if "calczernikes" in taskName.lower():
            if "detector" in dataCoord and dataCoord["detector"] in INTRA_IDS:
                # TODO: need to not drop this for unpaired runs
                self.log.info(f"Dropping unpaired calcZernikes quantum for {dataCoord}")
                return True
        return False

    def getIntraFocalInputs(
        self,
        payload: Payload,
        pipelineGraph: PipelineGraph,
        expRecord: DimensionRecord,
    ) -> dict[str, list[DatasetRef]]:
        """Get any subset inputs needed for the TrivialQuantumGraphBuilder.

        This MUST only be called for step1a non-FAM images.
        TODO: add protection for this.

        Parameters
        ----------
        payload : `lsst.rubintv.production.payloads.Payload`
            The payload to process.
        pipelineGraph : `lsst.pipe.base.PipelineGraph`
            The pipeline graph.

        Returns
        -------
        inputRefs : `dict` [`str`, `list` [`lsst.daf.butler.DatasetRef`]]
            The input dataset references needed for the builder.
        """
        inputRefs: dict[str, list[DatasetRef]] = {}
        if subset := pipelineGraph.task_subsets.get("detector-pair-merge-task"):
            if payload.dataId["detector"] not in EXTRA_IDS:
                return inputRefs  # no-op for the intras
            if payload.dataId["detector"] not in EXTRA_IDS and payload.dataId["detector"] not in INTRA_IDS:
                # guard against madness
                raise ValueError("pair-merge-task should only be defined for AOS step1a non-FAM pipelines")

            for taskLabel in subset:
                taskNode = pipelineGraph.tasks[taskLabel]
                for readEdge in taskNode.iter_all_inputs():
                    datasetTypeNode = pipelineGraph.dataset_types[readEdge.parent_dataset_type_name]
                    dataId = self.butler.registry.expandDataId(
                        visit=getExpIdOrVisitId(payload.dataId),
                        detector=int(payload.dataId["detector"]) + 1,
                        instrument=self.instrument,
                    )
                    inputRefs[readEdge.parent_dataset_type_name] = [
                        DatasetRef(
                            datasetTypeNode.dataset_type,
                            dataId,
                            run=self.runCollection,
                            id_generation_mode=DatasetIdGenEnum.DATAID_TYPE_RUN,
                        )
                    ]
        elif subset := pipelineGraph.task_subsets.get("visit-pair-merge-task"):
            if expRecord.observation_type.lower() != "cwfs":
                raise ValueError("visit-pair-merge-task should only be defined for AOS step1a FAM pipelines")
            if "intra" in expRecord.observation_reason.lower():
                return inputRefs  # no-op for the intras

            for taskLabel in subset:
                taskNode = pipelineGraph.tasks[taskLabel]
                for readEdge in taskNode.iter_all_inputs():
                    datasetTypeNode = pipelineGraph.dataset_types[readEdge.parent_dataset_type_name]
                    dataId = self.butler.registry.expandDataId(
                        visit=getExpIdOrVisitId(payload.dataId) - 1,
                        detector=payload.dataId["detector"],
                        instrument=self.instrument,
                    )
                    inputRefs[readEdge.parent_dataset_type_name] = [
                        DatasetRef(
                            datasetTypeNode.dataset_type,
                            dataId,
                            run=self.runCollection,
                            id_generation_mode=DatasetIdGenEnum.DATAID_TYPE_RUN,
                        )
                    ]
        return inputRefs

    def getQuantumGraphBuiler(
        self, payload: Payload, pipelineGraph: PipelineGraph
    ) -> tuple[QuantumGraphBuilder, str, dict[str, Any]]:
        expId = getExpIdOrVisitId(payload.dataId)
        expRecord = getExpRecordFromId(expId, self.instrument, self.butler)
        # isFamImage = expRecord.observation_type.lower() == "cwfs"
        dataId = payload.dataId

        builder: QuantumGraphBuilder
        if self.step == "step1b":
            self.log.info(f"Making AllDimensionQGBuilder for {self.step} for expId {expId} for {payload.who}")

            where = ""
            bind = {}

            # TODO: tidy this up
            idString = f"dataId_{0}_"
            where += " AND ".join(f"{k}={idString}{k}" for k in dataId.mapping)
            bind.update({f"{idString}{k}": v for k, v in dataId.mapping.items()})

            builder = AllDimensionsQuantumGraphBuilder(
                pipelineGraph,
                self.butler,
                where=where,
                bind=bind,
                clobber=True,
                input_collections=list(self.butler.collections.defaults) + [self.runCollection],
                output_run=self.runCollection,
            )
            return builder, where, bind

        else:  # all step1as
            dataIds: list[DataCoordinate] = [dataId]
            self.log.info(f"Making TrivialQG builder for {self.step} for expId {expId} for {payload.who}")

            # this is not for the extra quanta, this is making sure the visit
            # record stuff makes it into the QGG
            if "visit" in pipelineGraph.get_all_dimensions():
                visitDataCoord = self.butler.registry.expandDataId(
                    visit=expId, instrument=self.instrument, detector=payload.dataId["detector"]
                )
                dataIds.append(visitDataCoord)

            inputRefs: dict[str, list[DatasetRef]] = {  # all step1as have a raw as an input
                "raw": [cast(DatasetRef, self.butler.find_dataset("raw", payload.dataId))],
            }

            intraFocalInputs: dict[str, list[DatasetRef]] = {}
            if payload.who == "AOS":
                intraFocalInputs = self.getIntraFocalInputs(payload, pipelineGraph, expRecord)
                inputRefs.update(intraFocalInputs)

            builder = TrivialQuantumGraphBuilder(
                pipeline_graph=pipelineGraph,
                butler=self.butler,
                data_ids=dataIds,
                input_refs=inputRefs,
                clobber=True,  # TBD by Jim as to whether this should be removed
                input_collections=list(self.butler.collections.defaults) + [self.runCollection],
                output_run=self.runCollection,
                dataset_id_modes=dict.fromkeys(intraFocalInputs.keys(), DatasetIdGenEnum.DATAID_TYPE_RUN),
            )
            return builder, "", {}

    def callback(self, payload: Payload) -> None:
        """Method called on each payload from the queue.

        Executes the pipeline on the payload's dataId, outputting to the run
        specified in the payload.

        Parameters
        ----------
        payload : `lsst.rubintv.production.payloads.Payload`
            The payload to process.
        """
        dataId = payload.dataId
        expId = getExpIdOrVisitId(payload.dataId)  # for keying all the redis stuff by
        pipelineGraphBytes = payload.pipelineGraphBytes
        self.runCollection = payload.run  # TODO: remove this from being on the class at all
        who = payload.who  # who are we running this for?

        extraCoord = self.getExtraDataCoordinate(payload)
        dataIds = [dataId, extraCoord] if extraCoord is not None else [dataId]
        del dataId

        try:
            # NOTE: if someone sends a pipelineGraphBytes that's so different
            # from the pipelines built by buildPipelines that it consumes
            # different data products, things won't be cached, but assuming
            # that's not the case this should be OK. Otherwise, remake the
            # CachingLimitedButler with the new pipelineGraph here.
            pipelineGraph = pipelineGraphFromBytes(pipelineGraphBytes)

            self.log.info(f"Running pipeline for {dataIds}")

            # _waitForDataProduct waits for the item to land in the repo and
            # caches it on the butler, so we don't use the return, other than
            # to test that it arrived, so a) we can log and b) send the fail
            # to RubinTV. If it doesn't arrive the qg will be empty
            t0 = time.time()
            for dataId in dataIds:
                self.log.info(f"Waiting for {self.dataProduct} for {dataId}")
                if self.locationConfig.location == "bts":
                    # TODO : Find something better when we're not about to go
                    # on sky.
                    t = 60  # ideally this would be in config not code
                else:
                    t = 20  # ideally this wouldn't reassert the upsteam default but it makes this smaller
                dataProduct = self._waitForDataProduct(dataId, gettingButler=self.cachingButler, timeout=t)

                # self.dataProduct is the data product we are waiting for, so
                # if that's none the rest doesn't apply here, i.e. that's
                # success
                if self.dataProduct is not None and dataProduct is None:
                    # _waitForDataProduct logs a warning so no need to warn
                    record = None
                    if "exposure" in dataId.dimensions:
                        record = dataId.records["exposure"]
                    elif "visit" in dataId.dimensions:
                        # XXX is this ever true? Do we need this? How do visit
                        # records ever get in here? Maybe for step1b?
                        record = dataId.records["visit"]

                    if record is None:  # not an else block because mypy
                        self.log.warning(f"{dataId} has no visit/expRecord so can't log missing data product")
                        continue
                    failRecord = {
                        # it would look nicer to have this dict the other way
                        # around but we're merging dicts, so that would
                        # overwrite if there were _e.g. multiple raws missing
                        f"{dataId}-timeout": f"{self.dataProduct}",  # dict-items these merge cleanly now
                        "DISPLAY_VALUE": "💩",  # just keep overwriting this, doesn't matter
                    }
                    columnName = "Retrieval fails"
                    shardPath = getShardPath(self.locationConfig, record)
                    writeMetadataShard(shardPath, record.day_obs, {record.seq_num: {columnName: failRecord}})
            self.log.info(
                f"Spent {(time.time() - t0):.2f} seconds waiting for {len(dataIds)} {self.dataProduct}(s)"
                " (should be ~1s per id)"
            )

            builder, where, bind = self.getQuantumGraphBuiler(payload, pipelineGraph)

            with logDuration(self.log, f"Building quantum graph for {dataIds} for {who}"):
                qg: QuantumGraph = builder.build(
                    metadata={
                        "input": list(self.butler.collections.defaults) + [self.runCollection],
                        "output_run": self.runCollection,
                        "data_query": where,
                        "bind": bind,
                        "time": f"{datetime.datetime.now()}",
                    }
                )

            if not qg:
                raise RuntimeError(f"No work found for {dataIds}")

            nCpus = int(os.getenv("LIMITS_CPU", 1))
            self.log.info(f"Using {nCpus} CPUs for {self.instrument} {self.step} {who}")

            executor = SingleQuantumExecutor(
                butler=None,
                task_factory=TaskFactory(),
                limited_butler_factory=lambda _: self.cachingButler,
                clobber_outputs=True,  # check with Jim if this is how we should handle clobbering
                raise_on_partial_outputs=False,
                resources=ExecutionResources(num_cores=nCpus),
            )

            for node in qg:
                if self.doDropQuantum(node):
                    # XXX Can we just do this?! 😅 At minimum we'll need to
                    # report it as finished below
                    continue
                # just to make sure taskName is defined, so if this shows
                # up anywhere something is very wrong
                dataCoord = node.quantum.dataId  # pull this out before the try so you can use in except block
                assert dataCoord is not None, "dataCoord is None, this shouldn't be possible in RA"
                self.log.debug(f"Executing {node.taskDef.taskName} for {dataCoord}")
                taskName = node.taskDef.taskName  # pull this first for the except block in case of raise
                self.log.info(f"Starting to process {taskName}")

                quantum = None  # reset inside the loop so it can't be stale inside except block
                try:
                    # TODO: add per-quantum timing info here and return in
                    # PayloadResult
                    quantum, _ = executor.execute(node.task_node, node.quantum)
                    self.postProcessQuantum(quantum)
                    self.redisHelper.reportTaskFinished(self.instrument, taskName, dataCoord)

                except Exception as e:
                    # Track when the tasks finish, regardless of whether they
                    # succeeded.
                    self.log.exception(f"Task {taskName} failed: {e}")
                    self.redisHelper.reportTaskFinished(self.instrument, taskName, dataCoord, failed=True)
                    if quantum is not None:
                        self.postProcessQuantum(quantum)
                    raise e  # still raise the error once we've logged the quantum as finishing

            self.log.debug(f"Finished iterating over nodes in QG for {expId} for {who}")

            # finished looping over nodes
            if self.step == "step1a":
                self.log.debug(f"Announcing completion of step1a for {expId} for {who}")
                self.redisHelper.reportDetectorLevelFinished(
                    self.instrument, "step1a", who=who, processingId=expId
                )
            if self.step == "step1b":
                self.log.debug(f"Announcing completion of step1b for {expId} for {who}")
                self.redisHelper.reportVisitLevelFinished(self.instrument, "step1b", who=who)
                # TODO: probably add a utility function on the helper for this
                # and one for getting the most recent visit from the queue
                # which does the decoding too to provide a unified interface.
                if who == "SFM":
                    # in SFM this is never compound
                    self.redisHelper.redis.lpush(f"{self.instrument}-PSFPLOTTER", str(expId))

                    # required the visitSummary so needs to be post-step1b
                    (visitRecord,) = self.butler.registry.queryDimensionRecords("visit", visit=expId)
                    self.log.info(f"Sending {visitRecord.id} for fwhm plotting")
                    self.redisHelper.sendExpRecordToQueue(visitRecord, f"{self.instrument}-FWHMPLOTTER")
                    self.redisHelper.redis.lpush(f"{self.instrument}-ZERNIKE_PREDICTION_PLOTTER", str(expId))
            if self.step == "nightlyRollup":
                self.redisHelper.reportNightLevelFinished(self.instrument, who=who)

        except Exception as e:
            if self.step == "step1a":
                self.redisHelper.reportDetectorLevelFinished(
                    self.instrument, "step1a", who=who, processingId=expId, failed=True
                )
            if self.step == "step1b":
                self.redisHelper.reportVisitLevelFinished(self.instrument, "step1b", who=who, failed=True)
            if self.step == "nightlyRollup":
                self.redisHelper.reportNightLevelFinished(self.instrument, who=who, failed=True)
            raiseIf(self.doRaise, e, self.log)

    def postProcessQuantum(self, quantum: Quantum) -> None:
        """Write shards here, make sure to keep these bits quick!

        compoundId is a maybe-compound id, either a single exposure or a
        compound of multiple exposures, depending on the pipeline, joined with
        a "+".

        Also, anything you self.cachingButler.get() make sure to add to
        cache_on_put.

        # TODO: After OR3, move all this out to postprocessQuanta.py, and do
        the dispatch and function definitions in there to keep the runner
        clean.
        """
        taskName = quantum.taskName
        assert taskName is not None, "taskName is None, this shouldn't be possible in RA"  # mainly for mypy

        # need to catch the old and new isr tasks alike, and also not worry
        # about intermittent namespace stuttering
        if "isr" in taskName.lower():
            self.postProcessIsr(quantum)
        elif "calibratetask" in taskName.lower() or "calibrateimagetask" in taskName.lower():
            # TODO: think about if we could make dicts of some of the
            # per-CCD quantities like PSF size and 50 sigma source counts
            # etc. Would probably mean changes to mergeShardsAndUpload in
            # order to merge dict-like items into their corresponding
            # dicts.
            self.postProcessCalibrate(quantum)
        elif "postprocess.ConsolidateVisitSummaryTask".lower() in taskName.lower():
            # ConsolidateVisitSummaryTask regardless of quickLook or NV
            # pipeline, because this is the quantum that holds the
            # visitSummary
            self.postProcessVisitSummary(quantum)
        elif "AggregateZernikeTablesTask".lower() in taskName.lower():
            self.postProcessAggregateZernikeTables(quantum)
        elif "CalcZernikesTask".lower() in taskName.lower():
            self.postProcessCalcZernikes(quantum)
        else:
            return

    def postProcessIsr(self, quantum: Quantum) -> None:
        output_dataset_name = "post_isr_image"
        dRef = None
        try:
            dRef = quantum.outputs[output_dataset_name][0]
            exp = self.cachingButler.get(dRef)
        except Exception as e:
            self.log.warning(
                f"Failed to post-process *failed* quantum {quantum}. This is not unexpected"
                " but still merits a warning due to the failing quantum."
            )
            if dRef is not None:
                # it shouldn't ever be None here, but technically could be, so
                # check here for mypy, and reraise if it was None
                self.redisHelper.reportTaskFinished(self.instrument, "binnedIsrCreation", dRef.dataId)
            else:
                raise AssertionError(
                    f"Failed to post-process *failed* isr quantum {quantum} and dRef was None. This shouldn't"
                    " be possible."
                ) from e
            return

        expRecord = dRef.dataId.records["exposure"]
        assert expRecord is not None, "expRecord is None, this shouldn't be possible"

        writeBinnedImage(
            exp=exp,
            instrument=self.instrument,
            dayObs=expRecord.day_obs,
            seqNum=expRecord.seq_num,
            binSize=self.locationConfig.binning,
            dataProduct="post_isr_image",
            locationConfig=self.locationConfig,
        )
        self.log.info(f"Wrote binned {output_dataset_name} for {dRef.dataId}")
        self.redisHelper.reportTaskFinished(self.instrument, "binnedIsrCreation", dRef.dataId)
        if self.locationConfig.location in ["summit", "bts", "tts"]:  # don't fill ConsDB at USDF
            try:
                detectorNum = exp.getDetector().getId()
                postIsrMedian = float(np.nanmedian(exp.image.array))  # np.float isn't JSON serializable
                ccdvisitId = computeCcdExposureId(self.instrument, expRecord.id, detectorNum)
                self.consdbClient.insert(
                    instrument=self.instrument,
                    table=f"cdb_{self.instrument.lower()}.ccdexposure_quicklook",
                    obs_id=ccdvisitId,
                    values={"postisr_pixel_median": postIsrMedian},
                    allow_update=False,
                )
                self.log.info(f"Added post_isr_image pixel median to ConsDB for {dRef.dataId}")
                md = {expRecord.seq_num: {"PostISR pixel median": postIsrMedian}}
                shardPath = getShardPath(self.locationConfig, expRecord)
                writeMetadataShard(shardPath, expRecord.day_obs, md)
            except Exception:
                self.log.exception("Failed to populate ccdvisit1_quicklook row in ConsDB")

    def postProcessCalibrate(self, quantum: Quantum) -> None:
        output_dataset_name = "preliminary_visit_image"

        try:
            dRef = quantum.outputs[output_dataset_name][0]
            exp = self.cachingButler.get(dRef)
        except Exception:
            self.log.warning(
                f"Failed to post-process *failed* quantum {quantum}. This is not unexpected"
                " but still merits a warning due to the failing quantum."
            )
            return

        visitRecord = dRef.dataId.records["visit"]
        assert visitRecord is not None, "visitRecord is None, this shouldn't be possible"

        writeBinnedImage(
            exp=exp,
            instrument=self.instrument,
            dayObs=visitRecord.day_obs,
            seqNum=visitRecord.seq_num,
            binSize=self.locationConfig.binning,
            dataProduct="preliminary_visit_image",
            locationConfig=self.locationConfig,
        )
        # use a custom "task label" here because step1b on the summit is
        # triggered off the end of calibrate, and so needs to have that key in
        # redis remaining in order to run, and the dequeue action of the
        # creation of the focal plane mosaic removes that (as it should). If
        # anything, the binned post_isr_images should probably use this
        # mechanism too, and anything else which forks off the main processing
        # trunk.
        self.redisHelper.reportTaskFinished(self.instrument, "binnedVisitImageCreation", dRef.dataId)
        self.log.info(f"Wrote binned {output_dataset_name} for {dRef.dataId}")

        summaryStats = exp.getInfo().getSummaryStats()
        if summaryStats:
            detNum = exp.detector.getId()
            self.redisHelper.reportVisitSummaryStats(
                visitRecord.instrument, visitRecord.id, detector=detNum, stats=summaryStats
            )

        try:
            # TODO: DM-45438 either have NV write to a different table or have
            # it know where this is running and stop attempting this write at
            # USDF.
            summaryStats = exp.getInfo().getSummaryStats()
            detectorNum = exp.getDetector().getId()
            # consDBPopulator validates the location and only inserts if it's
            # summit-like (summit, bts, tts)
            self.consDBPopulator.populateCcdVisitRow(visitRecord, detectorNum, summaryStats)
            self.log.info(f"Populated consDB ccd-visit row for {dRef.dataId} for {detectorNum}")
        except Exception:
            if self.locationConfig.location == "summit":
                self.log.exception("Failed to populate ccd-visit row in ConsDB")
            else:
                self.log.info(f"Failed to populate ccd-visit row in ConsDB at {self.locationConfig.location}")

        stats: None | ExposureSummaryStats = exp.getInfo().getSummaryStats()
        if stats:
            detId: int = exp.detector.getId()
            self.redisHelper.reportVisitSummaryStats(self.instrument, visitRecord.id, detId, stats)

    def postProcessVisitSummary(self, quantum: Quantum) -> None:
        output_dataset_name = "preliminary_visit_summary"

        try:
            dRef = quantum.outputs[output_dataset_name][0]
            vs = self.cachingButler.get(dRef)
        except Exception:
            self.log.warning(
                f"Failed to post-process *failed* quantum {quantum}. This is not unexpected"
                " but still merits a warning due to the failing quantum."
            )
            return
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dRef.dataId)

        # 0.2"/pix is virtually exact - the detector median on an image gave
        # 0.2000821, so round that off for the fallback value.
        nominalPlateScale = 0.2
        SIGMA2FWHM = np.sqrt(8 * np.log(2))

        scales: list[float] = []
        camera = getCameraFromInstrumentName(self.instrument)
        for row in vs:
            detector = camera[row["id"]]
            scales.append(
                row.wcs.getPixelScale(detector.getBBox().getCenter()).asArcseconds()
                if row.wcs is not None
                else nominalPlateScale
            )
        pixToArcseconds = np.nanmedian(scales)
        # check if pixToArcseconds is within 10% of nominalPlateScale
        if not (0.9 * nominalPlateScale < pixToArcseconds < 1.1 * nominalPlateScale):
            self.log.warning(f"Unusual pixel scale {pixToArcseconds=} not within 10% of nominal")

        e1 = (vs["psfIxx"] - vs["psfIyy"]) / (vs["psfIxx"] + vs["psfIyy"])
        e2 = 2 * vs["psfIxy"] / (vs["psfIxx"] + vs["psfIyy"])

        zeropoint = np.nanmean(vs["zeroPoint"])
        expTime = np.nanmean(vs["expTime"])
        realZeropoint = zeropoint - 2.5 * np.log10(expTime)
        outputDict = {
            "PSF FWHM": np.nanmean(vs["psfSigma"]) * SIGMA2FWHM * pixToArcseconds,
            "PSF e1": np.nanmean(e1),
            "PSF e2": np.nanmean(e2),
            "Sky mean": np.nanmean(vs["skyBg"]),
            "Sky RMS": np.nanmean(vs["skyNoise"]),
            "Variance plane mean": np.nanmean(vs["meanVar"]),
            "PSF star count": np.nanmean(vs["nPsfStar"]),
            "Astrometric bias": np.nanmean(vs["astromOffsetMean"]),
            "Astrometric scatter": np.nanmean(vs["astromOffsetStd"]),
            "Zeropoint": zeropoint,
            "Real zeropoint": realZeropoint,
        }

        # flag all these as measured items to color the cell
        labels = {"_" + k: "measured" for k in outputDict.keys()}
        outputDict.update(labels)
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        rowData = {seqNum: outputDict}

        camera = getCameraFromInstrumentName(self.instrument)
        detectors: list[int] = [det.getId() for det in camera]
        fwhmValues = []
        for detectorId in detectors:
            row = vs[vs["id"] == detectorId]
            if len(row) > 0:
                psfSigma = row["psfSigma"][0]
                fwhm = psfSigma * SIGMA2FWHM * pixToArcseconds  # Convert to microns (0.2"/pixel)
                fwhmValues.append(float(fwhm))

        fwhmValuesArray = np.array(fwhmValues)[~np.isnan(fwhmValues)]
        fwhmValues05, fwhmValues95 = np.percentile(fwhmValuesArray, [5, 95])
        psfGradient = np.sqrt(fwhmValues95**2 - fwhmValues05**2)
        outputDict["PSF gradient"] = psfGradient
        if psfGradient >= PSF_GRADIENT_BAD:  # note this flag must be set after the measured labels
            outputDict["_PSF gradient"] = "bad"
        elif psfGradient >= PSF_GRADIENT_WARNING:
            outputDict["_PSF gradient"] = "warning"

        shardPath = getShardPath(self.locationConfig, expRecord)
        writeMetadataShard(shardPath, dayObs, rowData)
        try:
            # TODO: DM-45438 either have NV write to a different table or have
            # it know where this is running and stop attempting this write at
            # USDF.
            # always write, as consDBPopulator validates location
            self.consDBPopulator.populateVisitRow(vs, expRecord, allowUpdate=True)
            self.log.info(f"Populated consDB visit row for {expRecord.id}")
        except Exception:
            self.log.exception("Failed to populate visit row in ConsDB")

    def postProcessCalcZernikes(self, quantum: Quantum) -> None:
        """Post-process the Zernike table to send results to ConsDB."""
        # protect import to stop the whole package depending on ts_wep. If this
        # becomes a problem we could copy the functions or just accept that RA
        # needs T&S software.
        from lsst.ts.wep.utils.zernikeUtils import makeDense

        try:
            dRef = quantum.outputs["zernikes"][0]
            zkTable = self.cachingButler.get(dRef)
        except Exception:
            self.log.warning(
                f"Failed to post-process *failed* quantum {quantum}. This is not unexpected"
                " but still merits a warning due to the failing quantum."
            )
            return

        # *ideally* this would be pulled from maxconfig.nollIndices() but
        # that's not possible here, but a) they never run above 28, and b)
        # ConsDB only goes out that far, so we'd truncate there anyway, so we
        # just hardcode it here.
        MAX_NOLL_INDEX = 28

        # Get the physical rotation from the EFD. Ideally this would be pulled
        # from the ConsDB, but that's calculated elsewhere in RA, and although
        # that process is much quicker, using it here is introducing an
        # unnecessary race condition, so it's better to recalculate it here.
        visitRecord = dRef.dataId.records["visit"]
        assert visitRecord is not None, "visitRecord is None, this shouldn't be possible"
        if zkTable.meta is None:  # do the timing before the EFD query
            zkTable.meta = {}
        zkTable.meta["shutter_to_zernike_time"] = float((Time.now() - visitRecord.timespan.end).sec)

        data = getEfdData(self.efdClient, "lsst.sal.MTRotator.rotation", expRecord=visitRecord)
        physicalRotation = np.nanmean(data["actualPosition"])

        detector = dRef.dataId.records["detector"]  # not a detector object, but a detector dimension
        assert detector is not None, "detector is None, this shouldn't be possible"
        detectorId: int = detector.id  # hence .id rather than .getId()

        zkTable = zkTable[zkTable["label"] == "average"]
        zkColsHere = zkTable.meta["opd_columns"]
        nollIndicesHere = np.asarray(zkTable.meta["noll_indices"])
        # Grab Zernike values, convert to dense array, save
        zkSparse = zkTable[zkColsHere].to_pandas().values[0]
        zkDense = makeDense(zkSparse, nollIndicesHere, MAX_NOLL_INDEX)
        rotationMatrix = zernikeRotMatrix(MAX_NOLL_INDEX, -np.deg2rad(physicalRotation))
        # we only track z4 upwards and ConsDB only has slots for z4 to z28
        zernikeValues: np.ndarray = zkDense / 1e3 @ rotationMatrix[4:, 4:]

        consDbValues: dict[str, float] = {}
        for i in range(len(zernikeValues)):  # these start at z4 and are dense so contain zeros
            value = float(zernikeValues[i])  # make a real float for ConsDB
            if value == 0:  # skip the ones which were zero due to sparseness so they're null in the DB
                continue
            consDbValues[f"z{i + 4}"] = float(zernikeValues[i])

        # consDB validates the location and only inserts if it's summit-like
        self.consDBPopulator.populateCcdVisitRowZernikes(visitRecord, detectorId, consDbValues)

    def postProcessAggregateZernikeTables(self, quantum: Quantum) -> None:
        # protect import to stop the whole package depending on ts_wep. If this
        # becomes a problem we could copy the functions or just accept that RA
        # needs T&S software.
        from lsst.ts.wep.utils import convertZernikesToPsfWidth, makeDense

        try:
            dRef = quantum.outputs["aggregateZernikesAvg"][0]
            zernikes = self.cachingButler.get(dRef)
        except Exception:
            self.log.warning(
                f"Failed to post-process *failed* quantum {quantum}. This is not unexpected"
                " but still merits a warning due to the failing quantum."
            )
            return
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dRef.dataId)

        if zernikes.meta is None:
            zernikes.meta = {}
        zernikes.meta["shutter_to_zernike_time"] = float((Time.now() - expRecord.timespan.end).sec)

        rowSums = []

        nollIndices = zernikes.meta["nollIndices"]
        maxNollIndex = np.max(zernikes.meta["nollIndices"])
        for row in zernikes:
            zkOcs = row["zk_deviation_OCS"]
            detector = row["detector"]
            zkDense = makeDense(zkOcs, nollIndices, maxNollIndex)
            zkDense -= self.ofcData.y2_correction[detector][: len(zkDense)]
            zkFwhm = convertZernikesToPsfWidth(zkDense)
            rowSums.append(np.sqrt(np.sum(zkFwhm**2)))

        average_result = np.nanmean(rowSums)
        residual = 1.06 * np.log(1 + average_result)  # adjustement per John Franklin's paper

        outputDict = {"Residual AOS FWHM": f"{residual:.2f}"}

        donutBlurFwhm = float("nan")  # needs to be defined for lower block but nans are removed on send
        if "estimatorInfo" in zernikes.meta and zernikes.meta["estimatorInfo"] is not None:
            # if danish is run then fwhm is in the metadata, if TIE then it's
            # not. danish models the width of the Kolmogorov profile needed to
            # convolve with the geometric donut model (the optics) to match the
            # donut. If AI_DONUT then "estimatorInfo" might not be present.
            if donutBlurFwhm := zernikes.meta["estimatorInfo"].get("fwhm"):
                outputDict["Donut Blur FWHM"] = f"{donutBlurFwhm:.2f}"

        labels = {"_" + k: "measured" for k in outputDict.keys()}
        outputDict.update(labels)
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        rowData = {seqNum: outputDict}

        shardPath = getShardPath(self.locationConfig, expRecord)
        writeMetadataShard(shardPath, dayObs, rowData)

        consDbValues: dict[str, int | float] = {}
        try:
            self.log.info(f"Sending donut blur {donutBlurFwhm:.2f} for {expRecord.id} to consDB")
            # visit_id is required for updates
            consDbValues = {"aos_fwhm": residual, "visit_id": expRecord.id}
            if donutBlurFwhm:
                consDbValues["donut_blur_fwhm"] = donutBlurFwhm
            self.consDBPopulator.populateArbitrary(
                expRecord.instrument,
                "visit1_quicklook",
                consDbValues,
                expRecord.day_obs,
                expRecord.seq_num,
                True,  # insert into existing an row requires allowUpdate
            )
        except Exception as e:
            self.log.error(
                f"Failed to write donut blur and/or AOS residual for {expRecord.id} with {consDbValues} {e}"
            )
            raiseIf(self.doRaise, e, self.log)
            return
