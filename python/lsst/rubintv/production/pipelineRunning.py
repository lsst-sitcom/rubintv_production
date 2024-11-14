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
import time
from typing import TYPE_CHECKING

import numpy as np

from lsst.ctrl.mpexec import SingleQuantumExecutor, TaskFactory
from lsst.pipe.base import Pipeline, QuantumGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.caching_limited_butler import CachingLimitedButler
from lsst.summit.utils import ConsDbClient, computeCcdExposureId

from .baseChannels import BaseButlerChannel
from .consdbUtils import ConsDBPopulator
from .payloads import pipelineGraphFromBytes, pipelineGraphToBytes
from .redisUtils import RedisHelper
from .slac.mosaicing import writeBinnedImage
from .utils import getShardPath, raiseIf, writeMetadataShard

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, DataCoordinate, Quantum

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
    "lsst.pipe.tasks.calibrate.CalibrateTask",  # end of step1 for quickLook pipeline
    "lsst.pipe.tasks.postprocess.TransformSourceTableTask",  # end of step1 for nightly pipeline
    "lsst.pipe.tasks.postprocess.ConsolidateVisitSummaryTask",  # end of step2a for quickLook pipeline
    "lsst.analysis.tools.tasks.refCatSourceAnalysis.RefCatSourceAnalysisTask",  # end of step2a for nightly
)

NO_COPY_ON_CACHE: set = {"bias", "dark", "flat", "defects", "camera"}


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
    pipeline : `str`
        The path to the pipeline yaml file.
    step : `str`
        The step of the pipeline to run with this worker.
    awaitsDataProduct : `str`
        The data product that this runner needs in order to run. Should be set
        to `"raw"` for step1 runners and `None` for all other ones, as their
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
        pipeline: str,  # not pulled from the locationConfig to allow notebook/debug usage
        step: str,
        awaitsDataProduct: str | None,
        podDetails: PodDetails,
        *,
        doRaise=False,
    ):
        super().__init__(
            locationConfig=locationConfig,
            instrument=instrument,
            butler=butler,
            watcherType="redis",
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
        self.pipeline = f"{pipeline}#{step}"
        self.pipelineGraph = Pipeline.fromFile(self.pipeline).to_graph(registry=self.butler.registry)
        self.pipelineGraphBytes = pipelineGraphToBytes(self.pipelineGraph)
        self.podDetails = podDetails

        self.runCollection: str | None = None
        self.cachingButler = self.makeCachingLimitedButler(butler)
        self.log.info(f"Pipeline running configured to consume from {self.podDetails.queueName}")

        self.consdbClient = ConsDbClient("http://consdb-pq.consdb:8080/consdb")
        self.redisHelper = RedisHelper(butler, self.locationConfig)

        self.consDBPopulator = ConsDBPopulator(self.consdbClient, self.redisHelper)

    def makeCachingLimitedButler(self, butler: Butler) -> CachingLimitedButler:
        cachedOnGet = set()
        cachedOnPut = set()
        for name in self.pipelineGraph.dataset_types.keys():
            if self.pipelineGraph.consumers_of(name):
                if self.pipelineGraph.producer_of(name) is not None:
                    cachedOnPut.add(name)
                else:
                    cachedOnGet.add(name)

        noCopyOnCache = NO_COPY_ON_CACHE
        self.log.info(f"Creating CachingLimitedButler with {cachedOnPut=}, {cachedOnGet=}, {noCopyOnCache=}")
        return CachingLimitedButler(butler, cachedOnPut, cachedOnGet, noCopyOnCache)

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

    def callback(self, payload: Payload) -> None:
        """Method called on each payload from the queue.

        Executes the pipeline on the payload's dataId, outputting to the run
        specified in the payload.

        Parameters
        ----------
        payload : `lsst.rubintv.production.payloads.Payload`
            The payload to process.
        """
        self.log.setLevel("DEBUG")  # XXX remove before merging
        dataIds: list[DataCoordinate] = payload.dataIds
        pipelineGraphBytes: bytes = payload.pipelineGraphBytes
        self.runCollection = payload.run
        who = payload.who  # who are we running this for?

        compoundId = ""
        sampleDataId = dataIds[0]  # they'd better all be the same or there's bigger problems I think
        if "exposure" in sampleDataId:
            compoundId = "+".join([f"{dataId['exposure']}" for dataId in dataIds])
        elif "visit" in sampleDataId:
            compoundId = "+".join([f"{dataId['visit']}" for dataId in dataIds])
        else:  # for day_obs or instrument level dataIds.
            compoundId = "+".join([f"{dataId}" for dataId in dataIds])

        self.log.debug(f"Processing {compoundId=}")

        # XXX reinstate or remove? we deal with isr differently now...
        # if not self.doProcessImage(dataId):
        #     return
        self.log.debug(f"{self.step=} {self.dataProduct=}")

        try:
            if pipelineGraphBytes is not None and pipelineGraphBytes != self.pipelineGraphBytes:
                self.log.warning("Pipeline graph has changed, updating")
                self.pipelineGraphBytes = pipelineGraphBytes
                self.pipelineGraph = pipelineGraphFromBytes(pipelineGraphBytes)
                # need to remake the caching butler if the pipeline changes
                self.cachingButler = self.makeCachingLimitedButler(self.butler)

            # chain all the dataId components together with AND, and an OR
            # between each dataId. Make sure to bind the dataId components
            # correctly by using a unique string for each dataId.
            where = ""
            bind = {}
            for i, dataId in enumerate(dataIds):
                idString = f"dataId_{i}_"
                where += " AND ".join(f"{k}={idString}{k}" for k in dataId.mapping)
                bind.update({f"{idString}{k}": v for k, v in dataId.mapping.items()})
                where += " OR "
            if where.endswith(" OR "):
                where = where[:-4]
            del dataId

            self.log.debug(f"{where=}\n{bind=}")

            builder = AllDimensionsQuantumGraphBuilder(
                self.pipelineGraph,
                self.butler,
                where=where,
                bind=bind,
                clobber=True,
                input_collections=list(self.butler.collections.defaults) + [self.runCollection],
                output_run=self.runCollection,
            )

            self.log.info(f"Running pipeline for {dataIds}")

            # _waitForDataProduct waits for the raw to land in the repo and
            # caches it on the butler, so don't bother to catch the return.
            # Then check for empty qg and raise if that is the case.
            t0 = time.time()
            for dataId in dataIds:
                self.log.debug(f"waiting for {self.dataProduct} for {dataId}")
                self._waitForDataProduct(dataId, gettingButler=self.cachingButler)
            self.log.info(
                f"Spent {(time.time()-t0):.2f} seconds waiting for {len(dataIds)} {self.dataProduct}(s)"
                " (should be ~1s per id)"
            )

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

            executor = SingleQuantumExecutor(
                None,
                taskFactory=TaskFactory(),
                limited_butler_factory=lambda _: self.cachingButler,
                clobberOutputs=True,  # check with Jim if this is how we should handle clobbering
            )

            for node in qg:
                # just to make sure taskName is defined, so if this shows
                # up anywhere something is very wrong
                taskName = "something is deeply wrong"
                dataCoord = node.quantum.dataId  # pull this out before the try so you can use in except block
                assert dataCoord is not None, "dataCoord is None, this shouldn't be possible in RA"
                try:
                    self.log.debug(f"Executing {node.taskDef.taskName} for {dataCoord}")
                    # TODO: add per-quantum timing info here and return in
                    # PayloadResult

                    taskName = node.taskDef.taskName  # pull this first for the except block in case of raise
                    self.log.info(f"Starting to process {taskName}")

                    quantum, _ = executor.execute(node.task_node, node.quantum)
                    self.postProcessQuantum(quantum)
                    self.redisHelper.reportTaskFinished(self.instrument, taskName, dataCoord)

                except Exception as e:
                    # don't use quantum directly in this block in case it is
                    # not defined due to executor.execute() raising

                    # Track when the tasks finish, regardless of whether they
                    # succeeded.

                    # TODO: consider whether to track all the intermediate
                    # tasks, or only the points used for triggering other
                    # workflows.
                    self.log.exception(f"Task {taskName} failed: {e}")
                    self.redisHelper.reportTaskFinished(self.instrument, taskName, dataCoord, failed=True)
                    raise e  # still raise the error once we've logged the quantum as finishing

            self.log.debug(f"Finished iterating over nodes in QG for {compoundId} for {who}")

            # finished looping over nodes
            if self.step == "step1":
                self.log.debug(f"Announcing completion of step1 for {compoundId} for {who}")
                self.redisHelper.reportDetectorLevelFinished(
                    self.instrument, "step1", who=who, processingId=compoundId
                )
            if self.step == "step2a":
                self.log.debug(f"Announcing completion of step2a for {compoundId} for {who}")
                self.redisHelper.reportVisitLevelFinished(self.instrument, "step2a", who=who)
                # TODO: probably add a utility function on the helper for this
                # and one for getting the most recent visit from the queue
                # which does the decoding too to provide a unified interface.
                if who == "SFM":
                    self.redisHelper.redis.lpush(f"{self.instrument}-PSFPLOTTER", compoundId)
            if self.step == "nightlyRollup":
                self.redisHelper.reportNightLevelFinished(self.instrument, who=who)

        except Exception as e:
            if self.step == "step2a":
                self.redisHelper.reportVisitLevelFinished(self.instrument, "step2a", who=who, failed=True)
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

        # ned to catch the old and new isr tasks alike, and also not worry
        # about intermittent namespace stuttering
        if "isr" in taskName.lower():
            self.postProcessIsr(quantum)
        elif "calibratetask" in taskName.lower():
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
        else:
            return

    def postProcessIsr(self, quantum: Quantum) -> None:
        dRef = quantum.outputs["postISRCCD"][0]
        exp = self.cachingButler.get(dRef)

        writeBinnedImage(
            exp=exp,
            instrument=self.instrument,
            outputPath=self.locationConfig.calculatedDataPath,
            binSize=self.locationConfig.binning,
        )
        self.log.info(f"Wrote binned postISRCCD for {dRef.dataId}")

        if self.locationConfig.location in ["summit", "bts", "tts"]:  # don't fill ConsDB at USDF
            try:
                (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dRef.dataId)
                detectorNum = exp.getDetector().getId()
                postIsrMedian = float(np.nanmedian(exp.image.array))  # np.float isn't JSON serializable
                ccdvisitId = computeCcdExposureId(self.instrument, expRecord.id, detectorNum)
                self.consdbClient.insert(
                    instrument=self.instrument,
                    table=f"cdb_{self.instrument.lower()}.ccdvisit1_quicklook",
                    obs_id=ccdvisitId,
                    values={"postisr_pixel_median": postIsrMedian},
                    allow_update=False,
                )
                self.log.info(f"Added postISR pixel median to ConsDB for {dRef.dataId}")
                md = {expRecord.seq_num: {"PostISR pixel median": postIsrMedian}}
                shardPath = getShardPath(self.locationConfig, expRecord)
                writeMetadataShard(shardPath, expRecord.day_obs, md)
            except Exception:
                self.log.exception("Failed to populate ccdvisit1_quicklook row in ConsDB")

    def postProcessCalibrate(self, quantum: Quantum) -> None:
        # This is very similar indeed to postProcessIsr, but we it's not worth
        # refactoring yet, especially as they will probably diverge in the
        # future.
        dRef = quantum.outputs["calexp"][0]
        exp = self.cachingButler.get(dRef)

        writeBinnedImage(
            exp=exp,
            instrument=self.instrument,
            outputPath=self.locationConfig.binnedCalexpPath,
            binSize=self.locationConfig.binning,
        )
        # use a custom "task label" here because step2a on the summit is
        # triggered off the end of calibrate, and so needs to have that key in
        # redis remaining in order to run, and the dequeue action of the
        # creation of the focal plane mosaic removes that (as it should). If
        # anything, the binned postISR images should probably use this
        # mechanism too, and anything else which forks off the main processing
        # trunk.
        self.redisHelper.reportTaskFinished(self.instrument, "binnedCalexpCreation", dRef.dataId)
        self.log.info(f"Wrote binned calexp for {dRef.dataId}")

        try:
            # TODO: DM-45438 either have NV write to a different table or have
            # it know where this is running and stop attempting this write at
            # USDF.
            if self.locationConfig.location in ["summit", "bts", "tts"]:  # don't fill ConsDB at USDF
                summaryStats = exp.getInfo().getSummaryStats()
                (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dRef.dataId)
                detectorNum = exp.getDetector().getId()
                self.consDBPopulator.populateCcdVisitRow(expRecord, detectorNum, summaryStats)
                self.log.info(f"Populated consDB ccd-visit row for {dRef.dataId} for {detectorNum}")
        except Exception:
            if self.locationConfig.location == "summit":
                self.log.exception("Failed to populate ccd-visit row in ConsDB")
            else:
                self.log.info(f"Failed to populate ccd-visit row in ConsDB at {self.locationConfig.location}")

    def postProcessVisitSummary(self, quantum: Quantum) -> None:
        dRef = quantum.outputs["visitSummary"][0]
        vs = self.cachingButler.get(dRef)
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dRef.dataId)

        nominalPlateScale = 0.199225  # XXX remove the hard-coding for ComCam
        pixToArcseconds = np.nanmean(
            [
                row.wcs.getPixelScale().asArcseconds() if row.wcs is not None else nominalPlateScale
                for row in vs
            ]
        )
        SIGMA2FWHM = np.sqrt(8 * np.log(2))

        e1 = (vs["psfIxx"] - vs["psfIyy"]) / (vs["psfIxx"] + vs["psfIyy"])
        e2 = 2 * vs["psfIxy"] / (vs["psfIxx"] + vs["psfIyy"])

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
            "Zeropoint": np.nanmean(vs["zeroPoint"]),
        }

        # flag all these as measured items to color the cell
        labels = {"_" + k: "measured" for k in outputDict.keys()}
        outputDict.update(labels)
        dayObs = expRecord.day_obs
        seqNum = expRecord.seq_num
        rowData = {seqNum: outputDict}

        shardPath = getShardPath(self.locationConfig, expRecord)
        writeMetadataShard(shardPath, dayObs, rowData)
        try:
            # TODO: DM-45438 either have NV write to a different table or have
            # it know where this is running and stop attempting this write at
            # USDF.
            if self.locationConfig.location in ["summit", "bts", "tts"]:
                self.consDBPopulator.populateVisitRow(vs, self.instrument)
                self.log.info(f"Populated consDB visit row for {expRecord.id}")
        except Exception:
            self.log.exception("Failed to populate visit row in ConsDB")
