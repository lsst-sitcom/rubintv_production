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


import datetime

import numpy as np

import lsst.summit.utils.butlerUtils as butlerUtils
from lsst.ctrl.mpexec import SingleQuantumExecutor, TaskFactory
from lsst.pipe.base import Pipeline, QuantumGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.caching_limited_butler import CachingLimitedButler
from lsst.summit.utils import ConsDbClient

from .baseChannels import BaseButlerChannel
from .consdbUtils import ConsDBPopulator
from .payloads import pipelineGraphFromBytes, pipelineGraphToBytes
from .slac.mosaicing import writeBinnedImage
from .utils import getShardPath, raiseIf, writeMetadataShard

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

NO_COPY_ON_CACHE = ("bias", "dark", "flat", "defects", "camera")


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
        locationConfig,
        butler,
        instrument,
        pipeline,  # not pulled from the locationConfig to allow notebook/debug usage
        step,
        awaitsDataProduct,
        queueName,
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
            queueName=queueName,
            doRaise=doRaise,
        )
        self.instrument = instrument
        self.butler = butler
        self.step = step
        self.pipeline = pipeline + f"#{step}"
        self.pipelineGraph = Pipeline.fromFile(self.pipeline).to_graph(registry=self.butler.registry)
        self.pipelineGraphBytes = pipelineGraphToBytes(self.pipelineGraph)

        self.runCollection = None
        self.limitedButler = self.makeLimitedButler(butler)
        self.log.info(f"Pipeline running configured to consume from {queueName}")

        self.consdbClient = ConsDbClient("http://consdb-pq.consdb:8080/consdb")
        self.consDBPopulator = ConsDBPopulator(self.consdbClient, self.watcher.redisHelper)

    def makeLimitedButler(self, butler):
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

    def doProcessImage(self, dataId):
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

    def callback(self, payload):
        """Method called on each payload from the queue.

        Executes the pipeline on the payload's dataId, outputting to the run
        specified in the payload.

        Parameters
        ----------
        payload : `lsst.rubintv.production.payloads.Payload`
            The payload to process.
        """
        dataId = payload.dataId
        pipelineGraphBytes = payload.pipelineGraphBytes
        self.runCollection = payload.run

        if not self.doProcessImage(dataId):
            return

        try:
            if pipelineGraphBytes is not None and pipelineGraphBytes != self.pipelineGraphBytes:
                self.log.warning("Pipeline graph has changed, updating")
                self.pipelineGraphBytes = pipelineGraphFromBytes(pipelineGraphBytes)
                # need to remake the caching butler if the pipeline changes
                self.limitedButler = self.makeLimitedButler(self.butler)

            where = " AND ".join(f"{k}=_{k}" for k in dataId.mapping)
            bind = {f"_{k}": v for k, v in dataId.mapping.items()}
            builder = AllDimensionsQuantumGraphBuilder(
                self.pipelineGraph,
                self.butler,
                where=where,
                bind=bind,
                clobber=True,
                input_collections=self.butler.collections + (self.runCollection,),
                output_run=self.runCollection,
            )

            self.log.info(f"Running pipeline for {dataId}")

            # _waitForDataProduct waits for the raw to land in the repo and
            # caches it on the butler, so don't bother to catch the return.
            # Then check for empty qg and raise if that is the case.
            self._waitForDataProduct(dataId, gettingButler=self.limitedButler)

            qg: QuantumGraph = builder.build(
                metadata={
                    "input": self.butler.collections + (self.runCollection,),
                    "output_run": self.runCollection,
                    "data_query": where,
                    "bind": bind,
                    "time": f"{datetime.datetime.now()}",
                }
            )

            if not qg:
                raise RuntimeError(f"No work found for {dataId}")

            executor = SingleQuantumExecutor(
                None,
                taskFactory=TaskFactory(),
                limited_butler_factory=lambda _: self.limitedButler,
                clobberOutputs=True,  # check with Jim if this is how we should handle clobbering
            )

            if "exposure" in payload.dataId:
                processingId = payload.dataId["exposure"]  # this works for step1 and step2a
            else:
                # TODO need to work out what to do for non-exposure-containing
                # dataIds. Do we even need this anymore though, now we have a
                # step2a finished counter?
                processingId = 1

            for node in qg:
                try:
                    # TODO: add per-quantum timing info here and return in
                    # PayloadResult

                    taskName = node.taskDef.taskName
                    self.log.info(f"Starting to process {taskName}")
                    quantum = executor.execute(node.taskDef, node.quantum)
                    self.postProcessQuantum(quantum, processingId)
                    self.watcher.redisHelper.reportFinished(self.instrument, taskName, processingId)

                except Exception as e:
                    # don't use quantum directly in this block in case it is
                    # not defined due to executor.execute() raising

                    # Track when the tasks finish, regardless of whether they
                    # succeeded.

                    # TODO: consider whether to track all the intermediate
                    # tasks, or only the points used for triggering other
                    # workflows.
                    self.log.exception(f"Task {taskName} failed: {e}")
                    self.watcher.redisHelper.reportFinished(
                        self.instrument, taskName, processingId, failed=True
                    )
                    raise e  # still raise the error once we've logged the quantum as finishing

            # finished looping over nodes
            if self.step == "step2a":
                self.watcher.redisHelper.reportVisitLevelFinished(self.instrument, "step2a")
                # TODO: probably add a utility function on the helper for this
                # and one for getting the most recent visit from the queue
                # which does the decoding too to provide a unified interface.
                if "visit" in payload.dataId:
                    visit = f"{payload.dataId['visit']}"
                else:
                    visit = f"{payload.dataId['exposure']}"
                self.watcher.redisHelper.redis.lpush("LSSTComCamSim-PSFPLOTTER", visit)
            if self.step == "nightlyRollup":
                self.watcher.redisHelper.reportNightLevelFinished(self.instrument)

        except Exception as e:
            if self.step == "step2a":
                self.watcher.redisHelper.reportVisitLevelFinished(self.instrument, "step2a", failed=True)
            if self.step == "nightlyRollup":
                self.watcher.redisHelper.reportNightLevelFinished(self.instrument, failed=True)
            raiseIf(self.doRaise, e, self.log)

    def postProcessQuantum(self, quantum, processingId):
        """Write shards here, make sure to keep these bits quick!

        Also, anything you self.limitedButler.get() make sure to add to
        cache_on_put.

        # TODO: After OR3, move all this out to postprocessQuanta.py, and do
        the dispatch and function definitions in there to keep the runner
        clean.
        """
        match quantum.taskName:
            case "lsst.ip.isr.isrTask.IsrTask":
                self.postProcessIsr(quantum)
            case "lsst.pipe.tasks.calibrate.CalibrateTask":
                # TODO: think about if we could make dicts of some of the
                # per-CCD quantities like PSF size and 50 sigma source counts
                # etc. Would probably mean changes to mergeShardsAndUpload in
                # order to merge dict-like items into their corresponding
                # dicts.

                self.postProcessCalibrate(quantum, processingId)
            case "lsst.pipe.tasks.postprocess.ConsolidateVisitSummaryTask":
                # ConsolidateVisitSummaryTask regardless of quickLook or NV
                # pipeline, because this is the quantum that holds the
                # visitSummary
                self.postProcessVisitSummary(quantum)
            case _:
                return

    def postProcessIsr(self, quantum):
        dRef = quantum.outputs["postISRCCD"][0]
        exp = self.limitedButler.get(dRef)

        writeBinnedImage(
            exp=exp,
            instrument=self.instrument,
            outputPath=self.locationConfig.calculatedDataPath,
            binSize=self.locationConfig.binning,
        )

        self.log.info(f"Wrote binned postISRCCD for {dRef.dataId}")

    def postProcessCalibrate(self, quantum, processingId):
        # This is very similar indeed to postProcessIsr, but we it's not worth
        # refactoring yet, especially as they will probably diverge in the
        # future.
        dRef = quantum.outputs["calexp"][0]
        exp = self.limitedButler.get(dRef)

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
        self.watcher.redisHelper.reportFinished(self.instrument, "binnedCalexpCreation", processingId)
        self.log.info(f"Wrote binned calexp for {dRef.dataId}")

        try:  # TODO: remove the try once this is known to work
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

    def postProcessVisitSummary(self, quantum):
        dRef = quantum.outputs["visitSummary"][0]
        vs = self.limitedButler.get(dRef)
        (expRecord,) = self.butler.registry.queryDimensionRecords("exposure", dataId=dRef.dataId)

        pixToArcseconds = np.nanmean([row.wcs.getPixelScale().asArcseconds() for row in vs])
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
        try:  # TODO: remove the try once this is known to work
            self.consDBPopulator.populateVisitRow(vs, self.instrument)
            self.log.info(f"Populated consDB visit row for {expRecord.id}")
        except Exception:
            self.log.exception("Failed to populate visit row in ConsDB")

    def clobber(self, object, datasetType, visitDataId):
        """Put object in the butler.

        If there is one already there, remove it beforehand.

        Parameters
        ----------
        object : `object`
            Any object to put in the butler.
        datasetType : `str`
            Dataset type name to put it as.
        visitDataId : `lsst.daf.butler.DataCoordinate`
            The data coordinate record of the exposure to put. Must contain the
            visit id.
        """
        self.butler.registry.registerRun(self.outputRunName)
        if butlerUtils.datasetExists(self.butler, datasetType, visitDataId):
            self.log.warning(f"Overwriting existing {datasetType} for {visitDataId}")
            dRef = self.butler.registry.findDataset(datasetType, visitDataId)
            self.butler.pruneDatasets([dRef], disassociate=True, unstore=True, purge=True)
        self.butler.put(object, datasetType, dataId=visitDataId, run=self.outputRunName)
        self.log.info(f"Put {datasetType} for {visitDataId}")
