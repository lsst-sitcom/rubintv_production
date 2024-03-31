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
import io

import lsst.summit.utils.butlerUtils as butlerUtils

from lsst.utils.iteration import ensure_iterable

from lsst.pipe.base import Pipeline, PipelineGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.caching_limited_butler import CachingLimitedButler
from lsst.pipe.tasks.postprocess import ConsolidateVisitSummaryTask
from lsst.ctrl.mpexec import SingleQuantumExecutor, TaskFactory

from .utils import raiseIf
from .baseChannels import BaseButlerChannel
from .slac.mosaicing import writeBinnedImage
from .payloads import pipelineGraphToBytes

__all__ = [
    'SingleCorePipelineRunner',
]

# TODO: post OR3 maybe get this from the pipeline graph?
# TODO: post OR3 add something to allow us to trigger steps based on time or
# something that allows us to still run when a task fails. Could even maybe
# just be a finally block in the quantum execution code to allow us to count
# fails. Only downside there is that it won't be robust to OOM kills.

# record when these tasks finish per-quantum so we can trigger off the counts
TASK_ENDPOINTS_TO_TRACK = (
    'lsst.ip.isr.isrTask.IsrTask',  # for focal plane mosaics
    'lsst.pipe.tasks.calibrate.CalibrateTask',  # end of step1 for quickLook pipeline
    'lsst.pipe.tasks.postprocess.TransformSourceTableTask',  # end of step1 for nightly pipeline
    'lsst.pipe.tasks.postprocess.ConsolidateVisitSummaryTask',  # end of step2a for quickLook pipeline
    'lsst.analysis.tools.tasks.refCatSourceAnalysis.RefCatSourceAnalysisTask',  # end of step2a for nightly
)


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
    detector : `int`
        The detector number.
    embargo : `bool`, optional
        Use the embargo repo?
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
        doRaise=False
    ):
        super().__init__(locationConfig=locationConfig,
                         instrument=instrument,
                         # writeable true is required to define visits
                         butler=butler,
                         watcherType='redis',  # XXX remove this hard coding
                         detectors=ensure_iterable([]),  # XXX deal with this on the base class
                         dataProduct=awaitsDataProduct,  # XXX consider renaming this on baseclass post OR3
                         channelName='auxtel_calibrateCcd',  # XXX really the init should take an Uploader
                         queueName=queueName,
                         doRaise=doRaise,
                         )
        self.instrument = instrument
        self.butler = butler
        self.pipeline = pipeline + f"#{step}"
        self.pipelineGraph = Pipeline.fromFile(self.pipeline).to_graph(registry=self.butler.registry)
        self.pipelineGraphBytes = pipelineGraphToBytes(self.pipelineGraph)

        self.runCollection = None
        self.limitedButler = self.makeLimitedButler(butler)
        self.log.info(f"Pipeline running configured to consume from {queueName}")

    def makeLimitedButler(self, butler):
        cachedOnGet = set()
        cachedOnPut = {'whatever_the_json_object_ends up being called'}  # XXX update this!
        for name in self.pipelineGraph.dataset_types.keys():
            if self.pipelineGraph.consumers_of(name):
                if self.pipelineGraph.producer_of(name) is not None:
                    cachedOnPut.add(name)
                else:
                    cachedOnGet.add(name)

        noCopyOnCache = ('bias', 'dark', 'flat', 'defects', 'camera')
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
        # XXX add any necessary data-drive logic here to choose if we process
        return True

    def callback(self, payload):
        """Method called on each new expRecord as it is found in the repo.

        Runs on the quickLookExp and writes shards with various measured
        quantities, as calculated by the CharacterizeImageTask and
        CalibrateTask.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record.
        """
        # XXX dataId, awaitsDatasetTypeName will both be in the payload so
        # unpack here.
        dataId = payload.dataId
        pipelineGraphBytes = payload.pipelineGraphBytes
        self.runCollection = payload.run

        if not self.doProcessImage(dataId):
            return

        try:
            if pipelineGraphBytes is not None and pipelineGraphBytes != self.pipelineGraphBytes:
                self.log.warning('Pipeline graph has changed, updating')
                with io.BytesIO(pipelineGraphBytes) as f:
                    self.pipelineGraph = PipelineGraph._read_stream(f)  # to be public soon
                    self.pipelineGraphBytes = pipelineGraphBytes
                    self.limitedButler = self.makeLimitedButler(self.butler)

            where = " AND ".join(f'{k}=_{k}' for k in dataId.mapping)
            bind = {f'_{k}': v for k, v in dataId.mapping.items()}
            builder = AllDimensionsQuantumGraphBuilder(
                self.pipelineGraph,
                self.butler,
                where=where,
                bind=bind,
                clobber=True,
                input_collections=self.butler.collections + (self.runCollection, ),
                output_run=self.runCollection,
            )

            self.log.info(f'Running pipeline for {dataId}')
            # this does the waiting, but stays in the cache so don't even catch
            # the return XXX this needs to be made more generic to deal with
            # step2
            # 1) it needs to wait for other data products, depending on what
            #    the pipeline is, and
            # 2) it needs to do the same thing the bot testing code does of
            #    waiting up to the nominal timeout and then moving on with what
            #    it got

            # this waits for it to land and caches on the butler but don't
            # bother to even catch it. Then check for empty qg and raise is
            # that is the case.
            self._waitForDataProduct(dataId, gettingButler=self.limitedButler)

            qg = builder.build(
                metadata={
                    "input": self.butler.collections + (self.runCollection, ),
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
                clobberOutputs=True,  # XXX think about what to do wrt clobbering
            )

            for node in qg:
                # XXX can also add timing info here
                self.log.info(f'Starting to process {node.taskDef}')
                quantum = executor.execute(node.taskDef, node.quantum)
                self.postProcessQuantum(quantum)

                # don't track all the intermediate tasks, only points used
                # for triggering other workflows
                if quantum.taskName in TASK_ENDPOINTS_TO_TRACK:
                    expId = payload.dataId['exposure']  # this works for step1 and step2a
                    self.watcher.redisHelper.reportFinished(self.instrument, quantum.taskName, expId)

            # XXX put the visit info summary stuff inside the pipeline itself
            # and then the rollup over detectors in a gather-type process.

        except Exception as e:
            raiseIf(self.doRaise, e, self.log)

    def postProcessQuantum(self, quantum):
        """Write shards here, make sure to keep these bits quick!

        Also, anything you self.limitedButler.get() make sure to add to
        cache_on_put.
        """
        match quantum.taskName:
            case 'lsst.ip.isr.isrTask.IsrTask':
                self.postProcessIsr(quantum)
            case _:
                # can match here and do fancy dispatch
                return

    def postProcessIsr(self, quantum):
        dRef = quantum.outputs['postISRCCD'][0]
        exp = self.limitedButler.get(dRef)

        writeBinnedImage(
            exp=exp,
            instrument=self.instrument,
            outputPath=self.locationConfig.calculatedDataPath,
            binSize=self.locationConfig.binning
        )
        # XXX This MUST be replaced with a redis based signal post OR3!
        # TODO: reviewer, do not let Merlin get away with this!)
        # expId = dRef.dataId.id ??
        # class FakeExpRecord:
        #     dataId = dRef.dataId
        # writeDataIdFile(self.locationConfig.dataIdScanPath, 'binnedImage', expRecord)

        self.log.info(f'Wrote binned image for {dRef.dataId}')

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
            self.log.warning(f'Overwriting existing {datasetType} for {visitDataId}')
            dRef = self.butler.registry.findDataset(datasetType, visitDataId)
            self.butler.pruneDatasets([dRef], disassociate=True, unstore=True, purge=True)
        self.butler.put(object, datasetType, dataId=visitDataId, run=self.outputRunName)
        self.log.info(f'Put {datasetType} for {visitDataId}')

    def putVisitSummary(self, visitId):
        """Create and butler.put the visitSummary for this visit.

        Note that this only works like this while we have a single detector.

        Note: the whole method takes ~0.25s so it is probably not worth
        cluttering the class with the ConsolidateVisitSummaryTask at this
        point, though it could be done.

        Parameters
        ----------
        visitId : `lsst.daf.butler.DataCoordinate`
            The visit id to create and put the visitSummary for.
        """
        dRefs = list(self.butler.registry.queryDatasets('calexp',
                                                        dataId=visitId,
                                                        collections=self.outputRunName).expanded())
        if len(dRefs) != 1:
            raise RuntimeError(f'Found {len(dRefs)} calexps for {visitId} and it should have exactly 1')

        ddRef = self.butler.getDirectDeferred(dRefs[0])
        visit = ddRef.dataId.byName()['visit']  # this is a raw int
        consolidateTask = ConsolidateVisitSummaryTask()  # if this ctor is slow move to class
        expCatalog = consolidateTask._combineExposureMetadata(visit, [ddRef])
        self.clobber(expCatalog, 'visitSummary', visitId)
        return
