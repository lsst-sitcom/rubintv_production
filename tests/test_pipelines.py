# This file is part of summit_utils.
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

"""Test cases for utils."""
import logging
import unittest
from contextlib import contextmanager
from typing import Iterator

import lsst.utils.tests
from lsst.daf.butler import Butler, DimensionRecord
from lsst.pipe.base.quantum_graph import PredictedQuantumGraph
from lsst.rubintv.production.payloads import Payload
from lsst.rubintv.production.pipelineRunning import SingleCorePipelineRunner
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.processingControl import buildPipelines
from lsst.rubintv.production.utils import getAutomaticLocationConfig
from lsst.summit.utils.utils import getSite

_LOG = logging.getLogger("lsst.rubintv.production.tests.test_pipelines")


@contextmanager
def swallowLogs() -> Iterator[None]:
    root = logging.getLogger()
    oldLevel = root.level
    handlerLevels = [h.level for h in root.handlers]

    try:
        root.setLevel(logging.CRITICAL + 1)
        for h in root.handlers:
            h.setLevel(logging.CRITICAL + 1)
        yield
    finally:
        root.setLevel(oldLevel)
        for h, lvl in zip(root.handlers, handlerLevels):
            h.setLevel(lvl)


NO_BUTLER = True
if getSite() in ["staff-rsp", "rubin-devl"]:
    NO_BUTLER = False

EXPECTED_PIPELINES = [
    "BIAS",
    "DARK",
    "FLAT",
    "ISR",
    "SFM",
    "AOS_DANISH",
    "AOS_TIE",
    "AOS_REFIT_WCS",
    "AOS_AI_DONUT",
    "AOS_TARTS_UNPAIRED",
    "AOS_FAM_TIE",
    "AOS_FAM_DANISH",
    "AOS_UNPAIRED_DANISH",
]

EXPECTED_AOS_PIPELINES = [p for p in EXPECTED_PIPELINES if p.startswith("AOS")]
EXPECTED_FAM_PIPEPLINES = [p for p in EXPECTED_AOS_PIPELINES if "FAM" in p]
EXPECTED_UNPAIRED_PIPELINES = [p for p in EXPECTED_AOS_PIPELINES if "UNPAIRED" in p]
EXPECTED_AOS_NON_FAM_PIPELINES = [
    p for p in EXPECTED_AOS_PIPELINES if "FAM" not in p and "UNPAIRED" not in p and "AOS" in p
]

# TODO: still need to add step1b tests for all the other pipelines


class TestPipelineGeneration(lsst.utils.tests.TestCase):
    def setUp(self) -> None:
        self.locationConfig = getAutomaticLocationConfig()
        self.instrument = "LSSTCam"
        self.butler = Butler.from_config(
            self.locationConfig.lsstCamButlerPath,
            instrument=self.instrument,
            collections=[
                f"{self.instrument}/defaults",
                self.locationConfig.getOutputChain(self.instrument),
                "u/gmegias/intrinsic_aberrations_collection_temp",
            ],
            writeable=True,
        )
        self.graphs, self.pipelines = buildPipelines("LSSTCam", self.locationConfig, self.butler)

        for pipelineName in EXPECTED_PIPELINES:
            self.assertIn(pipelineName, self.pipelines)

        # check no unexpected pipelines either so that we're always explicit
        # that we're testing all the ones we know about.  XXX turn this back on
        # for pipelineName in self.pipelines.keys():
        #     self.assertIn(pipelineName, EXPECTED_PIPELINES,
        # f"Unexpected pipeline {pipelineName} found")

        where = "exposure.day_obs=20251115 AND exposure.seq_num in (226..228,436) AND instrument='LSSTCam'"
        records = self.butler.query_dimension_records("exposure", where=where)
        self.assertEqual(len(records), 4)
        rd = {r.seq_num: r for r in records}
        self.records: dict[str, DimensionRecord] = {}
        self.records["inFocus"] = rd[226]
        self.records["intra"] = rd[227]
        self.records["extra"] = rd[228]
        self.records["dark"] = rd[436]
        self.intraDetector = 192
        self.extraDetector = 191
        self.scienceDetector = 94
        self.podDetails = PodDetails(
            instrument="FAKE_INSTRUMENT", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=0, depth=0
        )

    def testCalibPipelines(self) -> None:
        # calib pipelines run the verify<product>Isr tasks but the quanta that
        # they actually execute are isr quanta, so check they exist with the
        # right names, but check the quanta counts under 'isr'
        for pipelineName in ["BIAS", "DARK", "FLAT"]:
            taskName = f"verify{pipelineName.lower().capitalize()}Isr"
            taskExpectations: dict[str, int] = {taskName: 1}
            quantaExpectations: dict[str, int] = {"isr": 1}
            self.runTest(
                step="step1a",
                imageType="inFocus",
                detector=self.scienceDetector,
                pipelinesToRun=[pipelineName],
                taskExpectations=taskExpectations,
                quantaExpectations=quantaExpectations,
            )

    def testIsrOnly(self) -> None:
        taskExpectations: dict[str, int] = {"isr": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.scienceDetector,
            pipelinesToRun=["ISR"],
            taskExpectations=taskExpectations,
        )

    def testAosSfmPipelinesStep1a(self) -> None:
        taskExpectations: dict[str, int] = {"isr": 1, "calibrateImage": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.scienceDetector,
            pipelinesToRun=["SFM"],
            taskExpectations=taskExpectations,
        )

    def testCalibsPipeline(self) -> None:
        taskExpectations: dict[str, int] = {"isr": 1}
        self.runTest(
            step="step1a",
            imageType="dark",
            detector=self.scienceDetector,
            pipelinesToRun=["ISR"],
            taskExpectations=taskExpectations,
        )

    def testAosFamPipelinesStep1aExtraFocal(self) -> None:
        taskExpectations: dict[str, int] = {"isr": 1, "calcZernikes": 1}
        self.runTest(
            step="step1a",
            imageType="extra",
            detector=self.scienceDetector,
            pipelinesToRun=EXPECTED_FAM_PIPEPLINES,
            taskExpectations=taskExpectations,
        )

    def testAosFamPipelinesStep1aIntraFocal(self) -> None:
        # unpaired intra should have no calcZernikes
        taskExpectations: dict[str, int] = {"isr": 1, "calcZernikes": 0}
        self.runTest(
            step="step1a",
            imageType="intra",
            detector=self.scienceDetector,
            pipelinesToRun=EXPECTED_FAM_PIPEPLINES,
            taskExpectations=taskExpectations,
        )

    def testAosRegularPipelines(self) -> None:
        taskExpectationsExtra: dict[str, int] = {"isr": 1, "calcZernikes": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.extraDetector,
            pipelinesToRun=EXPECTED_AOS_NON_FAM_PIPELINES,
            taskExpectations=taskExpectationsExtra,
        )

        # no calcZernikes for intrafocal for unpaired pipelines
        taskExpectationsIntra: dict[str, int] = {"isr": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.intraDetector,
            pipelinesToRun=EXPECTED_AOS_NON_FAM_PIPELINES,
            taskExpectations=taskExpectationsIntra,
        )

    def testAosRegularUnpairedPipelines(self) -> None:
        taskExpectationsExtra: dict[str, int] = {"isr": 1, "calcZernikes": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.extraDetector,
            pipelinesToRun=EXPECTED_UNPAIRED_PIPELINES,
            taskExpectations=taskExpectationsExtra,
        )

        # calcZernikes *is* expected for intra detectors for unpaired pipelines
        taskExpectationsIntra: dict[str, int] = {"isr": 1, "calcZernikes": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.intraDetector,
            pipelinesToRun=EXPECTED_UNPAIRED_PIPELINES,
            taskExpectations=taskExpectationsIntra,
        )

    def testAosRegularPipelinesStep1b(self) -> None:
        taskExpectations: dict[str, int] = {"plotAOSTask": 1}
        self.runTest(
            step="step1b",
            imageType="inFocus",
            pipelinesToRun=EXPECTED_AOS_NON_FAM_PIPELINES,
            taskExpectations=taskExpectations,
        )

    def testRaisingNonFAM(self) -> None:
        for pipeline in EXPECTED_AOS_NON_FAM_PIPELINES:
            # all detectors should fail for intra + extra images for non-FAM
            for imageType in ["intra", "extra"]:
                for detector in [self.intraDetector, self.extraDetector, self.scienceDetector]:
                    failingToFailMsg = f"Failed to raise for {pipeline=}, {imageType=}, {detector=}"
                    with self.assertRaises(ValueError, msg=failingToFailMsg):
                        self.runTest(
                            step="step1a",
                            imageType=imageType,
                            detector=detector,
                            pipelinesToRun=[pipeline],
                            taskExpectations={},
                        )

    def testRaisingFAM(self) -> None:
        for pipeline in EXPECTED_FAM_PIPEPLINES:
            # all images should fail for all corner chips for FAM
            for imageType in ["inFocus", "intra", "extra"]:
                for detector in [self.intraDetector, self.extraDetector]:
                    failingToFailMsg = f"Failed to raise for {pipeline=}, {imageType=}, {detector=}"
                    with self.assertRaises(ValueError, msg=failingToFailMsg):
                        self.runTest(
                            step="step1a",
                            imageType=imageType,
                            detector=detector,
                            pipelinesToRun=[pipeline],
                            taskExpectations={},
                        )

    def runTest(
        self,
        *,
        step: str,
        imageType: str,
        pipelinesToRun: list[str],
        detector: int | None = None,
        taskExpectations: dict[str, int] | None = None,
        quantaExpectations: dict[str, int] | None = None,
    ) -> None:
        taskExpectations = taskExpectations or {}
        quantaExpectations = quantaExpectations or taskExpectations
        if step == "step1a":
            dataCoord = self.butler.registry.expandDataId(
                exposure=self.records[imageType].id,
                detector=detector,
                instrument=self.instrument,
            )
        elif step == "step1b":
            dataCoord = self.butler.registry.expandDataId(
                visit=self.records[imageType].id,
                instrument=self.instrument,
            )
        else:
            raise ValueError(f"Unknown step {step}")

        with swallowLogs():
            for pipelineName in pipelinesToRun:
                extraInfo = f"{imageType=} in {step} with {detector=} running {pipelineName}"
                print(f"Checking {pipelineName}:{step} with {dataCoord}, expecting {taskExpectations}")
                self.assertIn(pipelineName, self.pipelines, f"Pipeline {pipelineName} not found")

                graph = self.pipelines[pipelineName].graphs[step]

                sfmRunner = SingleCorePipelineRunner(
                    butler=self.butler,
                    locationConfig=self.locationConfig,
                    instrument=self.instrument,
                    step=step,
                    awaitsDataProduct="raw" if step == "step1a" else None,
                    podDetails=self.podDetails,
                    doRaise=False,
                )
                payload = Payload(dataCoord, b"", "LSSTCam/runs/quickLookTesting/126", who="AOS")
                payload = Payload.from_json(payload.to_json(), self.butler)  # fully formed
                sfmRunner.runCollection = payload.run
                qgb, _, _, _ = sfmRunner.getQuantumGraphBuilder(payload, graph)
                qg = qgb.finish().assemble()
                self.assertIsInstance(qg, PredictedQuantumGraph)

                taskNames = list(qg.quanta_by_task.keys())
                # Check that all expected tasks are present
                for taskSubStringToExpect in taskExpectations.keys():
                    taskSubStringToExpect = taskSubStringToExpect.lower()
                    foundTask = False
                    for taskName in taskNames:
                        taskNameLower = taskName.lower()
                        if taskSubStringToExpect in taskNameLower:
                            foundTask = True
                            break
                    self.assertTrue(
                        foundTask,
                        f"Expected task containing '{taskSubStringToExpect}' not found in {taskNames}",
                    )

                # Check that expected tasks have the correct number of quanta
                for taskSubStringToExpect, numTasksToExpectForString in taskExpectations.items():
                    taskSubStringToExpect = taskSubStringToExpect.lower()
                    for taskName in taskNames:
                        taskNameLower = taskName.lower()
                        if taskSubStringToExpect in taskNameLower:
                            self.assertEqual(
                                len(qg.quanta_by_task[taskName]),
                                numTasksToExpectForString,
                                (
                                    f"Task '{taskName}' has {len(qg.quanta_by_task[taskName])} quanta,"
                                    f" expected {numTasksToExpectForString} in pipeline {pipelineName}"
                                    f" for {extraInfo}. Found tasks: {taskNames}"
                                ),
                            )

                executionQuanta = qg.build_execution_quanta()
                self.assertIsInstance(executionQuanta, dict)

                executionQuanta = qg.build_execution_quanta()

                # quantaTaskList deliberately may contain duplicates
                quantaTaskList = [
                    q.taskName.lower() for q in executionQuanta.values() if q.taskName is not None
                ]
                for taskSubStringToExpect, numTasksToExpectForString in quantaExpectations.items():
                    taskSubStringToExpect = taskSubStringToExpect.lower()
                    count = sum(1 for t in quantaTaskList if taskSubStringToExpect in t)
                    self.assertEqual(
                        count,
                        numTasksToExpectForString,
                        (
                            f"Execution quanta: Task containing '{taskSubStringToExpect}' has"
                            f" {count} quanta, expected {numTasksToExpectForString} for {extraInfo}."
                            f" Found tasks: {quantaTaskList}"
                        ),
                    )


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
