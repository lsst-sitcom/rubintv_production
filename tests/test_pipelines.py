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
    # "BIAS",
    # "DARK",
    # "FLAT",
    # "ISR",
    "SFM",
    "AOS_DANISH",
    # "AOS_TIE",
    # "AOS_REFIT_WCS",
    # "AOS_AI_DONUT",
    # "AOS_TARTS",
    # "AOS_FAM_TIE",
    "AOS_FAM_DANISH",
    # "AOS_UNPAIRED_DANISH",
]

EXPECTED_AOS_PIPELINES = [p for p in EXPECTED_PIPELINES if p.startswith("AOS")]
EXPECTED_FAM_PIPEPLINES = [p for p in EXPECTED_AOS_PIPELINES if "FAM" in p]
EXPECTED_UNPAIRED_PIPELINES = [p for p in EXPECTED_AOS_PIPELINES if "UNPAIRED" in p]


class TestPipelineGeneration(lsst.utils.tests.TestCase):
    @unittest.skipIf(NO_BUTLER, "Skipping butler-driven tests")
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

    def testAosFamPipelinesStep1a(self) -> None:
        step = "step1a"
        dataCoord = self.butler.registry.expandDataId(
            exposure=self.records["extra"].id,
            detector=self.scienceDetector,
            instrument=self.instrument,
        )
        with swallowLogs():
            for pipelineName in EXPECTED_FAM_PIPEPLINES:
                print(f"Checking pipeline {pipelineName} with {dataCoord}")
                self.assertIn(pipelineName, self.pipelines)

                graph = self.pipelines["AOS_FAM_DANISH"].graphs[step]

                sfmRunner = SingleCorePipelineRunner(
                    butler=self.butler,
                    locationConfig=self.locationConfig,
                    instrument=self.instrument,
                    step=step,
                    awaitsDataProduct="raw",
                    podDetails=self.podDetails,
                    doRaise=True,
                )
                payload = Payload(dataCoord, b"", "LSSTCam/runs/quickLookTesting/126", who="AOS")
                payload = Payload.from_json(payload.to_json(), self.butler)  # fully formed
                sfmRunner.runCollection = payload.run
                qgb, _, _, _ = sfmRunner.getQuantumGraphBuilder(payload, graph)
                qg = qgb.finish().assemble()
                self.assertIsInstance(qg, PredictedQuantumGraph)
                self.assertEqual(len(qg.quanta_by_task["calcZernikesTask"]), 1)

                quanta = qg.build_execution_quanta()
                self.assertIsInstance(quanta, dict)
                self.assertGreaterEqual(len(quanta), 1)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
