import atexit
import io
import itertools
import logging
import multiprocessing
import os
import shutil
import signal
import subprocess
import sys
import time
import traceback
from multiprocessing import Manager, Process
from pathlib import Path
from queue import Empty
from unittest.mock import patch

import redis
import yaml


# Disable logging.basicConfig to avoid interference with log capture
def do_nothing(*args, **kwargs):
    pass


logging.basicConfig = do_nothing

# these imports need to come after the log patching
from lsst.daf.butler.cli.cliLog import CliLog  # noqa: E402

# Initialize once and disable to prevent interference with log capture
CliLog.initLog(False)
CliLog.initLog = do_nothing  # type: ignore

# Import test utilities
from ciutils import Check, TestScript, conditional_redirect  # type: ignore # noqa: E402

from lsst.rubintv.production.redisUtils import RedisHelper  # noqa: E402

# Only import from lsst packages after logging is configured
from lsst.rubintv.production.utils import LocationConfig, getDoRaise, runningCI  # noqa: E402


class TestConfig:
    """Centralized configuration for the test suite."""

    def __init__(self) -> None:
        # Test execution settings
        self.do_run_meta_tests = True
        self.do_check_yaml_files = True
        self.copy_plots_to_public_html = True
        self.debug = False

        # Redis settings
        self.redis_host = "127.0.0.1"
        self.redis_port = "6111"
        self.redis_password = "redis_password"
        self.redis_init_wait_time = 3
        self.capture_redis_output = True

        # Test durations
        self.meta_test_duration = 30
        self.test_duration = 400

        # Date for testing
        self.today = 20240101

        # File paths
        self.ci_dir = os.path.dirname(os.path.abspath(__file__))
        self.package_dir = os.path.abspath(os.path.join(self.ci_dir, "../../"))

        # Initialize test scripts and yaml files
        self._init_test_scripts()
        self._init_yaml_files()

    def _init_test_scripts(self) -> None:
        """Initialize test script definitions."""
        # LATISS pods
        # latiss_scripts = [
        #     TestScript(
        #         "scripts/LATISS/runHeadNode.py",
        #         ["usdf_testing"],
        #         display_on_pass=True,
        #         tee_output=False,
        #     ),
        #     TestScript(
        #         "scripts/LATISS/runSfmRunner.py",
        #         ["usdf_testing", "0"],
        #         display_on_pass=True,
        #         tee_output=False,
        #     ),
        #     TestScript(
        #         "scripts/LATISS/runStep1bRunner.py",
        #         ["usdf_testing", "0"],
        #         tee_output=False,
        #     ),
        #     TestScript(
        #         "scripts/LATISS/runOneOffExpRecord.py",
        #         ["usdf_testing"],
        #         display_on_pass=False,
        #     ),
        #     TestScript(
        #         "scripts/LATISS/runOneOffPostIsr.py",
        #         ["usdf_testing"],
        #         display_on_pass=False,
        #     ),
        # ]

        # LSSTCam pods
        lsstcam_scripts = [
            TestScript(
                "scripts/LSSTCam/runPlotter.py",
                ["usdf_testing"],
                display_on_pass=False,
                tee_output=False,
            ),
            TestScript(
                "scripts/LSSTCam/runFWHMPlotting.py",
                ["usdf_testing"],
                display_on_pass=False,
                tee_output=False,
            ),
            TestScript(
                "scripts/LSSTCam/runRadialPlotting.py",
                ["usdf_testing"],
                display_on_pass=False,
                tee_output=False,
            ),
            TestScript(
                "scripts/LSSTCam/runPsfPlotting.py",
                ["usdf_testing"],
                display_on_pass=False,
                tee_output=False,
            ),
            TestScript(
                "scripts/LSSTCam/runStep1bWorker.py",
                ["usdf_testing", "0"],
                display_on_pass=True,
                tee_output=False,
            ),
            TestScript("scripts/LSSTCam/runNightlyWorker.py", ["usdf_testing", "0"], tee_output=False),
        ]

        # SFM Runners for LSSTCam
        # these do both the in-focus image and the FAM pair
        detectorNumbers = [
            90,
            91,
            92,
            93,
            94,
            95,
            96,
            97,
            98,
            144,
            145,
            146,
            147,
            148,
            149,
            150,
            151,
            152,
        ]
        sfm_runners = [
            TestScript(
                "scripts/LSSTCam/runSfmRunner.py",
                ["usdf_testing", str(detectorNumbers[0])],
                display_on_pass=True,
                tee_output=False,
            )
        ]
        sfm_runners.extend(
            [
                TestScript("scripts/LSSTCam/runSfmRunner.py", ["usdf_testing", str(i)])
                for i in detectorNumbers[1:]
            ]
        )

        # AOS Workers for LSSTCam
        # these do the corner chips, and the run as dual dataIds on the
        # split chips, so need only 4
        # XXX TODO: bugfix the need to use odd numbers! fix AOS_WORKER_MAPPING
        aos_workers = [
            TestScript(
                "scripts/LSSTCam/runAosWorker.py",
                ["usdf_testing", "1"],
                display_on_pass=True,
                tee_output=True,
            )
        ]
        aos_workers.extend(
            [TestScript("scripts/LSSTCam/runAosWorker.py", ["usdf_testing", str(i)]) for i in [3, 5, 7]]
        )

        # Additional LSSTCam scripts
        # runStep1baAosWorker deals with the outputs from the regular CWFS and
        # the FAM output, so make 3
        additional_lsstcam_scripts = [
            TestScript(
                "scripts/LSSTCam/runStep1bAosWorker.py",
                ["usdf_testing", "0", "1", "2"],
                display_on_pass=True,
            ),
            TestScript(
                "scripts/LSSTCam/runOneOffExpRecord.py",
                ["usdf_testing"],
                tee_output=False,
                display_on_pass=True,
            ),
            TestScript(
                "scripts/LSSTCam/runOneOffPostIsr.py",
                ["usdf_testing"],
                tee_output=False,
                display_on_pass=False,
            ),
            TestScript(
                "scripts/LSSTCam/runOneOffVisitImage.py",
                ["usdf_testing"],
                tee_output=False,
                display_on_pass=False,
            ),
            TestScript(
                "scripts/LSSTCam/runHeadNode.py",
                ["usdf_testing"],
                delay=5,
                tee_output=True,
                display_on_pass=True,
            ),
            TestScript("tests/ci/drip_feed_data.py", ["usdf_testing"], delay=0, display_on_pass=False),
        ]

        # Combine all test scripts
        self.test_scripts_round_1 = (
            latiss_scripts + lsstcam_scripts + sfm_runners + aos_workers + additional_lsstcam_scripts
        )

        # Scripts to run after processing pods are torn down
        self.test_scripts_round_3 = [
            TestScript("scripts/LSSTCam/runButlerWatcher.py", ["usdf_testing"]),
        ]

        # Meta tests that are expected to fail
        self.meta_tests_fail_expected = [
            TestScript("meta_test_raise.py"),
            TestScript("meta_test_sys_exit_non_zero.py"),
        ]

        # Meta tests that are expected to pass
        self.meta_tests_pass_expected = [
            TestScript("meta_test_runs_ok.py"),
            TestScript("meta_test_debug_config.py", do_debug=True),
            TestScript("meta_test_patching.py"),
            TestScript("meta_test_env.py"),
            TestScript("meta_test_s3_upload.py"),
            TestScript("meta_test_logging_capture.py"),
            TestScript("meta_test_logging_capture.py", tee_output=True),
        ]

        # Convert relative paths to absolute paths
        self._resolve_paths()

    def _resolve_paths(self) -> None:
        """Convert relative script paths to absolute paths."""
        self.test_scripts_round_1 = [
            TestScript.from_existing(script, os.path.join(self.package_dir, script.path))
            for script in self.test_scripts_round_1
        ]

        self.test_scripts_round_3 = [
            TestScript.from_existing(script, os.path.join(self.package_dir, script.path))
            for script in self.test_scripts_round_3
        ]

        self.meta_tests_fail_expected = [
            TestScript.from_existing(script, os.path.join(self.ci_dir, script.path))
            for script in self.meta_tests_fail_expected
        ]

        self.meta_tests_pass_expected = [
            TestScript.from_existing(script, os.path.join(self.ci_dir, script.path))
            for script in self.meta_tests_pass_expected
        ]

    def _init_yaml_files(self) -> None:
        """Initialize YAML file paths for configuration checks."""
        self.yaml_files_to_check = [
            "config/config_bts.yaml",
            "config/config_tts.yaml",
            "config/config_summit.yaml",
            "config/config_usdf_testing.yaml",
            "config/config_usdf.yaml",
        ]

        # Convert to absolute paths
        self.yaml_files_to_check = [os.path.join(self.package_dir, file) for file in self.yaml_files_to_check]


class RedisManager:
    """Manages Redis server operations."""

    def __init__(self, config: TestConfig) -> None:
        self.config = config
        self.redis_process = None

    def is_redis_running(self) -> bool:
        """Check if redis-server is already running."""
        try:
            # Run pgrep to find redis-server processes
            result = subprocess.run(["pgrep", "-f", "redis-server"], capture_output=True, text=True)
            # Get process IDs if any
            redis_pids = result.stdout.strip().split("\n") if result.stdout.strip() else []
            return bool(redis_pids and redis_pids[0])
        except Exception as e:
            print(f"Error checking Redis process: {e}")
            return False

    def start(self) -> None:
        """Start the Redis server."""
        host = self.config.redis_host
        port = self.config.redis_port
        password = self.config.redis_password

        # Check if Redis is already running
        if self.is_redis_running():
            raise RuntimeError("Redis server is already running. Cannot start another instance.")

        # Set environment variables
        os.environ["REDIS_HOST"] = host
        os.environ["REDIS_PORT"] = port
        os.environ["REDIS_PASSWORD"] = password

        capture_kwargs = {}
        if self.config.capture_redis_output:
            capture_kwargs["stdout"] = subprocess.PIPE
            capture_kwargs["stderr"] = subprocess.PIPE

        print(f"Starting Redis on {host}:{port}")
        self.redis_process = subprocess.Popen(
            ["redis-server", "--port", port, "--bind", host, "--requirepass", password], **capture_kwargs
        )  # type: ignore[call-overload]
        assert self.redis_process is not None
        print(f"✅ Redis server started on {host}:{port} with PID: {self.redis_process.pid}")

        # Wait for Redis to initialize
        wait_time = self.config.redis_init_wait_time
        print(f"Waiting for {wait_time}s to let Redis startup finish")
        time.sleep(wait_time)

        self.clear_database()

    def clear_database(self) -> None:
        """Clear the Redis database."""
        r = redis.Redis(
            host=self.config.redis_host, port=int(self.config.redis_port), password=self.config.redis_password
        )
        r.flushall()
        print("Cleared Redis database")

    def check_connection(self) -> None:
        """Verify Redis connection is working properly."""
        host = self.config.redis_host
        port = self.config.redis_port
        password = self.config.redis_password

        r = redis.Redis(host=host, port=int(port), password=password)

        # Ping Redis
        if not r.ping():
            raise RuntimeError("Could not ping Redis")

        # Set and read back a test key
        r.set("test_key", "test_value")
        result = r.get("test_key")
        if result is None:
            raise RuntimeError("Could not retrieve test key from Redis")
        value = result.decode("utf-8")
        if value != "test_value":
            raise RuntimeError("Could not set and read back a test key in Redis")

        r.flushall()  # Clear the database
        print("✅ Successfully pinged Redis and set/read back a test key")

    def check_final_contents(self, checks: list[Check]) -> None:
        """Check Redis contents after test execution."""
        # no need for a butler or location config when just monitoring
        # so ignore arg types for the helper init
        redisHelper = RedisHelper(None, None)  # type: ignore[arg-type]
        redisHelper.displayRedisContents()

        # Check LSSTCam data
        self._check_lsstcam_data(redisHelper, checks)

        # Check LATISS data
        self._check_latiss_data(redisHelper, checks)

        # Check for failure keys
        self._check_failure_keys(redisHelper, checks)

    def _check_lsstcam_data(self, redisHelper: RedisHelper, checks: list[Check]) -> None:
        """Check LSSTCam data in Redis."""
        inst = "LSSTCam"

        visits_sfm = [2025041500088]
        visits_aos = ["2025041500088", "20250415000086+20250415000087"]

        n_visits_sfm = len(visits_sfm)
        n_visits_aos = len(visits_aos)

        # Check SFM step1b
        n_step1b_sfm = redisHelper.getNumVisitLevelFinished(inst, "step1b", "SFM")
        if n_step1b_sfm != n_visits_sfm:
            checks.append(
                Check(
                    False,
                    f"Expected {n_visits_sfm} SFM step1b for {inst} to have finished, got {n_step1b_sfm}",
                )
            )
        else:
            checks.append(Check(True, f"{n_step1b_sfm}x {inst} SFM step1b finished"))

        # Check AOS step1b
        n_step1b_aos = redisHelper.getNumVisitLevelFinished(inst, "step1b", "AOS")
        if n_visits_aos != n_step1b_aos:
            checks.append(
                Check(False, f"Expected {n_visits_aos} AOS step1b to have finished, got {n_step1b_aos}")
            )
        else:
            checks.append(Check(True, f"{n_visits_aos}x AOS step1b finished"))

        # Check nightly rollup
        key = f"{inst}-SFM-NIGHTLYROLLUP-FINISHEDCOUNTER"
        n_nightly_rollups = int(redisHelper.redis.get(key) or 0)
        if n_nightly_rollups != 1:
            checks.append(Check(False, f"Expected 1 nightly rollup finished, got {n_nightly_rollups}"))
        else:
            checks.append(Check(True, f"{n_nightly_rollups}x nightly rollup finished"))

    def _check_latiss_data(self, redisHelper: RedisHelper, checks: list[Check]) -> None:
        """Check LATISS data in Redis."""
        inst = "LATISS"

        visits_sfm = [2024081300632]
        n_visits_sfm = len(visits_sfm)

        n_step1b_sfm = redisHelper.getNumVisitLevelFinished(inst, "step1b", "SFM")
        if n_step1b_sfm != n_visits_sfm:
            checks.append(
                Check(
                    False,
                    f"Expected {n_visits_sfm} SFM step1b for {inst} to have finished, got {n_step1b_sfm}",
                )
            )
        else:
            checks.append(Check(True, f"{n_step1b_sfm}x {inst} SFM step1b finished"))

    def _check_failure_keys(self, redisHelper: RedisHelper, checks: list[Check]) -> None:
        """Check for failure keys in Redis."""
        allKeys = redisHelper.redis.keys()
        failed_keys = [key.decode("utf-8") for key in allKeys if "FAILED" in key.decode("utf-8")]
        if failed_keys:
            checks.append(Check(False, f"Found failed keys: {failed_keys}"))
        else:
            checks.append(Check(True, "No failed keys found in Redis"))

    def terminate(self) -> None:
        """Terminate the Redis server."""
        if self.redis_process:
            self.redis_process.terminate()
            self.redis_process.wait()
            print("Terminated Redis process")


class ProcessManager:
    """Manages test script processes and output collection."""

    def __init__(self) -> None:
        self.manager = Manager()
        self.exit_codes = self.manager.dict()
        self.outputs = self.manager.dict()
        self.processes: dict[Process, TestScript] = {}

    def run_test_scripts(self, scripts: list[TestScript], timeout: int, is_meta_tests: bool = False) -> None:
        """Run test scripts with timeout and collect results."""
        start_time = time.time()
        output_queue: multiprocessing.Queue[tuple[TestScript, int | str | None, str, str, str]] = (
            multiprocessing.Queue()
        )
        reported_outputs: set[TestScript] = set()

        # Check for debug mode
        doing_debug = not is_meta_tests and any(s.do_debug for s in scripts)
        if doing_debug:
            if sum(s.do_debug for s in scripts) > 1:
                debug_attempts = [s for s in scripts if s.do_debug]
                script_string = "\n".join([str(s) for s in debug_attempts])
                err_msg = (
                    f"You can only interactively debug one script at a time! Attempted:\n{script_string}"
                )
                raise RuntimeError(err_msg)
            print("\n\n⚠️ ⚠️ INTERACTIVE SCRIPT DEBUG MODE ENABLED ⚠️ ⚠️")
            print("     tests will continue until killed manually\n\n")
            timeout = 9999999  # Keep alive indefinitely when debugging

        # Start all processes
        for script in scripts:
            p = Process(target=self._exec_script, args=(script, output_queue))
            p.start()
            self.processes[p] = script
            print(f"Launched {script} with pid={p.pid}...")

        # Main monitoring loop
        last_secs = None
        try:
            while (time_remaining := (timeout - (time.time() - start_time))) > 0:
                # Collect outputs from the queue
                self._collect_outputs_from_queue(output_queue, reported_outputs)

                # Update timer display
                mins, secs = divmod(time_remaining, 60)
                if int(secs) != last_secs:
                    last_secs = int(secs)
                    timer = (
                        f"{int(mins):02d}:{int(secs):02d} remaining" if time_remaining > 0 else "time's up"
                    )
                    end = "\r" if time_remaining > 0 else "\n"
                    n_alive = sum([p.is_alive() for p in self.processes])
                    print(f"{timer} with {n_alive} processes running", end=end)
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Interrupted by user, terminating processes...")

        # Process termination and cleanup
        print("\nTime's up or interrupted. Collecting remaining outputs before termination...")
        self._terminate_processes(output_queue, reported_outputs)

        # Check for any missing script outputs
        self._check_missing_outputs(scripts, reported_outputs)

    def _exec_script(self, test_script: TestScript, output_queue) -> None:
        """Execute a test script in a separate process and capture output."""

        def termination_handler(signum, frame) -> None:
            print("Termination signal received, exiting...")
            raise KeyboardInterrupt()

        signal.signal(signal.SIGTERM, termination_handler)

        script_path = test_script.path
        script_args = test_script.args

        f_stdout = io.StringIO()
        f_stderr = io.StringIO()
        log_stream = io.StringIO()

        # Setup log capture
        log_handler = logging.StreamHandler(log_stream)
        log_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))

        # Add handler to the root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)
        root_logger.setLevel(logging.INFO)

        exit_code: str | int | None = None
        original_stdout = sys.stdout
        original_stderr = sys.stderr

        # Set up debugging if needed
        lsstDebug = None
        if test_script.do_debug:
            import ciutils  # type: ignore
            import lsstDebug  # type: ignore

            def getConnection():
                return {"port": 4444, "addr": "127.0.0.1"}

            ciutils.getConnection = getConnection

        try:
            with conditional_redirect(test_script.tee_output, f_stdout, f_stderr, log_handler, root_logger):
                with patch("lsst.summit.utils.utils.getCurrentDayObs_int", return_value=20240101):
                    with open(script_path, "r") as script_file:
                        script_content = script_file.read()
                    exec_globals = {
                        "__name__": "__main__",
                        "__file__": script_path,
                        "sys": sys,
                        "logging": logging,
                        "lsst.daf.butler.cli.cliLog": CliLog,
                        "CliLog": CliLog,
                        "lsstDebug": lsstDebug,
                    }
                    sys.argv = [script_path] + script_args if script_args else [script_path]
                    time.sleep(test_script.delay)
                    exec(script_content, exec_globals)
                    exit_code = 0
        except Exception:
            traceback.print_exc(file=f_stderr)
            exit_code = 1
        except SystemExit as e:
            logging.info(f"Script exited with status: {e}")
            exit_code = e.code if e.code is not None else 0
        except KeyboardInterrupt:
            exit_code = "timeout"
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr

            log_output = log_stream.getvalue()
            output_queue.put((test_script, exit_code, f_stdout.getvalue(), f_stderr.getvalue(), log_output))

            # Clean up logger handlers
            root_logger.removeHandler(log_handler)
            log_handler.close()

    def _collect_outputs_from_queue(self, queue, reported_outputs, timeout=0) -> int:
        """Collect outputs from the queue without blocking indefinitely."""
        collected = 0
        try:
            while True:
                try:
                    # Use a small timeout to avoid blocking forever
                    script, exit_code, stdout, stderr, logs = queue.get(timeout=timeout)
                    self.exit_codes[script] = exit_code
                    self.outputs[script] = (stdout, stderr, logs)
                    reported_outputs.add(script)
                    collected += 1
                except Empty:
                    break
        except Exception as e:
            print(f"Error collecting from queue: {e}")

        return collected

    def _terminate_processes(self, output_queue, reported_outputs) -> None:
        """Terminate all running processes and collect their outputs."""
        # Give processes a chance to finish naturally
        self._collect_outputs_from_queue(output_queue, reported_outputs, timeout=2)

        # Terminate remaining processes
        remaining_processes = [p for p in self.processes.keys() if p.is_alive()]
        if remaining_processes:
            print(f"Sending SIGTERM to {len(remaining_processes)} remaining processes...")
            for p in remaining_processes:
                script = self.processes[p]
                if script not in reported_outputs:
                    print(f"Terminating {script} (PID {p.pid})...")
                    p.terminate()

        # Collect outputs again
        self._collect_outputs_from_queue(output_queue, reported_outputs, timeout=2)

        # Wait for processes to terminate
        for i in range(5):
            if not any(p.is_alive() for p in self.processes):
                break

            remaining = sum(1 for p in self.processes if p.is_alive())
            if remaining > 0:
                print(f"{remaining} processes still alive, waiting {i + 1}s more...")
                time.sleep(i + 1)

            self._collect_outputs_from_queue(output_queue, reported_outputs)

        # Force kill any remaining processes
        for p in [p for p in self.processes if p.is_alive()]:
            print(f"Forcefully terminating {self.processes[p]} (PID: {p.pid})")
            if p.pid is not None:  # shoudn't be None by mypy doesn't know that
                os.kill(p.pid, signal.SIGKILL)

        # Final cleanup
        for p in self.processes:
            p.join(timeout=1)

        self._collect_outputs_from_queue(output_queue, reported_outputs, timeout=5)

    def _check_missing_outputs(self, scripts, reported_outputs) -> None:
        """Check for any scripts that didn't report output."""
        missing_scripts = set(scripts) - reported_outputs
        if missing_scripts:
            print(f"WARNING: Failed to collect outputs for {len(missing_scripts)} scripts:")
            for script in missing_scripts:
                print(f"  - {script}")
                # Set default values for missing outputs
                self.exit_codes[script] = "missing"
                self.outputs[script] = ("", f"ERROR: Failed to collect output for {script}", "")


class ResultCollector:
    """Collects and analyzes test results."""

    def __init__(self) -> None:
        self.checks: list[Check] = []

    def check_meta_test_results(
        self, process_manager: ProcessManager, scripts_fail: list[TestScript], scripts_pass: list[TestScript]
    ) -> bool:
        """Validate meta test results against expectations."""
        passed = True

        # Check failure tests
        for script in scripts_fail:
            code = process_manager.exit_codes[script]
            if code in (0, "timeout"):
                print(f"❌ Test {script} was expected to fail but returned a zero-like exit code: {code}")
                print(process_manager.outputs[script][1])
                passed = False
            else:
                print(f"✅ Test {script} passed (by failing) with exit code: {code}")

        # Check passing tests
        for script in scripts_pass:
            code = process_manager.exit_codes[script]
            if code not in (0, "timeout"):
                print(f"❌ Test {script} was expected to pass but returned a non-zero exit code: {code}")
                print(process_manager.outputs[script][1])
                passed = False
            else:
                print(f"✅ Test {script} passed with exit code: {code}")

        # Check log capture
        capture_ok = self._check_log_capture(process_manager)
        passed = passed and capture_ok

        if not passed:
            raise RuntimeError("Meta-tests did not pass as expected - fix the test suite and try again.")

        return passed

    def _check_log_capture(self, process_manager: ProcessManager) -> bool:
        """Verify log capture is working correctly."""

        def get_log_capture_scripts():
            scripts = [
                s for s in process_manager.exit_codes.keys() if "meta_test_logging_capture.py" in s.path
            ]
            return scripts

        stdout_expected = ["This is in stdout"]
        log_expected = [
            "logger at info level",
            "logger at warning level",
            "logger at error level",
            "logger at info level - post CliLog.initLog()",
            "logger at warning level - post CliLog.initLog()",
            "logger at error level - post CliLog.initLog()",
        ]
        passed = True

        for script in get_log_capture_scripts():
            stdout, stderr, logs = process_manager.outputs[script]
            missing_items = []

            for line in stdout_expected:
                if line not in stdout:
                    print(f"❌ Test {script} did not capture stdout as expected.")
                    missing_items.append(line)
                    passed = False

            for line in log_expected:
                if line not in logs:
                    print(f"❌ Test {script} did not capture logs as expected.")
                    missing_items.append(line)
                    passed = False

            if missing_items:
                print(f"❌ Missing log items: {missing_items}")
            else:
                print(f"✅ Test {script} captured all stdout and logs as expected.")

        return passed

    def check_script_results(self, process_manager: ProcessManager, scripts: list[TestScript]) -> None:
        """Check the results of test scripts and update checks list."""
        for script in scripts:
            result = process_manager.exit_codes[script]
            stdout, stderr, log_output = process_manager.outputs[script]

            if result in ["timeout", 0]:
                self.checks.append(Check(True, f"{script} passed"))
                if script.display_on_pass:
                    print(f"\n🙂 *Passing* logs from {script}:")
                    print(f"stdout:\n{stdout}")
                    print(f"stderr:\n{stderr}")
                    print(f"logs:\n{log_output}")
            else:
                print(f"🚨 {script}: Failed with exit code {result}. Stdout, stderr and logs below 🚨")
                print(f"stdout:\n{stdout}")
                print(f"stderr:\n{stderr}")
                print(f"logs:\n{log_output}")
                print("\n")
                self.checks.append(Check(False, f"{script} failed"))

    def check_yaml_files(self, yaml_files: list[str]) -> bool:
        """Check YAML files for consistent keys."""
        # Dictionary to hold the keys for each file
        file_keys = {}

        # Load all YAML files
        for filename in yaml_files:
            with open(filename, "r") as file:
                try:
                    data = yaml.safe_load(file)
                    if data:
                        file_keys[filename] = set(data.keys())
                    else:
                        file_keys[filename] = set()
                except yaml.YAMLError as exc:
                    print(f"Error loading {filename}: {exc}")

        # Get the set of all keys across all files
        all_keys = set().union(*file_keys.values())

        # Prepare the report of missing keys
        missing_keys_report = {}
        for filename, keys in file_keys.items():
            missing_keys = all_keys - keys
            if missing_keys:
                missing_keys_report[filename] = missing_keys

        # Print the report
        if missing_keys_report:
            print("Missing Keys Report:")
            package_dir = os.path.dirname(yaml_files[0]).split("/config")[0]
            for filename, missing_keys in missing_keys_report.items():
                rel_filename = filename.replace(package_dir + "/", "")
                print(f"{rel_filename} is missing keys:")
                for key in missing_keys:
                    print(f"  {key}")
                print()
            return False
        else:
            print("All files contain the same keys.")
            return True

    def check_plots(self, config: TestConfig) -> None:
        """Check that expected plots were generated."""
        locationConfig = LocationConfig("usdf_testing")

        expected = [  # (path, size) tuples
            # Regular LSSTCam plots -------
            ("LSSTCam/20250415/LSSTCam_calexp_mosaic_dayObs_20250415_seqNum_000088.jpg", 5000),
            ("LSSTCam/20250415/LSSTCam_event_timeline_dayObs_20250415_seqNum_000086.png", 5000),
            ("LSSTCam/20250415/LSSTCam_event_timeline_dayObs_20250415_seqNum_000087.png", 5000),
            ("LSSTCam/20250415/LSSTCam_event_timeline_dayObs_20250415_seqNum_000088.png", 5000),
            ("LSSTCam/20250415/LSSTCam_focal_plane_mosaic_dayObs_20250415_seqNum_000086.jpg", 5000),
            ("LSSTCam/20250415/LSSTCam_focal_plane_mosaic_dayObs_20250415_seqNum_000087.jpg", 5000),
            ("LSSTCam/20250415/LSSTCam_focal_plane_mosaic_dayObs_20250415_seqNum_000088.jpg", 5000),
            ("LSSTCam/20250415/LSSTCam_mount_dayObs_20250415_seqNum_000086.png", 5000),
            ("LSSTCam/20250415/LSSTCam_mount_dayObs_20250415_seqNum_000087.png", 5000),
            ("LSSTCam/20250415/LSSTCam_mount_dayObs_20250415_seqNum_000088.png", 5000),
            ("LSSTCam/20250415/LSSTCam_fwhm_focal_plane_dayObs_20250415_seqNum_000088.png", 5000),
            ("LSSTCam/20250415/LSSTCam_witness_detector_dayObs_20250415_seqNum_000088.png", 5000),
            ("LSSTCam/20250415/LSSTCam_imexam_dayObs_20250415_seqNum_000088.png", 5000),
            # AOS plots -------
            # FAM donut galleries
            ("LSSTCam/20250415/LSSTCam_fp_donut_gallery_dayObs_20250415_seqNum_000086.png", 5000),
            ("LSSTCam/20250415/LSSTCam_fp_donut_gallery_dayObs_20250415_seqNum_000087.png", 5000),
            # Extrafocal id for FAM plot
            ("LSSTCam/20250415/LSSTCam_zk_measurement_pyramid_dayObs_20250415_seqNum_000087.png", 5000),
            # CWFS plot
            ("LSSTCam/20250415/LSSTCam_zk_measurement_pyramid_dayObs_20250415_seqNum_000088.png", 5000),
            # Extrafocal id for FAM plot
            ("LSSTCam/20250415/LSSTCam_zk_residual_pyramid_dayObs_20250415_seqNum_000087.png", 5000),
            # CWFS plot
            ("LSSTCam/20250415/LSSTCam_zk_residual_pyramid_dayObs_20250415_seqNum_000088.png", 5000),
            # LATISS plots -------
            ("LATISS/20240813/LATISS_mount_dayObs_20240813_seqNum_000632.png", 5000),
            ("LATISS/20240813/LATISS_monitor_dayObs_20240813_seqNum_000632.png", 5000),
            ("LATISS/20240813/LATISS_imexam_dayObs_20240813_seqNum_000632.png", 5000),
            ("LATISS/20240813/LATISS_specexam_dayObs_20240813_seqNum_000632.png", 5000),
        ]

        destinationDir = Path("~/public_html/ra_ci_automated_output/").expanduser()
        if config.copy_plots_to_public_html:
            if destinationDir.exists():
                shutil.rmtree(destinationDir)
            if destinationDir.exists():
                self.checks.append(
                    Check(False, "Failed to remove output dir - files in there cannot be trusted!")
                )

        for file, expected_size in expected:
            full_path = os.path.join(locationConfig.plotPath, file)
            if os.path.exists(full_path):
                if config.copy_plots_to_public_html:
                    destination = destinationDir / file
                    os.makedirs(destination.parent, exist_ok=True)
                    shutil.copy(full_path, destination)
                file_size = os.path.getsize(full_path)
                if file_size >= expected_size:
                    self.checks.append(Check(True, f"Found expected plot {file} with size {file_size} bytes"))
                else:
                    self.checks.append(
                        Check(False, f"Plot {file} exists but is too small: {file_size} bytes")
                    )
            else:
                self.checks.append(Check(False, f"Did not find expected plot {file}"))

    def print_final_result(self) -> bool:
        """Print final test results and return overall pass status."""
        fails = [check for check in self.checks if not check.passed]
        passes = [check for check in self.checks if check.passed]
        n_fails = len(fails)
        n_passes = len(passes)
        terminal_width = os.get_terminal_size().columns

        # Determine the colors and text to display
        if n_fails > 0:
            pass_color = "\033[92m"  # green
            fail_color = "\033[91m"  # red
            text = f"{pass_color}{n_passes} passing tests\033[0m, {fail_color}{n_fails} failing tests\033[0m"
            padding_color = fail_color
        else:
            pass_color = fail_color = "\033[92m"  # all green
            text = f"{pass_color}{n_passes} passing tests, {n_fails} failing tests\033[0m"
            padding_color = pass_color

        # Calculate the padding
        padding_length = (terminal_width - len(text)) // 2
        padding = f"{padding_color}{'-' * padding_length}\033[0m"

        # Print the centered text with colored padding
        for fail in fails:
            print(fail)
        for test_pass in passes:
            print(test_pass)
        print(f"{padding}{text}{padding}")

        return n_fails == 0


class TestRunner:
    """Main class for orchestrating the test suite."""

    def __init__(self) -> None:
        self.config = TestConfig()
        self.redis_manager = RedisManager(self.config)
        self.process_manager = ProcessManager()
        self.result_collector = ResultCollector()

    def check_system_requirements(self) -> None:
        """Check system size and load for running tests."""
        number_of_cores = os.cpu_count()
        assert number_of_cores is not None
        number_of_scripts = len(self.config.test_scripts_round_1)

        if number_of_scripts > number_of_cores:
            print(
                f"The number of test scripts ({number_of_scripts}) is greater than the number of"
                f" cores ({number_of_cores}).\nThis test suite needs to be run on a bigger system."
            )
            sys.exit(1)

        load1, load5, load15 = os.getloadavg()
        if any(load > 50 for load in [load1, load5, load15]):
            print("⚠️  High system load detected, results could be affected ⚠️ ")

        approx_cores_free = (100 - load5) / 100 * number_of_cores
        if number_of_scripts > approx_cores_free:
            print(
                f"⚠️  Number of test scripts ({number_of_scripts}) is greater than the approximate number"
                f"  of free cores {approx_cores_free:.1f} ⚠️ "
            )

    def setup_environment(self) -> None:
        """Set up environment variables for testing."""
        # Set environment variables for rapid analysis
        os.environ["RAPID_ANALYSIS_LOCATION"] = "usdf_testing"
        os.environ["RAPID_ANALYSIS_CI"] = "true"
        os.environ["RAPID_ANALYSIS_DO_RAISE"] = "True"

        # Verify environment settings
        if getDoRaise() is not True:
            raise RuntimeError("getDoRaise is not True")

        if runningCI() is not True:
            raise RuntimeError("runningCI is not True")

    def check_test_scripts_exist(self) -> None:
        """Ensure all test scripts exist."""
        all_scripts = itertools.chain(
            self.config.test_scripts_round_1,
            self.config.meta_tests_fail_expected,
            self.config.meta_tests_pass_expected,
        )

        for test_script in all_scripts:
            if not os.path.isfile(test_script.path):
                raise FileNotFoundError(f"Test script {test_script.path} not found - your tests are doomed")

    def run_meta_tests(self) -> None:
        """Run meta tests to verify the test framework."""
        print(f"Running meta-tests to test the CI suite for the next {self.config.meta_test_duration}s...")

        self.process_manager.run_test_scripts(
            self.config.meta_tests_fail_expected + self.config.meta_tests_pass_expected,
            self.config.meta_test_duration,
            is_meta_tests=True,
        )

        self.result_collector.check_meta_test_results(
            self.process_manager, self.config.meta_tests_fail_expected, self.config.meta_tests_pass_expected
        )

        print("✅ All meta-tests passed, running real tests now...\n")

    def delete_output_files(self) -> None:
        """Delete previous output files."""
        locationConfig = LocationConfig("usdf_testing")
        deletion_locations = [
            locationConfig.binnedVisitImagePath,
            locationConfig.calculatedDataPath,
            locationConfig.plotPath,
        ]

        for location in deletion_locations:
            if os.path.exists(location):
                shutil.rmtree(location)
                print(f"✅ Deleted output directory: {location}")

        # Reinitialize to create directories as needed
        locationConfig = LocationConfig("usdf_testing")

        # Verify directories are empty
        for location in deletion_locations:
            if any(os.path.isfile(os.path.join(location, f)) for f in os.listdir(location)):
                raise RuntimeError(f"Failed to delete files in {location}")

    def run(self) -> None:
        """Run the entire test suite."""
        # Check system capabilities
        self.check_system_requirements()

        # Setup testing environment
        self.setup_environment()

        # Check YAML files if configured
        if self.config.do_check_yaml_files:
            yaml_files_ok = self.result_collector.check_yaml_files(self.config.yaml_files_to_check)
            if not yaml_files_ok:
                self.result_collector.checks.append(Check(False, "YAML check"))

        # Verify test scripts exist
        self.check_test_scripts_exist()

        # Run meta tests if configured
        if self.config.do_run_meta_tests:
            self.run_meta_tests()

        # Start Redis and register shutdown handler
        atexit.register(self.redis_manager.terminate)
        self.redis_manager.start()
        self.redis_manager.check_connection()

        # Delete previous output files
        self.delete_output_files()

        # Run the main test scripts
        self.process_manager.run_test_scripts(self.config.test_scripts_round_1, self.config.test_duration)

        # Verify all scripts reported results
        self._verify_all_scripts_reported()

        # Check test results
        print("\nTest Results:")
        self.result_collector.check_script_results(self.process_manager, self.config.test_scripts_round_1)

        # Check for plots and Redis results
        self.result_collector.check_plots(self.config)
        self.redis_manager.check_final_contents(self.result_collector.checks)

        # Print final results and exit with appropriate status
        overall_pass = self.result_collector.print_final_result()
        if not overall_pass:
            sys.exit(1)

    def _verify_all_scripts_reported(self) -> None:
        """Verify that all test scripts have reported their results."""
        expected: list[TestScript] = []
        if self.config.do_run_meta_tests:
            expected.extend(self.config.meta_tests_fail_expected)
            expected.extend(self.config.meta_tests_pass_expected)
        expected.extend(self.config.test_scripts_round_1)

        exit_codes_keys = set(self.process_manager.exit_codes.keys())
        outputs_keys = set(self.process_manager.outputs.keys())
        expected_set = set(expected)

        if exit_codes_keys != expected_set or outputs_keys != expected_set:
            missing_exit_codes = expected_set - exit_codes_keys
            missing_outputs = expected_set - outputs_keys
            extra_exit_codes = exit_codes_keys - expected_set
            extra_outputs = outputs_keys - expected_set

            msg = (
                "Not all test scripts have had their results collected somehow - this is drastically wrong!\n"
            )
            if missing_exit_codes:
                msg += "Missing exit codes for:\n" + "\n".join(str(m) for m in missing_exit_codes) + "\n"
            if missing_outputs:
                msg += "Missing outputs for:\n" + "\n".join(str(m) for m in missing_outputs) + "\n"
            if extra_exit_codes:
                msg += "Unexpected exit codes for:\n" + "\n".join(str(m) for m in extra_exit_codes) + "\n"
            if extra_outputs:
                msg += "Unexpected outputs for:\n" + "\n".join(str(m) for m in extra_outputs) + "\n"

            raise RuntimeError(msg)


def main() -> None:
    """Main entry point for the test suite."""
    runner = TestRunner()
    runner.run()


if __name__ == "__main__":
    main()
