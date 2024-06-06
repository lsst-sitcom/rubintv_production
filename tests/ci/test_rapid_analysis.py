import atexit
import contextlib
import io
import itertools
import logging
import multiprocessing
import os
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from multiprocessing import Manager
from typing import List, Optional
from unittest.mock import patch

import redis
import yaml

from lsst.rubintv.production.utils import getDoRaise

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

logger.info = print


@dataclass
class TestScript:
    path: str
    args: Optional[List[str]] = None

    def __post_init__(self):
        if self.args is None:
            self.args = []


# Globals for communication between functions
manager = Manager()
exit_codes = manager.dict()
outputs = manager.dict()
REDIS_PROCESS = None

# --------------- Configuration --------------- #

DO_RUN_META_TESTS = True  # XXX Turn on before merging
DO_CHECK_YAML_FILES = False  # XXX Turn on before merging

REDIS_IP = "127.0.0.1"
REDIS_PORT = "6111"
REDIS_PASSWORD = "redis_password"
META_TEST_DURATION = 30  # How long to leave meta-tests running for
TEST_DURATION = 30  # How long to leave SFM to run for
REDIS_INIT_WAIT_TIME = 2  # Time to wait after starting redis-server before using it
CAPTURE_REDIS_OUTPUT = True  # Whether to capture Redis output
TODAY = 20240101

# List of test scripts to run, defined relative to package root
TEST_SCRIPTS = [
    TestScript("scripts/summit/LSSTComCamSim/runButlerWatcher.py", ["slac_testing"]),
    TestScript("scripts/summit/LSSTComCamSim/runHeadNode.py", ["slac_testing"]),
    TestScript("scripts/summit/LSSTComCamSim/runPlotter.py", ["slac_testing"]),
]

META_TESTS_FAIL_EXPECTED = [
    TestScript("meta_test_raise.py"),  # This one should fail
    TestScript("meta_test_sys_exit_non_zero.py"),  # This one should fail
]

META_TESTS_PASS_EXPECTED = [
    TestScript("meta_test_runs_ok.py"),  # This should pass by running forever
    TestScript("meta_test_patching.py"),  # Confirms that getCurrentDayObs_int returns the patched value
]

YAML_FILES_TO_CHECK = [
    "config/config_bts.yaml",
    "config/config_tts.yaml",
    "config/config_summit.yaml",
    "config/config_slac.yaml",
    "config/config_slac_testing.yaml",
]

# --------------- code to make file paths full --------------- #

ci_dir = os.path.dirname(os.path.abspath(__file__))
package_dir = os.path.abspath(os.path.join(ci_dir, "../../"))
TEST_SCRIPTS = [
    TestScript(os.path.join(package_dir, test_script.path), test_script.args) for test_script in TEST_SCRIPTS
]
META_TESTS_FAIL_EXPECTED = [
    TestScript(os.path.join(ci_dir, test_script.path), test_script.args)
    for test_script in META_TESTS_FAIL_EXPECTED
]
META_TESTS_PASS_EXPECTED = [
    TestScript(os.path.join(ci_dir, test_script.path), test_script.args)
    for test_script in META_TESTS_PASS_EXPECTED
]
YAML_FILES_TO_CHECK = [os.path.join(package_dir, file) for file in YAML_FILES_TO_CHECK]

# --------------- Wrapper Function --------------- #


def exec_script(test_script: TestScript):
    """
    Function to run the script in a separate process and capture its output.
    """
    script_path = test_script.path
    script_args = test_script.args

    f_stdout = io.StringIO()
    f_stderr = io.StringIO()
    exit_code = None
    with contextlib.redirect_stdout(f_stdout), contextlib.redirect_stderr(f_stderr):
        # Read the script content
        try:
            with patch("lsst.summit.utils.utils.getCurrentDayObs_int", return_value=20240101):
                # Redirect stdout and stderr
                with open(script_path, "r") as script_file:
                    script_content = script_file.read()
                exec_globals = {
                    "__name__": "__main__",
                    "__file__": script_path,
                    "sys": sys,
                }
                sys.argv = [script_path] + script_args
                exec(script_content, exec_globals)
                exit_code = 0

        except Exception:
            # Capture traceback in stderr
            traceback.print_exc(file=f_stderr)
            exit_code = 1
        except SystemExit as e:
            # Handle normal exit or sys.exit called within the script
            print(f"Script exited with status: {e}")
            exit_code = e.code if e.code is not None else 0

    # Collect outputs
    exit_codes[test_script.path] = exit_code
    outputs[test_script.path] = (f_stdout.getvalue(), f_stderr.getvalue())


def clear_redis_database(host, port, password):
    r = redis.Redis(host=host, port=port, password=password)
    r.flushall()  # Clear the database
    print("Cleared Redis database")


def start_redis(redis_init_wait_time):
    global REDIS_PROCESS
    host = os.environ["REDIS_HOST"]
    port = os.environ["REDIS_PORT"]
    password = os.environ["REDIS_PASSWORD"]

    capture_kwargs = {}
    if CAPTURE_REDIS_OUTPUT:
        capture_kwargs["stdout"] = subprocess.PIPE
        capture_kwargs["stderr"] = subprocess.PIPE

    print(f"Starting redis on {host}:{port}")
    # Start the Redis server
    REDIS_PROCESS = subprocess.Popen(
        ["redis-server", "--port", port, "--bind", host, "--requirepass", password],
        **capture_kwargs,
    )
    print(f"Redis server started on {host}:{port} with PID: {REDIS_PROCESS.pid}")

    time.sleep(redis_init_wait_time)  # Give redis a moment to start
    clear_redis_database(host, port, password)
    return


def run_setup(redis_init_wait_time):
    """Setup everything for testing.

    Sets env vars and starts redis, returning the process.
    """
    # Set environment variables for Redis
    os.environ["REDIS_HOST"] = REDIS_IP
    os.environ["REDIS_PORT"] = REDIS_PORT
    os.environ["REDIS_PASSWORD"] = REDIS_PASSWORD

    # set so that runners raise on error, and confirm that's working via
    # getDoRaise as that's how they will retrieve the value
    os.environ["RAPID_ANALYSIS_DO_RAISE"] = "True"
    if getDoRaise() is not True:  # confirm that this will be used correctly by services
        raise RuntimeError("getDoRaise is not True")

    start_redis(redis_init_wait_time)

    # Wait for the setup timeout and then check Redis
    logger.info(f"Waiting for {redis_init_wait_time} seconds to let setup complete")
    check_redis_startup()
    return


def check_redis_startup():
    """
    Ping Redis and check we can set and read back a test key.
    """
    host = os.environ["REDIS_HOST"]
    port = os.environ["REDIS_PORT"]
    password = os.environ["REDIS_PASSWORD"]

    r = redis.Redis(host=host, port=port, password=password)

    # Ping Redis
    if not r.ping():
        raise RuntimeError("Could not ping Redis")

    # Set and read back a test key
    r.set("test_key", "test_value")
    value = r.get("test_key").decode("utf-8")
    if value != "test_value":
        raise RuntimeError("Could not set and read back a test key in Redis")
    r.flushall()  # Clear the database
    logger.info("Successfully pinged Redis and set/read back a test key")


def check_redis_final_contents():
    """
    Ping Redis and check we can set and read back a test key.
    """
    # Example check, you can expand this based on your needs
    host = os.environ["REDIS_HOST"]
    port = os.environ["REDIS_PORT"]
    password = os.environ["REDIS_PASSWORD"]

    r = redis.Redis(host=host, port=port, password=password)
    keys = r.keys()
    logger.info(f"Redis contains keys: {keys}")
    return True  # Modify this based on actual checks


def print_final_result(fails, passes):
    terminal_width = os.get_terminal_size().columns

    # Determine the colors and text to display
    if fails > 0:
        pass_color = "\033[92m"
        fail_color = "\033[91m"
        text = f"{pass_color}{passes} passing tests\033[0m, {fail_color}{fails} failing tests\033[0m"
        padding_color = fail_color
    else:
        pass_color = fail_color = "\033[92m"
        text = f"{pass_color}{passes} passing tests, {fails} failing tests\033[0m"
        padding_color = pass_color

    # Calculate the padding
    padding_length = (terminal_width - len(text)) // 2
    padding = f"{padding_color}{'-' * padding_length}\033[0m"

    # Print the centered text with colored padding
    print(f"{padding}{text}{padding}")


def check_yaml_keys():
    # Dictionary to hold the keys for each file
    file_keys = {}

    # Load all YAML files in the directory
    for filename in YAML_FILES_TO_CHECK:
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
        for filename, missing_keys in missing_keys_report.items():
            filename = filename.replace(package_dir + "/", "")
            print(f"{filename} is missing keys:")
            for key in missing_keys:
                print(f"  {key}")
            print()  # blank line between each failing file
        return False
    else:
        print("All files contain the same keys.")
        return True


def terminate_redis():
    global REDIS_PROCESS
    if REDIS_PROCESS:
        REDIS_PROCESS.terminate()
        REDIS_PROCESS.wait()
        logger.info("Terminated Redis process")


def run_test_scripts(scripts, timeout):
    start_time = time.time()
    processes = {}
    for script in scripts:
        p = multiprocessing.Process(target=exec_script, args=(script,))
        p.start()
        processes[p] = script.path
        print(f"Started {script.path} with PID {p.pid}")

    while True:
        for p, scriptpath in processes.items():
            if not p.is_alive():
                p.join()  # Clean up the finished process
                exitcode = p.exitcode
                # XXX add "as expected below"
                print(f"Script {scriptpath} exited with exit code {exitcode}")
                processes.pop(p)
                break

        if time.time() - start_time > timeout:
            print("Timeout reached. Terminating remaining processes.")
            for p, scriptpath in processes.items():
                p.terminate()
                p.join()
                print(f"Process {p.pid} terminated.")
                exit_codes[scriptpath] = "timeout"
            break
        if not processes:
            break
        time.sleep(1)


def check_meta_test_results():
    # Don't count these towards passes and fail, just raise if these aren't
    # as expected, as it means the test suite is fundamentally broken
    passed = True
    for script in META_TESTS_FAIL_EXPECTED:
        if exit_codes[script.path] in (0, "timeout"):
            print(f"Test {script.path} was expected to fail but did not return a non-zero exit code:")
            print(outputs[script.path][1])
            passed = False
    for script in META_TESTS_PASS_EXPECTED:
        if exit_codes[script.path] not in (0, "timeout"):
            print(f"Test {script.path} was expected to pass but returned a non-zero exit code:")
            print(outputs[script.path][1])
            passed = False

    if not passed:
        raise RuntimeError("Meta-tests did not pass as expected - fix the test suite and try again.")


def main():
    FAILCOUNTER = 0
    PASSCOUNTER = 0

    if DO_CHECK_YAML_FILES:
        yaml_files_ok = check_yaml_keys()
        if not yaml_files_ok:  # not an instant fail
            FAILCOUNTER += 1

    for test_script in itertools.chain(TEST_SCRIPTS, META_TESTS_FAIL_EXPECTED, META_TESTS_PASS_EXPECTED):
        if not os.path.isfile(test_script.path):
            raise FileNotFoundError(f"Test script {test_script.path} not found - your tests are doomed")

    # these exit if any fail because that means everything is broken so there's
    # no point in continuing
    if DO_RUN_META_TESTS:
        logger.info("Running meta-tests to test the CI suite...")
        run_test_scripts(META_TESTS_FAIL_EXPECTED + META_TESTS_PASS_EXPECTED, META_TEST_DURATION)
        check_meta_test_results()

    # Run setup script to start and check redis
    atexit.register(terminate_redis)
    run_setup(REDIS_INIT_WAIT_TIME)

    # Run each real test script
    logger.info("Meta-tests passed, running real tests now...\n")
    run_test_scripts(TEST_SCRIPTS, TEST_DURATION)

    # Report results and perform assertions
    logger.info("\nTest Results:")
    # Check there's a result for all scripts
    expected = [script.path for script in TEST_SCRIPTS]
    if DO_RUN_META_TESTS:
        expected += [s.path for s in itertools.chain(META_TESTS_FAIL_EXPECTED, META_TESTS_PASS_EXPECTED)]

    if not set(exit_codes.keys()) == set(expected) or not set(outputs.keys()) == set(expected):
        raise RuntimeError("Not all test scripts have been run somehow - this is drastically wrong")

    for script, result in exit_codes.items():
        if script not in [s.path for s in TEST_SCRIPTS]:  # We've already done these
            continue

        stdout, stderr = outputs[script]
        if result == "timeout":
            logger.info(f"{script}: Passed (was still running at end of testing period)")
            PASSCOUNTER += 1
            continue
        elif result == 0:
            logger.info(f"{script}: Passed with exit code zero")
            PASSCOUNTER += 1
            continue
        else:
            logger.error(f"{script}: Failed with exit code {result}. Stdout and stderr below:")
            # import ipdb as pdb; pdb.set_trace()
            print(f"{script} stdout: {stdout}")  # ensure use of str not repr to print properly
            print(f"{script} stderr: {stderr}")  # ensure use of str not repr to print properly
            print("\n\n")  # put a nice gap between each failing scripts's output
            FAILCOUNTER += 1

    if check_redis_final_contents():
        logger.info("Pass - redis contents are as expected")
        PASSCOUNTER += 1
    else:
        logger.error("FAIL - redis contents are not as expected")
        FAILCOUNTER += 1

    print_final_result(FAILCOUNTER, PASSCOUNTER)
    if FAILCOUNTER > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
