import atexit
import io
import itertools
import logging
import multiprocessing
import os
import signal
import subprocess
import sys
import time
import traceback
from multiprocessing import Manager
from unittest.mock import patch

import redis
import yaml
from ciutils import TestScript, conditional_redirect


def do_nothing(*args, **kwargs):
    pass


# ensure nothing downstream can use this function as it has global consequences
# which stop the log capture working
logging.basicConfig = do_nothing


# these imports need to come after the log patching (I think)
from lsst.daf.butler.cli.cliLog import CliLog  # noqa: E402

# call this once and then also disable it so it doesn't interfere with the log
# capture later on
CliLog.initLog(False)
CliLog.initLog = do_nothing


# only import from lsst.anything once the logging configs have been frozen
# noqa: E402
# from lsst.rubintv.production.utils import getAutomaticLocationConfig
from lsst.rubintv.production.utils import getDoRaise  # noqa: E402

# --------------- Configuration --------------- #

DO_RUN_META_TESTS = True  # XXX Turn on before merging
DO_CHECK_YAML_FILES = True  # XXX Turn on before merging

REDIS_HOST = "127.0.0.1"
REDIS_PORT = "6111"
REDIS_PASSWORD = "redis_password"
META_TEST_DURATION = 30  # How long to leave meta-tests running for
TEST_DURATION = 400  # How long to leave SFM to run for
REDIS_INIT_WAIT_TIME = 3  # Time to wait after starting redis-server before using it
CAPTURE_REDIS_OUTPUT = True  # Whether to capture Redis output
TODAY = 20240101
DEBUG = False

# List of test scripts to run, defined relative to package root
TEST_SCRIPTS_ROUND_1 = [
    # the main RA testing - runs data through the processing pods
    TestScript("scripts/summit/LSSTComCamSim/runPlotter.py", ["slac_testing"]),
    TestScript(
        "scripts/summit/LSSTComCamSim/runStep2aWorker.py",
        ["slac_testing", "0"],
        tee_output=True,
        # do_debug=True,
    ),
    TestScript("scripts/summit/LSSTComCamSim/runNightlyWorker.py", ["slac_testing", "0"], tee_output=True),
    TestScript(
        "scripts/summit/LSSTComCamSim/runSfmRunner.py",
        ["slac_testing", "0"],
        display_on_pass=True,
        tee_output=True,
        do_debug=True,
    ),
    TestScript("scripts/summit/LSSTComCamSim/runSfmRunner.py", ["slac_testing", "1"]),
    TestScript("scripts/summit/LSSTComCamSim/runSfmRunner.py", ["slac_testing", "2"]),
    TestScript("scripts/summit/LSSTComCamSim/runSfmRunner.py", ["slac_testing", "3"]),
    TestScript("scripts/summit/LSSTComCamSim/runSfmRunner.py", ["slac_testing", "4"]),
    TestScript("scripts/summit/LSSTComCamSim/runSfmRunner.py", ["slac_testing", "5"]),
    TestScript("scripts/summit/LSSTComCamSim/runSfmRunner.py", ["slac_testing", "6"]),
    TestScript("scripts/summit/LSSTComCamSim/runSfmRunner.py", ["slac_testing", "7"]),
    TestScript("scripts/summit/LSSTComCamSim/runSfmRunner.py", ["slac_testing", "8"]),
    TestScript(
        "scripts/summit/LSSTComCamSim/runHeadNode.py",
        ["slac_testing"],
        delay=5,  # we do NOT want the head node to fanout work before workers report in - that's a fail
        tee_output=True,
        display_on_pass=True,
    ),
    TestScript("tests/ci/drip_feed_data.py", ["slac_testing"], delay=0, display_on_pass=True),
]

TEST_SCRIPTS_ROUND_2 = [
    # run after the processing pods are torn down, so that, for example, the
    # butler watcher can be checked without attempting to process the image it
    # drops into redis
    # XXX need to get this to actually run
    # XXX need to add check that this actually output to redis
    TestScript("scripts/summit/LSSTComCamSim/runButlerWatcher.py", ["slac_testing"]),
]

META_TESTS_FAIL_EXPECTED = [
    TestScript("meta_test_raise.py"),  # This one should fail
    TestScript("meta_test_sys_exit_non_zero.py"),  # This one should fail
]

META_TESTS_PASS_EXPECTED = [
    TestScript("meta_test_runs_ok.py"),  # This should pass by running forever
    TestScript("meta_test_debug_config.py", do_debug=True),  # check the remote connection magic works
    TestScript("meta_test_patching.py"),  # Confirms that getCurrentDayObs_int returns the patched value
    TestScript("meta_test_env.py"),  # Confirms things we're manipulating via the env are set in workers
    TestScript("meta_test_s3_upload.py"),  # confirms S3 uploads work and are mocked correctly
    TestScript("meta_test_logging_capture.py"),  # check logs are captured when not being teed
    TestScript("meta_test_logging_capture.py", tee_output=True),  # confirm teeing works
]

YAML_FILES_TO_CHECK = [
    # TODO Add the commented out files back in when you're ready
    # "config/config_bts.yaml",
    # "config/config_tts.yaml",
    "config/config_summit.yaml",
    # "config/config_slac.yaml",
    "config/config_slac_testing.yaml",
]

# --------------- code to make file paths full --------------- #

ci_dir = os.path.dirname(os.path.abspath(__file__))
package_dir = os.path.abspath(os.path.join(ci_dir, "../../"))
TEST_SCRIPTS_ROUND_1 = [
    TestScript.from_existing(test_script, os.path.join(package_dir, test_script.path))
    for test_script in TEST_SCRIPTS_ROUND_1
]

TEST_SCRIPTS_ROUND_2 = [
    TestScript.from_existing(test_script, os.path.join(package_dir, test_script.path))
    for test_script in TEST_SCRIPTS_ROUND_2
]

META_TESTS_FAIL_EXPECTED = [
    TestScript.from_existing(test_script, os.path.join(ci_dir, test_script.path))
    for test_script in META_TESTS_FAIL_EXPECTED
]

META_TESTS_PASS_EXPECTED = [
    TestScript.from_existing(test_script, os.path.join(ci_dir, test_script.path))
    for test_script in META_TESTS_PASS_EXPECTED
]
YAML_FILES_TO_CHECK = [os.path.join(package_dir, file) for file in YAML_FILES_TO_CHECK]


# Globals for communication between functions
manager = Manager()
exit_codes = manager.dict()
outputs = manager.dict()
REDIS_PROCESS = None


def exec_script(test_script: TestScript, output_queue):
    """
    Function to run the script in a separate process and capture its output.
    """

    def termination_handler(signum, frame):
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

    exit_code = None
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    lsstDebug = None
    if test_script.do_debug:
        import ciutils
        import lsstDebug

        def getConnection():
            debugConfig = {
                "port": 4444,
                "addr": "127.0.0.1",
            }
            return debugConfig

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
                    "logging": logging,  # Pass the logging module to the script
                    "lsst.daf.butler.cli.cliLog": CliLog,
                    "CliLog": CliLog,
                    "lsstDebug": lsstDebug,
                }
                sys.argv = [script_path] + script_args
                time.sleep(test_script.delay)
                exec(script_content, exec_globals)
                exit_code = 0
    except Exception:
        traceback.print_exc(file=f_stderr)
        exit_code = 1
    except SystemExit as e:
        logging.info(f"Script exited with status: {e}")
        exit_code = e.code if e.code is not None else 0
    except KeyboardInterrupt:  # this is the timeout error now
        exit_code = "timeout"
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr

        log_output = log_stream.getvalue()
        if DEBUG:
            print(f"Putting outputs for script {test_script} into queue")
        output_queue.put((test_script, exit_code, f_stdout.getvalue(), f_stderr.getvalue(), log_output))

        # Clean up logger handlers
        root_logger.removeHandler(log_handler)
        log_handler.close()


def check_system_size_and_load():
    number_of_cores = os.cpu_count()
    number_of_scripts = len(TEST_SCRIPTS_ROUND_1)
    if number_of_scripts > number_of_cores:
        print(
            f"The number of test scripts ({number_of_scripts}) is greater than"
            f" the number of cores ({number_of_cores}).\nThis test suite needs to be run on a bigger system."
        )
        sys.exit(1)

    load1, load5, load15 = os.getloadavg()  # 1, 5 and 15 min load averages
    if any(load > 50 for load in [load1, load5, load15]):
        # double space after warning sign is necessary
        print("⚠️  High system load detected, results could be affected ⚠️ ")

    approx_cores_free = (100 - load5) / 100 * number_of_cores
    if number_of_scripts > approx_cores_free:
        print(
            # double space after warning sign is necessary
            f"⚠️  Number of test scripts ({number_of_scripts}) is greater than the approximage number of free"
            f" cores {approx_cores_free:.1f} ⚠️ "
        )


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
    print(f"✅ Redis server started on {host}:{port} with PID: {REDIS_PROCESS.pid}")
    print(f"Waiting for {redis_init_wait_time}s to let redis startup finish")

    time.sleep(redis_init_wait_time)  # Give redis a moment to start
    clear_redis_database(host, port, password)
    return


def run_setup():
    """Setup everything for testing.

    Sets env vars and starts redis, returning the process.
    """
    # set other env vars required for RA config
    os.environ["RAPID_ANALYSIS_LOCATION"] = "slac_testing"

    # Set environment variables for Redis
    os.environ["REDIS_HOST"] = REDIS_HOST
    os.environ["REDIS_PORT"] = REDIS_PORT
    os.environ["REDIS_PASSWORD"] = REDIS_PASSWORD

    # set so that runners raise on error, and confirm that's working via
    # getDoRaise as that's how they will retrieve the value
    os.environ["RAPID_ANALYSIS_DO_RAISE"] = "True"
    if getDoRaise() is not True:  # confirm that this will be used correctly by services
        raise RuntimeError("getDoRaise is not True")

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
    print("✅ Successfully pinged Redis and set/read back a test key")


def check_redis_final_contents():
    """
    Ping Redis and check we can set and read back a test key.
    """
    from lsst.rubintv.production.redisUtils import RedisHelper

    redisHelper = RedisHelper(None, None)  # doesn't actually need a butler or a LocationConfig here
    redisHelper.displayRedisContents()

    inst = "LSSTComCamSim"
    passed = True

    visits = [
        7024061300017,
    ]  # for expansion
    n_visits = len(visits)

    n_step2a = redisHelper.getNumVisitLevelFinished(inst, "step2a")
    if n_step2a != n_visits:
        print(f"❌ Expected {n_visits} step2a to have finished, got {n_step2a}")
        passed = False
    else:
        print(f"✅ {n_step2a}x step2a finished")

    key = f"{inst}-NIGHTLYROLLUP-FINISHEDCOUNTER"
    n_nightly_rollups = int(redisHelper.redis.get(key) or 0)
    if n_nightly_rollups != n_visits:
        print(f"❌ Expected {n_visits} nightly rollup finished, got {n_nightly_rollups}")
        passed = False
    else:
        print(f"✅ {n_visits} nightly rollup finished")
    return passed


def print_final_result(fails, passes):
    n_fails = len(fails)
    n_passes = len(passes)
    terminal_width = os.get_terminal_size().columns

    # Determine the colors and text to display
    if n_fails > 0:
        pass_color = "\033[92m"
        fail_color = "\033[91m"
        text = f"{pass_color}{n_passes} passing tests\033[0m, {fail_color}{n_fails} failing tests\033[0m"
        padding_color = fail_color
    else:
        pass_color = fail_color = "\033[92m"
        text = f"{pass_color}{n_passes} passing tests, {n_fails} failing tests\033[0m"
        padding_color = pass_color

    # Calculate the padding
    padding_length = (terminal_width - len(text)) // 2
    padding = f"{padding_color}{'-' * padding_length}\033[0m"

    # Print the centered text with colored padding
    for fail in fails:
        print(f"❌ {fail} failed")
    for testPass in passes:
        print(f"✅ {testPass} passed")
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
        print("Terminated Redis process")


def run_test_scripts(scripts, timeout, is_meta_tests=False):
    start_time = time.time()
    processes = {}
    output_queue = multiprocessing.Queue()

    # allow_debug is so we don't increase timeout on the meta tests
    doing_debug = not is_meta_tests and any(s.do_debug for s in scripts)
    if doing_debug:
        if sum(s.do_debug for s in scripts) > 1:
            debug_attempts = [s for s in scripts if s.do_debug]
            script_string = "\n".join([str(s) for s in debug_attempts])
            err_msg = f"You can only interactively debug one script at a time! Attempted:\n{script_string}"
            raise RuntimeError(err_msg)
        print("\n\n⚠️ ⚠️ INTERACTIVE SCRIPT DEBUG MODE ENABLED ⚠️ ⚠️")
        print("     tests will continue until killed maually\n\n")
        timeout = 9999999  # keeping thigns alive forever when debugging

    for script in scripts:
        p = multiprocessing.Process(target=exec_script, args=(script, output_queue))
        p.start()
        processes[p] = script
        print(f"Launched {script} with pid={p.pid}...")

    last_secs = None
    while (time_remaining := (timeout - (time.time() - start_time))) > 0:
        # Check running time
        mins, secs = divmod(time_remaining, 60)
        if int(secs) != last_secs:  # only update when the seconds change
            last_secs = int(secs)
            timer = f"{int(mins):02d}:{int(secs):02d} remaining" if time_remaining > 0 else "time's up"
            end = "\r" if time_remaining > 0 else "\n"  # overwrite until the end, then leave it showing
            n_alive = sum([p.is_alive() for p in processes])
            print(f"{timer} with {n_alive} processes running", end=end)
        time.sleep(1)

    for p in list(processes.keys()):
        if p.is_alive():
            if DEBUG:
                print(f"Terminating running process {processes[p]} at timeout.")
            p.terminate()  # Send SIGTERM signal

    print("Post SIGTERM sleep")
    time.sleep(3)  # leave time to die

    # Ensure all processes have terminated
    for p in list(processes.keys()):
        if DEBUG:
            print(f"Joining terminated process {p.pid}.")
        p.join(timeout=3)

    if DEBUG:
        print("Finished terminating running processes, collecting outputs...")

    while not output_queue.empty():
        script, exit_code, stdout, stderr, log_output = output_queue.get()
        exit_codes[script] = exit_code
        outputs[script] = (stdout, stderr, log_output)
        if DEBUG:
            print(f"Collected output for {script}")


def check_log_capture():
    def get_log_capture_scripts():
        scripts = [s for s in META_TESTS_PASS_EXPECTED if "meta_test_logging_capture.py" in s.path]
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

    for logging_capture_script in get_log_capture_scripts():
        stdout, stderr, logs = outputs[logging_capture_script]  # single strings with \n embedded
        missing_items = []
        for line in stdout_expected:
            if line not in stdout:
                print(f"❌ Test {logging_capture_script} did not capture stdout as expected.")
                missing_items.append(line)
                passed = False

        for line in log_expected:
            if line not in logs:
                print(f"❌ Test {logging_capture_script} did not capture logs as expected.")
                missing_items.append(line)
                passed = False

        if missing_items:
            print(f"❌ Missing log items: {missing_items}")
        else:
            print(f"✅ Test {logging_capture_script} captured all stdout and logs as expected.")

    return passed


def check_meta_test_results():
    # Don't count these towards passes and fail, just raise if these aren't
    # as expected, as it means the test suite is fundamentally broken

    passed = True
    for script in META_TESTS_FAIL_EXPECTED:
        code = exit_codes[script]
        if code in (0, "timeout"):
            print(f"❌ Test {script} was expected to fail but returned a zero-like exit code: {code}")
            print(outputs[script][1])
            passed = False
        else:
            print(f"✅ Test {script} passed (by failing) with exit code: {code}")
    for script in META_TESTS_PASS_EXPECTED:
        code = exit_codes[script]
        if code not in (0, "timeout"):
            print(f"❌ Test {script} was expected to pass but returned a non-zero exit code: {code}")
            print(outputs[script][1])
            passed = False
        else:
            print(f"✅ Test {script} passed with exit code: {code}")

    capture_ok = check_log_capture()
    passed = passed and capture_ok

    if not passed:
        raise RuntimeError("Meta-tests did not pass as expected - fix the test suite and try again.")


def main():
    FAILS = []
    PASSES = []

    check_system_size_and_load()

    # setup env vars for all processes
    run_setup()  # needs to come before meta tests as they test the env vars

    if DO_CHECK_YAML_FILES:
        yaml_files_ok = check_yaml_keys()
        if not yaml_files_ok:  # not an instant fail
            FAILS.append("YAML check")

    # these exit if any fail because that means everything is broken so there's
    # no point in continuing
    if DO_RUN_META_TESTS:
        for test_script in itertools.chain(
            TEST_SCRIPTS_ROUND_1, META_TESTS_FAIL_EXPECTED, META_TESTS_PASS_EXPECTED
        ):
            if not os.path.isfile(test_script.path):
                raise FileNotFoundError(f"Test script {test_script.path} not found - your tests are doomed")
        print(f"Running meta-tests to test the CI suite for the next {META_TEST_DURATION}s...")
        run_test_scripts(
            META_TESTS_FAIL_EXPECTED + META_TESTS_PASS_EXPECTED, META_TEST_DURATION, is_meta_tests=True
        )
        check_meta_test_results()
        print("✅ All meta-tests passed, running real tests now...\n")

    atexit.register(terminate_redis)
    start_redis(REDIS_INIT_WAIT_TIME)
    check_redis_startup()  # Wait for the setup timeout and then check Redis

    # Run each real test script
    run_test_scripts(TEST_SCRIPTS_ROUND_1, TEST_DURATION)

    # Check there's a result for all scripts
    expected = [script for script in TEST_SCRIPTS_ROUND_1]
    if DO_RUN_META_TESTS:
        expected += [s for s in itertools.chain(META_TESTS_FAIL_EXPECTED, META_TESTS_PASS_EXPECTED)]
    if not set(exit_codes.keys()) == set(expected) or not set(outputs.keys()) == set(expected):
        raise RuntimeError(
            "Not all test scripts have had their results collected somehow - this is drastically wrong!"
        )

    print("\nTest Results:")
    for script, result in exit_codes.items():
        if script not in [s for s in TEST_SCRIPTS_ROUND_1]:  # We've already done these
            continue

        stdout, stderr, log_output = outputs[script]
        if result in ["timeout", 0]:
            PASSES.append(script)
            if script.display_on_pass:
                print(f"\n🙂 *Passing* logs from {script}:")
                print(f"stdout:\n{stdout}")  # ensure use of str not repr to print properly
                print(f"stdout:\n{stderr}")  # ensure use of str not repr to print properly
                print(f"logs:\n{log_output}")  # ensure use of str not repr to print properly
            continue
        else:
            print(f"🚨 {script}: Failed with exit code {result}. Stdout, stderr and logs below 🚨")
            print(f"stdout:\n{stdout}")  # ensure use of str not repr to print properly
            print(f"stdout:\n{stderr}")  # ensure use of str not repr to print properly
            print(f"logs:\n{log_output}")  # ensure use of str not repr to print properly
            print("\n")  # put a nice gap between each failing scripts's output
            FAILS.append(script)

    if check_redis_final_contents():
        PASSES.append("Redis content check")
    else:
        FAILS.append("Redis content check")

    print_final_result(FAILS, PASSES)
    if len(FAILS) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
