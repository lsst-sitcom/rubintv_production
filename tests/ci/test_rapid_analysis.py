import atexit
import importlib
import logging
import matplotlib
import multiprocessing
import os
import queue
import signal
import subprocess
import sys
import time
import traceback
from multiprocessing import Manager
from unittest.mock import patch

import redis
import yaml

from ciutils import (ScriptRunInformation, MetaTestScriptRunInformation, ScriptRunBaseInformation,
                     ScriptResultInformation, ScriptTestInformation, QueueWriter, TestManager, 
                     TestState, ScriptManager, TestInformation, LogUtils)


log_utils = LogUtils()
_log = log_utils.initialize_main_logger()


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
    ScriptRunInformation(name="runPlotter", args=["slac_testing"]),
    ScriptRunInformation(
        name="runStep2aWorker",
        args=[0]
    ),
    ScriptRunInformation(name="runNightlyWorker", args=[0]),
    ScriptRunInformation(
        name="runSfmRunner",
        args=[0]
    ),
    ScriptRunInformation(name="runSfmRunner", args=[1]),
    ScriptRunInformation(name="runSfmRunner", args=[2]),
    ScriptRunInformation(name="runSfmRunner", args=[3]),
    ScriptRunInformation(name="runSfmRunner", args=[4]),
    ScriptRunInformation(name="runSfmRunner", args=[5]),
    ScriptRunInformation(name="runSfmRunner", args=[6]),
    ScriptRunInformation(name="runSfmRunner", args=[7]),
    ScriptRunInformation(name="runSfmRunner", args=[8]),
    ScriptRunInformation(
        name="runHeadNode",
        args=["slac_testing"],
        delay=5,  # we do NOT want the head node to fanout work before workers report in - that's a fail
    ),
    ScriptRunInformation(
        name="drip_feed_data",
        path='tests.ci.meta_test_scripts',
        delay=0
    )
]

TEST_SCRIPTS_ROUND_2 = [
    # run after the processing pods are torn down, so that, for example, the
    # butler watcher can be checked without attempting to process the image it
    # drops into redis
    # XXX need to get this to actually run
    # XXX need to add check that this actually output to redis
    ScriptRunInformation(name="runButlerWatcher", args=["slac_testing"]),
]

META_TEST_SCRIPTS = [
    MetaTestScriptRunInformation(name="meta_test_raise", should_fail=True),
    MetaTestScriptRunInformation(name="meta_test_sys_exit_non_zero", should_fail=True),
    # This should pass by running forever
    MetaTestScriptRunInformation(name="meta_test_runs_ok"),
    # check the remote connection magic works
    MetaTestScriptRunInformation(name="meta_test_debug_config", do_debug=True),
    # Confirms that getCurrentDayObs_int returns the patched value
    MetaTestScriptRunInformation(name="meta_test_patching"),
    # Confirms things we're manipulating via the env are set in workers
    MetaTestScriptRunInformation(name="meta_test_env"),
    # confirms S3 uploads work and are mocked correctly
    MetaTestScriptRunInformation(name="meta_test_s3_upload"),
    # check logs are captured when not being teed
    MetaTestScriptRunInformation(name="meta_test_logging_capture"),
    # confirm teeing works
    MetaTestScriptRunInformation(name="meta_test_logging_capture")
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

YAML_FILES_TO_CHECK = [os.path.join(package_dir, file) for file in YAML_FILES_TO_CHECK]


# Globals for communication between functions
manager = Manager()
outputs = manager.dict()
REDIS_PROCESS = None


def create_folder(path, name) -> str:
    """
    Function to create a folder  if it does not exist.

    Parameters
    ----------
    path: str
        The path of the folder to be created.
    name: str
        The name of the folder to be created.

    Returns
    -------
    folder_path: str
        The path of the folder created.
    """
    folder_path = os.path.join(path, name)
    if not os.path.exists(path):
        os.makedirs(folder_path)
    return folder_path


def exec_script(script_process_info: ScriptRunBaseInformation, out_queue, error_queue):
    """
    Function to run the script in a separate process and capture its output.
    """
    avoid_raising = False

    def termination_handler(signum, frame):
        logging.warning(f"{script_process_info.name} has not a dedicated SIGTERM SIGNAL!")
        # Avoid raising exception on the finally block
        if not avoid_raising:
            raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, termination_handler)
    sys.stdout = QueueWriter(out_queue)  # type: ignore
    sys.stderr = open(os.devnull, 'w')

    if script_process_info.do_debug:
        import ciutils

        def getConnection():
            debugConfig = {
                "port": 4444,
                "addr": "127.0.0.1",
            }
            return debugConfig

        ciutils.getConnection = getConnection  # type: ignore

    # one file for each process
    LogUtils.initialize_script_logger(script_process_info)
    return_information = ScriptResultInformation()
    try:
        with patch("lsst.summit.utils.utils.getCurrentDayObs_int", return_value=20240101):
            time.sleep(script_process_info.delay)
            module_path = f'{script_process_info.path}.{script_process_info.name}'
            module = importlib.import_module(module_path)
            launch_test = getattr(module, 'main')
            launch_test(*script_process_info.args)
    except KeyboardInterrupt:  # this is the timeout error now
        return_information = ScriptResultInformation(0, "timeout error", traceback.format_exc())
    except Exception as ex:
        print(f"Script {os.getpid()} exited with exception: {ex}")
        logging.error(f"Script {os.getpid()} exited with exception: {ex}")
        return_information = ScriptResultInformation(exit_code=-1,
                                                     error_message=str(ex),
                                                     traceback=traceback.format_exc())
    except SystemExit as e:
        message = f"Script exited with status: {e}"
        logging.info(message)
        exit_code = e.code if e.code is not None else -1
        iexit_code = exit_code if isinstance(exit_code, int) else -2
        return_information = ScriptResultInformation(iexit_code, message)
    finally:
        # print(f"Sending from {os.getpid()} value: {error_information}")
        avoid_raising = True
        error_queue.put(return_information)


def check_system_size_and_load():
    number_of_cores = os.cpu_count()
    number_of_scripts = len(TEST_SCRIPTS_ROUND_1)
    if number_of_scripts > number_of_cores:
        _log.error(
            f"The number of test scripts ({number_of_scripts}) is greater than"
            f" the number of cores ({number_of_cores}).\nThis test suite needs to be run on a bigger system."
        )
        sys.exit(1)

    load1, load5, load15 = os.getloadavg()  # 1, 5 and 15 min load averages
    if any(load > 50 for load in [load1, load5, load15]):
        # double space after warning sign is necessary
        _log.warning("⚠️  High system load detected, results could be affected ⚠️ ")

    approx_cores_free = (100 - load5) / 100 * number_of_cores
    if number_of_scripts > approx_cores_free:
        _log.warning(
            # double space after warning sign is necessary
            f"⚠️  Number of test scripts ({number_of_scripts}) is greater than the approximage number of free"
            f" cores {approx_cores_free:.1f} ⚠️ "
        )


def clear_redis_database(host, port, password):
    r = redis.Redis(host=host, port=port, password=password)
    r.flushall()  # Clear the database
    _log.info("Cleared Redis database")


def start_redis(redis_init_wait_time):
    global REDIS_PROCESS
    host = os.environ["REDIS_HOST"]
    port = os.environ["REDIS_PORT"]
    password = os.environ["REDIS_PASSWORD"]

    capture_kwargs = {}
    if CAPTURE_REDIS_OUTPUT:
        capture_kwargs["stdout"] = subprocess.PIPE
        capture_kwargs["stderr"] = subprocess.PIPE

    _log.info(f"Starting redis on {host}:{port}")
    # Start the Redis server
    REDIS_PROCESS = subprocess.Popen(
        ["redis-server", "--port", port, "--bind", host, "--requirepass", password],
        **capture_kwargs,
    )
    _log.info(f"✅ Redis server started on {host}:{port} with PID: {REDIS_PROCESS.pid}")
    _log.info(f"Waiting for {redis_init_wait_time}s to let redis startup finish")

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
    _log.info("✅ Successfully pinged Redis and set/read back a test key")


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
        _log.error(f"❌ Expected {n_visits} step2a to have finished, got {n_step2a}")
        passed = False
    else:
        _log.info(f"✅ {n_step2a}x step2a finished")

    key = f"{inst}-NIGHTLYROLLUP-FINISHEDCOUNTER"
    n_nightly_rollups = int(redisHelper.redis.get(key) or 0)
    if n_nightly_rollups != n_visits:
        _log.error(f"❌ Expected {n_visits} nightly rollup finished, got {n_nightly_rollups}")
        passed = False
    else:
        _log.info(f"✅ {n_visits} nightly rollup finished")

    allKeys = redisHelper.redis.keys()
    failed_keys = [key.decode("utf-8") for key in allKeys if "FAILED" in key.decode("utf-8")]
    if failed_keys:
        _log.error(f"❌ Found failed keys: {failed_keys}")
        passed = False
    if not passed:
        return TestInformation("Redis check", TestState.ERROR)
    return TestInformation("Redis check", TestState.SUCCEED)


def print_final_result(test_manager: TestManager):
    terminal_width = os.get_terminal_size().columns

    n_fails = test_manager.get_number_failed_tests()
    n_passes = test_manager.get_number_succeed_tests()

    if n_fails > 0:
        pass_color = "\033[92m"
        fail_color = "\033[91m"
        text = f"{pass_color}{n_passes} passing tests\033[0m, {fail_color}{n_fails} failing tests\033[0m"
        padding_color = fail_color
    else:
        pass_color = fail_color = "\033[92m"
        text = f"{pass_color}{n_passes} passing tests, {n_fails} failing tests\033[0m"
        padding_color = pass_color

    padding_length = (terminal_width - len(text)) // 2
    padding = f"{padding_color}{'-' * padding_length}\033[0m"
    for script_information in test_manager:
        if script_information.state == TestState.SUCCEED:
            _log.info(f"✅ {script_information.name} passed")
    for script_information in test_manager:
        if script_information.state == TestState.ERROR:
            _log.error(f"❌ {script_information.name} failed")

    _log.info(f"{padding}{text}{padding}")


def check_yaml_keys() -> TestInformation:
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
                _log.error(f"Error loading {filename}: {exc}")

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
        _log.info("Missing Keys Report:")
        for filename, missing_keys in missing_keys_report.items():
            filename = filename.replace(package_dir + "/", "")
            _log.error(f"{filename} is missing keys:")
            for key in missing_keys:
                _log.error(f"  {key}")
            _log.info("-----------------")  # blank line between each failing file
        return TestInformation("YAML check", TestState.ERROR)
    else:
        _log.info("All files contain the same keys.")
        return TestInformation("YAML check", TestState.SUCCEED)


def terminate_redis():
    global REDIS_PROCESS
    try:
        if REDIS_PROCESS:
            REDIS_PROCESS.terminate()
            REDIS_PROCESS.wait()
            _log.info("Terminated Redis process")
    except Exception as e:
        _log.error(f"Error terminating Redis process: {e}")


def run_test_scripts(
    scripts_to_launch: list[ScriptRunBaseInformation],
    timeout: float,
    is_meta_tests=False
) -> ScriptManager:

    script_manager = ScriptManager()

    start_time = time.time()
    error_queue: multiprocessing.Queue = multiprocessing.Queue()
    output_queue: multiprocessing.Queue = multiprocessing.Queue()

    for script_process_info in scripts_to_launch:
        p = multiprocessing.Process(target=exec_script, args=(script_process_info, output_queue, error_queue))
        p.start()
        if p.pid is None:
            _log.warning(f"Warning: Process {script_process_info.name} failed to start")
        else:
            script = ScriptTestInformation(p.pid, script_process_info, p, TestState.RUNNING)
            script_manager.add_script(script)
            _log.info(f"Launched {script.name} with pid={p.pid}...")

    out = False
    last_secs = None
    while not out:
        try:
            information_message = error_queue.get(block=True, timeout=1)
            if isinstance(information_message, str):
                _log.info(f"Non regulated messaged in stderr: {information_message}")
            else:
                script_information = script_manager.get_script_info(information_message.pid)
                script_information.result = information_message
                if script_information.is_final_state_as_expected():
                    script_information.state = TestState.SUCCEED
                else:
                    script_information.state = TestState.ERROR
                    _log.error(f"❌ Process Error: {information_message.pid} ")
                    _log.error("________________________________________________________")
                    _log.error(f"{information_message}")
                    _log.error("________________________________________________________")
                    _log.error("Al script will be stop!")
                    out = True
        except queue.Empty:
            while not output_queue.empty():
                out_message = output_queue.get(block=False)
                _log.info(f" Client Message > {out_message}")
        finally:
            time_remaining = (timeout - (time.time() - start_time))
            if time.time() - start_time > timeout:
                out = True
            mins, secs = divmod(time_remaining, 60)
            if int(secs) != last_secs:  # only update when the seconds change
                last_secs = int(secs)
                timer = f"{int(mins):02d}:{int(secs):02d} remaining" if time_remaining > 0 else "time's up"
                end = "\r" if time_remaining > 0 else "\n"  # overwrite until the end, then leave it showing
                n_alive = script_manager.get_number_running_scripts()
                print(f"{timer} with {n_alive} processes running", end=end)
    stop_processes(script_manager)
    get_end_messages(script_manager, output_queue, error_queue)
    check_possible_orphan_processes(script_manager)
    _log.debug("Finished terminating running processes...")
    return script_manager


def check_possible_orphan_processes(script_manager: ScriptManager):
    number_of_running_processes = script_manager.get_number_running_scripts()
    if number_of_running_processes == 0:
        return
    running_processes = script_manager.get_processes({TestState.RUNNING})
    for p in running_processes:
        if not p.is_alive():
            if p.pid is None:
                _log.warn("WARNING: Unable to set the process to SUCCEED manually")
            else:
                script_information = script_manager.get_script_info(p.pid)
                _log.warn(f"WARNING: process: {script_information} set manually to SUCCEED")
                script_information.state = TestState.SUCCEED
        else:
            _log.warn(f"WARNING: process: {p} is still running")


def stop_processes(script_manager: ScriptManager):
    running_processes = script_manager.get_processes({TestState.RUNNING})
    for p in running_processes:
        if p.is_alive():
            _log.info(f"Terminating running process {p.pid} at timeout.")
            p.terminate()  # Send SIGTERM signal

    _log.info("Post SIGTERM sleep")
    time.sleep(3)  # leave time to die

    # Ensure all processes have terminated
    for p in running_processes:
        # if DEBUG:
        _log.info(f"Joining terminated process {p.pid}.")
        p.join(timeout=3)


def get_end_messages(script_manager: ScriptManager, output_queue, error_queue):
    running_processes = script_manager.get_processes({TestState.RUNNING})
    out = False
    while not out:
        try:
            out_message = output_queue.get(block=True, timeout=1)
            _log.info(f"Client Message > {out_message}")
        except queue.Empty:
            out = True
    for p in running_processes:
        try:
            information_message = error_queue.get(block=True, timeout=5)
        except Exception:
            pass
        #  print(f"Get information message from {information_message.pid}:"
        #      f"{script_manager.get_number_running_scripts()}")
        script_information = script_manager.get_script_info(information_message.pid)
        script_information.result = information_message
        if information_message.exit_code == 0:
            script_information.state = TestState.SUCCEED
        else:
            script_information.state = TestState.ERROR


def check_log_capture(script_manager: ScriptManager):
    def get_log_capture_scripts():
        return ["meta_test_logging_capture"]

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
        script_information = script_manager.get_script_info_by_name(logging_capture_script)
        log_file = LogUtils.get_script_log_file(script_information)
        missing_items = []
        with open(log_file, "r") as f:
            logs = f.readlines()
            for line in log_expected:
                log_found = any(line in string for string in logs)
                if not log_found:
                    _log.error(f"❌ Test {logging_capture_script} did not capture logs as expected.")
                    missing_items.append(line)
                    passed = False
        if missing_items:
            _log.error(f"❌ Missing log items: {missing_items}")
        else:
            _log.info(f"✅ Test {logging_capture_script} captured all stdout and logs as expected.")

    return passed


def check_meta_test_results(script_manager: ScriptManager):
    # Don't count these towards passes and fail, just raise if these aren't
    # as expected, as it means the test suite is fundamentally broken

    passed = True
    for script in script_manager:
        if not script.is_final_state_as_expected():
            passed = False
            if script.should_fail:
                _log.error(f"❌ Test {script.name} was expected to fail but returned a zero-like exit code:"
                           f" {script.result.exit_code}")
            else:
                _log.error(f"❌ Test {script.name} failed with exit code {script.result.exit_code}")
        else:
            _log.info(f"✅ Test {script.name} passed with exit code: {script.result.exit_code}")

    capture_ok = check_log_capture(script_manager)
    passed = passed and capture_ok

    if not passed:
        raise RuntimeError("Meta-tests did not pass as expected - fix the test suite and try again.")


def main():

    matplotlib.use('Agg')
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

    test_manager = TestManager()

#    if not check_folder_exists(LOGS_PATH):
#        print("Logs folder doesnt exists")
#        exit(-1)

    check_system_size_and_load()
    logging.getLogger().setLevel(logging.INFO)

    # setup env vars for all processes
    run_setup()  # needs to come before meta tests as they test the env vars

    if DO_CHECK_YAML_FILES:
        yaml_files_result = check_yaml_keys()
        test_manager.add_test(yaml_files_result)
        # if not yaml_files_ok:  # not an instant fail
        #    FAILS.append("YAML check")

    # these exit if any fail because that means everything is broken so there's
    # no point in continuing
    if DO_RUN_META_TESTS:
        # for test_script in itertools.chain(
        #    TEST_SCRIPTS_ROUND_1, META_TESTS_FAIL_EXPECTED, META_TESTS_PASS_EXPECTED
        # ):
        # if not os.path.isfile(test_script.path):
        #    raise FileNotFoundError(f"Test script {test_script.path} not found - your tests are doomed")
        _log.info(f"Running meta-tests to test the CI suite for the next {META_TEST_DURATION}s...")
        script_results = run_test_scripts(
            META_TEST_SCRIPTS, META_TEST_DURATION, is_meta_tests=True
        )
        check_meta_test_results(script_results)
        print("✅ All meta-tests passed, running real tests now...\n")

    atexit.register(terminate_redis)
    start_redis(REDIS_INIT_WAIT_TIME)
    check_redis_startup()  # Wait for the setup timeout and then check Redis

    # Run each real test script
    script_results = run_test_scripts(TEST_SCRIPTS_ROUND_1, TEST_DURATION)
    for script_result in script_results:
        test_manager.add_test(script_result)
    # Check there's a result for all scripts
    # expected = [script.pid for script in TEST_SCRIPTS_ROUND_1]
    # if DO_RUN_META_TESTS:
    #    expected += [s.pid for s in itertools.chain(META_TESTS_FAIL_EXPECTED, META_TESTS_PASS_EXPECTED)]
    # print(expected, script_results.keys())
    # if not set(script_results.keys()) == set(expected):
    #    raise RuntimeError(
    #        "Not all test scripts have had their results collected somehow - this is drastically wrong!"
    #    )

    # print("\nTest Results:")
    # for script_info in script_results:
    #    if script_info.state == TestState.SUCCEED:
    #        if script.display_on_pass:
    #            pass
    #             print(f"\n🙂 *Passing* logs from {script_info.name}:")
    #             #  print(f"stdout:\n{stdout}")  # ensure use of str not repr to print properly
    #             #  print(f"stdout:\n{stderr}")  # ensure use of str not repr to print properly
    #             #  print(f"logs:\n{log_output}")  # ensure use of str not repr to print properly
    #         continue
    #        else:
    #            pass
    #         print(f"🚨 {script_info.name} with pid {script_info.pid}:"
    #               f"Failed with exit code {script_info.result.exit_code}."
    #               f"Stdout, stderr and logs below 🚨")
    #         #  print(f"stdout:\n{stdout}")  # ensure use of str not repr to print properly
    #         #  print(f"stdout:\n{stderr}")  # ensure use of str not repr to print properly
    #         #  print(f"logs:\n{log_output}")  # ensure use of str not repr to print properly
    #         #  print("\n")  # put a nice gap between each failing scripts's output

    redis_check = check_redis_final_contents()
    test_manager.add_test(redis_check)

    print_final_result(test_manager)
    if script_results.get_number_failed_scripts() > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
