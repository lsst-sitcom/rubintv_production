import os
import subprocess
import sys
import time

import redis

# Import the TestConfig class to access Redis configuration
from test_rapid_analysis import TestConfig

from lsst.rubintv.production.utils import getDoRaise


def check_redis_process(expect_running=False):
    """Check if redis-server is running using pgrep."""
    try:
        # Run pgrep to find redis-server processes
        result = subprocess.run(["pgrep", "-f", "redis-server"], capture_output=True, text=True)

        # Get process IDs if any
        redis_pids = result.stdout.strip().split("\n") if result.stdout.strip() else []

        # Check if the expectation matches reality
        is_running = bool(redis_pids and redis_pids[0])
        if is_running != expect_running:
            state = "running" if is_running else "not running"
            expected = "to be running" if expect_running else "not to be running"
            print(f"Redis is {state} but expected {expected}")
            if is_running:
                print(f"Redis PIDs: {redis_pids}")
            return False
        return True
    except Exception as e:
        print(f"Error checking Redis process: {e}")
        return False


def start_test_redis():
    """Start a Redis server for testing."""
    # Get Redis configuration
    config = TestConfig()
    host = config.redis_host
    port = config.redis_port
    password = config.redis_password

    # Set environment variables
    os.environ["REDIS_HOST"] = host
    os.environ["REDIS_PORT"] = port
    os.environ["REDIS_PASSWORD"] = password

    print(f"Starting Redis on {host}:{port}")
    redis_process = subprocess.Popen(
        ["redis-server", "--port", port, "--bind", host, "--requirepass", password],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    print(f"Started Redis server with PID: {redis_process.pid}")

    # Wait for Redis to initialize
    time.sleep(3)

    return redis_process, host, port, password


def check_redis_connection(host, port, password):
    """Check if Redis connection works."""
    try:
        r = redis.Redis(host=host, port=int(port), password=password)

        # Ping Redis
        if not r.ping():
            print("Could not ping Redis")
            return False

        # Set and read back a test key
        r.set("test_key", "test_value")
        value = r.get("test_key").decode("utf-8")
        if value != "test_value":
            print("Could not set and read back a test key in Redis")
            return False

        r.flushall()  # Clear the database
        return True
    except Exception as e:
        print(f"Redis connection error: {e}")
        return False


def stop_redis(process):
    """Stop the Redis server."""
    if process:
        process.terminate()
        process.wait(timeout=5)
        print(f"Terminated Redis process PID: {process.pid}")


def main():
    all_tests_passed = True

    # 1. Check environment variables are correctly set
    print("Checking environment variables...")
    if getDoRaise() is not True:
        print("ERROR: getDoRaise is not True")
        all_tests_passed = False

    # 2. Check that no Redis process is running initially
    print("Checking for existing Redis processes...")
    if not check_redis_process(expect_running=False):
        print("WARNING: Redis already running, this might interfere with tests")
        all_tests_passed = False

    # 3. Start Redis and verify it's running
    print("Starting Redis server...")
    redis_process, host, port, password = start_test_redis()

    # 4. Check that Redis process is now running
    print("Verifying Redis process is running...")
    if not check_redis_process(expect_running=True):
        print("ERROR: Redis failed to start")
        all_tests_passed = False

    # 5. Test Redis connection
    print("Testing Redis connection...")
    if not check_redis_connection(host, port, password):
        print("ERROR: Redis connection failed")
        all_tests_passed = False
    else:
        print("Redis connection successful")

    # 6. Stop Redis
    print("Stopping Redis server...")
    stop_redis(redis_process)

    # 7. Verify Redis is no longer running
    print("Verifying Redis process is stopped...")
    time.sleep(1)  # Give it a moment to fully terminate
    if not check_redis_process(expect_running=False):
        print("ERROR: Redis didn't shut down properly")
        all_tests_passed = False

    # 8. Verify environment variables match TestConfig
    print("Verifying Redis environment variables...")
    config = TestConfig()
    if host != os.environ["REDIS_HOST"] or host != config.redis_host:
        print(f"ERROR: Redis host mismatch: {host=}, {os.environ['REDIS_HOST']=}, {config.redis_host=}")
        all_tests_passed = False

    if port != os.environ["REDIS_PORT"] or port != config.redis_port:
        print(f"ERROR: Redis port mismatch: {port=}, {os.environ['REDIS_PORT']=}, {config.redis_port=}")
        all_tests_passed = False

    if password != os.environ["REDIS_PASSWORD"] or password != config.redis_password:
        print(f"ERROR: Redis password mismatch: {password=}, config.redis_password=<hidden>")
        all_tests_passed = False

    if all_tests_passed:
        print("✅ All environment and Redis tests passed!")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
