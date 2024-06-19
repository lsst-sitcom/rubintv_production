import os

from test_rapid_analysis import REDIS_HOST, REDIS_PASSWORD, REDIS_PORT

from lsst.rubintv.production.utils import getDoRaise


def main():
    # check things we're manipulating via the env are set in workers
    if getDoRaise() is not True:
        raise RuntimeError("getDoRaise is not True")

    host = os.environ["REDIS_HOST"]
    port = os.environ["REDIS_PORT"]
    password = os.environ["REDIS_PASSWORD"]
    assert host == REDIS_HOST, f"Failed to pick up correct redis value from the env {host=}"
    assert port == REDIS_PORT, f"Failed to pick up correct redis value from the env {port=}"
    assert password == REDIS_PASSWORD, f"Failed to pick up correct redis value from the env {password=}"


if __name__ == "__main__":
    main()
