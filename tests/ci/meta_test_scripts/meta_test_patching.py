from lsst.summit.utils.utils import getCurrentDayObs_int


def main():
    # Check if the function is patched correctly
    mocked_value = getCurrentDayObs_int()
    print(f"Mock function output via patched getCurrentDayObs_int: {mocked_value}")

    expected = 20240101
    if mocked_value != expected:
        raise RuntimeError(f"Patched function failed to return patched value: {mocked_value} != {expected}")
    else:
        print(f"Patched function returned the expected value: {mocked_value}")


if __name__ == "__main__":
    main()
