from meta_test_scripts.metaToImport import TestClass


def main():
    test = TestClass()
    success = test.someFunction()
    if not success:
        raise RuntimeError("Failed to collect debug connection in imported class")


if __name__ == "__main__":
    main()
