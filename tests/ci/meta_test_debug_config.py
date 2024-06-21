from metaToImport import TestClass


def main():
    test = TestClass()
    success = test.someFunction()
    if not success:
        raise RuntimeError("Failed to collect debug connectio in imported class")


if __name__ == "__main__":
    main()
