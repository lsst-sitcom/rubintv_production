import time


def main():
    time.sleep(1)
    raise RuntimeError("This is a test runtime error, raising as it should do.")


if __name__ == "__main__":
    main()
