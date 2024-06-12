import logging


def main():
    print("This is in stdout")
    logger = logging.getLogger()
    logger.info("logger at info level")
    logger.warning("logger at warning level")
    logger.error("logger at error level")

    from lsst.daf.butler.cli.cliLog import CliLog

    CliLog.initLog(longlog=False)
    CliLog.initLog(longlog=True)
    logger.info("logger at info level - post CliLog.initLog()")
    logger.warning("logger at warning level - post CliLog.initLog()")
    logger.error("logger at error level - post CliLog.initLog()")


if __name__ == "__main__":
    main()
