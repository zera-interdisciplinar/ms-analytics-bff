"""Cronjob entrypoint for the Zera ms-analytics-bff service."""

from logger.logger import Logger

myLogger = Logger()


def run_job() -> int:
    myLogger.Info("Starting ms-analytics-bff cronjob")
    myLogger.Info("Analytics cronjob finished")
    return 0


def main() -> None:
    raise SystemExit(run_job())


if __name__ == "__main__":
    main()
