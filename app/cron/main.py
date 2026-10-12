"""Cronjob entrypoint for the Zera ms-analytics-bff service."""

from app.pipeline.run import run_pipeline
from logger.logger import Logger

myLogger = Logger()


def run_job() -> int:
    myLogger.Info("Starting ms-analytics-bff cronjob")
    try:
        result = run_pipeline()
        myLogger.Info(
            f"Analytics pipeline finished batch_id={result['batch_id']}"
        )
        return 0
    except Exception as exc:
        myLogger.Error("Analytics pipeline failed: ", exc)
        return 1


def main() -> None:
    raise SystemExit(run_job())


if __name__ == "__main__":
    main()
