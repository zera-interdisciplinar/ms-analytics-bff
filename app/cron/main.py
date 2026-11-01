"""Cronjob entrypoint for the Zera ms-analytics-bff service."""

from app.api.warehouse import AnalyticsWarehouse, live_warehouse, save_silver
from app.pipeline.run import run_pipeline
from config.environments import Environments
from logger.logger import Logger

myLogger = Logger()


def run_job(warehouse: AnalyticsWarehouse | None = None) -> int:
    myLogger.Info("Starting ms-analytics-bff cronjob")
    try:
        result = run_pipeline()
        store = warehouse or live_warehouse(Environments())
        save_silver(result["silver"], store)
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
