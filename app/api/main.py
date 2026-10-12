"""FastAPI entrypoint for the Zera ms-analytics-bff service."""

import uvicorn
from fastapi import FastAPI

from config.environments import Environments
from logger.logger import Logger

myLogger = Logger()
envs = Environments()


def create_app() -> FastAPI:
    application = FastAPI(title="ms-analytics-bff")

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return application


app = create_app()


def run() -> None:
    myLogger.Info("Starting ms-analytics-bff API")
    uvicorn.run(app, host=envs.APP_HOST, port=int(envs.APP_PORT))


if __name__ == "__main__":
    run()
