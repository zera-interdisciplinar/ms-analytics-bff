"""Loads environment variables for ms-analytics-bff.

Values come from the process environment (and an optional `.env` in the project
root). Missing keys fall back to the defaults defined here.
"""

import os

from dotenv import load_dotenv

from logger.logger import Logger

load_dotenv()
myLogger = Logger()
myLogger.Info("Loading environment variables from .env file")


class Environments:
    """Runtime configuration read from the environment at instantiation time."""

    APP_ENV: str
    APP_PORT: str
    APP_HOST: str

    def __init__(self) -> None:
        myLogger.Info("Initializing environment variables with default values")

        self.APP_ENV = os.getenv("APP_ENV", "DEV")
        self.APP_PORT = os.getenv("APP_PORT", "8000")
        self.APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
