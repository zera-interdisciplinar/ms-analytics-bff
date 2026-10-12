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
    PIPELINE_MAX_ROWS: str
    JWT_ISSUER: str
    JWT_JWKS_URL: str
    JWT_PUBLIC_KEY: str
    SSE_POLL_SECONDS: str
    SSE_HEARTBEAT_SECONDS: str
    CORS_ORIGINS: str
    PG_HOST: str
    PG_PORT: str
    PG_DATABASE: str
    PG_USER: str
    PG_PASSWORD: str
    NEO4J_URI: str
    NEO4J_USER: str
    NEO4J_PASSWORD: str
    NEO4J_DATABASE: str

    def __init__(self) -> None:
        myLogger.Info("Initializing environment variables with default values")

        self.APP_ENV = os.getenv("APP_ENV", "DEV")
        self.APP_PORT = os.getenv("APP_PORT", "8000")
        self.APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
        self.PIPELINE_MAX_ROWS = os.getenv("PIPELINE_MAX_ROWS", "100000")
        self.JWT_ISSUER = os.getenv("JWT_ISSUER", "ms-administrative-core")
        self.JWT_JWKS_URL = os.getenv("JWT_JWKS_URL", "")
        self.JWT_PUBLIC_KEY = os.getenv("JWT_PUBLIC_KEY", "")
        self.SSE_POLL_SECONDS = os.getenv("SSE_POLL_SECONDS", "2")
        self.SSE_HEARTBEAT_SECONDS = os.getenv("SSE_HEARTBEAT_SECONDS", "15")
        self.CORS_ORIGINS = os.getenv("CORS_ORIGINS", "")
        self.PG_HOST = os.getenv("PG_HOST", "")
        self.PG_PORT = os.getenv("PG_PORT", "15432")
        self.PG_DATABASE = os.getenv("PG_DATABASE", "admin_core")
        self.PG_USER = os.getenv("PG_USER", "admin_core")
        self.PG_PASSWORD = os.getenv("PG_PASSWORD", "")
        self.NEO4J_URI = os.getenv("NEO4J_URI", "")
        self.NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
        self.NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
        self.NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "")
