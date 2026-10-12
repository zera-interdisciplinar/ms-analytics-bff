import pytest

from config.environments import Environments

ENV_VARS = [
    "APP_ENV",
    "APP_PORT",
    "APP_HOST",
    "PIPELINE_MAX_ROWS",
    "PG_HOST",
    "PG_PORT",
    "PG_DATABASE",
    "PG_USER",
    "PG_PASSWORD",
    "NEO4J_URI",
    "NEO4J_USER",
    "NEO4J_PASSWORD",
    "NEO4J_DATABASE",
]


@pytest.fixture
def clean_env(monkeypatch):
    """Drops any value a real .env may have loaded at import time."""
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_defaults_are_applied_when_env_is_empty(clean_env):
    envs = Environments()

    assert envs.APP_ENV == "DEV"
    assert envs.APP_PORT == "8000"
    assert envs.APP_HOST == "0.0.0.0"
    assert envs.PIPELINE_MAX_ROWS == "100000"
    assert envs.PG_HOST == ""
    assert envs.PG_PASSWORD == ""
    assert envs.NEO4J_URI == ""
    assert envs.NEO4J_PASSWORD == ""


def test_env_values_override_defaults(clean_env, monkeypatch):
    monkeypatch.setenv("APP_ENV", "PROD")
    monkeypatch.setenv("APP_PORT", "9000")
    monkeypatch.setenv("APP_HOST", "127.0.0.1")

    envs = Environments()

    assert envs.APP_ENV == "PROD"
    assert envs.APP_PORT == "9000"
    assert envs.APP_HOST == "127.0.0.1"


def test_values_are_read_at_instantiation_time(clean_env, monkeypatch):
    monkeypatch.setenv("APP_ENV", "QA")
    first = Environments()

    monkeypatch.setenv("APP_ENV", "PROD")
    second = Environments()

    assert first.APP_ENV == "QA"
    assert second.APP_ENV == "PROD"


def test_instantiation_logs_initialization(clean_env, capsys):
    Environments()

    assert "Initializing environment variables with default values" in capsys.readouterr().out
