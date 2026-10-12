import pytest

from logger.logger import Logger

FIXED_TIMESTAMP = "2026-08-09 12:00:00"


@pytest.fixture(autouse=True)
def freeze_timestamp(monkeypatch):
    monkeypatch.setattr(Logger, "_timestamp", staticmethod(lambda: FIXED_TIMESTAMP))


def test_info_prints_message_in_blue(capsys):
    Logger().Info("booting up")

    out = capsys.readouterr().out
    assert out == f"{Logger.BLUE}[{FIXED_TIMESTAMP}] [INFO] booting up{Logger.RESET}\n"


def test_error_prints_message_and_error_in_red(capsys):
    Logger().Error("failed to connect: ", ValueError("timeout"))

    out = capsys.readouterr().out
    assert out == f"{Logger.RED}[{FIXED_TIMESTAMP}] [ERROR] failed to connect: timeout{Logger.RESET}\n"


def test_debug_prints_message_in_cyan(capsys):
    Logger().Debug("state dump")

    out = capsys.readouterr().out
    assert out == f"{Logger.CYAN}[{FIXED_TIMESTAMP}] [DEBUG] state dump{Logger.RESET}\n"


def test_warning_prints_message_in_yellow(capsys):
    Logger().Warning("retrying")

    out = capsys.readouterr().out
    assert out == f"{Logger.YELLOW}[{FIXED_TIMESTAMP}] [WARNING] retrying{Logger.RESET}\n"


def test_color_codes_are_distinct():
    codes = [Logger.BLUE, Logger.RED, Logger.CYAN, Logger.YELLOW, Logger.RESET]
    assert len(set(codes)) == len(codes)


def test_instances_do_not_carry_state():
    """Methods only print, so two instances must behave identically."""
    assert Logger().Info("a") is None
    assert Logger().Info("a") is None
