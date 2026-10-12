import pytest

from app.cron.main import main, run_job


def test_run_job_completes_successfully(capsys):
    assert run_job() == 0

    out = capsys.readouterr().out
    assert "Starting ms-analytics-bff cronjob" in out
    assert "Analytics cronjob finished" in out


def test_main_exits_with_job_status(monkeypatch):
    monkeypatch.setattr("app.cron.main.run_job", lambda: 0)

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 0
