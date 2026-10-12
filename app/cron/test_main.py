import pytest

from app.cron.main import main, run_job


def test_run_job_completes_successfully(monkeypatch, capsys):
    monkeypatch.setattr(
        "app.cron.main.run_pipeline",
        lambda: {"batch_id": "lote-teste"},
    )

    assert run_job() == 0

    out = capsys.readouterr().out
    assert "Starting ms-analytics-bff cronjob" in out
    assert "Analytics pipeline finished batch_id=lote-teste" in out


def test_run_job_returns_error_when_pipeline_fails(monkeypatch, capsys):
    def boom():
        raise RuntimeError("fonte indisponível")

    monkeypatch.setattr("app.cron.main.run_pipeline", boom)

    assert run_job() == 1
    assert "Analytics pipeline failed" in capsys.readouterr().out


def test_main_exits_with_job_status(monkeypatch):
    monkeypatch.setattr("app.cron.main.run_job", lambda: 0)

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 0
