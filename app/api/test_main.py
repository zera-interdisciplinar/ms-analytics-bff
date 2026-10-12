from fastapi.testclient import TestClient

from app.api.main import create_app, run


def test_health_returns_ok():
    client = TestClient(create_app())

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_run_starts_uvicorn(monkeypatch):
    called = {}

    def fake_run(app, host, port):
        called["app"] = app
        called["host"] = host
        called["port"] = port

    monkeypatch.setattr("app.api.main.uvicorn.run", fake_run)

    run()

    assert called["host"] == "0.0.0.0"
    assert called["port"] == 8000
    assert called["app"].title == "ms-analytics-bff"
