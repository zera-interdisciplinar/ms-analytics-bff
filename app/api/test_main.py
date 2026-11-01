import json

from fastapi.testclient import TestClient

from app.api.conftest import EMPLOYEE_A, MANAGER_A, MANAGER_B, UNIT_A2, UNIT_B, encode_token
from app.api.main import create_app, run
from app.api.sse import format_comment
from app.api.warehouse import InMemoryWarehouse


def _client(api_envs, warehouse=None, snapshot=None):
    holder = warehouse or InMemoryWarehouse()
    if snapshot is not None:
        holder.save(snapshot)
    return TestClient(
        create_app(runtime=api_envs, warehouse=holder, sse_max_events=1)
    ), holder


def _auth(rsa_keys, user_id: str, role: str = "MANAGER") -> dict[str, str]:
    return {"Authorization": f"Bearer {encode_token(rsa_keys, user_id, role)}"}


def _first_sse_event(body: str) -> tuple[str, dict]:
    event = ""
    data = ""
    for line in body.splitlines():
        if line.startswith("event:"):
            event = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            data = line.split(":", 1)[1].strip()
        elif line == "" and event:
            return event, json.loads(data)
    if event and data:
        return event, json.loads(data)
    raise AssertionError(f"no SSE event received: {body!r}")


def test_sse_comment_is_a_heartbeat_frame():
    assert format_comment("heartbeat") == ": heartbeat\n\n"


def test_health_returns_ok(api_envs):
    client, _ = _client(api_envs)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_overview_requires_token(api_envs, warehouse):
    client, _ = _client(api_envs, warehouse)

    response = client.get("/api/v1/dashboard/overview")

    assert response.status_code == 401


def test_overview_rejects_service_token(api_envs, warehouse, rsa_keys):
    client, _ = _client(api_envs, warehouse)
    token = encode_token(rsa_keys, "ms-inventory", extra={"scope": "notifications:write", "role": ""})

    response = client.get(
        "/api/v1/dashboard/overview",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 403


def test_overview_reads_cron_slice_for_home_unit(api_envs, warehouse, rsa_keys):
    client, _ = _client(api_envs, warehouse)

    response = client.get(
        "/api/v1/dashboard/overview",
        headers=_auth(rsa_keys, MANAGER_A),
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    event, payload = _first_sse_event(response.text)
    assert event == "snapshot"
    assert payload["organization"]["name"] == "Empresa A"
    assert payload["applied_filters"]["unit_id"] == "unit-a"
    assert payload["kpis"]["itens_cadastrados"]["value"] == 2
    names = [row["nome"] for row in payload["por_categoria"]]
    assert "Bateria" not in names
    assert "Mobília" not in names


def test_manager_can_read_another_unit_of_same_company(api_envs, warehouse, rsa_keys):
    client, _ = _client(api_envs, warehouse)

    response = client.get(
        "/api/v1/dashboard/overview",
        headers={**_auth(rsa_keys, MANAGER_A), "X-Unit-Id": UNIT_A2},
    )
    _, payload = _first_sse_event(response.text)

    assert payload["applied_filters"]["unit_id"] == UNIT_A2
    assert payload["kpis"]["itens_cadastrados"]["value"] == 1
    assert payload["por_categoria"][0]["nome"] == "Mobília"


def test_overview_does_not_leak_other_company(api_envs, warehouse, rsa_keys):
    client, _ = _client(api_envs, warehouse)

    response = client.get(
        "/api/v1/dashboard/overview?q=Segredo",
        headers=_auth(rsa_keys, MANAGER_A),
    )

    event, payload = _first_sse_event(response.text)
    assert event == "snapshot"
    assert payload["kpis"]["itens_cadastrados"]["value"] == 0


def test_other_company_does_not_see_org_a_kpis(api_envs, warehouse, rsa_keys):
    client, _ = _client(api_envs, warehouse)

    response = client.get(
        "/api/v1/dashboard/overview",
        headers=_auth(rsa_keys, MANAGER_B),
    )
    _, payload = _first_sse_event(response.text)

    assert payload["organization"]["name"] == "Empresa B"
    assert payload["applied_filters"]["unit_id"] == "unit-b"
    assert payload["kpis"]["itens_cadastrados"]["value"] == 1
    assert payload["kpis"]["impacto_recuperado"]["kg"] == 0.0


def test_employee_cannot_query_foreign_unit(api_envs, warehouse, rsa_keys):
    client, _ = _client(api_envs, warehouse)

    response = client.get(
        f"/api/v1/dashboard/overview?unit_id={UNIT_B}",
        headers=_auth(rsa_keys, EMPLOYEE_A, "EMPLOYEE"),
    )

    assert response.status_code == 400


def test_access_token_query_is_accepted(api_envs, warehouse, rsa_keys):
    client, _ = _client(api_envs, warehouse)
    token = encode_token(rsa_keys, MANAGER_A)

    response = client.get(f"/api/v1/dashboard/overview?access_token={token}")

    event, payload = _first_sse_event(response.text)
    assert event == "snapshot"
    assert payload["organization"]["id"] == "org-a"


def test_invalid_authorization_header_is_rejected(api_envs, warehouse):
    client, _ = _client(api_envs, warehouse)

    response = client.get(
        "/api/v1/dashboard/overview",
        headers={"Authorization": "Basic abc"},
    )

    assert response.status_code == 401


def test_cors_allows_configured_origin(api_envs, monkeypatch, warehouse):
    monkeypatch.setenv("CORS_ORIGINS", "https://gestao.zera.test")
    from config.environments import Environments

    client = TestClient(
        create_app(runtime=Environments(), warehouse=warehouse, sse_max_events=1)
    )
    response = client.options(
        "/api/v1/dashboard/overview",
        headers={
            "Origin": "https://gestao.zera.test",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert response.headers.get("access-control-allow-origin") == "https://gestao.zera.test"


def test_unknown_user_is_forbidden(api_envs, warehouse, rsa_keys):
    client, _ = _client(api_envs, warehouse)

    response = client.get(
        "/api/v1/dashboard/overview",
        headers=_auth(rsa_keys, "ghost"),
    )

    assert response.status_code == 403


def test_unavailable_when_cron_has_not_saved(api_envs, rsa_keys):
    client, _ = _client(api_envs)

    response = client.get(
        "/api/v1/dashboard/overview",
        headers=_auth(rsa_keys, MANAGER_A),
    )
    event, payload = _first_sse_event(response.text)

    assert event == "unavailable"
    assert payload["detail"] == "snapshot not ready"


def test_internal_publish_route_is_gone(api_envs):
    client, _ = _client(api_envs)

    response = client.post("/internal/snapshots", json={"batch_id": "x"})

    assert response.status_code == 404


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
