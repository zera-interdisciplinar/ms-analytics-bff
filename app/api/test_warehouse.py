import json
from unittest.mock import MagicMock, patch

import pytest

from app.api.conftest import ORG_A, ORG_B, UNIT_A, UNIT_A2, UNIT_B, MANAGER_A, MANAGER_B
from app.api.serialize import to_wire
from app.api.warehouse import (
    InMemoryWarehouse,
    PostgresWarehouse,
    identities_from,
    live_warehouse,
    partition,
    save_silver,
)
from app.pipeline.types import SilverSnapshot
from config.environments import Environments
import pandas as pd


def test_partition_keeps_each_unit_isolated(snapshot):
    parts = {(org, unit): sliced for org, unit, sliced in partition(snapshot)}

    assert (ORG_A, UNIT_A) in parts
    assert (ORG_A, UNIT_A2) in parts
    assert (ORG_B, UNIT_B) in parts
    assert list(parts[(ORG_A, UNIT_A)]["itens"]["id_item"]) == ["item-a1", "item-a2"]
    assert list(parts[(ORG_A, UNIT_A2)]["itens"]["id_item"]) == ["item-a3"]
    assert "Segredo da outra empresa" not in set(parts[(ORG_A, UNIT_A)]["itens"]["nome_item"])
    assert list(parts[(ORG_B, UNIT_B)]["itens"]["nome_item"]) == ["Segredo da outra empresa"]
    assert list(parts[(ORG_A, UNIT_A)]["includes"]["source_id"]) == ["disp-a"]
    assert list(parts[(ORG_B, UNIT_B)]["includes"]["source_id"]) == ["disp-b"]


def test_warehouse_load_is_always_by_company_and_unit(snapshot):
    warehouse = InMemoryWarehouse()
    warehouse.save(snapshot)

    unit_a = warehouse.load(ORG_A, UNIT_A)
    leaked = warehouse.load(ORG_A, UNIT_B)
    other = warehouse.load(ORG_B, UNIT_B)

    assert unit_a is not None
    assert leaked is None
    assert other is not None
    assert "Bateria" not in set(unit_a["itens"]["categoria"].astype("string"))
    assert warehouse.identity(MANAGER_A)["organization_id"] == ORG_A
    assert warehouse.identity(MANAGER_B)["organization_id"] == ORG_B
    assert warehouse.is_empty() is False


def test_save_silver_persists_identity_and_unit_slice(snapshot):
    warehouse = InMemoryWarehouse()
    silver: SilverSnapshot = {
        "batch_id": snapshot["batch_id"],
        "extracted_at": snapshot["extracted_at"],
        "manifest": pd.DataFrame(),
        "itens": snapshot["itens"],
        "eventos": snapshot["eventos"],
        "bases": {
            "pg_user_account": snapshot["users"],
            "pg_unit": snapshot["units"],
            "pg_organization": snapshot["organizations"],
            "pg_alert": snapshot["alerts"],
            "neo_disposal": snapshot["disposals"],
            "neo_relationships": snapshot["includes"],
        },
        "composicao": snapshot["composicao"],
        "qualidade": pd.DataFrame(),
        "dicionario": pd.DataFrame(),
    }

    save_silver(silver, warehouse)

    assert warehouse.identity(MANAGER_A)["unit_id"] == UNIT_A
    assert UNIT_A2 in warehouse.identity(MANAGER_A)["allowed_unit_ids"]
    assert len(identities_from(snapshot)) == 3


def test_wire_payload_is_json_with_event_timestamps(snapshot):
    sliced = {**snapshot, "eventos": snapshot["eventos"].copy()}
    sliced["eventos"]["data_ocorrencia"] = pd.Timestamp("2026-09-26T20:19:12")

    json.dumps(to_wire(sliced))


def test_postgres_warehouse_requires_credentials(monkeypatch):
    monkeypatch.delenv("PG_HOST", raising=False)
    monkeypatch.delenv("PG_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="PG_HOST e PG_PASSWORD"):
        PostgresWarehouse(Environments())


@patch("app.api.warehouse.pg8000.dbapi.connect")
def test_postgres_save_writes_identity_and_unit_rows(connect, monkeypatch, snapshot):
    monkeypatch.setenv("PG_HOST", "db.local")
    monkeypatch.setenv("PG_PASSWORD", "secret")
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connect.return_value = connection

    PostgresWarehouse(Environments()).save(snapshot)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("CREATE SCHEMA IF NOT EXISTS analytics" in sql for sql in statements)
    assert any("DELETE FROM analytics.user_scope" in sql for sql in statements)
    assert any("INSERT INTO analytics.unit_snapshot" in sql for sql in statements)
    unit_inserts = [
        call.args[1] for call in cursor.execute.call_args_list
        if "INSERT INTO analytics.unit_snapshot" in call.args[0]
    ]
    keys = {(row[0], row[1]) for row in unit_inserts}
    assert (ORG_A, UNIT_A) in keys
    assert (ORG_B, UNIT_B) in keys
    connection.commit.assert_called_once()


@patch("app.api.warehouse.pg8000.dbapi.connect")
def test_postgres_load_queries_organization_and_unit(connect, monkeypatch, snapshot):
    monkeypatch.setenv("PG_HOST", "db.local")
    monkeypatch.setenv("PG_PASSWORD", "secret")
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        {"batch_id": "lote-1", "extracted_at": "t", "tables": {}},
    )
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connect.return_value = connection

    loaded = PostgresWarehouse(Environments()).load(ORG_A, UNIT_A)

    sql = cursor.execute.call_args.args[0]
    params = cursor.execute.call_args.args[1]
    assert "organization_id = %s AND unit_id = %s" in sql
    assert params == (ORG_A, UNIT_A)
    assert loaded is not None
    assert loaded["batch_id"] == "lote-1"


@patch("app.api.warehouse.pg8000.dbapi.connect")
def test_live_warehouse_uses_postgres(connect, monkeypatch):
    monkeypatch.setenv("PG_HOST", "db.local")
    monkeypatch.setenv("PG_PASSWORD", "secret")

    warehouse = live_warehouse(Environments())

    assert isinstance(warehouse, PostgresWarehouse)
