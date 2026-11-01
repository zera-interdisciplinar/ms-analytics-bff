from unittest.mock import MagicMock, patch

import pytest

from config.environments import Environments
from app.pipeline.sources import Neo4jSource, PostgresSource, live_sources
from app.pipeline.tables import PG_FIELDS


def test_postgres_source_requires_host_and_password(monkeypatch):
    monkeypatch.delenv("PG_HOST", raising=False)
    monkeypatch.delenv("PG_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="PG_HOST e PG_PASSWORD"):
        PostgresSource(Environments())


def test_neo4j_source_requires_uri_and_password(monkeypatch):
    monkeypatch.delenv("NEO4J_URI", raising=False)
    monkeypatch.delenv("NEO4J_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="NEO4J_URI e NEO4J_PASSWORD"):
        Neo4jSource(Environments())


def _live_envs(monkeypatch):
    monkeypatch.setenv("PG_HOST", "db.local")
    monkeypatch.setenv("PG_PASSWORD", "secret")
    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.setenv("NEO4J_PASSWORD", "secret")
    return Environments()


@patch("app.pipeline.sources.pg8000.dbapi.connect")
def test_postgres_fetch_columns_and_entities(connect, monkeypatch):
    envs = _live_envs(monkeypatch)
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.side_effect = [
        [("unit", "id", "uuid")],
        [("u1", "Centro", "org-1", "2024-01-01", "2024-01-01")],
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connect.return_value = connection

    source = PostgresSource(envs)
    assert source.fetch_columns() == [("unit", "id", "uuid")]

    extracted = source.fetch_entities(
        {"unit": PG_FIELDS["unit"]}, max_rows=10,
    )
    assert extracted["unit"][0]["id"] == "u1"
    assert extracted["unit"][0]["name"] == "Centro"


@patch("app.pipeline.sources.GraphDatabase.driver")
def test_neo4j_verify_and_read_graph(driver_factory, monkeypatch):
    envs = _live_envs(monkeypatch)
    ok = MagicMock()
    ok.single.return_value = {"ok": 1}
    session = MagicMock()
    session.run.side_effect = [
        ok,
        [{"label": "Item"}],
        [{"relationshipType": "IS_MODEL"}],
    ]
    tx = MagicMock()
    node_row = {"p": {"id": "item-1", "embedding": [0.1]}}
    edge_row = MagicMock()
    edge_row.data.return_value = {
        "id": "rel-1", "source_id": "item-1", "target_id": "model-1",
        "relationship_type": "IS_MODEL",
    }
    tx.run.side_effect = [[node_row], [edge_row]]
    session.execute_read.side_effect = lambda fn: fn(tx)

    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = session
    driver_cm = MagicMock()
    driver_cm.__enter__.return_value = driver
    driver_factory.return_value = driver_cm

    source = Neo4jSource(envs)
    labels, rels = source.verify()
    assert labels == ["Item"]
    assert rels == ["IS_MODEL"]

    nodes, edges = source.read_graph(["Item"], max_rows=10)
    assert nodes["Item"] == [{"id": "item-1"}]
    assert edges[0]["relationship_type"] == "IS_MODEL"


def test_live_sources_build_both_clients(monkeypatch):
    postgres, neo4j = live_sources(_live_envs(monkeypatch))
    assert isinstance(postgres, PostgresSource)
    assert isinstance(neo4j, Neo4jSource)
