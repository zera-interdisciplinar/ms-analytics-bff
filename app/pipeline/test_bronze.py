import pytest

from app.pipeline.bronze import run_bronze
from app.pipeline.conftest import FakeNeo4j, FakePostgres, postgres_schema
from app.pipeline.tables import NEO_LABELS, PG_ENTITIES


def test_bronze_extracts_postgres_and_neo4j_into_one_batch(fake_postgres, fake_neo4j):
    result = run_bronze(fake_postgres, fake_neo4j)

    entities = set(result["manifest"]["entity"])
    expected = {f"pg_{name}" for name in PG_ENTITIES}
    expected.update(f"neo_{label.lower()}" for label in NEO_LABELS)
    expected.add("neo_relationships")

    assert entities == expected
    assert result["counts"]["pg_unit"] == 1
    assert result["counts"]["neo_item"] == 1
    assert result["bases"]["neo_item"].iloc[0]["id"] == "item-1"
    assert "embedding" not in result["bases"]["neo_item"].iloc[0]


def test_bronze_fails_when_postgres_schema_changes(fake_neo4j):
    postgres = FakePostgres(columns=[("unit", "id", "uuid")])

    with pytest.raises(ValueError, match="esquema PostgreSQL mudou"):
        run_bronze(postgres, fake_neo4j)


def test_bronze_fails_when_neo_labels_are_missing(fake_postgres):
    neo4j = FakeNeo4j(labels=["Item"])

    with pytest.raises(ValueError, match="Rótulos esperados ausentes"):
        run_bronze(fake_postgres, neo4j)


def test_bronze_uses_complete_pg_schema_helper():
    tables = {table for table, _, _ in postgres_schema()}
    assert tables == set(PG_ENTITIES)
