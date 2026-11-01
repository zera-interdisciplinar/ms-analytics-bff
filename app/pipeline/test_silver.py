import uuid
from copy import deepcopy

import pytest

from app.pipeline.bronze import run_bronze
from app.pipeline.conftest import FakeNeo4j, FakePostgres, neo_nodes, postgres_tables
from app.pipeline.silver import run_silver


def test_silver_integrates_the_bronze_dataframes(fake_postgres, fake_neo4j):
    bronze = run_bronze(fake_postgres, fake_neo4j)

    result = run_silver(bronze)

    itens = result["itens"]
    eventos = result["eventos"]

    assert result["batch_id"] == bronze["batch_id"]
    assert len(itens) == 1
    assert len(eventos) == 1
    assert result["manifest"]["batch_id"].nunique() == 1
    assert itens.iloc[0]["id_item"] == "item-1"
    assert itens.iloc[0]["modelo"] == "ZBook"
    assert itens.iloc[0]["categoria"] == "Informática"
    assert itens.iloc[0]["unidade"] == "Unidade Centro"
    assert "id_responsavel" in itens.columns
    assert "neo_disposal" in result["bases"]
    assert bool(itens.iloc[0]["possui_material_reciclavel"]) is True
    assert eventos.iloc[0]["id_item"] == "item-1"


def test_silver_joins_postgres_uuid_keys_to_neo4j_strings():
    unit = uuid.UUID("2708ed37-1673-4814-8b50-3c16bacb01da")
    org = uuid.UUID("5796d0eb-3465-4dac-95e3-c1a1a8f4f0a6")
    tables = deepcopy(postgres_tables())
    tables["organization"][0]["id"] = org
    tables["unit"][0]["id"] = unit
    tables["unit"][0]["organization_id"] = org
    for tabela, campo in (
        ("user_account", "unit_id"),
        ("address", "unit_id"),
        ("alert", "unit_id"),
        ("disposal_report", "unit_id"),
    ):
        tables[tabela][0][campo] = unit
    nodes = neo_nodes()
    for rotulo in ("Item", "Event", "Model", "Category", "UnitInventorySettings"):
        nodes[rotulo][0]["unitId"] = str(unit)

    result = run_silver(run_bronze(FakePostgres(tables=tables), FakeNeo4j(nodes=nodes)))
    item = result["itens"].iloc[0]

    assert bool(item["unidade_encontrada"]) is True
    assert item["id_organizacao"] == str(org)
    assert item["unidade"] == "Unidade Centro"


def test_silver_requires_bronze_snapshot():
    with pytest.raises(ValueError, match="Execute a Bronze primeiro"):
        run_silver(None)
