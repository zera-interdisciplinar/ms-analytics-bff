import pandas as pd
import pytest
from neo4j.time import DateTime

from app.pipeline.bronze import run_bronze
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


def test_silver_requires_bronze_snapshot():
    with pytest.raises(ValueError, match="Execute a Bronze primeiro"):
        run_silver(None)


def test_silver_parses_neo4j_datetime_on_events(fake_postgres, fake_neo4j):
    bronze = run_bronze(fake_postgres, fake_neo4j)
    events = bronze["bases"]["neo_event"].copy()
    events["occurredAt"] = DateTime(2026, 9, 26, 20, 19, 12, 996834017)
    bronze["bases"]["neo_event"] = events

    result = run_silver(bronze)

    when = result["eventos"].iloc[0]["data_ocorrencia"]
    assert pd.notna(when)
    assert pd.Timestamp(when).year == 2026
