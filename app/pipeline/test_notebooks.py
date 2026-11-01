"""Databricks notebooks must keep the same projections as app.pipeline."""

from __future__ import annotations

from pathlib import Path

from app.pipeline.silver import COLUNAS_NEO4J, NOMES_PORTUGUES
from app.pipeline.tables import NEO_LABELS

NOTEBOOKS = Path(__file__).resolve().parents[2] / "notebooks"
BRONZE = (NOTEBOOKS / "Bronze - Zera.py").read_text(encoding="utf-8")
SILVER = (NOTEBOOKS / "Silver - Zera.py").read_text(encoding="utf-8")
GOLD = (NOTEBOOKS / "Gold - Zera.py").read_text(encoding="utf-8")


def test_bronze_extracts_the_same_graph_as_the_pipeline() -> None:
    for label in NEO_LABELS:
        assert f"'{label}'" in BRONZE
    assert "INCLUDES" in BRONZE
    assert "HAS_EVENT" in BRONZE


def test_silver_projects_the_same_neo4j_columns_as_the_pipeline() -> None:
    for entidade, colunas in COLUNAS_NEO4J.items():
        assert f"'{entidade}'" in SILVER
        for coluna in colunas:
            assert f"'{coluna}'" in SILVER, f"Silver missing {entidade}.{coluna}"
    assert "'cancelled'" in SILVER
    assert NOMES_PORTUGUES["createdBy"] in SILVER
    assert NOMES_PORTUGUES["createdByName"] in SILVER


def test_gold_reads_the_silver_tables_this_notebook_writes() -> None:
    assert "workspace.zera_silver.itens_tratados" in GOLD
    assert "workspace.zera_silver.eventos" in GOLD
    assert "workspace.zera_silver.pg_disposal_report" in GOLD
    assert "workspace.zera_silver.neo_relationships" in GOLD
    assert "workspace.zera_pipeline" not in GOLD
