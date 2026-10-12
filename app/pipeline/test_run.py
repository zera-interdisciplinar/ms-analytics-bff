import pytest

from app.pipeline.run import run_pipeline


def test_pipeline_runs_bronze_silver_gold_on_the_same_batch(
    fake_postgres, fake_neo4j,
):
    result = run_pipeline(postgres=fake_postgres, neo4j=fake_neo4j)

    itens = result["silver"]["itens"]
    kpis = result["gold"]["tabelas"]["kpis_analiticos"]

    assert result["batch_id"] == result["bronze"]["batch_id"]
    assert result["silver"]["batch_id"] == result["batch_id"]
    assert result["gold"]["batch_id"] == result["batch_id"]
    assert len(itens) == 1
    assert "itens_cadastrados" in set(kpis["indicador"])


def test_pipeline_uses_live_sources_when_clients_are_omitted(monkeypatch):
    fake_postgres = object()
    fake_neo4j = object()
    monkeypatch.setattr(
        "app.pipeline.run.live_sources",
        lambda envs: (fake_postgres, fake_neo4j),
    )
    monkeypatch.setattr(
        "app.pipeline.run.run_bronze",
        lambda postgres, neo4j, max_rows: {"batch_id": "lote-1"},
    )
    monkeypatch.setattr(
        "app.pipeline.run.run_silver", lambda bronze: {"batch_id": "lote-1"},
    )
    monkeypatch.setattr(
        "app.pipeline.run.run_gold", lambda silver: {"batch_id": "lote-1"},
    )

    result = run_pipeline()

    assert result["batch_id"] == "lote-1"


def test_pipeline_rejects_layers_on_different_batches(monkeypatch):
    monkeypatch.setattr(
        "app.pipeline.run.run_bronze",
        lambda postgres, neo4j, max_rows: {"batch_id": "a"},
    )
    monkeypatch.setattr(
        "app.pipeline.run.run_silver", lambda bronze: {"batch_id": "b"},
    )
    monkeypatch.setattr(
        "app.pipeline.run.run_gold", lambda silver: {"batch_id": "a"},
    )

    with pytest.raises(ValueError, match="diverged on batch_id"):
        run_pipeline(postgres=object(), neo4j=object())
