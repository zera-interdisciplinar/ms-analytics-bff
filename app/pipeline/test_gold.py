from app.pipeline.bronze import run_bronze
from app.pipeline.gold import run_gold
from app.pipeline.silver import run_silver


def test_gold_builds_kpis_from_silver_dataframes(fake_postgres, fake_neo4j):
    silver = run_silver(run_bronze(fake_postgres, fake_neo4j))

    result = run_gold(silver)

    kpis = result["tabelas"]["kpis_analiticos"]
    gerais = result["tabelas"]["kpis_gerais"]
    por_categoria = result["tabelas"]["itens_por_categoria"]
    cadastrados = kpis.loc[kpis["indicador"].eq("itens_cadastrados"), "valor"].iloc[0]
    gerais_map = dict(zip(gerais["indicador"], gerais["valor"]))

    assert result["kpis"]["itens_cadastrados"] == 1
    assert cadastrados == 1
    assert gerais_map["alertas_registrados"] == 1
    assert gerais_map["relatorios_descarte_registrados"] == 1
    assert por_categoria.iloc[0]["categoria"] == "Informática"
