"""Gold: KPIs and analytical tables from the Silver batch."""

from __future__ import annotations

import json
from typing import cast

import numpy as np
import pandas as pd

from app.pipeline.types import GoldSnapshot, SilverSnapshot
from logger.logger import Logger

myLogger = Logger()


def run_gold(silver_snapshot: SilverSnapshot) -> GoldSnapshot:
    tabelas: dict[str, pd.DataFrame] = {}
    status_alerta_resolvido: set[str] = set()
    limite_linhas = 100_000
    manifest = silver_snapshot["manifest"]
    if manifest["batch_id"].nunique() != 1:
        raise ValueError("Manifesto Silver deve ter um único lote.")
    batch_id = str(manifest["batch_id"].iloc[0])
    bases = silver_snapshot["bases"]

    def percentual(numerador: float, denominador: float) -> float | None:
        return numerador / denominador * 100 if denominador else None

    def publicar(nome: str, df: pd.DataFrame) -> None:
        publicado = df.copy()
        publicado["batch_id"] = batch_id
        publicado["published_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
        tabelas[nome] = publicado

    itens = silver_snapshot["itens"].rename(columns={
        "situacao": "status",
        "indicador_danos_origem": "hasDamages",
        "danos": "damages",
        "alerta_qualidade_dos_dados": "flag_atencao_dados",
    })
    eventos = silver_snapshot["eventos"].rename(columns={
        "data_ocorrencia": "occurred_at",
        "id_unidade": "unitId",
        "tipo_evento": "type",
        "situacao_destino": "toStatus",
        "id_item": "itemId",
    })
    relatorios = bases["pg_disposal_report"]
    alertas = bases["pg_alert"]
    relacoes_material = bases["neo_relationships"]
    materiais_pipeline = bases["neo_material"].rename(columns={
        "id": "material_id",
    })
    composicao = (
        relacoes_material.loc[
            relacoes_material["relationship_type"].eq("MADE_OF"),
            ["batch_id", "extracted_at", "source_id", "target_id"],
        ]
        .rename(columns={"source_id": "id_modelo", "target_id": "material_id"})
        .merge(
            materiais_pipeline[["batch_id", "material_id", "name", "recyclable", "hazardous"]],
            on=["batch_id", "material_id"], how="left", validate="many_to_one",
        )
    )
    cadastros: dict[str, pd.DataFrame] = {
        "pg_unit": bases["pg_unit"],
        "pg_organization": bases["pg_organization"],
        "neo_material": bases["neo_material"],
        "neo_model": bases["neo_model"],
    }
    configuracoes_unidade = bases["neo_unitinventorysettings"]
    itens["percentual_vida_util_consumida"] = (
        itens["idade_meses"] / itens["vida_util_estimada_meses"] * 100
    ).where(itens["vida_util_estimada_meses"].gt(0))
    itens["mes_ano_cadastro"] = pd.to_datetime(
        itens["data_cadastro"], errors="coerce",
    ).dt.strftime("%Y-%m")
    itens["faixa_idade"] = pd.cut(
        itens["idade_meses"],
        bins=[0, 12, 24, 48, np.inf],
        labels=[
            "Menos de 12 meses", "12 a menos de 24 meses",
            "24 a menos de 48 meses", "48 meses ou mais",
        ],
        right=False,
    ).astype("string").fillna("Não informada")

    for df in [
        itens, eventos, relatorios, alertas, composicao,
        configuracoes_unidade, *cadastros.values(),
    ]:
        if not df.empty and not df["batch_id"].eq(batch_id).all():
            raise ValueError("As tabelas Silver pertencem a lotes diferentes.")
    if not itens["id_item"].is_unique:
        raise ValueError("IDs de item repetidos na Silver.")

    data_referencia = pd.Timestamp(manifest["extracted_at"].iloc[0]).date()
    total_itens = len(itens)
    myLogger.Info(f"Gold QA referencia={data_referencia} itens={total_itens}")

    indicadores: list[list[object]] = []

    def registrar_kpi(
        nome: str,
        valor: float | None,
        unidade: str,
        denominador: float,
        formula: str,
        importancia: str,
        ressalva: str = "",
        recorte: str | None = None,
    ) -> None:
        valor = None if valor is None or pd.isna(valor) else float(valor)
        indicadores.append([
            nome, valor, unidade, denominador, formula, importancia, ressalva, recorte,
        ])

    registrar_kpi(
        "itens_cadastrados", total_itens, "itens", total_itens,
        "IDs distintos", "Dimensionar o cadastro",
        "Inclui DRAFT; não equivale a estoque ativo.",
    )

    por_categoria = (
        itens.assign(categoria=itens["categoria"].fillna("Não informada"))
        .groupby("categoria", dropna=False)
        .agg(quantidade_itens=("id_item", "nunique"))
        .reset_index()
    )
    por_categoria["percentual_itens"] = por_categoria["quantidade_itens"].map(
        lambda quantidade: percentual(quantidade, total_itens)
    )
    publicar("itens_por_categoria", por_categoria)
    for linha in por_categoria.itertuples(index=False):
        registrar_kpi(
            "itens_por_categoria", linha.quantidade_itens, "itens", total_itens,
            "Contagem de IDs distintos por categoria",
            "Comparar a composição do inventário", recorte=linha.categoria,
        )

    por_situacao = (
        itens.assign(status=itens["status"].fillna("Não informada"))
        .groupby("status", dropna=False)
        .agg(quantidade_itens=("id_item", "nunique"))
        .reset_index()
        .rename(columns={"status": "situacao"})
    )
    por_situacao["percentual_itens"] = por_situacao["quantidade_itens"].map(
        lambda quantidade: percentual(quantidade, total_itens)
    )
    publicar("itens_por_situacao", por_situacao)
    for linha in por_situacao.itertuples(index=False):
        registrar_kpi(
            "itens_por_situacao", linha.quantidade_itens, "itens", total_itens,
            "Contagem de IDs distintos por situação",
            "Acompanhar a composição do inventário", recorte=linha.situacao,
        )

    danos_conhecidos = itens["possui_danos"].dropna()
    itens_com_danos = int(danos_conhecidos.astype(bool).sum())
    registrar_kpi(
        "itens_com_danos", itens_com_danos, "itens", len(danos_conhecidos),
        "Contagem de itens com indicador de danos verdadeiro",
        "Dimensionar os itens com danos informados",
        "O denominador inclui apenas itens com informação conhecida.",
    )

    total_modelos = cadastros["neo_model"]["id"].nunique()
    registrar_kpi(
        "modelos_cadastrados", total_modelos, "modelos", total_modelos,
        "Contagem de IDs distintos", "Dimensionar o catálogo de modelos",
    )
    registrar_kpi(
        "alertas_registrados", len(alertas), "alertas", len(alertas),
        "Contagem de IDs de alerta", "Acompanhar os alertas gerados",
    )
    registrar_kpi(
        "eventos_por_item", len(eventos) / total_itens if total_itens else None,
        "eventos/item", total_itens, "Total de eventos / total de itens",
        "Dimensionar o histórico médio registrado por item",
        "Não é uma taxa mensal; inclui eventos de criação.",
    )
    registrar_kpi(
        "potencial_reciclavel_itens_pct",
        percentual(
            int(itens.loc[
                itens["possui_material_reciclavel"].notna(),
                "possui_material_reciclavel",
            ].sum()),
            int(itens["possui_material_reciclavel"].notna().sum()),
        ),
        "%", int(itens["possui_material_reciclavel"].notna().sum()),
        "Itens com potencial reciclável / itens com classificação conhecida × 100",
        "Descrever o potencial da composição cadastrada",
        "Não comprova reciclagem nem representa percentual de massa.",
    )

    classificacao_material = cadastros["neo_material"]["recyclable"]
    materiais_classificados = int(classificacao_material.notna().sum())
    registrar_kpi(
        "materiais_com_classificacao_reciclabilidade_pct",
        percentual(materiais_classificados, len(classificacao_material)), "%",
        len(classificacao_material), "Classificados / total de materiais × 100",
        "Avaliar cobertura da informação ambiental do catálogo",
    )

    alertas_com_situacao = alertas["status"].dropna().astype("string").str.upper()
    denominador_alertas = len(alertas_com_situacao)
    alertas_resolvidos = int(alertas_com_situacao.isin(status_alerta_resolvido).sum())
    taxa_alertas_resolvidos = (
        percentual(alertas_resolvidos, denominador_alertas)
        if status_alerta_resolvido else None
    )
    ressalva_status_alerta = (
        "Confirmar os valores de status que significam alerta resolvido."
        if not status_alerta_resolvido
        else "Sem alertas com status conhecido: não calculável."
    )
    registrar_kpi(
        "taxa_alertas_resolvidos_pct", taxa_alertas_resolvidos, "%",
        denominador_alertas,
        "Alertas resolvidos / alertas com situação conhecida × 100",
        "Acompanhar o tratamento dos alertas", ressalva_status_alerta,
    )

    kpis = pd.DataFrame(indicadores, columns=[
        "indicador", "valor", "unidade", "denominador",
        "formula", "importancia", "ressalva", "recorte",
    ])
    publicar("kpis_analiticos", kpis)
    contagens = {
        "itens_cadastrados": total_itens,
        "eventos_registrados": len(eventos),
        "unidades_cadastradas": len(cadastros["pg_unit"]),
        "organizacoes_cadastradas": len(cadastros["pg_organization"]),
        "materiais_cadastrados": len(cadastros["neo_material"]),
        "modelos_cadastrados": len(cadastros["neo_model"]),
        "unidades_com_inventario": int(itens["id_unidade"].dropna().nunique()),
        "relatorios_descarte_registrados": len(relatorios),
        "alertas_registrados": len(alertas),
    }
    publicar("kpis_gerais",
        pd.DataFrame(contagens.items(), columns=["indicador", "valor"]),
    )
    publicar("itens_analise", itens)
    publicar("itens_dashboard", itens)

    por_status = por_situacao.rename(columns={
        "situacao": "status", "percentual_itens": "percentual",
    })
    publicar("itens_por_status", por_status)
    grupos = [
        "id_unidade", "unidade", "organizacao", "cidade", "uf",
        "categoria", "status", "condicao",
    ]
    perfil = (
        itens.groupby(grupos, dropna=False)
        .agg(
            quantidade_itens=("id_item", "size"),
            idade_media_meses=("idade_meses", "mean"),
        )
        .reset_index()
    )
    publicar("perfil_inventario", perfil)
    publicar("itens_por_unidade_categoria", perfil)
    itens_por_unidade = (
        itens.dropna(subset=["id_unidade"])
        .groupby(["id_unidade", "unidade"], dropna=False)
        .agg(quantidade_itens=("id_item", "nunique"))
        .reset_index()
    )
    publicar("unidades_com_inventario", itens_por_unidade)

    if configuracoes_unidade["unitId"].duplicated().any():
        raise ValueError("Há mais de uma configuração de capacidade por unidade.")
    capacidade_unidade = configuracoes_unidade[["unitId", "stockCapacity"]].rename(
        columns={"unitId": "id_unidade", "stockCapacity": "capacidade_cadastrada"},
    )
    ocupacao_por_unidade = itens_por_unidade.merge(
        capacidade_unidade, on="id_unidade", how="left", validate="one_to_one",
    )
    capacidade_positiva = ocupacao_por_unidade["capacidade_cadastrada"].gt(0)
    ocupacao_por_unidade["ocupacao_capacidade_pct"] = (
        ocupacao_por_unidade["quantidade_itens"]
        .div(ocupacao_por_unidade["capacidade_cadastrada"])
        .mul(100)
        .where(capacidade_positiva)
    )
    ocupacao_por_unidade["definicao_validada"] = False
    publicar("ocupacao_capacidade_unidade_candidata", ocupacao_por_unidade)

    distribuicao = (
        itens.groupby(["categoria", "faixa_idade"], dropna=False)
        .size()
        .reset_index(name="quantidade_itens")
    )
    publicar("distribuicao_idade", distribuicao)
    por_material = itens[["id_item", "id_modelo"]].merge(
        composicao, on="id_modelo", how="inner",
    )
    por_material = (
        por_material.groupby(
            ["material_id", "name", "recyclable", "hazardous"], dropna=False,
        )["id_item"]
        .nunique()
        .reset_index(name="itens_associados")
    )
    publicar("itens_por_material", por_material)

    datas_cadastro = pd.to_datetime(itens["data_cadastro"], errors="coerce").dt.normalize()
    datas_validas = datas_cadastro.dropna()
    itens_calendario = itens.assign(
        id_unidade=itens["id_unidade"].fillna("Não informada")
    )
    unidades_calendario = pd.concat([
        cadastros["pg_unit"]["id"], itens_calendario["id_unidade"],
    ], ignore_index=True).fillna("Não informada").drop_duplicates().tolist()

    if datas_validas.empty or not unidades_calendario:
        diario = pd.DataFrame(columns=["data", "id_unidade", "cadastros"])
    else:
        calendario = pd.date_range(datas_validas.min(), datas_validas.max(), freq="D")
        linhas_calendario = len(calendario) * len(unidades_calendario)
        if linhas_calendario > limite_linhas:
            raise ValueError(
                "O calendário diário excedeu o limite de linhas desta análise pandas."
            )
        grade_diaria = pd.MultiIndex.from_product(
            [calendario, unidades_calendario], names=["data", "id_unidade"]
        ).to_frame(index=False)
        cadastros_diarios_observados = (
            itens_calendario.assign(data=datas_cadastro)
            .dropna(subset=["data"])
            .groupby(["data", "id_unidade"], dropna=False)
            .size()
            .reset_index(name="cadastros")
        )
        diario = grade_diaria.merge(
            cadastros_diarios_observados,
            on=["data", "id_unidade"], how="left", validate="one_to_one",
        )
        diario["cadastros"] = diario["cadastros"].fillna(0).astype("int64")
        diario["data"] = diario["data"].dt.date
    publicar("evolucao_cadastros", diario)
    publicar("cadastros_diarios", diario)

    if diario.empty:
        mensal = pd.DataFrame(columns=[
            "mes_ano_cadastro", "id_unidade", "cadastros",
            "mes_anterior_observado", "cadastros_mes_anterior", "crescimento_pct",
        ])
    else:
        mensal = diario.assign(
            mes_ano=pd.to_datetime(diario["data"]).dt.strftime("%Y-%m")
        ).groupby(["mes_ano", "id_unidade"], dropna=False)["cadastros"].sum().reset_index()
        mensal = mensal.rename(columns={"mes_ano": "mes_ano_cadastro"})
        mensal = mensal.sort_values(["id_unidade", "mes_ano_cadastro"])
        mensal["mes_anterior_observado"] = mensal.groupby(
            "id_unidade", dropna=False,
        )["mes_ano_cadastro"].shift()
        mensal["cadastros_mes_anterior"] = mensal.groupby(
            "id_unidade", dropna=False,
        )["cadastros"].shift()
        mes_atual = pd.to_datetime(mensal["mes_ano_cadastro"], format="%Y-%m")
        mes_anterior = pd.to_datetime(mensal["mes_anterior_observado"], format="%Y-%m")
        consecutivos = (mes_atual - pd.DateOffset(months=1)).eq(mes_anterior)
        mensal["crescimento_pct"] = (
            (mensal["cadastros"] / mensal["cadastros_mes_anterior"] - 1) * 100
        ).where(consecutivos & mensal["cadastros_mes_anterior"].gt(0))
    publicar("cadastros_mensais", mensal)

    eventos["data"] = pd.to_datetime(eventos["occurred_at"]).dt.date
    eventos_diarios = (
        eventos.groupby(["data", "unitId", "type"], dropna=False)
        .size().reset_index(name="quantidade_eventos")
    )
    publicar("eventos_diarios", eventos_diarios)
    eventos["mes"] = pd.to_datetime(eventos["occurred_at"]).dt.strftime("%Y-%m")
    eventos_mensais = (
        eventos.groupby(["mes", "type", "unitId"], dropna=False)
        .size()
        .reset_index(name="quantidade_eventos")
        .sort_values(["mes", "type", "unitId"])
    )
    publicar("eventos_mensais", eventos_mensais)
    publicar("alertas_por_status",
        alertas.groupby(["status", "unit_id"], dropna=False)
        .size().reset_index(name="quantidade_alertas"),
    )
    publicar("alertas_por_gravidade",
        alertas.groupby(["severity", "unit_id"], dropna=False)
        .size().reset_index(name="quantidade_alertas"),
    )
    publicar("alertas_por_status_e_gravidade",
        alertas.groupby(["status", "severity", "kind", "unit_id"], dropna=False)
        .size().reset_index(name="quantidade_alertas"),
    )
    publicar("relatorios_descarte_por_status",
        relatorios.groupby(["status", "unit_id"], dropna=False)
        .size().reset_index(name="quantidade_relatorios"),
    )

    if int(diario["cadastros"].sum()) != total_itens:
        raise ValueError("As contagens temporais não reconciliam com os itens.")
    conclusoes = [
        f'Cadastro QA: {total_itens} itens, {len(eventos)} eventos, {len(cadastros["neo_material"])} materiais.',
        f"Relatórios de descarte: {len(relatorios)}; alertas: {len(alertas)}.",
    ]
    if relatorios.empty:
        conclusoes.append("Sem relatórios de descarte: taxa de confirmação não calculável.")
    if not status_alerta_resolvido:
        conclusoes.append(
            "Taxa de alertas resolvidos pendente: falta confirmar quais valores de status contam como resolvidos."
        )
    if materiais_classificados < len(classificacao_material):
        conclusoes.append(
            f"Classificação de reciclabilidade preenchida em {materiais_classificados} de {len(classificacao_material)} materiais."
        )
    publicar("conclusoes_gerenciais",
        pd.DataFrame({"ordem": range(1, len(conclusoes) + 1), "conclusao": conclusoes}),
    )
    publicar("manifest", manifest)
    resultado_kpis = json.loads(
        kpis[["indicador", "valor", "unidade", "denominador", "ressalva", "recorte"]]
        .to_json(orient="records", force_ascii=False)
    )
    myLogger.Info(f"Gold finished batch_id={batch_id}")
    return {
        "batch_id": batch_id,
        "kpis": contagens,
        "tabelas": tabelas,
        "metricas": cast(list[dict[str, object]], resultado_kpis),
        "conclusoes": conclusoes,
    }
