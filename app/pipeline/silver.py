"""Silver: validate Bronze snapshots, standardize types and integrate inventory."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from app.pipeline.types import BronzeSnapshot, QualityCheck, SilverSnapshot
from logger.logger import Logger

myLogger = Logger()

COLUNAS_POSTGRES: dict[str, list[str]] = {
    "address": [
        "id", "city", "state", "unit_id", "recycling_business_id",
        "created_at", "updated_at",
    ],
    "alert": [
        "id", "status", "unit_id", "created_at", "updated_at", "event_id",
        "rule_id", "user_id", "severity", "kind", "occurred_at",
    ],
    "disposal_report": [
        "id", "status", "user_id", "unit_id", "confirmed_by", "confirmed_at",
        "batch_id", "created_at", "updated_at",
    ],
    "organization": ["id", "name", "status", "created_at", "updated_at", "plan"],
    "recycling_business": ["id", "name", "created_at", "updated_at"],
    "unit": ["id", "name", "organization_id", "created_at", "updated_at"],
    "user_account": [
        "id", "role", "status", "unit_id", "created_at", "updated_at", "manager_id",
    ],
}

COLUNAS_NEO4J: dict[str, list[str]] = {
    "category": ["id", "unitId", "name"],
    "event": ["id", "itemId", "unitId", "type", "toStatus", "occurredAt"],
    "item": [
        "id", "unitId", "name", "status", "condition", "createdAt", "updatedAt",
        "acquiredAt", "lastEventAt", "manufacturingYear", "usageIntensity",
        "hasDamages", "damages", "createdBy", "createdByName",
    ],
    "disposal": [
        "id", "unitId", "destination", "disposedAt", "cancelled", "createdAt",
    ],
    "material": ["id", "name", "code", "recyclable", "hazardous"],
    "model": [
        "id", "unitId", "name", "manufacturer", "estimatedWeightKg",
        "expectedLifespanMonths", "warrantyMonths", "approvalStatus",
    ],
    "relationships": ["id", "source_id", "target_id", "relationship_type"],
    "unitinventorysettings": ["unitId", "stockCapacity"],
}

NOMES_PORTUGUES: dict[str, str] = {
    "acquiredAt": "data_aquisicao",
    "category_id": "id_categoria",
    "category_name": "categoria",
    "city": "cidade",
    "condition": "condicao",
    "createdBy": "id_responsavel",
    "createdByName": "responsavel",
    "createdAt": "data_cadastro",
    "damages": "danos",
    "estimatedWeightKg": "peso_estimado_kg",
    "expectedLifespanMonths": "vida_util_estimada_meses",
    "hasDamages": "indicador_danos_origem",
    "id": "id_item",
    "lastEventAt": "data_ultimo_evento",
    "manufacturer": "fabricante",
    "manufacturingYear": "ano_fabricacao",
    "model_id": "id_modelo",
    "model_name": "modelo",
    "name": "nome_item",
    "organization_id": "id_organizacao",
    "organization_name": "organizacao",
    "status": "situacao",
    "state": "uf",
    "unitId": "id_unidade",
    "unit_name": "unidade",
    "updatedAt": "data_atualizacao",
    "usageIntensity": "intensidade_uso",
    "warrantyMonths": "garantia_meses",
}

NOMES_EVENTOS: dict[str, str] = {
    "id": "id_evento",
    "itemId": "id_item",
    "unitId": "id_unidade",
    "type": "tipo_evento",
    "toStatus": "situacao_destino",
    "occurredAt": "data_ocorrencia",
}

NOMES_COMPOSICAO: dict[str, str] = {
    "model_id": "id_modelo",
    "material_id": "id_material",
    "name": "nome_material",
    "recyclable": "material_reciclavel",
    "hazardous": "material_perigoso",
}


def run_silver(bronze_snapshot: BronzeSnapshot | None) -> SilverSnapshot:
    if bronze_snapshot is None or not bronze_snapshot["bases"]:
        raise ValueError("Execute a Bronze primeiro.")
    bases_bronze = bronze_snapshot["bases"]
    batch_id = bronze_snapshot["batch_id"]
    extracted_at = bronze_snapshot["extracted_at"]
    manifest = bronze_snapshot["manifest"]
    data_referencia = (
        pd.Timestamp(extracted_at)
        .tz_convert("UTC")
        .tz_localize(None)
        .normalize()
    )
    bases: dict[str, pd.DataFrame] = {}
    verificacoes: list[QualityCheck] = []
    myLogger.Info(f"Silver lote={batch_id} referencia={data_referencia.date()}")

    def contar_linhas_duplicadas(df: pd.DataFrame) -> int:
        dados_comparacao = df.copy()
        for coluna in dados_comparacao.columns:
            serie = dados_comparacao[coluna]
            if pd.api.types.is_object_dtype(serie.dtype):
                contem_valores_aninhados = serie.map(
                    lambda valor: isinstance(valor, (list, dict, set, np.ndarray))
                ).any()
                if contem_valores_aninhados:
                    dados_comparacao[coluna] = serie.map(
                        lambda valor: json.dumps(
                            valor, ensure_ascii=False, sort_keys=True, default=str,
                        ) if isinstance(valor, (list, dict, set, np.ndarray)) else valor
                    )
        return int(dados_comparacao.duplicated().sum())

    def carregar_entidade(entidade: str, colunas: list[str]) -> pd.DataFrame:
        bruto = bases_bronze[entidade]
        quantidade_esperada = int(
            manifest.loc[manifest["entity"].eq(entidade), "row_count"].iloc[0]
        )
        if len(bruto) != quantidade_esperada:
            raise ValueError("Contagem diferente do manifesto: " + entidade)
        df = bruto.reindex(columns=colunas)
        df = df.rename(columns={"batch_id": "lote_origem_id"})
        df["batch_id"] = batch_id
        df["extracted_at"] = extracted_at
        chave = "id" if "id" in df.columns else "unitId"
        ausentes = int(
            (df[chave].isna() | df[chave].astype("string").str.strip().eq("")).sum()
        )
        repetidos = int(df[chave].duplicated().sum())
        verificacoes.extend([
            (entidade, "chave_ausente", ausentes),
            (entidade, "chave_duplicada", repetidos),
        ])
        if ausentes or repetidos:
            raise ValueError("Chave inválida em " + entidade)
        return df

    def converter_data(df: pd.DataFrame, coluna: str, entidade: str) -> None:
        origem = df[coluna].copy()
        nativas = origem.map(
            lambda valor: valor.to_native() if hasattr(valor, "to_native") else valor
        )
        datas = pd.to_datetime(nativas, errors="coerce", format="mixed")
        invalidas = int((origem.notna() & datas.isna()).sum())
        nomes_datas = {
            "created_at": "data_criacao_sql",
            "updated_at": "data_atualizacao_sql",
            "confirmed_at": "data_confirmacao_sql",
            "occurred_at": "data_ocorrencia_sql",
            "createdAt": "data_cadastro",
            "updatedAt": "data_atualizacao",
            "acquiredAt": "data_aquisicao",
            "lastEventAt": "data_ultimo_evento",
            "occurredAt": "data_ocorrencia",
        }
        verificacoes.append(
            (entidade, "datas_invalidas_" + nomes_datas.get(coluna, coluna), invalidas)
        )
        if invalidas:
            raise ValueError("Data inválida: " + entidade + "." + coluna)
        if isinstance(datas.dtype, pd.DatetimeTZDtype):
            raise ValueError("Confirmar fuso da fonte antes de converter: " + coluna)
        df[coluna] = datas

    def presenca_material(serie: pd.Series) -> bool | object:
        if serie.fillna(False).any():
            return True
        return False if serie.notna().all() else pd.NA

    for entidade, colunas in COLUNAS_POSTGRES.items():
        bases["pg_" + entidade] = carregar_entidade("pg_" + entidade, colunas)
    for entidade, colunas in COLUNAS_NEO4J.items():
        bases["neo_" + entidade] = carregar_entidade("neo_" + entidade, colunas)

    perfis = []
    for entidade, df in bases.items():
        duplicatas_linha_completa = contar_linhas_duplicadas(df)
        for coluna in df.columns:
            if coluna not in ["batch_id", "extracted_at"]:
                nulos = int(df[coluna].isna().sum())
                perfis.append([
                    entidade, coluna, str(df[coluna].dtype), len(df), nulos,
                    nulos / len(df) * 100 if len(df) else np.nan,
                    duplicatas_linha_completa,
                ])
    perfil = pd.DataFrame(perfis, columns=[
        "entidade", "coluna_origem", "tipo", "linhas", "nulos",
        "percentual_nulos", "duplicatas_linha_completa",
    ])
    for df in bases.values():
        for coluna in df.columns:
            if coluna != "damages":
                df[coluna] = df[coluna].map(
                    lambda valor: valor.strip() if isinstance(valor, str) else valor
                )

    for entidade, colunas in COLUNAS_POSTGRES.items():
        df = bases["pg_" + entidade]
        if entidade == "alert":
            df["status"] = df["status"].astype("string").str.upper()
        for coluna in colunas:
            if coluna.endswith("_at"):
                converter_data(df, coluna, "pg_" + entidade)

    colunas_numericas = [
        "manufacturingYear", "usageIntensity", "estimatedWeightKg",
        "expectedLifespanMonths", "warrantyMonths", "stockCapacity",
    ]
    colunas_booleanas = ["hasDamages", "recyclable", "hazardous", "cancelled"]
    colunas_datas_neo4j = {
        "item": ["createdAt", "updatedAt", "acquiredAt", "lastEventAt"],
        "event": ["occurredAt"],
        "disposal": ["disposedAt", "createdAt"],
    }
    for entidade, colunas in COLUNAS_NEO4J.items():
        df = bases["neo_" + entidade]
        for coluna in colunas_datas_neo4j.get(entidade, []):
            converter_data(df, coluna, "neo_" + entidade)
        for coluna in set(colunas) & set(colunas_numericas):
            origem = df[coluna].copy()
            df[coluna] = pd.to_numeric(origem, errors="coerce").astype("Float64")
            invalidos = int((origem.notna() & df[coluna].isna()).sum())
            if invalidos:
                raise ValueError("Número inválido: " + entidade + "." + coluna)
        for coluna in set(colunas) & set(colunas_booleanas):
            validos = df[coluna].dropna().map(lambda valor: isinstance(valor, bool))
            invalidos = int((~validos).sum())
            if invalidos:
                raise ValueError("Booleano inválido: " + entidade + "." + coluna)
            df[coluna] = df[coluna].astype("boolean")
        if entidade == "material":
            verificacoes.append((
                entidade,
                "classificacao_reciclabilidade_ausente",
                int(df["recyclable"].isna().sum()),
            ))

    itens = bases["neo_item"].copy()
    itens["status"] = itens["status"].astype("string").str.upper()
    itens["condition"] = itens["condition"].astype("string").str.upper()
    relacoes = bases["neo_relationships"]
    item_modelo = (
        relacoes.loc[
            relacoes["relationship_type"].eq("IS_MODEL"), ["source_id", "target_id"]
        ].rename(columns={"source_id": "id", "target_id": "model_id"})
    )
    modelo_categoria = (
        relacoes.loc[
            relacoes["relationship_type"].eq("BELONGS_TO"), ["source_id", "target_id"]
        ].rename(columns={"source_id": "model_id", "target_id": "category_id"})
    )
    modelos = (
        bases["neo_model"][[
            "id", "name", "manufacturer", "estimatedWeightKg",
            "expectedLifespanMonths", "warrantyMonths",
        ]].rename(columns={"id": "model_id", "name": "model_name"})
    )
    categorias = (
        bases["neo_category"][["id", "name"]]
        .rename(columns={"id": "category_id", "name": "category_name"})
    )
    unidades = (
        bases["pg_unit"][["id", "name", "organization_id"]]
        .rename(columns={"id": "unitId", "name": "unit_name"})
    )
    organizacoes = (
        bases["pg_organization"][["id", "name"]]
        .rename(columns={"id": "organization_id", "name": "organization_name"})
    )
    enderecos = bases["pg_address"][
        ["id", "unit_id", "city", "state", "created_at"]
    ].dropna(subset=["unit_id"]).copy()
    unidades_com_multiplos_enderecos = int(
        enderecos.loc[enderecos["unit_id"].duplicated(keep=False), "unit_id"].nunique()
    )
    verificacoes.append((
        "pg_address", "unidades_com_multiplos_enderecos", unidades_com_multiplos_enderecos,
    ))
    enderecos = (
        enderecos.sort_values(["unit_id", "created_at", "id"], na_position="first")
        .drop_duplicates("unit_id", keep="last")
        .rename(columns={"unit_id": "unitId"})
    )
    itens = itens.merge(item_modelo, on="id", how="left", validate="one_to_one")
    itens = itens.merge(modelos, on="model_id", how="left", validate="many_to_one")
    itens = itens.merge(modelo_categoria, on="model_id", how="left", validate="many_to_one")
    itens = itens.merge(categorias, on="category_id", how="left", validate="many_to_one")
    itens = itens.merge(unidades, on="unitId", how="left", validate="many_to_one")
    itens = itens.merge(organizacoes, on="organization_id", how="left", validate="many_to_one")
    itens = itens.merge(
        enderecos[["unitId", "city", "state"]], on="unitId", how="left",
        validate="many_to_one",
    )
    itens["unidade_encontrada"] = itens["unit_name"].notna()
    itens["modelo_encontrado"] = itens["model_name"].notna()
    eventos = bases["neo_event"].copy()
    verificacoes.append((
        "eventos",
        "item_nao_encontrado",
        int((~eventos["itemId"].isin(itens["id"])).sum()),
    ))

    materiais = (
        bases["neo_material"][["id", "name", "recyclable", "hazardous"]]
        .rename(columns={"id": "material_id"})
    )
    composicao = (
        relacoes.loc[
            relacoes["relationship_type"].eq("MADE_OF"), ["source_id", "target_id"]
        ].rename(columns={"source_id": "model_id", "target_id": "material_id"})
    )
    composicao = composicao.merge(
        materiais, on="material_id", how="left", validate="many_to_one",
    )
    composicao["batch_id"] = batch_id
    resumo_materiais = composicao.groupby("model_id", dropna=False).agg(
        quantidade_materiais_associados=("material_id", "size"),
        possui_material_reciclavel=("recyclable", presenca_material),
        possui_material_perigoso=("hazardous", presenca_material),
    ).reset_index()
    itens = itens.merge(resumo_materiais, on="model_id", how="left", validate="many_to_one")
    for coluna in ["possui_material_reciclavel", "possui_material_perigoso"]:
        itens[coluna] = itens[coluna].astype("boolean")
    composicao = composicao.rename(columns={
        "model_id": "id_modelo",
        "material_id": "id_material",
        "name": "nome_material",
        "recyclable": "material_reciclavel",
        "hazardous": "material_perigoso",
    })

    itens = itens.rename(columns=NOMES_PORTUGUES)
    eventos = eventos.rename(columns=NOMES_EVENTOS)
    dicionario = pd.concat([
        pd.DataFrame([
            ["itens_tratados", origem, destino]
            for origem, destino in NOMES_PORTUGUES.items()
        ]),
        pd.DataFrame([
            ["eventos", origem, destino] for origem, destino in NOMES_EVENTOS.items()
        ]),
        pd.DataFrame([
            ["composicao_modelo_material", origem, destino]
            for origem, destino in NOMES_COMPOSICAO.items()
        ]),
    ], ignore_index=True)
    dicionario.columns = ["tabela", "coluna_original", "coluna_tratada"]

    itens["data_referencia"] = data_referencia
    itens["alerta_unidade_ausente"] = ~itens["unidade_encontrada"]
    itens["alerta_modelo_ausente"] = ~itens["modelo_encontrado"]
    itens["alerta_aquisicao_futura"] = itens["data_aquisicao"].gt(data_referencia)
    itens["alerta_atualizacao_anterior"] = itens["data_atualizacao"].lt(itens["data_cadastro"])
    itens["alerta_peso_invalido"] = itens["peso_estimado_kg"].le(0)
    itens["alerta_vida_util_invalida"] = itens["vida_util_estimada_meses"].le(0)
    itens["alerta_ano_fabricacao_invalido"] = (
        itens["ano_fabricacao"].le(0)
        | itens["ano_fabricacao"].gt(data_referencia.year)
    )
    danos_lista = itens["danos"].map(
        lambda valor: (
            len(valor) > 0 if isinstance(valor, (list, np.ndarray)) else pd.NA
        )
    ).astype("boolean")
    itens["possui_danos"] = itens["indicador_danos_origem"].fillna(danos_lista).astype("boolean")
    itens["alerta_danos_divergentes"] = (
        itens["indicador_danos_origem"] != danos_lista
    ).astype("boolean")
    colunas_alerta = [coluna for coluna in itens.columns if coluna.startswith("alerta_")]
    itens["alerta_qualidade_dos_dados"] = itens[colunas_alerta].fillna(False).any(axis=1)
    for coluna in colunas_alerta:
        verificacoes.append(
            ("itens_tratados", coluna, int(itens[coluna].fillna(False).sum()))
        )

    itens["ano_cadastro"] = itens["data_cadastro"].dt.year.astype("Int64")
    itens["mes_cadastro"] = itens["data_cadastro"].dt.month.astype("Int64")
    itens["mes_ano_cadastro"] = itens["data_cadastro"].dt.strftime("%Y-%m")
    idade_dias = (data_referencia - itens["data_aquisicao"].dt.normalize()).dt.days
    itens["idade_meses"] = (idade_dias / (365.25 / 12)).where(~itens["alerta_aquisicao_futura"])
    vida_valida = itens["vida_util_estimada_meses"].gt(0)
    itens["percentual_vida_util_consumida"] = (
        itens["idade_meses"] / itens["vida_util_estimada_meses"] * 100
    ).where(vida_valida)
    itens["meses_restantes_estimados"] = (
        itens["vida_util_estimada_meses"] - itens["idade_meses"]
    ).where(vida_valida)
    itens["ultrapassou_vida_util_estimada"] = (
        itens["percentual_vida_util_consumida"].ge(100).astype("boolean")
    )
    itens.loc[
        itens["percentual_vida_util_consumida"].isna(), "ultrapassou_vida_util_estimada"
    ] = pd.NA
    itens["faixa_idade"] = pd.cut(
        itens["idade_meses"],
        bins=[0, 12, 24, 48, np.inf],
        labels=[
            "Menos de 12 meses",
            "12 a menos de 24 meses",
            "24 a menos de 48 meses",
            "48 meses ou mais",
        ],
        right=False,
    ).astype("string").fillna("Não informada")

    if len(itens) != len(bases["neo_item"]) or not itens["id_item"].is_unique:
        raise ValueError("A integração alterou a quantidade ou a unicidade dos itens.")
    qualidade = pd.DataFrame(
        verificacoes, columns=["entidade", "verificacao", "ocorrencias"],
    )
    qualidade["id_lote"] = batch_id
    myLogger.Info(f"Silver finished {len(itens)} itens")
    return {
        "batch_id": batch_id,
        "extracted_at": extracted_at,
        "manifest": manifest,
        "itens": itens,
        "eventos": eventos,
        "bases": bases,
        "composicao": composicao,
        "qualidade": qualidade,
        "dicionario": dicionario,
    }
