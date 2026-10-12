"""Bronze: snapshot extraction from PostgreSQL and Neo4j."""

from __future__ import annotations

import datetime as dt
import uuid

import pandas as pd

from app.pipeline.tables import NEO_LABELS, PG_FIELDS
from app.pipeline.types import (
    BronzeSnapshot,
    InventoryRow,
    Neo4jClient,
    PostgresClient,
    Record,
)
from logger.logger import Logger

myLogger = Logger()


def run_bronze(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    max_rows: int = 100_000,
) -> BronzeSnapshot:
    colunas_postgres = postgres.fetch_columns()
    colunas_disponiveis = {(tabela, coluna) for tabela, coluna, _ in colunas_postgres}
    colunas_ausentes = [
        f"public.{tabela}.{coluna}"
        for tabela, campos in PG_FIELDS.items()
        for coluna, _ in campos
        if (tabela, coluna) not in colunas_disponiveis
    ]
    if colunas_ausentes:
        raise ValueError(
            "O esquema PostgreSQL mudou; revisar a projeção: "
            + ", ".join(colunas_ausentes)
        )

    rotulos_neo4j, _relacionamentos = neo4j.verify()
    rotulos_ausentes = sorted(set(NEO_LABELS) - set(rotulos_neo4j))
    if rotulos_ausentes:
        raise ValueError(
            "Rótulos esperados ausentes no Neo4j: " + ", ".join(rotulos_ausentes)
        )

    batch_id = str(uuid.uuid4())
    extracted_at = dt.datetime.now(dt.timezone.utc).isoformat()
    inventory: list[InventoryRow] = []
    bases: dict[str, pd.DataFrame] = {}

    def save_raw(entity: str, records: list[Record]) -> None:
        df = pd.DataFrame.from_records(records) if records else pd.DataFrame()
        bases[entity] = df
        inventory.append((batch_id, extracted_at, entity, len(df)))
        myLogger.Info(f"{entity}: {len(df)} registros extraídos")

    myLogger.Info("Extracting PostgreSQL snapshots")
    for entity, records in postgres.fetch_entities(PG_FIELDS, max_rows).items():
        save_raw("pg_" + entity, records)

    myLogger.Info("Extracting Neo4j snapshots")
    nodes, edges = neo4j.read_graph(NEO_LABELS, max_rows)
    for label, records in nodes.items():
        save_raw("neo_" + label.lower(), records)
    save_raw("neo_relationships", edges)

    manifest = pd.DataFrame(
        inventory,
        columns=["batch_id", "extracted_at", "entity", "row_count"],
    )
    counts = {entity: count for _, _, entity, count in inventory}
    myLogger.Info(f"Bronze finished batch_id={batch_id}")
    return {
        "batch_id": batch_id,
        "extracted_at": extracted_at,
        "counts": counts,
        "manifest": manifest,
        "bases": bases,
    }
