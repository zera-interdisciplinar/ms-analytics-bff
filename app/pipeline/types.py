"""Shared types for the medallion pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, TypedDict

import pandas as pd

type Record = dict[str, object]
type PgColumn = tuple[str, str, str]
type PgField = tuple[str, str]
type PgFields = dict[str, list[PgField]]
type InventoryRow = tuple[str, str, str, int]
type QualityCheck = tuple[str, str, int]


class PostgresClient(Protocol):
    def fetch_columns(self) -> list[PgColumn]: ...

    def fetch_entities(
        self, pg_fields: PgFields, max_rows: int,
    ) -> dict[str, list[Record]]: ...


class Neo4jClient(Protocol):
    def verify(self) -> tuple[list[str], list[str]]: ...

    def read_graph(
        self, labels: Sequence[str], max_rows: int,
    ) -> tuple[dict[str, list[Record]], list[Record]]: ...


class BronzeSnapshot(TypedDict):
    batch_id: str
    extracted_at: str
    counts: dict[str, int]
    manifest: pd.DataFrame
    bases: dict[str, pd.DataFrame]


class SilverSnapshot(TypedDict):
    batch_id: str
    extracted_at: str
    manifest: pd.DataFrame
    itens: pd.DataFrame
    eventos: pd.DataFrame
    bases: dict[str, pd.DataFrame]
    composicao: pd.DataFrame
    qualidade: pd.DataFrame
    dicionario: pd.DataFrame


class GoldSnapshot(TypedDict):
    batch_id: str
    kpis: dict[str, int]
    tabelas: dict[str, pd.DataFrame]
    metricas: list[dict[str, object]]
    conclusoes: list[str]


class PipelineResult(TypedDict):
    batch_id: str
    bronze: BronzeSnapshot
    silver: SilverSnapshot
    gold: GoldSnapshot
