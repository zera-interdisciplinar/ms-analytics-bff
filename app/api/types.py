"""Typed contracts for the dashboard SSE API."""

from __future__ import annotations

from typing import Literal, TypedDict

import pandas as pd

from app.pipeline.types import SilverSnapshot

Role = Literal["MANAGER", "EMPLOYEE"]
DashboardView = Literal["todos", "pendentes", "categoria"]
PeriodPreset = Literal["30d", "6m", "12m"]


class AccessToken(TypedDict):
    user_id: str
    email: str
    role: Role


class Tenant(TypedDict):
    user_id: str
    role: Role
    organization_id: str
    organization_name: str
    unit_id: str
    allowed_unit_ids: list[str]


class DashboardFilters(TypedDict):
    period: PeriodPreset
    date_from: str
    date_to: str
    category: str | None
    status: str | None
    unit_id: str | None
    responsible: str | None
    query: str | None
    view: DashboardView


class DashboardSnapshot(TypedDict):
    batch_id: str
    extracted_at: str
    itens: pd.DataFrame
    eventos: pd.DataFrame
    users: pd.DataFrame
    units: pd.DataFrame
    organizations: pd.DataFrame
    alerts: pd.DataFrame
    disposals: pd.DataFrame
    composicao: pd.DataFrame
    includes: pd.DataFrame


class SnapshotWire(TypedDict):
    batch_id: str
    extracted_at: str
    tables: dict[str, list[dict[str, object]]]


def empty_frame() -> pd.DataFrame:
    return pd.DataFrame()


def dashboard_snapshot_from_silver(silver: SilverSnapshot) -> DashboardSnapshot:
    bases = silver["bases"]
    relacoes = bases.get("neo_relationships", empty_frame())
    if relacoes.empty or "relationship_type" not in relacoes.columns:
        includes = empty_frame()
    else:
        includes = relacoes.loc[
            relacoes["relationship_type"].eq("INCLUDES")
        ].copy()
    return {
        "batch_id": silver["batch_id"],
        "extracted_at": silver["extracted_at"],
        "itens": silver["itens"],
        "eventos": silver["eventos"],
        "users": bases.get("pg_user_account", empty_frame()),
        "units": bases.get("pg_unit", empty_frame()),
        "organizations": bases.get("pg_organization", empty_frame()),
        "alerts": bases.get("pg_alert", empty_frame()),
        "disposals": bases.get("neo_disposal", empty_frame()),
        "composicao": silver["composicao"],
        "includes": includes,
    }
