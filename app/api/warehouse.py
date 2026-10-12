"""Cron writes analytics by organization+unit; the API only reads that slice."""

from __future__ import annotations

import json
from contextlib import closing
from typing import Protocol

import pandas as pd
import pg8000.dbapi

from app.api.serialize import from_wire, to_wire
from app.api.types import (
    DashboardSnapshot,
    Tenant,
    dashboard_snapshot_from_silver,
    empty_frame,
)
from app.pipeline.types import SilverSnapshot
from config.environments import Environments
from logger.logger import Logger

myLogger = Logger()

SCHEMA_STATEMENTS: tuple[str, ...] = (
    "CREATE SCHEMA IF NOT EXISTS analytics",
    """
    CREATE TABLE IF NOT EXISTS analytics.user_scope (
        user_id TEXT PRIMARY KEY,
        organization_id TEXT NOT NULL,
        organization_name TEXT NOT NULL,
        unit_id TEXT NOT NULL,
        allowed_unit_ids JSONB NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS analytics.unit_snapshot (
        organization_id TEXT NOT NULL,
        unit_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        extracted_at TEXT NOT NULL,
        payload JSONB NOT NULL,
        PRIMARY KEY (organization_id, unit_id)
    )
    """,
)


class AnalyticsWarehouse(Protocol):
    def save(self, snapshot: DashboardSnapshot) -> None: ...

    def identity(self, user_id: str) -> Tenant | None: ...

    def load(self, organization_id: str, unit_id: str) -> DashboardSnapshot | None: ...

    def is_empty(self) -> bool: ...


def save_silver(silver: SilverSnapshot, warehouse: AnalyticsWarehouse) -> None:
    warehouse.save(dashboard_snapshot_from_silver(silver))


def partition(snapshot: DashboardSnapshot) -> list[tuple[str, str, DashboardSnapshot]]:
    units = snapshot["units"]
    if units.empty or "id" not in units.columns:
        return []
    parts: list[tuple[str, str, DashboardSnapshot]] = []
    for unit_id, organization_id in zip(
        units["id"].astype("string"),
        units["organization_id"].astype("string"),
        strict=True,
    ):
        parts.append((
            str(organization_id),
            str(unit_id),
            _slice(snapshot, str(organization_id), str(unit_id)),
        ))
    return parts


def identities_from(snapshot: DashboardSnapshot) -> list[Tenant]:
    users = snapshot["users"]
    units = snapshot["units"]
    organizations = snapshot["organizations"]
    if users.empty or units.empty:
        return []
    org_name: dict[str, str] = {}
    if not organizations.empty and "id" in organizations.columns:
        for organization_id, name in zip(
            organizations["id"].astype("string"),
            organizations["name"].astype("string"),
            strict=True,
        ):
            org_name[str(organization_id)] = str(name)
    units_by_org: dict[str, list[str]] = {}
    unit_org: dict[str, str] = {}
    for unit_id, organization_id in zip(
        units["id"].astype("string"),
        units["organization_id"].astype("string"),
        strict=True,
    ):
        unit_org[str(unit_id)] = str(organization_id)
        units_by_org.setdefault(str(organization_id), []).append(str(unit_id))
    found: list[Tenant] = []
    for user_id, home_unit in zip(
        users["id"].astype("string"),
        users["unit_id"].astype("string"),
        strict=True,
    ):
        organization_id = unit_org.get(str(home_unit))
        if not organization_id:
            continue
        found.append({
            "user_id": str(user_id),
            "role": "MANAGER",
            "organization_id": organization_id,
            "organization_name": org_name.get(organization_id, ""),
            "unit_id": str(home_unit),
            "allowed_unit_ids": list(units_by_org.get(organization_id, [str(home_unit)])),
        })
    return found


class InMemoryWarehouse:
    def __init__(self) -> None:
        self._identities: dict[str, Tenant] = {}
        self._snapshots: dict[tuple[str, str], DashboardSnapshot] = {}

    def save(self, snapshot: DashboardSnapshot) -> None:
        self._identities = {
            identity["user_id"]: identity for identity in identities_from(snapshot)
        }
        self._snapshots = {
            (organization_id, unit_id): sliced
            for organization_id, unit_id, sliced in partition(snapshot)
        }
        myLogger.Info(
            f"Analytics warehouse saved batch_id={snapshot['batch_id']} "
            f"units={len(self._snapshots)}"
        )

    def identity(self, user_id: str) -> Tenant | None:
        return self._identities.get(user_id)

    def load(self, organization_id: str, unit_id: str) -> DashboardSnapshot | None:
        return self._snapshots.get((organization_id, unit_id))

    def is_empty(self) -> bool:
        return not self._identities and not self._snapshots


class PostgresWarehouse:
    def __init__(self, envs: Environments) -> None:
        if not envs.PG_HOST or not envs.PG_PASSWORD:
            raise ValueError(
                "PG_HOST e PG_PASSWORD são obrigatórios para persistir o analytics."
            )
        self._kwargs: dict[str, str | int] = {
            "host": envs.PG_HOST,
            "port": int(envs.PG_PORT),
            "database": envs.PG_DATABASE,
            "user": envs.PG_USER,
            "password": envs.PG_PASSWORD,
            "timeout": 30,
        }

    def save(self, snapshot: DashboardSnapshot) -> None:
        identities = identities_from(snapshot)
        parts = partition(snapshot)
        with closing(pg8000.dbapi.connect(**self._kwargs)) as connection:
            with closing(connection.cursor()) as cursor:
                for statement in SCHEMA_STATEMENTS:
                    cursor.execute(statement)
                cursor.execute("DELETE FROM analytics.user_scope")
                cursor.execute("DELETE FROM analytics.unit_snapshot")
                for identity in identities:
                    cursor.execute(
                        """
                        INSERT INTO analytics.user_scope (
                            user_id, organization_id, organization_name,
                            unit_id, allowed_unit_ids
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            identity["user_id"],
                            identity["organization_id"],
                            identity["organization_name"],
                            identity["unit_id"],
                            json.dumps(identity["allowed_unit_ids"]),
                        ),
                    )
                for organization_id, unit_id, sliced in parts:
                    cursor.execute(
                        """
                        INSERT INTO analytics.unit_snapshot (
                            organization_id, unit_id, batch_id, extracted_at, payload
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            organization_id,
                            unit_id,
                            sliced["batch_id"],
                            sliced["extracted_at"],
                            json.dumps(to_wire(sliced)),
                        ),
                    )
            connection.commit()
        myLogger.Info(
            f"Analytics warehouse saved batch_id={snapshot['batch_id']} units={len(parts)}"
        )

    def identity(self, user_id: str) -> Tenant | None:
        with closing(pg8000.dbapi.connect(**self._kwargs)) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, organization_id, organization_name,
                           unit_id, allowed_unit_ids
                    FROM analytics.user_scope
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        allowed = row[4]
        if isinstance(allowed, str):
            allowed = json.loads(allowed)
        return {
            "user_id": str(row[0]),
            "role": "MANAGER",
            "organization_id": str(row[1]),
            "organization_name": str(row[2]),
            "unit_id": str(row[3]),
            "allowed_unit_ids": [str(value) for value in allowed],
        }

    def load(self, organization_id: str, unit_id: str) -> DashboardSnapshot | None:
        with closing(pg8000.dbapi.connect(**self._kwargs)) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(
                    """
                    SELECT payload
                    FROM analytics.unit_snapshot
                    WHERE organization_id = %s AND unit_id = %s
                    """,
                    (organization_id, unit_id),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        payload = row[0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return from_wire(payload)

    def is_empty(self) -> bool:
        with closing(pg8000.dbapi.connect(**self._kwargs)) as connection:
            with closing(connection.cursor()) as cursor:
                try:
                    cursor.execute("SELECT 1 FROM analytics.unit_snapshot LIMIT 1")
                except pg8000.dbapi.DatabaseError:
                    return True
                return cursor.fetchone() is None


def live_warehouse(envs: Environments) -> PostgresWarehouse:
    return PostgresWarehouse(envs)


def _slice(
    snapshot: DashboardSnapshot, organization_id: str, unit_id: str,
) -> DashboardSnapshot:
    itens = _eq(snapshot["itens"], "id_unidade", unit_id)
    itens = _eq(itens, "id_organizacao", organization_id)
    eventos = _eq(snapshot["eventos"], "id_unidade", unit_id)
    alerts = _eq(snapshot["alerts"], "unit_id", unit_id)
    disposals = _eq(snapshot["disposals"], "unitId", unit_id)
    users = _eq(snapshot["users"], "unit_id", unit_id)
    units = _eq(snapshot["units"], "id", unit_id)
    organizations = _eq(snapshot["organizations"], "id", organization_id)
    disposal_ids = (
        set(disposals["id"].astype("string"))
        if not disposals.empty and "id" in disposals.columns
        else set()
    )
    includes = snapshot["includes"]
    if not includes.empty and "source_id" in includes.columns:
        includes = includes.loc[includes["source_id"].astype("string").isin(disposal_ids)]
    else:
        includes = empty_frame() if includes.empty else includes.iloc[0:0]
    return {
        "batch_id": snapshot["batch_id"],
        "extracted_at": snapshot["extracted_at"],
        "itens": itens,
        "eventos": eventos,
        "users": users,
        "units": units,
        "organizations": organizations,
        "alerts": alerts,
        "disposals": disposals,
        "composicao": snapshot["composicao"],
        "includes": includes,
    }


def _eq(frame: pd.DataFrame, column: str, value: str) -> pd.DataFrame:
    if frame.empty or column not in frame.columns:
        return frame.iloc[0:0] if len(frame) else frame
    return frame.loc[frame[column].astype("string").eq(value)].copy()
