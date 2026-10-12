"""JSON wire format for analytics.unit_snapshot.payload."""

from __future__ import annotations

import pandas as pd

from app.api.types import DashboardSnapshot, SnapshotWire, empty_frame

_TABLES: tuple[str, ...] = (
    "itens",
    "eventos",
    "users",
    "units",
    "organizations",
    "alerts",
    "disposals",
    "composicao",
    "includes",
)


def to_wire(snapshot: DashboardSnapshot) -> SnapshotWire:
    tables: dict[str, list[dict[str, object]]] = {}
    for name in _TABLES:
        frame = snapshot[name]  # type: ignore[literal-required]
        tables[name] = _records(frame)
    return {
        "batch_id": snapshot["batch_id"],
        "extracted_at": snapshot["extracted_at"],
        "tables": tables,
    }


def from_wire(payload: SnapshotWire) -> DashboardSnapshot:
    tables = payload.get("tables") or {}
    snapshot: DashboardSnapshot = {
        "batch_id": str(payload["batch_id"]),
        "extracted_at": str(payload["extracted_at"]),
        "itens": empty_frame(),
        "eventos": empty_frame(),
        "users": empty_frame(),
        "units": empty_frame(),
        "organizations": empty_frame(),
        "alerts": empty_frame(),
        "disposals": empty_frame(),
        "composicao": empty_frame(),
        "includes": empty_frame(),
    }
    for name in _TABLES:
        snapshot[name] = pd.DataFrame.from_records(tables.get(name, []))  # type: ignore[literal-required]
    return snapshot


def _records(frame: pd.DataFrame) -> list[dict[str, object]]:
    if frame.empty:
        return []
    serialized = frame.astype(object).where(frame.notna(), None)
    return serialized.to_dict(orient="records")
