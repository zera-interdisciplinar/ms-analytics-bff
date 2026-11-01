"""Tenant-scoped dashboard metrics for the gestão overview page."""

from __future__ import annotations

import datetime as dt
from typing import cast

import pandas as pd
from fastapi import HTTPException

from app.api.types import (
    AccessToken,
    DashboardFilters,
    DashboardSnapshot,
    DashboardView,
    PeriodPreset,
    Tenant,
)

PENDING_STATUSES: frozenset[str] = frozenset({"PENDING_APPROVAL"})
REMOVED_STATUS = "REMOVED"
DISPOSED_EVENT = "DISPOSED"
RECYCLING = "RECYCLING"


def parse_filters(
    period: str | None,
    date_from: str | None,
    date_to: str | None,
    category: str | None,
    status: str | None,
    unit_id: str | None,
    responsible: str | None,
    query: str | None,
    view: str | None,
    today: dt.date | None = None,
) -> DashboardFilters:
    preset = (period or "6m").strip().lower()
    if preset not in {"30d", "6m", "12m"}:
        raise HTTPException(status_code=400, detail="period must be 30d, 6m or 12m")
    view_name = (view or "todos").strip().lower()
    if view_name not in {"todos", "pendentes", "categoria"}:
        raise HTTPException(status_code=400, detail="view must be todos, pendentes or categoria")
    reference = today or dt.date.today()
    start, end = _period_bounds(cast(PeriodPreset, preset), reference)
    if date_from or date_to:
        if not date_from or not date_to:
            raise HTTPException(status_code=400, detail="from and to must be sent together")
        start = _parse_date(date_from, "from")
        end = _parse_date(date_to, "to")
        if start > end:
            raise HTTPException(status_code=400, detail="from must be on or before to")
    return {
        "period": cast(PeriodPreset, preset),
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "category": _blank_to_none(category),
        "status": _blank_to_none(status),
        "unit_id": _blank_to_none(unit_id),
        "responsible": _blank_to_none(responsible),
        "query": _blank_to_none(query),
        "view": cast(DashboardView, view_name),
    }


def resolve_tenant(snapshot: DashboardSnapshot, token: AccessToken) -> Tenant:
    users = snapshot["users"]
    units = snapshot["units"]
    organizations = snapshot["organizations"]
    if users.empty or "id" not in users.columns:
        raise HTTPException(status_code=403, detail="forbidden")
    matches = users.loc[users["id"].astype("string").eq(token["user_id"])]
    if matches.empty:
        raise HTTPException(status_code=403, detail="forbidden")
    user = matches.iloc[0]
    user_unit_id = str(user["unit_id"])
    unit_rows = units.loc[units["id"].astype("string").eq(user_unit_id)]
    if unit_rows.empty:
        raise HTTPException(status_code=403, detail="forbidden")
    organization_id = str(unit_rows.iloc[0]["organization_id"])
    org_rows = organizations.loc[organizations["id"].astype("string").eq(organization_id)]
    organization_name = (
        str(org_rows.iloc[0]["name"]) if not org_rows.empty else ""
    )
    allowed = (
        units.loc[units["organization_id"].astype("string").eq(organization_id), "id"]
        .astype("string")
        .tolist()
    )
    return {
        "user_id": token["user_id"],
        "role": token["role"],
        "organization_id": organization_id,
        "organization_name": organization_name,
        "unit_id": user_unit_id,
        "allowed_unit_ids": allowed,
    }


def scoped_unit_id(tenant: Tenant, requested_unit_id: str | None) -> str:
    allowed = set(tenant["allowed_unit_ids"])
    if tenant["role"] == "EMPLOYEE":
        allowed = {tenant["unit_id"]}
    unit_id = requested_unit_id or tenant["unit_id"]
    if unit_id not in allowed:
        raise HTTPException(status_code=400, detail="unit_id inválido")
    return unit_id


def build_overview(
    snapshot: DashboardSnapshot,
    tenant: Tenant,
    filters: DashboardFilters,
) -> dict[str, object]:
    unit_ids = [scoped_unit_id(tenant, filters["unit_id"])]
    start = dt.date.fromisoformat(filters["date_from"])
    end = dt.date.fromisoformat(filters["date_to"])
    previous_start, previous_end = _previous_window(start, end)

    itens = _org_items(snapshot["itens"], tenant["organization_id"], unit_ids)
    eventos = _frame_in_units(snapshot["eventos"], "id_unidade", unit_ids)
    alerts = _frame_in_units(snapshot["alerts"], "unit_id", unit_ids)
    disposals = _active_disposals(snapshot["disposals"], unit_ids)
    includes = snapshot["includes"]

    current_items = _apply_item_filters(
        itens, snapshot["composicao"], filters, created_until=end,
    )
    previous_items = _apply_item_filters(
        itens, snapshot["composicao"], filters, created_until=previous_end,
    )
    current_disposals = _disposals_in_period(disposals, start, end)
    previous_disposals = _disposals_in_period(disposals, previous_start, previous_end)

    cadastrados = int(len(current_items))
    previous_count = int(len(previous_items))
    pendentes = current_items.loc[
        current_items["situacao"].astype("string").isin(PENDING_STATUSES)
    ] if not current_items.empty and "situacao" in current_items.columns else current_items.iloc[0:0]
    prioritarios = _priority_pending(pendentes)

    recycled_kg, total_disposed_kg = _disposed_weights(
        current_disposals, includes, current_items,
    )
    prev_recycled_kg, prev_total_kg = _disposed_weights(
        previous_disposals, includes, previous_items,
    )
    recycling_rate = _ratio_percent(recycled_kg, total_disposed_kg)
    previous_rate = _ratio_percent(prev_recycled_kg, prev_total_kg)
    circularidade = _circularity(current_items)

    return {
        "batch_id": snapshot["batch_id"],
        "extracted_at": snapshot["extracted_at"],
        "organization": {
            "id": tenant["organization_id"],
            "name": tenant["organization_name"],
        },
        "applied_filters": filters,
        "available_filters": _available_filters(itens, tenant, unit_ids),
        "kpis": {
            "itens_cadastrados": {
                "value": cadastrados,
                "change_percent": _change_percent(float(cadastrados), float(previous_count)),
            },
            "taxa_reciclagem": {
                "percent": recycling_rate,
                "change_points": _subtract_or_none(recycling_rate, previous_rate),
            },
            "aguardando_aprovacao": {
                "value": int(len(pendentes)),
                "prioritarios": int(len(prioritarios)),
            },
            "impacto_recuperado": {
                "kg": round(recycled_kg, 2),
                "tonnes": round(recycled_kg / 1000, 3),
            },
        },
        "itens_por_mes": _monthly_series(current_items, eventos, start, end),
        "circularidade": {
            "percent": circularidade,
        },
        "por_categoria": _count_by(current_items, "categoria", cadastrados),
        "por_status": _count_by(current_items, "situacao", cadastrados),
        "alertas": _alert_summary(alerts),
    }


def _blank_to_none(value: str | None) -> str | None:
    stripped = (value or "").strip()
    return stripped or None


def _parse_date(value: str, field: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field} must be YYYY-MM-DD") from exc


def _period_bounds(period: PeriodPreset, today: dt.date) -> tuple[dt.date, dt.date]:
    if period == "30d":
        return today - dt.timedelta(days=29), today
    months = 6 if period == "6m" else 12
    year = today.year
    month = today.month - (months - 1)
    while month <= 0:
        month += 12
        year -= 1
    return dt.date(year, month, 1), today


def _previous_window(start: dt.date, end: dt.date) -> tuple[dt.date, dt.date]:
    duration = (end - start).days
    previous_end = start - dt.timedelta(days=1)
    previous_start = previous_end - dt.timedelta(days=duration)
    return previous_start, previous_end


def _org_items(
    itens: pd.DataFrame, organization_id: str, unit_ids: list[str],
) -> pd.DataFrame:
    if itens.empty:
        return itens
    scoped = itens.loc[itens["id_organizacao"].astype("string").eq(organization_id)]
    scoped = scoped.loc[scoped["id_unidade"].astype("string").isin(unit_ids)]
    if "situacao" in scoped.columns:
        scoped = scoped.loc[~scoped["situacao"].astype("string").eq(REMOVED_STATUS)]
    return scoped


def _frame_in_units(frame: pd.DataFrame, column: str, unit_ids: list[str]) -> pd.DataFrame:
    if frame.empty or column not in frame.columns:
        return frame.iloc[0:0] if len(frame) else frame
    return frame.loc[frame[column].astype("string").isin(unit_ids)]


def _active_disposals(disposals: pd.DataFrame, unit_ids: list[str]) -> pd.DataFrame:
    scoped = _frame_in_units(disposals, "unitId", unit_ids)
    if scoped.empty or "cancelled" not in scoped.columns:
        return scoped
    cancelled = scoped["cancelled"].fillna(False).astype(bool)
    return scoped.loc[~cancelled]


def _apply_item_filters(
    itens: pd.DataFrame,
    composicao: pd.DataFrame,
    filters: DashboardFilters,
    created_until: dt.date,
) -> pd.DataFrame:
    if itens.empty:
        return itens
    filtered = itens.copy()
    if "data_cadastro" in filtered.columns:
        created = pd.to_datetime(filtered["data_cadastro"], errors="coerce")
        created_date = created.dt.date
        filtered = filtered.loc[created.isna() | (created_date <= created_until)]
    if filters["category"] and "categoria" in filtered.columns:
        filtered = filtered.loc[filtered["categoria"].astype("string").eq(filters["category"])]
    if filters["status"] and "situacao" in filtered.columns:
        filtered = filtered.loc[
            filtered["situacao"].astype("string").str.upper().eq(filters["status"].upper())
        ]
    if filters["responsible"] and "id_responsavel" in filtered.columns:
        filtered = filtered.loc[
            filtered["id_responsavel"].astype("string").eq(filters["responsible"])
        ]
    if filters["view"] == "pendentes" and "situacao" in filtered.columns:
        filtered = filtered.loc[filtered["situacao"].astype("string").isin(PENDING_STATUSES)]
    query = filters["query"]
    if query:
        filtered = _search_items(filtered, composicao, query)
    return filtered


def _search_items(itens: pd.DataFrame, composicao: pd.DataFrame, query: str) -> pd.DataFrame:
    needle = query.casefold()
    id_match = itens["id_item"].astype("string").str.casefold().str.contains(needle, na=False)
    name_match = (
        itens["nome_item"].astype("string").str.casefold().str.contains(needle, na=False)
        if "nome_item" in itens.columns
        else False
    )
    model_match = (
        itens["modelo"].astype("string").str.casefold().str.contains(needle, na=False)
        if "modelo" in itens.columns
        else False
    )
    material_ids: set[str] = set()
    if not composicao.empty and "nome_material" in composicao.columns:
        hits = composicao.loc[
            composicao["nome_material"].astype("string").str.casefold().str.contains(needle, na=False),
            "id_modelo",
        ]
        material_ids = set(hits.astype("string"))
    material_match = (
        itens["id_modelo"].astype("string").isin(material_ids)
        if "id_modelo" in itens.columns
        else False
    )
    return itens.loc[id_match | name_match | model_match | material_match]


def _priority_pending(pendentes: pd.DataFrame) -> pd.DataFrame:
    if pendentes.empty:
        return pendentes
    damaged = (
        pendentes["possui_danos"].fillna(False).astype(bool)
        if "possui_danos" in pendentes.columns
        else False
    )
    return pendentes.loc[damaged]


def _disposals_in_period(
    disposals: pd.DataFrame, start: dt.date, end: dt.date,
) -> pd.DataFrame:
    if disposals.empty or "disposedAt" not in disposals.columns:
        return disposals.iloc[0:0] if len(disposals) else disposals
    when = pd.to_datetime(disposals["disposedAt"], errors="coerce").dt.date
    return disposals.loc[when.notna() & (when >= start) & (when <= end)]


def _disposed_weights(
    disposals: pd.DataFrame,
    includes: pd.DataFrame,
    itens: pd.DataFrame,
) -> tuple[float, float]:
    if disposals.empty:
        return _fallback_disposed_weights(itens)
    if includes.empty or "source_id" not in includes.columns:
        disposed = itens.iloc[0:0]
        if not itens.empty and "situacao" in itens.columns:
            disposed = itens.loc[itens["situacao"].astype("string").eq("DISPOSED")]
        return _fallback_disposed_weights(disposed)
    linked = includes.merge(
        disposals.rename(columns={"id": "source_id"}),
        on="source_id",
        how="inner",
    )
    if linked.empty:
        return _fallback_disposed_weights(itens)
    weights = _include_weight(linked)
    total = float(weights.sum())
    if total <= 0:
        disposed = itens.iloc[0:0]
        if not itens.empty and "situacao" in itens.columns:
            disposed = itens.loc[itens["situacao"].astype("string").eq("DISPOSED")]
        return _fallback_disposed_weights(disposed)
    if "destination" in linked.columns:
        recycled = float(weights.loc[linked["destination"].astype("string").str.upper().eq(RECYCLING)].sum())
    else:
        recycled = 0.0
    return recycled, total


def _include_weight(linked: pd.DataFrame) -> pd.Series:
    if "properties" in linked.columns:
        extracted = linked["properties"].map(_weight_from_properties)
        if extracted.notna().any():
            return pd.to_numeric(extracted, errors="coerce").fillna(0.0)
    if "weightKg" in linked.columns:
        return pd.to_numeric(linked["weightKg"], errors="coerce").fillna(0.0)
    return pd.Series(0.0, index=linked.index)


def _weight_from_properties(value: object) -> float | None:
    if isinstance(value, dict) and value.get("weightKg") is not None:
        try:
            return float(value["weightKg"])
        except (TypeError, ValueError):
            return None
    return None


def _fallback_disposed_weights(itens: pd.DataFrame) -> tuple[float, float]:
    if itens.empty or "peso_estimado_kg" not in itens.columns:
        return 0.0, 0.0
    disposed = itens
    if "situacao" in itens.columns:
        disposed = itens.loc[itens["situacao"].astype("string").eq("DISPOSED")]
    weights = pd.to_numeric(disposed["peso_estimado_kg"], errors="coerce").fillna(0.0)
    total = float(weights.sum())
    if "possui_material_reciclavel" in disposed.columns:
        recycled = float(weights.loc[disposed["possui_material_reciclavel"].fillna(False).astype(bool)].sum())
    else:
        recycled = 0.0
    return recycled, total


def _circularity(itens: pd.DataFrame) -> float | None:
    if itens.empty or "possui_material_reciclavel" not in itens.columns:
        return None
    known = itens["possui_material_reciclavel"].dropna()
    if known.empty:
        return None
    return _ratio_percent(float(known.astype(bool).sum()), float(len(known)))


def _ratio_percent(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator * 100, 2)


def _change_percent(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return round((current - previous) / previous * 100, 2)


def _subtract_or_none(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return round(current - previous, 2)


def _monthly_series(
    itens: pd.DataFrame,
    eventos: pd.DataFrame,
    start: dt.date,
    end: dt.date,
) -> list[dict[str, object]]:
    months: list[str] = []
    cursor = dt.date(start.year, start.month, 1)
    last = dt.date(end.year, end.month, 1)
    while cursor <= last:
        months.append(cursor.strftime("%Y-%m"))
        month = cursor.month + 1
        year = cursor.year
        if month > 12:
            month = 1
            year += 1
        cursor = dt.date(year, month, 1)

    created = pd.Series(dtype="string")
    if not itens.empty and "data_cadastro" in itens.columns:
        created = pd.to_datetime(itens["data_cadastro"], errors="coerce").dt.strftime("%Y-%m")
    forwarded = pd.Series(dtype="string")
    if not eventos.empty and "tipo_evento" in eventos.columns:
        disposed = eventos.loc[eventos["tipo_evento"].astype("string").str.upper().eq(DISPOSED_EVENT)]
        if not disposed.empty:
            forwarded = pd.to_datetime(disposed["data_ocorrencia"], errors="coerce").dt.strftime("%Y-%m")

    series: list[dict[str, object]] = []
    for month in months:
        series.append({
            "mes": month,
            "cadastrados": int((created == month).sum()) if len(created) else 0,
            "encaminhados": int((forwarded == month).sum()) if len(forwarded) else 0,
        })
    return series


def _count_by(itens: pd.DataFrame, column: str, total: int) -> list[dict[str, object]]:
    if itens.empty or column not in itens.columns:
        return []
    grouped = (
        itens.assign(recorte=itens[column].astype("string").fillna("Não informada"))
        .groupby("recorte", dropna=False)
        .agg(quantidade=("id_item", "nunique"))
        .reset_index()
        .sort_values("quantidade", ascending=False)
    )
    rows: list[dict[str, object]] = []
    for linha in grouped.itertuples(index=False):
        quantidade = int(linha.quantidade)
        rows.append({
            "nome": str(linha.recorte),
            "quantidade": quantidade,
            "percentual": _ratio_percent(float(quantidade), float(total)),
        })
    return rows


def _alert_summary(alerts: pd.DataFrame) -> dict[str, object]:
    if alerts.empty:
        return {"abertos": 0, "por_gravidade": []}
    status = alerts["status"].astype("string").str.upper() if "status" in alerts.columns else None
    open_alerts = alerts.loc[status.eq("OPEN")] if status is not None else alerts
    gravidade: list[dict[str, object]] = []
    if not open_alerts.empty and "severity" in open_alerts.columns:
        counts = (
            open_alerts.assign(gravidade=open_alerts["severity"].astype("string").fillna("UNKNOWN"))
            .groupby("gravidade")
            .size()
            .reset_index(name="quantidade")
        )
        gravidade = [
            {"gravidade": str(linha.gravidade), "quantidade": int(linha.quantidade)}
            for linha in counts.itertuples(index=False)
        ]
    return {"abertos": int(len(open_alerts)), "por_gravidade": gravidade}


def _available_filters(
    itens: pd.DataFrame, tenant: Tenant, unit_ids: list[str],
) -> dict[str, object]:
    categories = (
        sorted({str(value) for value in itens["categoria"].dropna().astype("string")})
        if not itens.empty and "categoria" in itens.columns
        else []
    )
    statuses = (
        sorted({str(value) for value in itens["situacao"].dropna().astype("string")})
        if not itens.empty and "situacao" in itens.columns
        else []
    )
    unit_names: dict[str, str] = {}
    if not itens.empty and "id_unidade" in itens.columns and "unidade" in itens.columns:
        for _, row in itens[["id_unidade", "unidade"]].drop_duplicates().iterrows():
            unit_names[str(row["id_unidade"])] = str(row["unidade"])
    units_payload = [
        {"id": unit_id, "name": unit_names.get(unit_id, unit_id)}
        for unit_id in unit_ids
        if tenant["role"] == "MANAGER" or unit_id == tenant["unit_id"]
    ]
    responsibles: list[dict[str, str]] = []
    if not itens.empty and "id_responsavel" in itens.columns:
        names = (
            itens[["id_responsavel", "responsavel"]]
            if "responsavel" in itens.columns
            else itens[["id_responsavel"]].assign(responsavel=None)
        )
        names = names.dropna(subset=["id_responsavel"]).drop_duplicates("id_responsavel")
        responsibles = [
            {
                "id": str(row["id_responsavel"]),
                "name": str(row["responsavel"]) if pd.notna(row.get("responsavel")) else "",
            }
            for _, row in names.iterrows()
        ]
    return {
        "categories": categories,
        "statuses": statuses,
        "units": units_payload,
        "responsibles": responsibles,
        "views": ["todos", "pendentes", "categoria"],
        "periods": ["30d", "6m", "12m"],
    }
