import datetime as dt

import pytest
from fastapi import HTTPException

from app.api.conftest import EMPLOYEE_A, MANAGER_A, MANAGER_B, ORG_A, UNIT_A, UNIT_A2, UNIT_B
from app.api.overview import (
    build_overview,
    parse_filters,
    resolve_tenant,
    scoped_unit_id,
)
from app.api.types import AccessToken


def _token(user_id: str, role: str = "MANAGER") -> AccessToken:
    return {"user_id": user_id, "email": f"{user_id}@zera.test", "role": role}


def _filters(**overrides):
    filters = parse_filters(
        "6m", None, None, None, None, None, None, None, "todos",
        today=dt.date(2026, 10, 12),
    )
    filters.update(overrides)
    return filters


def test_manager_sees_only_own_organization(snapshot):
    tenant = resolve_tenant(snapshot, _token(MANAGER_A))
    payload = build_overview(snapshot, tenant, _filters())

    returned_categories = {row["nome"] for row in payload["por_categoria"]}

    assert tenant["organization_id"] == ORG_A
    assert payload["kpis"]["itens_cadastrados"]["value"] == 2
    assert payload["kpis"]["aguardando_aprovacao"] == {"value": 1, "prioritarios": 1}
    assert payload["kpis"]["impacto_recuperado"]["kg"] == 3.1
    assert payload["kpis"]["taxa_reciclagem"]["percent"] == 100.0
    assert "Bateria" not in returned_categories
    assert "Mobília" not in returned_categories
    assert payload["alertas"]["abertos"] == 1


def test_other_company_manager_does_not_see_org_a(snapshot):
    tenant = resolve_tenant(snapshot, _token(MANAGER_B))
    payload = build_overview(snapshot, tenant, _filters())

    assert tenant["organization_id"] != ORG_A
    assert payload["kpis"]["itens_cadastrados"]["value"] == 1
    assert payload["kpis"]["impacto_recuperado"]["kg"] == 0.0
    assert payload["por_categoria"][0]["nome"] == "Bateria"


def test_employee_cannot_select_another_unit(snapshot):
    tenant = resolve_tenant(snapshot, _token(EMPLOYEE_A, "EMPLOYEE"))
    filters = _filters(unit_id=UNIT_B)

    with pytest.raises(HTTPException) as exc:
        scoped_unit_id(tenant, filters["unit_id"])

    assert exc.value.status_code == 400


def test_manager_cannot_select_unit_from_another_company(snapshot):
    tenant = resolve_tenant(snapshot, _token(MANAGER_A))
    filters = _filters(unit_id=UNIT_B)

    with pytest.raises(HTTPException) as exc:
        scoped_unit_id(tenant, filters["unit_id"])

    assert exc.value.status_code == 400


def test_unknown_user_is_forbidden(snapshot):
    with pytest.raises(HTTPException) as exc:
        resolve_tenant(snapshot, _token("ghost"))

    assert exc.value.status_code == 403


def test_search_does_not_cross_organizations(snapshot):
    tenant = resolve_tenant(snapshot, _token(MANAGER_A))
    payload = build_overview(snapshot, tenant, _filters(query="Segredo"))

    assert payload["kpis"]["itens_cadastrados"]["value"] == 0


def test_pending_view_keeps_only_approvals(snapshot):
    tenant = resolve_tenant(snapshot, _token(MANAGER_A))
    payload = build_overview(snapshot, tenant, _filters(view="pendentes"))

    assert payload["kpis"]["itens_cadastrados"]["value"] == 1
    assert payload["kpis"]["aguardando_aprovacao"]["value"] == 1


def test_period_rejects_inverted_range():
    with pytest.raises(HTTPException) as exc:
        parse_filters("6m", "2026-10-10", "2026-10-01", None, None, None, None, None, "todos")

    assert exc.value.status_code == 400


def test_employee_is_locked_to_own_unit(snapshot):
    tenant = resolve_tenant(snapshot, _token(EMPLOYEE_A, "EMPLOYEE"))

    assert scoped_unit_id(tenant, None) == UNIT_A


def test_manager_defaults_to_home_unit_not_whole_company(snapshot):
    tenant = resolve_tenant(snapshot, _token(MANAGER_A))
    payload = build_overview(snapshot, tenant, _filters())
    other = build_overview(snapshot, tenant, _filters(unit_id=UNIT_A2))

    assert payload["kpis"]["itens_cadastrados"]["value"] == 2
    assert other["kpis"]["itens_cadastrados"]["value"] == 1
    assert other["por_categoria"][0]["nome"] == "Mobília"


def test_recycling_falls_back_to_disposed_item_weight(snapshot):
    snapshot["includes"] = snapshot["includes"].iloc[0:0]
    snapshot["itens"].loc[snapshot["itens"]["id_item"].eq("item-a1"), "situacao"] = "DISPOSED"
    tenant = resolve_tenant(snapshot, _token(MANAGER_A))

    payload = build_overview(snapshot, tenant, _filters())

    assert payload["kpis"]["impacto_recuperado"]["kg"] == 2.0
