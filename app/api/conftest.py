from __future__ import annotations

import datetime as dt

import jwt
import pandas as pd
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.api.types import DashboardSnapshot
from app.api.warehouse import InMemoryWarehouse
from config.environments import Environments

ORG_A = "org-a"
ORG_B = "org-b"
UNIT_A = "unit-a"
UNIT_B = "unit-b"
UNIT_A2 = "unit-a2"
MANAGER_A = "manager-a"
EMPLOYEE_A = "employee-a"
MANAGER_B = "manager-b"
ISSUER = "ms-administrative-core"


@pytest.fixture(scope="session")
def rsa_keys() -> tuple[object, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")
    return key, public_pem


@pytest.fixture
def api_envs(rsa_keys, monkeypatch) -> Environments:
    _, public_pem = rsa_keys
    monkeypatch.setenv("JWT_ISSUER", ISSUER)
    monkeypatch.setenv("JWT_PUBLIC_KEY", public_pem)
    monkeypatch.setenv("JWT_JWKS_URL", "")
    monkeypatch.setenv("SSE_POLL_SECONDS", "30")
    monkeypatch.setenv("SSE_HEARTBEAT_SECONDS", "60")
    monkeypatch.setenv("CORS_ORIGINS", "")
    return Environments()


def encode_token(
    rsa_keys: tuple[object, str],
    user_id: str,
    role: str = "MANAGER",
    extra: dict[str, object] | None = None,
) -> str:
    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    payload: dict[str, object] = {
        "iss": ISSUER,
        "sub": user_id,
        "email": f"{user_id}@zera.test",
        "role": role,
        "iat": now,
        "exp": now + 3600,
    }
    if extra:
        payload.update(extra)
    private_key, _ = rsa_keys
    return jwt.encode(payload, private_key, algorithm="RS256")


def dashboard_snapshot() -> DashboardSnapshot:
    return {
        "batch_id": "lote-1",
        "extracted_at": "2026-10-12T12:00:00+00:00",
        "itens": pd.DataFrame([
            {
                "id_item": "item-a1", "id_organizacao": ORG_A, "id_unidade": UNIT_A,
                "unidade": "Unidade A", "organizacao": "Empresa A",
                "nome_item": "Notebook A", "modelo": "ZBook", "categoria": "Informática",
                "situacao": "IN_STOCK", "data_cadastro": "2026-09-01",
                "id_responsavel": EMPLOYEE_A, "responsavel": "Operário A",
                "possui_danos": False, "possui_material_reciclavel": True,
                "peso_estimado_kg": 2.0, "id_modelo": "model-a",
            },
            {
                "id_item": "item-a2", "id_organizacao": ORG_A, "id_unidade": UNIT_A,
                "unidade": "Unidade A", "organizacao": "Empresa A",
                "nome_item": "Monitor quebrado", "modelo": "ViewZ", "categoria": "Informática",
                "situacao": "PENDING_APPROVAL", "data_cadastro": "2026-09-10",
                "id_responsavel": EMPLOYEE_A, "responsavel": "Operário A",
                "possui_danos": True, "possui_material_reciclavel": True,
                "peso_estimado_kg": 5.0, "id_modelo": "model-a",
            },
            {
                "id_item": "item-a3", "id_organizacao": ORG_A, "id_unidade": UNIT_A2,
                "unidade": "Unidade A2", "organizacao": "Empresa A",
                "nome_item": "Cadeira A2", "modelo": "Seat", "categoria": "Mobília",
                "situacao": "IN_STOCK", "data_cadastro": "2026-09-05",
                "id_responsavel": MANAGER_A, "responsavel": "Gestora A",
                "possui_danos": False, "possui_material_reciclavel": True,
                "peso_estimado_kg": 1.0, "id_modelo": "model-a",
            },
            {
                "id_item": "item-b1", "id_organizacao": ORG_B, "id_unidade": UNIT_B,
                "unidade": "Unidade B", "organizacao": "Empresa B",
                "nome_item": "Segredo da outra empresa", "modelo": "Hidden",
                "categoria": "Bateria", "situacao": "IN_STOCK", "data_cadastro": "2026-09-01",
                "id_responsavel": MANAGER_B, "responsavel": "Gestora B",
                "possui_danos": False, "possui_material_reciclavel": False,
                "peso_estimado_kg": 8.0, "id_modelo": "model-b",
            },
        ]),
        "eventos": pd.DataFrame([
            {
                "id_evento": "ev-1", "id_item": "item-a1", "id_unidade": UNIT_A,
                "tipo_evento": "CREATED", "data_ocorrencia": "2026-09-01",
            },
            {
                "id_evento": "ev-2", "id_item": "item-a-old", "id_unidade": UNIT_A,
                "tipo_evento": "DISPOSED", "data_ocorrencia": "2026-08-15",
            },
        ]),
        "users": pd.DataFrame([
            {"id": MANAGER_A, "role": "MANAGER", "status": "ACTIVE", "unit_id": UNIT_A},
            {"id": EMPLOYEE_A, "role": "EMPLOYEE", "status": "ACTIVE", "unit_id": UNIT_A},
            {"id": MANAGER_B, "role": "MANAGER", "status": "ACTIVE", "unit_id": UNIT_B},
        ]),
        "units": pd.DataFrame([
            {"id": UNIT_A, "name": "Unidade A", "organization_id": ORG_A},
            {"id": UNIT_A2, "name": "Unidade A2", "organization_id": ORG_A},
            {"id": UNIT_B, "name": "Unidade B", "organization_id": ORG_B},
        ]),
        "organizations": pd.DataFrame([
            {"id": ORG_A, "name": "Empresa A"},
            {"id": ORG_B, "name": "Empresa B"},
        ]),
        "alerts": pd.DataFrame([
            {"id": "al-a", "status": "OPEN", "unit_id": UNIT_A, "severity": "HIGH"},
            {"id": "al-b", "status": "OPEN", "unit_id": UNIT_B, "severity": "HIGH"},
        ]),
        "disposals": pd.DataFrame([
            {
                "id": "disp-a", "unitId": UNIT_A, "destination": "RECYCLING",
                "disposedAt": "2026-09-20", "cancelled": False,
            },
            {
                "id": "disp-b", "unitId": UNIT_B, "destination": "LANDFILL",
                "disposedAt": "2026-09-20", "cancelled": False,
            },
        ]),
        "composicao": pd.DataFrame([
            {"id_modelo": "model-a", "id_material": "mat-1", "nome_material": "Alumínio"},
            {"id_modelo": "model-b", "id_material": "mat-2", "nome_material": "Bateria"},
        ]),
        "includes": pd.DataFrame([
            {
                "source_id": "disp-a", "target_id": "item-a1",
                "relationship_type": "INCLUDES",
                "properties": {"weightKg": 3.1},
            },
            {
                "source_id": "disp-b", "target_id": "item-b1",
                "relationship_type": "INCLUDES",
                "properties": {"weightKg": 9000.0},
            },
        ]),
    }


@pytest.fixture
def snapshot() -> DashboardSnapshot:
    return dashboard_snapshot()


@pytest.fixture
def warehouse(snapshot) -> InMemoryWarehouse:
    holder = InMemoryWarehouse()
    holder.save(snapshot)
    return holder
