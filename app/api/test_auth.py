import pytest
from fastapi import HTTPException

from app.api.auth import JwtVerifier
from app.api.conftest import MANAGER_A, UNIT_A, UNIT_B, encode_token
from app.api.serialize import from_wire, to_wire
from app.api.warehouse import InMemoryWarehouse
from config.environments import Environments


def test_expired_token_is_rejected(api_envs, rsa_keys):
    verifier = JwtVerifier(api_envs)
    token = encode_token(rsa_keys, "manager-a", extra={"exp": 1, "iat": 1})

    with pytest.raises(HTTPException) as exc:
        verifier.verify(token)

    assert exc.value.status_code == 401


def test_wrong_issuer_is_rejected(api_envs, rsa_keys):
    verifier = JwtVerifier(api_envs)
    token = encode_token(rsa_keys, "manager-a", extra={"iss": "another-service"})

    with pytest.raises(HTTPException) as exc:
        verifier.verify(token)

    assert exc.value.status_code == 401


def test_wire_roundtrip_keeps_batch(snapshot):
    restored = from_wire(to_wire(snapshot))

    assert restored["batch_id"] == snapshot["batch_id"]
    assert list(restored["itens"]["id_item"]) == list(snapshot["itens"]["id_item"])


def test_unknown_role_is_forbidden(api_envs, rsa_keys):
    verifier = JwtVerifier(api_envs)
    token = encode_token(rsa_keys, "manager-a", role="ADMIN")

    with pytest.raises(HTTPException) as exc:
        verifier.verify(token)

    assert exc.value.status_code == 403


def test_missing_jwt_config_returns_unavailable(monkeypatch):
    monkeypatch.setenv("JWT_PUBLIC_KEY", "")
    monkeypatch.setenv("JWT_JWKS_URL", "")
    verifier = JwtVerifier(Environments())

    with pytest.raises(HTTPException) as exc:
        verifier.verify("not-a-token")

    assert exc.value.status_code == 503
