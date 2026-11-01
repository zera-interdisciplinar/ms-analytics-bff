"""JWT resource-server validation aligned with ms-administrative-core."""

from __future__ import annotations

from typing import cast

import jwt
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from fastapi import HTTPException
from jwt import PyJWKClient

from app.api.types import AccessToken, Role
from config.environments import Environments
from logger.logger import Logger

myLogger = Logger()

_ALLOWED_ROLES: frozenset[str] = frozenset({"MANAGER", "EMPLOYEE"})


class JwtVerifier:
    def __init__(self, envs: Environments) -> None:
        self._issuer = envs.JWT_ISSUER
        pem = envs.JWT_PUBLIC_KEY.replace("\\n", "\n").strip()
        self._public_key: object | None = None
        self._jwk_client: PyJWKClient | None = None
        if pem:
            self._public_key = load_pem_public_key(pem.encode("utf-8"))
        elif envs.JWT_JWKS_URL.strip():
            self._jwk_client = PyJWKClient(envs.JWT_JWKS_URL.strip())
        else:
            myLogger.Warning(
                "JWT_PUBLIC_KEY/JWT_JWKS_URL ausentes — a API recusa tokens até configurar a chave."
            )

    def verify(self, token: str) -> AccessToken:
        signing_key = self._signing_key(token)
        try:
            payload = jwt.decode(
                token,
                signing_key,
                algorithms=["RS256"],
                issuer=self._issuer,
                options={"require": ["exp", "iat", "iss", "sub"]},
            )
        except jwt.ExpiredSignatureError as exc:
            raise HTTPException(status_code=401, detail="token expired") from exc
        except jwt.InvalidTokenError as exc:
            raise HTTPException(status_code=401, detail="invalid token") from exc

        if payload.get("scope") and not payload.get("role"):
            raise HTTPException(status_code=403, detail="service token is not allowed")

        role = str(payload.get("role") or "")
        if role not in _ALLOWED_ROLES:
            raise HTTPException(status_code=403, detail="forbidden")

        user_id = str(payload.get("sub") or "")
        if not user_id:
            raise HTTPException(status_code=401, detail="invalid token")

        return {
            "user_id": user_id,
            "email": str(payload.get("email") or ""),
            "role": cast(Role, role),
        }

    def _signing_key(self, token: str) -> object:
        if self._jwk_client is not None:
            try:
                return self._jwk_client.get_signing_key_from_jwt(token).key
            except jwt.PyJWKClientError as exc:
                raise HTTPException(status_code=401, detail="invalid token") from exc
        if self._public_key is None:
            raise HTTPException(status_code=503, detail="jwt is not configured")
        return self._public_key
