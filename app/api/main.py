"""FastAPI entrypoint for the Zera ms-analytics-bff service."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from app.api.auth import JwtVerifier
from app.api.overview import build_overview, parse_filters, scoped_unit_id
from app.api.sse import SSE_HEADERS, format_comment, format_event
from app.api.types import AccessToken
from app.api.warehouse import AnalyticsWarehouse, InMemoryWarehouse, live_warehouse
from config.environments import Environments
from logger.logger import Logger

myLogger = Logger()
envs = Environments()


def create_app(
    runtime: Environments | None = None,
    warehouse: AnalyticsWarehouse | None = None,
    verify_token: Callable[[str], AccessToken] | None = None,
    sse_max_events: int | None = None,
) -> FastAPI:
    resolved = runtime or Environments()
    store = warehouse or _default_warehouse(resolved)
    verifier = verify_token or JwtVerifier(resolved).verify
    application = FastAPI(title="ms-analytics-bff")
    application.state.warehouse = store
    application.state.sse_max_events = sse_max_events
    _configure_cors(application, resolved)

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/api/v1/dashboard/overview")
    async def dashboard_overview(
        request: Request,
        authorization: str | None = Header(default=None),
        access_token: str | None = Query(default=None),
        x_unit_id: str | None = Header(default=None, alias="X-Unit-Id"),
        period: str | None = Query(default="6m"),
        date_from: str | None = Query(default=None, alias="from"),
        date_to: str | None = Query(default=None, alias="to"),
        category: str | None = Query(default=None),
        status: str | None = Query(default=None),
        unit_id: str | None = Query(default=None),
        responsible: str | None = Query(default=None),
        q: str | None = Query(default=None),
        view: str | None = Query(default="todos"),
    ) -> StreamingResponse:
        token = verifier(_extract_token(authorization, access_token))
        requested_unit = (x_unit_id or unit_id or "").strip() or None
        identity = store.identity(token["user_id"])
        if identity is None and not store.is_empty():
            raise HTTPException(status_code=403, detail="forbidden")
        if identity is not None:
            identity = {**identity, "role": token["role"]}
            scoped_unit_id(identity, requested_unit)
        filters_period = parse_filters(
            period, date_from, date_to, category, status, requested_unit,
            responsible, q, view,
        )
        poll = max(1, int(resolved.SSE_POLL_SECONDS))
        heartbeat = max(poll, int(resolved.SSE_HEARTBEAT_SECONDS))

        async def events() -> AsyncIterator[str]:
            last_batch = ""
            elapsed = 0
            sent = 0
            limit = application.state.sse_max_events
            while True:
                current = store.identity(token["user_id"])
                if current is None:
                    if last_batch != "missing":
                        yield format_event("unavailable", {"detail": "snapshot not ready"})
                        last_batch = "missing"
                        sent += 1
                else:
                    tenant = {**current, "role": token["role"]}
                    try:
                        selected_unit = scoped_unit_id(tenant, requested_unit)
                    except HTTPException as exc:
                        yield format_event(
                            "error",
                            {"status": exc.status_code, "detail": str(exc.detail)},
                        )
                        break
                    filters = {**filters_period, "unit_id": selected_unit}
                    snapshot = store.load(tenant["organization_id"], selected_unit)
                    if snapshot is None:
                        if last_batch != "missing":
                            yield format_event(
                                "unavailable", {"detail": "snapshot not ready"},
                            )
                            last_batch = "missing"
                            sent += 1
                    elif snapshot["batch_id"] != last_batch:
                        yield format_event(
                            "snapshot",
                            build_overview(snapshot, tenant, filters),
                        )
                        last_batch = snapshot["batch_id"]
                        elapsed = 0
                        sent += 1
                    elif elapsed >= heartbeat:
                        yield format_comment("heartbeat")
                        elapsed = 0
                        sent += 1
                if limit is not None and sent >= limit:
                    break
                if await request.is_disconnected():
                    break
                await asyncio.sleep(poll)
                elapsed += poll

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers=SSE_HEADERS,
        )

    return application


def _default_warehouse(resolved: Environments) -> AnalyticsWarehouse:
    if resolved.PG_HOST and resolved.PG_PASSWORD:
        return live_warehouse(resolved)
    myLogger.Warning(
        "PG_HOST/PG_PASSWORD ausentes — a API sobe sem lote analytics até o cron gravar."
    )
    return InMemoryWarehouse()


def _extract_token(authorization: str | None, access_token: str | None) -> str:
    if authorization:
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() == "bearer" and value.strip():
            return value.strip()
        raise HTTPException(status_code=401, detail="invalid authorization header")
    if access_token and access_token.strip():
        return access_token.strip()
    raise HTTPException(status_code=401, detail="missing token")


def _configure_cors(application: FastAPI, resolved: Environments) -> None:
    origins = [origin.strip() for origin in resolved.CORS_ORIGINS.split(",") if origin.strip()]
    if not origins:
        return
    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=False,
        allow_methods=["GET", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Unit-Id"],
    )


app = create_app()


def run() -> None:
    myLogger.Info("Starting ms-analytics-bff API")
    uvicorn.run(app, host=envs.APP_HOST, port=int(envs.APP_PORT))


if __name__ == "__main__":
    run()
