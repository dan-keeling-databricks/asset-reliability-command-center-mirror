from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from backend.identity import LocalDatabricksIdentityProvider
from backend.lakebase_repository import LakebaseCommandCenterRepository, lakebase_conninfo
from backend.mcp import AssetReliabilityMCP
from backend.models import CommandResponse, FollowUpRequest
from backend.production_runtime import ProductionCommandCenterRuntime
from backend.repository import CommandCenterRepository, LocalCommandCenterRepository
from backend.runtime import (
    CommandCenterRuntime,
    LocalCommandCenterRuntime,
    initial_snapshot,
)
from backend.settings import Settings, get_settings


def create_app(
    settings: Settings | None = None,
    repository: CommandCenterRepository | None = None,
    runtime: CommandCenterRuntime | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    if repository is None:
        if settings.app_adapter == "lakebase":
            repository = LakebaseCommandCenterRepository(
                settings.lakebase_dsn or lakebase_conninfo(),
                schema=settings.lakebase_schema,
                job_principal=settings.lakebase_job_principal,
                migrate_on_start=settings.lakebase_migrate_on_start,
                sse_coalesce_seconds=settings.sse_coalesce_seconds,
                telemetry_retention_seconds=settings.telemetry_retention_seconds,
                telemetry_retention_interval_seconds=(
                    settings.telemetry_retention_interval_seconds
                ),
                telemetry_retention_batch_size=settings.telemetry_retention_batch_size,
            )
        else:
            repository = LocalCommandCenterRepository(initial_snapshot())
    if runtime is None:
        if settings.app_adapter == "lakebase":
            runtime = ProductionCommandCenterRuntime(repository, settings)  # type: ignore[arg-type]
        else:
            runtime = LocalCommandCenterRuntime(repository, settings)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        await runtime.start()
        application.state.runtime = runtime
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(
        title="Asset Reliability Command Center",
        version="0.2.0",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.repository = repository
    app.state.runtime = runtime
    local_identity = (
        LocalDatabricksIdentityProvider(settings.databricks_config_profile)
        if settings.local_databricks_auth
        else None
    )

    async def identity_token(forwarded_token: str | None) -> str | None:
        if forwarded_token or local_identity is None:
            return forwarded_token
        return await local_identity.token()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "adapter": settings.app_adapter}

    @app.get("/api/dashboard")
    async def dashboard(
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
    ) -> dict[str, Any]:
        snapshot = await repository.snapshot()
        runtime.capture_identity(snapshot.scenario.id, await identity_token(forwarded_token))
        return snapshot.model_dump(mode="json")

    @app.get("/api/incidents/{incident_id}")
    async def incident(incident_id: str) -> dict[str, Any]:
        detail = await repository.incident(incident_id)
        if not detail:
            raise HTTPException(status_code=404, detail="Incident not found")
        return detail.model_dump(mode="json")

    @app.post(
        "/api/scenario/restart",
        response_model=CommandResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def restart(
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
    ) -> CommandResponse:
        scenario_id = await runtime.restart()
        runtime.capture_identity(scenario_id, await identity_token(forwarded_token))
        return CommandResponse(scenario_id=scenario_id)

    @app.post(
        "/api/scenario/trigger-p204",
        response_model=CommandResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def trigger_p204(
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
    ) -> CommandResponse:
        snapshot = await repository.snapshot()
        runtime.capture_identity(snapshot.scenario.id, await identity_token(forwarded_token))
        try:
            command_id = await runtime.trigger_excursion()
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        snapshot = await repository.snapshot()
        return CommandResponse(
            scenario_id=snapshot.scenario.id,
            command_id=command_id,
        )

    @app.post(
        "/api/incidents/{incident_id}/approve",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def approve(incident_id: str) -> dict[str, str]:
        try:
            await runtime.approve(incident_id)
        except (LookupError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"status": "accepted"}

    @app.post(
        "/api/incidents/{incident_id}/follow-up",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def follow_up(incident_id: str, request: FollowUpRequest) -> dict[str, str]:
        try:
            await runtime.follow_up(incident_id, request.message)
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"status": "accepted"}

    @app.get("/api/events")
    async def events(
        request: Request,
        after: int = 0,
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    ) -> StreamingResponse:
        cursor = max(after, int(last_event_id or 0))

        async def stream():
            nonlocal cursor
            while not await request.is_disconnected():
                snapshot = await repository.wait_for_change(cursor, timeout=10)
                if snapshot.revision > cursor:
                    cursor = snapshot.revision
                    yield f"id: {cursor}\ndata: {snapshot.model_dump_json()}\n\n"
                else:
                    yield ": keep-alive\n\n"
                await asyncio.sleep(0)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    mcp_server = AssetReliabilityMCP(repository)

    @app.post("/mcp", include_in_schema=False)
    async def mcp(request: Request) -> Response:
        payload = await request.json()
        if not isinstance(payload, dict):
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32600, "message": "Invalid request"},
                },
                status_code=400,
            )
        response = await mcp_server.handle(payload)
        if response is None:
            return Response(status_code=202)
        return JSONResponse(response)

    dist = Path(__file__).resolve().parents[1] / "frontend" / "dist"
    if dist.exists():
        assets = dist / "assets"
        if assets.exists():
            app.mount("/assets", StaticFiles(directory=assets), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        async def spa(path: str) -> Response:
            candidate = dist / path
            if path and candidate.is_file():
                return FileResponse(candidate)
            return FileResponse(dist / "index.html")
    else:

        @app.get("/", include_in_schema=False)
        async def development_root() -> dict[str, str]:
            return {"message": "Run the Vite frontend at http://localhost:5173"}

    return app


app = create_app()
