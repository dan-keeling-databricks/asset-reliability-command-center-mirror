from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from backend.lakebase_repository import LakebaseCommandCenterRepository, lakebase_conninfo
from backend.mcp import AssetReliabilityMCP
from backend.models import (
    AppUser,
    CommandResponse,
    DashboardSnapshot,
    FollowUpRequest,
    OperatorHandoverRequest,
    ViewerState,
)
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
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def app_user(email: str | None, name: str | None) -> AppUser:
        if not email:
            if settings.app_adapter != "local":
                raise HTTPException(status_code=401, detail="Authenticated App user is required")
            email, name = "local.operator@databricks.com", "Local Operator"
        email = email.lower()
        fallback_name = email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()
        return AppUser(
            email=email,
            name=name if name and name.lower() != email else fallback_name,
        )

    async def register_and_snapshot(user: AppUser) -> DashboardSnapshot:
        await repository.register_user(user)
        snapshot = await repository.snapshot()
        operator_email = (
            snapshot.shift_operator.operator.email if snapshot.shift_operator.operator else None
        )
        snapshot.viewer = ViewerState(
            email=user.email,
            name=user.name,
            is_operator=user.email == operator_email,
        )
        return snapshot

    def require_operator(snapshot: DashboardSnapshot, user: AppUser) -> None:
        operator = snapshot.shift_operator.operator
        if not operator or operator.email != user.email:
            raise HTTPException(
                status_code=403,
                detail=f"Read-only: {operator.name if operator else 'another user'} is operator",
            )

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "adapter": settings.app_adapter}

    @app.get("/api/dashboard")
    async def dashboard(
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None, alias="X-Forwarded-Preferred-Username"
        ),
    ) -> dict[str, Any]:
        user = app_user(forwarded_email, forwarded_name)
        runtime.cache_user_identity(user.email, forwarded_token)
        snapshot = await register_and_snapshot(user)
        if snapshot.viewer and snapshot.viewer.is_operator:
            runtime.bind_operator_identity(snapshot.scenario.id, user.email)
        return snapshot.model_dump(mode="json")

    @app.get("/api/incidents/{incident_id}")
    async def incident(
        incident_id: str,
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
    ) -> dict[str, Any]:
        if forwarded_email:
            runtime.cache_user_identity(forwarded_email, forwarded_token)
        detail = await repository.incident(incident_id)
        if not detail:
            raise HTTPException(status_code=404, detail="Incident not found")
        model_outputs = {
            activity.summary
            for activity in detail.activities
            if activity.category == "model_call" and activity.summary
        }
        detail = detail.model_copy(
            update={
                "conversation": [
                    turn
                    for turn in detail.conversation
                    if turn.role != "agent" or turn.content not in model_outputs
                ]
            }
        )
        return detail.model_dump(mode="json")

    @app.post(
        "/api/scenario/restart",
        response_model=CommandResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def restart(
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None, alias="X-Forwarded-Preferred-Username"
        ),
    ) -> CommandResponse:
        user = app_user(forwarded_email, forwarded_name)
        runtime.cache_user_identity(user.email, forwarded_token)
        snapshot = await register_and_snapshot(user)
        require_operator(snapshot, user)
        scenario_id = await runtime.restart()
        runtime.capture_identity(scenario_id, forwarded_token)
        return CommandResponse(scenario_id=scenario_id)

    @app.post(
        "/api/scenario/trigger-p204",
        response_model=CommandResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def trigger_p204(
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None, alias="X-Forwarded-Preferred-Username"
        ),
    ) -> CommandResponse:
        user = app_user(forwarded_email, forwarded_name)
        runtime.cache_user_identity(user.email, forwarded_token)
        snapshot = await register_and_snapshot(user)
        require_operator(snapshot, user)
        runtime.capture_identity(snapshot.scenario.id, forwarded_token)
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
        "/api/operator/handover",
        response_model=CommandResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def handover_operator(
        request: OperatorHandoverRequest,
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None, alias="X-Forwarded-Preferred-Username"
        ),
    ) -> CommandResponse:
        if not request.confirmed:
            raise HTTPException(status_code=400, detail="Handover confirmation is required")
        user = app_user(forwarded_email, forwarded_name)
        runtime.cache_user_identity(user.email, forwarded_token)
        await repository.register_user(user)
        try:
            scenario_id = await runtime.handover_operator(
                request.expected_version, request.operator_email.lower(), user.email
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return CommandResponse(scenario_id=scenario_id)

    @app.post(
        "/api/incidents/{incident_id}/approve",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def approve(
        incident_id: str,
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None, alias="X-Forwarded-Preferred-Username"
        ),
    ) -> dict[str, str]:
        user = app_user(forwarded_email, forwarded_name)
        runtime.cache_user_identity(user.email, forwarded_token)
        require_operator(await register_and_snapshot(user), user)
        try:
            await runtime.approve(incident_id)
        except (LookupError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"status": "accepted"}

    @app.post(
        "/api/incidents/{incident_id}/follow-up",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def follow_up(
        incident_id: str,
        request: FollowUpRequest,
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None, alias="X-Forwarded-Preferred-Username"
        ),
    ) -> dict[str, str]:
        user = app_user(forwarded_email, forwarded_name)
        runtime.cache_user_identity(user.email, forwarded_token)
        require_operator(await register_and_snapshot(user), user)
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
        forwarded_token: str | None = Header(default=None, alias="X-Forwarded-Access-Token"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None, alias="X-Forwarded-Preferred-Username"
        ),
    ) -> StreamingResponse:
        cursor = max(after, int(last_event_id or 0))
        user = app_user(forwarded_email, forwarded_name)
        runtime.cache_user_identity(user.email, forwarded_token)
        await repository.register_user(user)

        async def stream():
            nonlocal cursor
            while not await request.is_disconnected():
                snapshot = await repository.wait_for_change(cursor, timeout=10)
                if snapshot.revision > cursor:
                    cursor = snapshot.revision
                    operator_email = (
                        snapshot.shift_operator.operator.email
                        if snapshot.shift_operator.operator
                        else None
                    )
                    snapshot.viewer = ViewerState(
                        email=user.email,
                        name=user.name,
                        is_operator=user.email == operator_email,
                    )
                    if snapshot.viewer.is_operator:
                        runtime.bind_operator_identity(snapshot.scenario.id, user.email)
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
