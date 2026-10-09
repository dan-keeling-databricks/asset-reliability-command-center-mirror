"""FastAPI routes for the Fleet telemetry analyst.

Mount :func:`create_genie_router` from the application composition root.  The router requires
the Databricks reverse proxy's user token for every Genie operation, ensuring queries execute
with the signed-in user's Unity Catalog permissions rather than the App service principal.
"""

from __future__ import annotations

import os

from databricks.sdk.errors import DatabricksError
from fastapi import APIRouter, Header, HTTPException, Query

from backend.genie_service import (
    DatabricksGenieClient,
    GenieCaller,
    GenieMessageView,
    GeniePlatformClient,
    GenieQuestion,
    GenieService,
    GenieSubmission,
)


def create_genie_router(
    *,
    space_id: str | None = None,
    host: str | None = None,
    source_label: str | None = None,
    platform_client: GeniePlatformClient | None = None,
) -> APIRouter:
    """Build the telemetry-only Genie router with injectable platform access for tests."""

    configured_space = space_id or os.getenv("DATABRICKS_GENIE_SPACE_ID", "")
    configured_host = host or os.getenv("DATABRICKS_HOST", "")
    client = platform_client
    if client is None:
        if not configured_host:
            raise ValueError("DATABRICKS_HOST is required for Genie integration")
        client = DatabricksGenieClient(configured_host)
    service = GenieService(
        space_id=configured_space,
        client=client,
        source_label=source_label or os.getenv(
            "GENIE_SOURCE_LABEL",
            "Unity Catalog · pump_telemetry_history",
        ),
    )
    router = APIRouter(prefix="/api/genie", tags=["fleet-telemetry-analyst"])

    def caller(
        forwarded_user: str | None,
        forwarded_email: str | None,
        forwarded_name: str | None,
    ) -> GenieCaller:
        subject = forwarded_user or forwarded_email
        if not subject:
            raise HTTPException(status_code=401, detail="Signed-in Databricks user is required")
        display_name = forwarded_name or forwarded_email or forwarded_user or "Signed in"
        return GenieCaller(subject=subject, email=forwarded_email, display_name=display_name)

    def require_token(forwarded_token: str | None) -> str:
        if not forwarded_token:
            raise HTTPException(
                status_code=401,
                detail="Genie requires the signed-in user's OBO access token",
            )
        return forwarded_token

    def translate_error(error: Exception) -> HTTPException:
        if isinstance(error, PermissionError):
            return HTTPException(status_code=403, detail=str(error))
        if isinstance(error, ValueError):
            return HTTPException(status_code=400, detail=str(error))
        if isinstance(error, DatabricksError):
            return HTTPException(
                status_code=502,
                detail="Databricks Genie could not complete the request",
            )
        return HTTPException(status_code=502, detail="Unable to reach Databricks Genie")

    @router.get("/whoami")
    async def whoami(
        forwarded_user: str | None = Header(default=None, alias="X-Forwarded-User"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None,
            alias="X-Forwarded-Preferred-Username",
        ),
        forwarded_token: str | None = Header(
            default=None,
            alias="X-Forwarded-Access-Token",
        ),
    ) -> dict[str, str | bool | None]:
        identity = caller(forwarded_user, forwarded_email, forwarded_name)
        return {
            "subject": identity.subject,
            "email": identity.email,
            "display_name": identity.display_name,
            "execution_mode": "on_behalf_of_user",
            "obo_ready": bool(forwarded_token),
        }

    @router.post("/messages", response_model=GenieSubmission, status_code=202)
    async def submit_message(
        question: GenieQuestion,
        forwarded_user: str | None = Header(default=None, alias="X-Forwarded-User"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None,
            alias="X-Forwarded-Preferred-Username",
        ),
        forwarded_token: str | None = Header(
            default=None,
            alias="X-Forwarded-Access-Token",
        ),
    ) -> GenieSubmission:
        try:
            return await service.submit(
                caller=caller(forwarded_user, forwarded_email, forwarded_name),
                token=require_token(forwarded_token),
                question=question,
            )
        except HTTPException:
            raise
        except Exception as error:
            raise translate_error(error) from error

    @router.get("/messages/{message_id}", response_model=GenieMessageView)
    async def get_message(
        message_id: str,
        conversation_id: str = Query(min_length=1, max_length=256),
        forwarded_user: str | None = Header(default=None, alias="X-Forwarded-User"),
        forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
        forwarded_name: str | None = Header(
            default=None,
            alias="X-Forwarded-Preferred-Username",
        ),
        forwarded_token: str | None = Header(
            default=None,
            alias="X-Forwarded-Access-Token",
        ),
    ) -> GenieMessageView:
        try:
            return await service.message(
                caller=caller(forwarded_user, forwarded_email, forwarded_name),
                token=require_token(forwarded_token),
                conversation_id=conversation_id,
                message_id=message_id,
            )
        except HTTPException:
            raise
        except Exception as error:
            raise translate_error(error) from error

    return router
