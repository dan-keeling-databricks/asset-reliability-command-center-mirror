from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OboIdentity:
    token: str
    subject: str | None


class OboIdentityRegistry:
    """Process-local OBO credentials; bearer tokens are deliberately never persisted."""

    def __init__(self) -> None:
        self._identities: dict[str, OboIdentity] = {}

    def capture(self, scenario_id: str, token: str | None) -> OboIdentity | None:
        if not token:
            return self._identities.get(scenario_id)
        identity = OboIdentity(token=token, subject=_jwt_subject(token))
        self._identities[scenario_id] = identity
        return identity

    def get(self, scenario_id: str) -> OboIdentity | None:
        return self._identities.get(scenario_id)

    def clear(self) -> None:
        self._identities.clear()


class LocalDatabricksIdentityProvider:
    """Resolve the current CLI user's bearer token for local live development only."""

    def __init__(self, profile: str | None = None, workspace_client: Any | None = None) -> None:
        if workspace_client is None:
            from databricks.sdk import WorkspaceClient

            workspace_client = WorkspaceClient(profile=profile)
        self._workspace_client = workspace_client

    async def token(self) -> str:
        headers = await asyncio.to_thread(self._workspace_client.config.authenticate)
        authorization = (headers or {}).get("Authorization", "")
        scheme, separator, token = authorization.partition(" ")
        if not separator or scheme.lower() != "bearer" or not token:
            raise RuntimeError("The Databricks CLI profile did not provide a bearer token")
        return token


def _jwt_subject(token: str) -> str | None:
    try:
        payload = token.split(".")[1]
        decoded = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        claims = json.loads(decoded)
        return claims.get("email") or claims.get("preferred_username") or claims.get("sub")
    except (IndexError, ValueError, json.JSONDecodeError):
        return None
