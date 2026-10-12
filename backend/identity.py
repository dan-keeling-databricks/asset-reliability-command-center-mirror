"""Process-local handling of forwarded operator credentials.

Bearer tokens are deliberately excluded from Lakebase and logs. Only the decoded subject is
persisted for audit, so an App restart requires a fresh authenticated request before a pending
Gateway action can resume.
"""

from __future__ import annotations

import base64
import hmac
import json
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class OboIdentity:
    """Opaque operator token plus the non-secret subject extracted for audit."""

    token: str
    subject: str | None


class OboIdentityRegistry:
    """Process-local OBO credentials; bearer tokens are deliberately never persisted."""

    def __init__(self) -> None:
        self._identities: dict[str, OboIdentity] = {}

    def capture(self, scenario_id: str, token: str | None) -> OboIdentity | None:
        """Bind a forwarded token to a scenario without persisting the credential."""

        if not token:
            return self._identities.get(scenario_id)
        identity = OboIdentity(token=token, subject=_jwt_subject(token))
        self._identities[scenario_id] = identity
        return identity

    def get(self, scenario_id: str) -> OboIdentity | None:
        return self._identities.get(scenario_id)

    def clear(self) -> None:
        self._identities.clear()


def is_expected_service_principal(
    headers: Mapping[str, str], expected_principal: str | None
) -> bool:
    """Validate the trusted identity injected by the Databricks Apps proxy.

    Unity Gateway reaches the source App with the OAuth client configured on the HTTP
    connection. The Apps proxy authenticates that token and replaces the forwarded identity
    headers before the request reaches FastAPI. We accept the client-id form used for service
    principals and the forwarded-email/user forms retained by older proxy versions. A missing
    expected principal fails closed in deployed mode.
    """

    if not expected_principal:
        return False
    expected = expected_principal.strip().lower()
    forwarded_human = headers.get("x-forwarded-email")
    if forwarded_human and not hmac.compare_digest(
        forwarded_human.strip().lower(), expected
    ):
        return False
    candidates = (
        headers.get("x-forwarded-service-principal"),
        headers.get("x-forwarded-client-id"),
        forwarded_human,
        headers.get("x-forwarded-user"),
    )
    return any(
        candidate is not None
        and hmac.compare_digest(candidate.strip().lower(), expected)
        for candidate in candidates
    )


def _jwt_subject(token: str) -> str | None:
    try:
        payload = token.split(".")[1]
        decoded = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        claims = json.loads(decoded)
        return claims.get("email") or claims.get("preferred_username") or claims.get("sub")
    except (IndexError, ValueError, json.JSONDecodeError):
        return None
