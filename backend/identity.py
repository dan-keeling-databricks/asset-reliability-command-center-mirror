"""Process-local handling of forwarded operator credentials.

Bearer tokens are deliberately excluded from Lakebase and logs. Only the decoded subject is
persisted for audit, so an App restart requires a fresh authenticated request before a pending
Gateway action can resume.
"""

from __future__ import annotations

import base64
import json
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


def _jwt_subject(token: str) -> str | None:
    try:
        payload = token.split(".")[1]
        decoded = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        claims = json.loads(decoded)
        return claims.get("email") or claims.get("preferred_username") or claims.get("sub")
    except (IndexError, ValueError, json.JSONDecodeError):
        return None
