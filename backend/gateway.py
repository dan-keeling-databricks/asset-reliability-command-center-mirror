"""MCP-over-Unity-Gateway transport and native human-approval decoding.

The application always sends operator OBO credentials on Gateway calls. An ``ASK`` response
is represented as data by :class:`GatewayAsk`; this module never approves or executes a
request locally.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import httpx


@dataclass(frozen=True)
class GatewayElicitation:
    elicitation_id: str
    url: str
    message: str


class GatewayAsk(Exception):
    """Signal that Gateway paused an exact serialized request for authorization."""

    def __init__(self, serialized_request: str, elicitation: GatewayElicitation) -> None:
        super().__init__(elicitation.message)
        self.serialized_request = serialized_request
        self.elicitation = elicitation


class GovernedGatewayClient:
    """Minimal MCP-over-Unity-Gateway client with native ASK handling."""

    def __init__(self, endpoint: str, client: httpx.AsyncClient | None = None) -> None:
        self.endpoint = endpoint
        self._client = client or httpx.AsyncClient(timeout=30)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def list_tools(self, token: str) -> list[dict[str, Any]]:
        """Discover tools on the one configured MCP service for the active operator."""

        request = {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": "tools/list",
            "params": {},
        }
        response = await self._send(json.dumps(request, separators=(",", ":")), token)
        return response.get("result", {}).get("tools", [])

    async def call_tool(self, name: str, arguments: dict[str, Any], token: str) -> dict[str, Any]:
        request = {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        return await self._send(json.dumps(request, separators=(",", ":")), token)

    async def replay(self, serialized_request: str, token: str) -> dict[str, Any]:
        """Retry the exact bytes represented by a persisted request after native approval."""

        return await self._send(serialized_request, token)

    async def _send(self, serialized_request: str, token: str) -> dict[str, Any]:
        """Send exact JSON-RPC bytes and translate Gateway's URL elicitation into ``ASK``."""

        response = await self._client.post(
            self.endpoint,
            content=serialized_request,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": "2025-11-25",
            },
        )
        response.raise_for_status()
        payload = _decode_gateway_response(response)
        error = payload.get("error")
        if error and _contains_code(error, -32042):
            elicitation = next(
                (
                    item
                    for item in _nested_dicts(error)
                    if item.get("mode") == "url" and item.get("elicitationId") and item.get("url")
                ),
                None,
            )
            if not elicitation:
                raise RuntimeError("Gateway returned ASK without a URL elicitation")
            raise GatewayAsk(
                serialized_request,
                GatewayElicitation(
                    elicitation_id=elicitation["elicitationId"],
                    url=elicitation["url"],
                    message=elicitation.get("message", "Approval is required"),
                ),
            )
        if error:
            raise RuntimeError(error.get("message", "Unity Gateway call failed"))
        return payload


def _decode_gateway_response(response: httpx.Response) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "")
    if "text/event-stream" not in content_type and not response.text.lstrip().startswith("event:"):
        return response.json()
    messages = []
    for line in response.text.splitlines():
        if line.startswith("data:"):
            messages.append(json.loads(line.removeprefix("data:").strip()))
    if not messages:
        raise RuntimeError("Unity Gateway returned an empty event stream")
    return messages[-1]


def _nested_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _nested_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _nested_dicts(child)


def _contains_code(value: Any, code: int) -> bool:
    return any(item.get("code") == code for item in _nested_dicts(value))
