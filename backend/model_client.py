"""Thin asynchronous client for Databricks-hosted chat and tool-calling endpoints."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx


@dataclass(frozen=True)
class ModelResult:
    content: str
    input_tokens: int
    output_tokens: int


@dataclass(frozen=True)
class ModelTurn:
    message: dict[str, Any]
    input_tokens: int
    output_tokens: int


class DatabricksModelClient:
    """Normalize Databricks model responses into the runtime's provider-neutral contract."""

    def __init__(self, host: str, client: httpx.AsyncClient | None = None) -> None:
        self.host = host.rstrip("/")
        if self.host and not self.host.startswith(("http://", "https://")):
            self.host = f"https://{self.host}"
        self._client = client or httpx.AsyncClient(timeout=60)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def chat(
        self, endpoint: str, messages: list[dict[str, str]], token: str | None = None
    ) -> ModelResult:
        turn = await self.turn(endpoint, messages, token=token)
        return ModelResult(
            content=str(turn.message.get("content") or ""),
            input_tokens=turn.input_tokens,
            output_tokens=turn.output_tokens,
        )

    async def turn(
        self,
        endpoint: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        token: str | None = None,
    ) -> ModelTurn:
        """Execute one model turn, optionally exposing runtime-discovered function tools."""

        if token:
            headers = {"Authorization": f"Bearer {token}"}
        else:
            from databricks.sdk import WorkspaceClient

            headers = await asyncio.to_thread(WorkspaceClient().config.authenticate)
        payload: dict[str, Any] = {
            "messages": messages,
            "max_tokens": 450,
            "temperature": 0.1,
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"
        response = await self._client.post(
            f"{self.host}/serving-endpoints/{quote(endpoint, safe='')}/invocations",
            headers={**(headers or {}), "Content-Type": "application/json"},
            json=payload,
        )
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
        message = payload["choices"][0]["message"]
        for tool_call in message.get("tool_calls", []):
            arguments = tool_call.get("function", {}).get("arguments")
            if isinstance(arguments, dict):
                tool_call["function"]["arguments"] = json.dumps(arguments)
        usage = payload.get("usage", {})
        return ModelTurn(
            message=message,
            input_tokens=int(usage.get("prompt_tokens", 0)),
            output_tokens=int(usage.get("completion_tokens", 0)),
        )
