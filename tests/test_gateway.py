import json

import httpx
import pytest

from backend.gateway import GatewayAsk, GovernedGatewayClient


@pytest.mark.asyncio
async def test_gateway_parses_native_ask_from_sse_and_preserves_exact_request() -> None:
    captured: dict[str, str] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.content.decode()
        captured["version"] = request.headers["MCP-Protocol-Version"]
        payload = {
            "jsonrpc": "2.0",
            "id": "call",
            "error": {
                "code": -32042,
                "message": "Approval required",
                "data": {
                    "elicitations": [
                        {
                            "mode": "url",
                            "elicitationId": "approval-7",
                            "url": "https://workspace.example/elicitation/approval-7",
                            "message": "A supervisor must approve this operational write.",
                        }
                    ]
                },
            },
        }
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=f"event: message\ndata: {json.dumps(payload)}\n\n",
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        gateway = GovernedGatewayClient("https://workspace.example/mcp", client)
        with pytest.raises(GatewayAsk) as caught:
            await gateway.call_tool("submit_work_order", {"incident_id": "i-1"}, "obo")

    assert caught.value.serialized_request == captured["body"]
    assert caught.value.elicitation.elicitation_id == "approval-7"
    assert captured["version"] == "2025-11-25"


@pytest.mark.asyncio
async def test_gateway_accepts_plain_json_tool_result() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": "1", "result": {"tools": [{"name": "read"}]}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        gateway = GovernedGatewayClient("https://workspace.example/mcp", client)
        tools = await gateway.list_tools("obo")

    assert tools == [{"name": "read"}]
