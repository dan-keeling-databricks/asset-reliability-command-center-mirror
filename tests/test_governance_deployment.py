"""Deployment reconciliation tests for the governed MCP service."""

from types import SimpleNamespace

import pytest

import scripts.deploy_governance as deployment
from backend.gateway import GatewayAsk, GatewayElicitation
from scripts.deploy_governance import (
    POLICY_FQN,
    TOOLS,
    _connection_drift,
    ensure_service,
    probe_submit_policy,
)


def test_connection_drift_ignores_redacted_secret_but_detects_routing_change() -> None:
    connection = SimpleNamespace(
        options={"host": "https://app.example", "base_path": "/mcp", "client_id": "app-sp"}
    )

    assert not _connection_drift(
        connection,
        {
            "host": "https://app.example",
            "base_path": "/mcp",
            "client_id": "app-sp",
            "client_secret": "<redacted>",
        },
    )
    assert _connection_drift(
        connection,
        {
            "host": "https://different.example",
            "base_path": "/mcp",
            "client_id": "app-sp",
            "client_secret": "<redacted>",
        },
    )


def test_service_drift_is_patched_to_declared_tools_and_ask_policy() -> None:
    calls: list[tuple[str, str, dict | None, dict | None]] = []
    current = {"include_tool_selectors": ["stale"]}

    class ApiClient:
        def do(self, method, path, body=None, query=None):
            nonlocal current
            calls.append((method, path, body, query))
            if method == "GET":
                return {"name": "service", "config": current}
            current = body["config"]
            return {"name": "reconciled", "config": body["config"]}

    service, status = ensure_service(
        SimpleNamespace(api_client=ApiClient()),
        "policy-function-id",
        retry_delay_seconds=0,
    )

    assert status == "updated"
    assert service["config"]["include_tool_selectors"] == TOOLS
    assert service["config"]["service_policies"] == [
        {
            "name": "submit_work_order_requires_approval",
            "policy_type": "POLICY_TYPE_CUSTOM",
            "handler": f"functions/{POLICY_FQN}",
            "function_id": "policy-function-id",
            "rank": 1,
        }
    ]
    assert [call[0] for call in calls] == ["GET", "PATCH", "GET"]
    assert calls[1][3] == {"update_mask": "config"}


def test_service_patch_is_retried_until_a_fresh_get_observes_declared_config() -> None:
    calls: list[str] = []
    desired = deployment._service_config("policy-function-id")
    get_count = 0

    class ApiClient:
        def do(self, method, _path, body=None, query=None):
            nonlocal get_count
            calls.append(method)
            if method == "PATCH":
                assert query == {"update_mask": "config"}
            if method == "GET":
                get_count += 1
                if get_count < 3:
                    return {"name": "stale", "config": {"include_tool_selectors": ["stale"]}}
                return {"name": "ready", "config": desired}
            return {"name": "patch-accepted", "config": body["config"]}

    service, status = ensure_service(
        SimpleNamespace(api_client=ApiClient()),
        "policy-function-id",
        max_attempts=3,
        retry_delay_seconds=0,
    )

    assert status == "updated"
    assert service["name"] == "ready"
    assert calls == ["GET", "PATCH", "GET", "PATCH", "GET"]


@pytest.mark.asyncio
async def test_policy_probe_discovers_exact_tools_then_authenticates_source_then_gets_ask(
    monkeypatch,
) -> None:
    events: list[str] = []

    class FakeGateway:
        def __init__(self, endpoint):
            assert endpoint.endswith(deployment.SERVICE_FQN)

        async def list_tools(self, token):
            assert token == "obo-token"
            events.append("list")
            return [{"name": name} for name in TOOLS]

        async def call_tool(self, name, arguments, token):
            assert token == "obo-token"
            events.append(name)
            if name == "get_asset_telemetry":
                assert arguments == {"asset_id": "P-204"}
                return {"result": {"isError": False, "structuredContent": {"asset_id": "P-204"}}}
            raise GatewayAsk(
                '{"method":"tools/call"}',
                GatewayElicitation("ask-1", "https://example.test/ask", "Approval required"),
            )

        async def close(self):
            events.append("close")

    monkeypatch.setattr(deployment, "GovernedGatewayClient", FakeGateway)
    client = SimpleNamespace(
        config=SimpleNamespace(
            host="https://workspace.example",
            authenticate=lambda: {"Authorization": "Bearer obo-token"},
        )
    )

    result = await probe_submit_policy(client, retry_delay_seconds=0)

    assert events == ["list", "get_asset_telemetry", "submit_work_order", "close"]
    assert result == {
        "tools": TOOLS,
        "source_authentication": "verified",
        "decision": "ASK",
        "elicitation_id": "ask-1",
        "execution": "not executed",
    }
    assert "obo-token" not in str(result)


@pytest.mark.asyncio
async def test_policy_probe_rejects_any_tool_surface_drift(monkeypatch) -> None:
    class FakeGateway:
        def __init__(self, _endpoint):
            pass

        async def list_tools(self, _token):
            return [{"name": name} for name in [*TOOLS, "unexpected_tool"]]

        async def close(self):
            pass

    monkeypatch.setattr(deployment, "GovernedGatewayClient", FakeGateway)
    client = SimpleNamespace(
        config=SimpleNamespace(
            host="https://workspace.example",
            authenticate=lambda: {"Authorization": "Bearer obo-token"},
        )
    )

    with pytest.raises(RuntimeError, match="exact declared tool set"):
        await probe_submit_policy(client, max_attempts=1, retry_delay_seconds=0)
