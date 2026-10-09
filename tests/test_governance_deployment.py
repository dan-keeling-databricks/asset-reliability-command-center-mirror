"""Deployment reconciliation tests for the governed MCP service."""

from types import SimpleNamespace

from scripts.deploy_governance import (
    POLICY_FQN,
    TOOLS,
    _connection_drift,
    ensure_service,
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
    calls: list[tuple[str, str, dict | None]] = []

    class ApiClient:
        def do(self, method, path, body=None):
            calls.append((method, path, body))
            if method == "GET":
                return {"name": "old", "config": {"include_tool_selectors": ["stale"]}}
            return {"name": "reconciled", "config": body["config"]}

    service, status = ensure_service(
        SimpleNamespace(api_client=ApiClient()), "policy-function-id"
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
    assert [call[0] for call in calls] == ["GET", "PATCH"]
