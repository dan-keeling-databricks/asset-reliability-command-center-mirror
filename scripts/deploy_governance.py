"""Deploy the Unity Gateway skill, App-backed MCP connection, service, and ASK policy."""

from __future__ import annotations

import asyncio
import json
import os
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel
from databricks.sdk.service.catalog import ConnectionType

from backend.gateway import GatewayAsk, GovernedGatewayClient

PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE", "fe_vm_rtm")
CATALOG = "classic_stable_realtime_demos_catalog"
SCHEMA = "asset_reliability_command_center"
PARENT = f"schemas/{CATALOG}.{SCHEMA}"
APP_NAME = "asset-reliability-center"
SKILL_ID = "asset-reliability-response"
SKILL_FQN = f"{CATALOG}.{SCHEMA}.{SKILL_ID}"
CONNECTION_ID = "asset_reliability_mcp_connection"
# MCP source connections must be workspace/metastore-root connections. A schema-level
# connection can acquire an OAuth token but does not materialize the credential that the
# Gateway MCP runtime looks up for source calls.
CONNECTION_FQN = CONNECTION_ID
SERVICE_ID = "asset_reliability_mcp"
SERVICE_FQN = f"{CATALOG}.{SCHEMA}.{SERVICE_ID}"
POLICY_FQN = f"{CATALOG}.{SCHEMA}.submit_work_order_policy"
TOOLS = [
    "get_asset_telemetry",
    "assess_failure_risk",
    "prepare_work_order",
    "submit_work_order",
]


def ensure_source_app_permission(client: WorkspaceClient, app) -> bool:
    """Allow the connection's M2M principal through the Databricks App boundary."""

    permissions = client.apps.get_permissions(APP_NAME)
    for entry in permissions.access_control_list or []:
        if entry.service_principal_name != app.service_principal_client_id:
            continue
        levels = {
            permission.permission_level
            for permission in entry.all_permissions or []
        }
        if AppPermissionLevel.CAN_USE in levels or AppPermissionLevel.CAN_MANAGE in levels:
            return False

    client.apps.update_permissions(
        APP_NAME,
        access_control_list=[
            AppAccessControlRequest(
                service_principal_name=app.service_principal_client_id,
                permission_level=AppPermissionLevel.CAN_USE,
            )
        ],
    )
    return True


def deploy_skill(client: WorkspaceClient) -> dict[str, object]:
    """Create the governed Skill if needed, upload all source files, and finalize it."""

    try:
        client.api_client.do("GET", f"/api/2.1/unity-catalog/skills/{SKILL_FQN}")
        created = False
    except NotFound:
        client.api_client.do(
            "POST",
            "/api/2.1/unity-catalog/skills",
            query={"parent": PARENT, "skill_id": SKILL_ID},
            body={
                "comment": "Evidence-led response procedure for autonomous asset anomalies"
            },
        )
        created = True

    skill_directory = Path(__file__).parents[1] / "skills" / SKILL_ID
    uploaded: list[str] = []
    for local_path in skill_directory.rglob("*"):
        if not local_path.is_file():
            continue
        relative_path = local_path.relative_to(skill_directory).as_posix()
        client.files.upload(
            f"/Skills/{CATALOG}/{SCHEMA}/{SKILL_ID}/{relative_path}",
            BytesIO(local_path.read_bytes()),
            overwrite=True,
        )
        uploaded.append(relative_path)

    finalized = client.api_client.do(
        "POST",
        f"/api/2.1/unity-catalog/skills/{SKILL_FQN}/finalize",
        body={},
    )
    return {"created": created, "uploaded": uploaded, "name": finalized.get("name")}


def _connection_options(client: WorkspaceClient, app, client_secret: str) -> dict[str, str]:
    parsed = urlparse(app.url)
    return {
        "host": f"{parsed.scheme}://{parsed.netloc}",
        "port": "443",
        "base_path": "/mcp",
        "client_id": app.service_principal_client_id,
        "client_secret": client_secret,
        "token_endpoint": f"{client.config.host}/oidc/v1/token",
        "oauth_scope": "all-apis",
        "is_mcp_connection": "true",
    }


def _connection_drift(connection, desired: dict[str, str]) -> bool:
    """Compare only non-secret options because GET redacts the OAuth client secret."""

    current = connection.options or {}
    return any(
        current.get(key) != value
        for key, value in desired.items()
        if key != "client_secret"
    )


def ensure_connection(client: WorkspaceClient, app) -> tuple[object, str]:
    """Create or reconcile the App-backed MCP source connection."""

    try:
        connection = client.connections.get(CONNECTION_FQN)
    except NotFound:
        # The generated secret is passed directly to the connection API and is never logged or
        # persisted in the source tree.
        secret = client.service_principal_secrets_proxy.create(
            str(app.service_principal_id), lifetime="31536000s"
        )
        try:
            connection = client.connections.create(
                name=CONNECTION_ID,
                connection_type=ConnectionType.HTTP,
                comment="App-hosted maintenance MCP tools for Asset Reliability",
                read_only=False,
                options=_connection_options(client, app, secret.secret),
            )
        except Exception:
            # Avoid leaving an unreferenced App-SP credential if connection creation fails.
            client.service_principal_secrets_proxy.delete(
                str(app.service_principal_id), secret.id
            )
            raise
        return connection, "created"

    desired_without_secret = _connection_options(client, app, "<redacted>")
    if not _connection_drift(connection, desired_without_secret):
        return connection, "unchanged"
    secret = client.service_principal_secrets_proxy.create(
        str(app.service_principal_id), lifetime="31536000s"
    )
    try:
        connection = client.connections.update(
            CONNECTION_FQN,
            _connection_options(client, app, secret.secret),
        )
    except Exception:
        client.service_principal_secrets_proxy.delete(
            str(app.service_principal_id), secret.id
        )
        raise
    return connection, "updated"


def _service_config(function_id: str) -> dict[str, object]:
    return {
        "source_connection": {"name": f"connections/{CONNECTION_FQN}"},
        "include_tool_selectors": TOOLS,
        "service_policies": [
            {
                "name": "submit_work_order_requires_approval",
                "policy_type": "POLICY_TYPE_CUSTOM",
                "handler": f"functions/{POLICY_FQN}",
                "function_id": function_id,
                "rank": 1,
            }
        ],
    }


def ensure_service(client: WorkspaceClient, function_id: str) -> tuple[dict, str]:
    """Create the governed service or patch its selectors and policy back to source."""

    desired = _service_config(function_id)

    try:
        service = client.api_client.do(
            "GET", f"/api/2.1/unity-catalog/mcp-services/{SERVICE_FQN}"
        )
    except NotFound:
        service = client.api_client.do(
            "POST",
            "/api/2.1/unity-catalog/mcp-services",
            query={"parent": PARENT, "mcp_service_id": SERVICE_ID},
            body={
                "comment": "Governed tools for the Asset Reliability Command Center",
                "config": desired,
            },
        )
        return service, "created"
    if service.get("config") == desired:
        return service, "unchanged"
    service = client.api_client.do(
        "PATCH",
        f"/api/2.1/unity-catalog/mcp-services/{SERVICE_FQN}",
        body={"config": desired},
    )
    return service, "updated"


async def probe_submit_policy(client: WorkspaceClient) -> dict[str, str]:
    """Prove the deployed service returns native ASK before the source tool executes."""

    authorization = client.config.authenticate().get("Authorization", "")
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise RuntimeError("Unable to obtain an OAuth token for the policy probe")
    gateway_url = f"{client.config.host}/ai-gateway/mcp-services/{SERVICE_FQN}"
    gateway = GovernedGatewayClient(gateway_url)
    try:
        await gateway.call_tool(
            "submit_work_order",
            {
                "asset_id": "P-204",
                "action": "Policy deployment probe; must not execute",
                "incident_id": "00000000-0000-0000-0000-000000000000",
                "planned_window": "never",
            },
            token,
        )
    except GatewayAsk as ask:
        return {
            "decision": "ASK",
            "elicitation_id": ask.elicitation.elicitation_id,
            "execution": "not executed",
        }
    finally:
        await gateway.close()
    raise RuntimeError("submit_work_order policy probe did not return native ASK")


def main() -> None:
    """Deploy governance dependencies in their required dependency order."""

    client = WorkspaceClient(profile=PROFILE)
    app = client.apps.get(APP_NAME)
    if not app.url or not app.service_principal_id or not app.service_principal_client_id:
        raise RuntimeError("Deploy and start the Databricks App before configuring governance")

    function = client.functions.get(POLICY_FQN)
    app_permission_created = ensure_source_app_permission(client, app)
    skill = deploy_skill(client)
    connection, connection_status = ensure_connection(client, app)
    service, service_status = ensure_service(client, function.function_id)
    policy_probe = asyncio.run(probe_submit_policy(client))

    print(
        json.dumps(
            {
                "skill": skill,
                "source_app_permission_created": app_permission_created,
                "connection": connection.full_name,
                "connection_status": connection_status,
                "service": service.get("name"),
                "service_status": service_status,
                "gateway_url": f"{client.config.host}/ai-gateway/mcp-services/{SERVICE_FQN}",
                "policy": POLICY_FQN,
                "tools": TOOLS,
                "policy_probe": policy_probe,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
