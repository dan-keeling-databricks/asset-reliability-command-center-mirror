"""Deploy the Unity Gateway skill, App-backed MCP connection, service, and ASK policy."""

from __future__ import annotations

import json
import os
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel
from databricks.sdk.service.catalog import ConnectionType

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


def ensure_connection(client: WorkspaceClient, app) -> tuple[object, bool]:
    try:
        return client.connections.get(CONNECTION_FQN), False
    except NotFound:
        # The generated secret is passed directly to the connection API and is never logged or
        # persisted in the source tree.
        secret = client.service_principal_secrets_proxy.create(
            str(app.service_principal_id), lifetime="31536000s"
        )
        parsed = urlparse(app.url)
        try:
            connection = client.connections.create(
                name=CONNECTION_ID,
                connection_type=ConnectionType.HTTP,
                comment="App-hosted maintenance MCP tools for Asset Reliability",
                read_only=False,
                options={
                    "host": f"{parsed.scheme}://{parsed.netloc}",
                    "port": "443",
                    "base_path": "/mcp",
                    "client_id": app.service_principal_client_id,
                    "client_secret": secret.secret,
                    "token_endpoint": f"{client.config.host}/oidc/v1/token",
                    "oauth_scope": "all-apis",
                    "is_mcp_connection": "true",
                },
            )
        except Exception:
            # Avoid leaving an unreferenced App-SP credential if connection creation fails.
            client.service_principal_secrets_proxy.delete(
                str(app.service_principal_id), secret.id
            )
            raise
        return connection, True


def ensure_service(client: WorkspaceClient, function_id: str) -> tuple[dict, bool]:
    try:
        service = client.api_client.do(
            "GET", f"/api/2.1/unity-catalog/mcp-services/{SERVICE_FQN}"
        )
        return service, False
    except NotFound:
        service = client.api_client.do(
            "POST",
            "/api/2.1/unity-catalog/mcp-services",
            query={"parent": PARENT, "mcp_service_id": SERVICE_ID},
            body={
                "comment": "Governed tools for the Asset Reliability Command Center",
                "config": {
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
                },
            },
        )
        return service, True


def main() -> None:
    client = WorkspaceClient(profile=PROFILE)
    app = client.apps.get(APP_NAME)
    if not app.url or not app.service_principal_id or not app.service_principal_client_id:
        raise RuntimeError("Deploy and start the Databricks App before configuring governance")

    function = client.functions.get(POLICY_FQN)
    app_permission_created = ensure_source_app_permission(client, app)
    skill = deploy_skill(client)
    connection, connection_created = ensure_connection(client, app)
    service, service_created = ensure_service(client, function.function_id)

    print(
        json.dumps(
            {
                "skill": skill,
                "source_app_permission_created": app_permission_created,
                "connection": connection.full_name,
                "connection_created": connection_created,
                "service": service.get("name"),
                "service_created": service_created,
                "gateway_url": f"{client.config.host}/ai-gateway/mcp-services/{SERVICE_FQN}",
                "policy": POLICY_FQN,
                "tools": TOOLS,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
