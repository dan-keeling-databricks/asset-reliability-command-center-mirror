# Databricks notebook source
# MAGIC %md
# MAGIC # Apply the App-owned Lakebase schema
# MAGIC
# MAGIC This one-shot classic-compute task runs as the existing Databricks App service
# MAGIC principal. That identity owns the `asset_reliability` PostgreSQL schema, so upgrade
# MAGIC DDL must run here rather than as the human deployer. The migration is idempotent and
# MAGIC completes before any producer or Real-Time Mode job is restarted.

# COMMAND ----------

"""Apply and verify the idempotent Lakebase schema as the App service principal.

The deployment orchestrator treats successful completion of this notebook as a hard barrier:
continuous workloads cannot start against source code that expects a newer database contract.
OAuth credentials are short-lived and are never printed or persisted.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import psycopg
from databricks.sdk import WorkspaceClient

dbutils.widgets.text(
    "lakebase_endpoint",
    "projects/asset-reliability-command-center/branches/production/endpoints/primary",
)
dbutils.widgets.text("lakebase_database", "databricks_postgres")
dbutils.widgets.text("lakebase_schema", "asset_reliability")

LAKEBASE_ENDPOINT = dbutils.widgets.get("lakebase_endpoint")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
LAKEBASE_SCHEMA = dbutils.widgets.get("lakebase_schema")
REQUIRED_COLUMNS = {
    ("scenario_commands", "lease_expires_at"),
    ("scenario_commands", "claim_version"),
    ("scenario_commands", "attempt_count"),
    ("agent_runs", "approval_claim_id"),
    ("agent_runs", "approval_claim_version"),
}

if not re.fullmatch(r"[a-z_][a-z0-9_]*", LAKEBASE_SCHEMA):
    raise ValueError("lakebase_schema must be a simple PostgreSQL identifier")


def lakebase_connection() -> psycopg.Connection[Any]:
    """Connect with the notebook run-as identity and a short-lived Lakebase credential."""

    workspace = WorkspaceClient()
    endpoint = workspace.postgres.get_endpoint(name=LAKEBASE_ENDPOINT)
    credential = workspace.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
    return psycopg.connect(
        host=endpoint.status.hosts.host,
        port=5432,
        dbname=LAKEBASE_DATABASE,
        user=workspace.current_user.me().user_name,
        password=credential.token,
        sslmode="require",
        connect_timeout=10,
        autocommit=False,
    )


def migration_sql() -> str:
    """Load the repository migration deployed alongside this notebook."""

    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    workspace_path = Path(notebook_path)
    if not str(workspace_path).startswith("/Workspace/"):
        workspace_path = Path("/Workspace") / str(workspace_path).lstrip("/")
    path = workspace_path.parents[2] / "sql" / "lakebase_schema.sql"
    migration = path.read_text(encoding="utf-8")
    if LAKEBASE_SCHEMA != "asset_reliability":
        migration = migration.replace("asset_reliability", LAKEBASE_SCHEMA)
    return migration


def verify_schema(connection: psycopg.Connection[Any]) -> list[str]:
    """Fail unless every column required by current continuous workloads is visible."""

    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = %s""",
            (LAKEBASE_SCHEMA,),
        )
        actual = {(str(row[0]), str(row[1])) for row in cursor.fetchall()}
    missing = sorted(REQUIRED_COLUMNS - actual)
    if missing:
        names = ", ".join(f"{table}.{column}" for table, column in missing)
        raise RuntimeError(f"Lakebase migration completed without required columns: {names}")
    return sorted(f"{table}.{column}" for table, column in REQUIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactional execution and contract verification
# MAGIC
# MAGIC PostgreSQL applies the complete idempotent migration in one transaction. A second
# MAGIC information-schema check makes the job a reliable dependency barrier rather than merely
# MAGIC assuming that submitted DDL succeeded.

# COMMAND ----------

with lakebase_connection() as connection:
    with connection.transaction():
        connection.execute(migration_sql())
    verified_columns = verify_schema(connection)

print(
    json.dumps(
        {
            "migration": "lakebase_schema.sql",
            "schema": LAKEBASE_SCHEMA,
            "status": "verified",
            "required_columns": verified_columns,
        },
        sort_keys=True,
    )
)
