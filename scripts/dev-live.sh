#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

profile="${DATABRICKS_CONFIG_PROFILE:-fe_vm_rtm}"
endpoint="projects/asset-reliability-command-center/branches/production/endpoints/primary"
backend_port="${BACKEND_PORT:-8000}"
export FRONTEND_PORT="${FRONTEND_PORT:-5173}"
export VITE_BACKEND_URL="http://127.0.0.1:$backend_port"
export ALERT_EVALUATOR_INTERVAL_SECONDS="${ALERT_EVALUATOR_INTERVAL_SECONDS:-0.2}"
workspace_host="$(databricks auth describe --profile "$profile" 2>/dev/null | awk '/^Host:/ {print $2}')"
endpoint_json="$(databricks postgres get-endpoint "$endpoint" --profile "$profile" --output json)"
current_user_json="$(databricks current-user me --profile "$profile" --output json)"

if ! command -v jq >/dev/null 2>&1; then
  echo "Live mode requires jq to read Databricks CLI responses." >&2
  exit 1
fi
if [[ -z "$workspace_host" ]]; then
  echo "Unable to determine the workspace host for profile $profile." >&2
  exit 1
fi

export APP_ADAPTER=lakebase
export LOCAL_DATABRICKS_AUTH=true
export DATABRICKS_CONFIG_PROFILE="$profile"
export DATABRICKS_HOST="$workspace_host"
export LAKEBASE_ENDPOINT="$endpoint"
# Lakebase OAuth credentials authenticate against the direct endpoint. The pooled host
# currently rejects these user credentials with SASL authentication failures.
export PGHOST="$(jq -r '.status.hosts.host' <<<"$endpoint_json")"
export PGPORT=5432
export PGDATABASE=databricks_postgres
export PGUSER="$(jq -r '.userName' <<<"$current_user_json")"
export PGSSLMODE=require
export LAKEBASE_SCHEMA=asset_reliability
export LAKEBASE_MIGRATE_ON_START=false
export GOVERNED_MCP_ENDPOINT="$workspace_host/ai-gateway/mcp-services/classic_stable_realtime_demos_catalog.asset_reliability_command_center.asset_reliability_mcp"
export GOVERNED_SKILL_ENDPOINT="$workspace_host/ai-gateway/skills/?schema=classic_stable_realtime_demos_catalog.asset_reliability_command_center"
export GOVERNED_SKILL_TOOL=skill_classic_stable_realtime_demos_catalog.asset_reliability_command_center.asset-reliability-response
export PRIMARY_MODEL_ENDPOINT=databricks-claude-sonnet-4-5
export FAST_MODEL_ENDPOINT=databricks-gpt-5-1

echo "Starting LIVE mode against $workspace_host using profile $profile."
echo "WARNING: UI controls will mutate the shared Databricks demo environment."

trap 'kill 0' EXIT
uv run uvicorn backend.main:app --host 127.0.0.1 --port "$backend_port" --reload &
npm --prefix frontend run dev
