# Manual deployment runbook

The repository intentionally has no CI/CD integration. A Git push does not modify the
workspace. Run these steps only when deployment is explicitly authorized.

## Target

- Workspace: `https://fevm-classic-stable-realtime-demos.cloud.databricks.com`
- CLI profile: `fe_vm_rtm`
- Bundle target: `dev`
- App: `asset-reliability-center`
- Existing catalog/schema:
  `classic_stable_realtime_demos_catalog.asset_reliability_command_center`
- Lakebase project: `asset-reliability-command-center`

## Prerequisites

- Databricks CLI 0.250 or later authenticated as the intended deployer
- `uv`, Node.js, and npm
- Permission to update the App, jobs, Lakebase project, UC schema, Skill, MCP service, and
  Gateway policy
- A clean `feat/{feature_name}` branch with all intended changes committed

Verify authentication:

```bash
databricks auth describe --profile fe_vm_rtm
databricks current-user me --profile fe_vm_rtm
```

## Local quality gate

```bash
uv sync
npm --prefix frontend install
uv run ruff check backend tests src scripts
uv run pytest
npm --prefix frontend run typecheck
npm --prefix frontend run build
uv export --no-dev --format requirements-txt --output-file requirements.txt
databricks bundle validate --target dev --profile fe_vm_rtm
```

Run `./scripts/dev.sh`, open `http://127.0.0.1:5173`, and validate changed UI behavior,
console output, and network requests before continuing.

## Deploy bundle and App

The enhanced App command uploads the bundle, updates declared resources, creates a snapshot
deployment, and waits for the App to start:

```bash
databricks apps deploy --target dev --profile fe_vm_rtm --auto-approve
```

The App executes the idempotent Lakebase migration during startup. Check status and recent
errors:

```bash
databricks apps get asset-reliability-center --profile fe_vm_rtm
databricks apps logs asset-reliability-center --profile fe_vm_rtm --tail-lines 200
```

## Deploy or update governance

Run the bundle policy-function job when `sql/setup_demo.sql` changes. Then deploy the Skill,
HTTP MCP connection, MCP service, App permission, and service policy:

```bash
databricks bundle run setup_unity_governance --target dev --profile fe_vm_rtm
DATABRICKS_CONFIG_PROFILE=fe_vm_rtm uv run python scripts/deploy_governance.py
```

The helper is idempotent and never prints or persists generated client secrets.

## Continuous job source changes

Bundle file sync does not replace an already-running continuous task. If producer or RTM
source changes, identify the active run, cancel it, and verify that the unpaused continuous
job starts a replacement run with the new workspace source. Do not cancel either job for an
App-only change.

```bash
databricks jobs list-runs --job-id 952037348530266 --limit 3 --profile fe_vm_rtm
databricks jobs list-runs --job-id 17156207332698 --limit 3 --profile fe_vm_rtm
```

## Remote acceptance

1. Open `https://asset-reliability-center-7474659563748021.aws.databricksapps.com`.
2. Confirm Lakebase connected state and low Lakebase query latency.
3. Clear/restart and verify P-202/P-207 launch concurrently and resolve with no action.
4. Trigger P-204 and verify a high-risk assessment.
5. Confirm `submit_work_order` shows native `ASK · Not executed`.
6. Approve through the native handoff and confirm the exact request resumes.
7. Close the overlay and confirm the command center shows maintenance scheduled plus retained
   no-action histories.
8. Inspect browser console and network traffic for errors.
9. Confirm producer and RTM jobs remain `RUNNING` and `UNPAUSED`, and query Lakebase freshness.

Record the Git commit, App deployment ID, continuous job run IDs, scenario ID, work-order ID,
latency measurement, and acceptance screenshots in `TASKS.md` or the release handoff.

## Rollback

Check out the last accepted commit on a temporary rollback branch and repeat the App deploy
command. If a continuous-job source changed, cancel its active run after the rollback bundle
is uploaded so the replacement loads the accepted source. Lakebase migrations must remain
forward-compatible; never use destructive rollback SQL during a live demo.
