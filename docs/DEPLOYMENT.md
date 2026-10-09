# Checked manual deployment

The repository intentionally has no CI/CD integration. A push or merge changes source control
only. An authorized operator deploys a clean, reviewed commit with `scripts/deploy.py`.

The entry point is deliberately conservative:

- every Databricks command receives an explicit profile and bundle target;
- the expected workspace URL and existing access group are verified before mutation;
- dependencies are installed from lock files;
- the full Python and frontend quality gate runs before deployment;
- compiled frontend freshness is checked rather than assumed;
- clean `HEAD` is written to the ignored `.build-info.json` included in the App snapshot;
- resources are reconciled in dependency order;
- both continuous streaming jobs restart on the deployed source;
- post-deployment platform checks run before human acceptance begins.

The script never handles an OAuth secret. Authentication remains in the selected Databricks CLI
profile, and generated MCP connection credentials remain inside the governance deployment helper.

## Target inputs

The current development environment is:

| Input | Value |
| --- | --- |
| Workspace | `https://fevm-classic-stable-realtime-demos.cloud.databricks.com` |
| CLI profile | `fe_vm_rtm` |
| Bundle target | `dev` |
| App | `asset-reliability-center` |
| Catalog | `classic_stable_realtime_demos_catalog` |
| Schema | `asset_reliability_command_center` |
| Existing group | `asset-reliability-command-center-operator` (`84285700224068`) |
| Lakebase project | `asset-reliability-command-center` |

These are command inputs, not identities embedded in application logic. Use the values for the
actual target environment; do not silently rely on a CLI default profile.

## Prerequisites

- Databricks CLI 1.17 or later with OAuth authentication for the intended workspace.
- `uv`, Node.js, and npm.
- Permission to update the App, jobs, pipeline, Genie Agent, Lakebase project, UC schema, Skill,
  MCP source connection/service, and policy function.
- Membership in the existing group passed to `--access-group`.
- A clean, committed `feat/{feature_name}` branch for pre-merge acceptance, or the reviewed merge
  commit on `main` for final deployment.
- Broker reachability from the classic producer, RTM, and pipeline compute.

Do not commit `.build-info.json`, generated credentials, tokens, `.env`, frontend build output, or
Databricks state.

## Preflight only

Run the entry point without `--execute` first:

```bash
uv run python scripts/deploy.py \
  --profile fe_vm_rtm \
  --target dev \
  --expected-host https://fevm-classic-stable-realtime-demos.cloud.databricks.com \
  --access-group asset-reliability-command-center-operator \
  --catalog classic_stable_realtime_demos_catalog \
  --schema asset_reliability_command_center \
  --app-name asset-reliability-center \
  --app-service-principal-client-id bd5178cb-8774-4d77-bc63-b4f491716d0b
```

Preflight performs the following checks and stops without changing workspace resources:

1. Require a clean Git worktree and resolve the exact commit.
2. Generate and reread `.build-info.json` containing only that commit.
3. Validate the Databricks CLI version.
4. Resolve the explicit profile, compare its host with `--expected-host`, verify the deploying
   user belongs to the supplied existing group, and confirm the explicit workload identity is the
   existing App service principal.
5. Run `uv sync --locked` and `npm ci`.
6. Run Ruff, all Pytest tests, TypeScript checking, and the Vite production build.
7. Confirm `requirements.txt` matches `uv.lock` and `frontend/dist` is newer than every frontend
   source input.
8. Validate the bundle with the explicit target/profile/group and prove no tracked file changed.

Any failure stops the process before remote mutation.

## Execute the ordered deployment

After reviewing the preflight output, repeat the same command with `--execute`:

```bash
uv run python scripts/deploy.py \
  --profile fe_vm_rtm \
  --target dev \
  --expected-host https://fevm-classic-stable-realtime-demos.cloud.databricks.com \
  --access-group asset-reliability-command-center-operator \
  --catalog classic_stable_realtime_demos_catalog \
  --schema asset_reliability_command_center \
  --app-name asset-reliability-center \
  --app-service-principal-client-id bd5178cb-8774-4d77-bc63-b4f491716d0b \
  --execute
```

The script applies these steps in order:

1. A dedicated least-privilege compute policy and deployer-run UC grant job prepare the existing
   App service principal for only the required classic workloads and runtime volume.
2. The producer, standalone RTM job, and continuous governed-history pipeline runner deploy under
   that workload identity and restart on the new source revision.
3. The script waits for a new healthy pipeline update and a published
   `pump_telemetry_history` view.
4. The idempotent grant job gives only the existing group access to the curated view, after which
   the complete bundle safely creates or updates Genie and its App binding.
5. `databricks apps deploy` creates the App snapshot; App compute is restarted only if the newly
   declared `dashboards.genie` and `ai-gateway` OBO scopes are not yet effective.
6. The policy-function job updates the UC function.
7. The governance helper reconciles the finalized Skill, App source permission, OAuth MCP source,
   MCP service tool selectors, and native `ASK` policy. Its probe must return `ASK` without source
   execution, after first proving exact tool discovery and a benign source read.
8. Read-only checks inspect the App, bundle summary, and curated view.

The pipeline wait defaults to ten minutes. Override only when classic-compute startup routinely
requires longer:

```bash
--table-timeout-seconds 900
```

## Live validation

Automated resource checks do not prove the business path. Complete this acceptance sequence in
the deployed App and record identifiers as text:

1. Confirm `/api/build-info` returns the deployed commit and `/api/health` is `ok`, not degraded.
2. Confirm the App reports fresh producer, RTM/telemetry, and evaluator state.
3. Inspect the producer, RTM, and governed-history pipeline in Databricks. Each must be running on
   its intended classic compute with current events advancing.
4. Query Bronze, Silver, quarantine, and `pump_telemetry_history`. Confirm the view exposes only
   telemetry fields and remains continuous across an App restart.
5. Open **Ask fleet telemetry** as a member of the existing group. Confirm the OBO identity, ask a
   starter question, inspect generated SQL and tabular results, and verify that scenario, incident,
   and work-order questions are outside the Agent's data scope.
6. Clear/restart the command center. Verify old agent tasks close before the new scenario, then
   confirm P-202 and P-207 start immediately and resolve concurrently with no action.
7. Trigger P-204. Confirm the autonomous Sonnet workflow loads the Skill, discovers the configured
   MCP service's tools, assesses high risk, and stops at native `ASK` before submission.
8. Attempting `/mcp` as a human App user must return `403`. Gateway source access as the configured
   App service principal must remain successful.
9. Approve once through the native Gateway URL. Confirm exactly one replay, one work order, one
   immutable governance receipt, and one terminal model response.
10. Close the overlay and confirm the command center retains both no-action histories and the
    scheduled-maintenance outcome without browser console or network errors.
11. Start another scenario only after verifying the previous tasks were cancelled and no old
    activity appears in the new timeline.

Follow [EVIDENCE.md](EVIDENCE.md) to save the one-time, sanitized, text-readable acceptance
package. Screenshots can supplement it but cannot replace raw output.

## Rollback

Rollback is forward-only for data schemas:

1. Create a temporary `feat/rollback_{reason}` branch at the last accepted commit.
2. Run the same preflight and deployment entry point with the same explicit target inputs.
3. Do not reverse or drop Lakebase/UC schema changes. Migrations must remain compatible with the
   live producer and readers.
4. Re-run the complete live validation, including the Gateway policy probe and continuous jobs.
5. Record the rollback commit and new deployment/run identifiers in the evidence or release log.

Never use `git reset --hard`, destructive table replacement, checkpoint deletion, or a pipeline
full refresh as a demo rollback mechanism.
