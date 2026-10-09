# Build and deployment status

Target workspace: `fevm-classic-stable-realtime-demos` (`fe_vm_rtm`). The project uses the
existing `classic_stable_realtime_demos_catalog` and dedicated
`asset_reliability_command_center` schema. It creates no additional catalog or access group.

## Implemented in the repository

| Domain | Capability | Status |
| --- | --- | --- |
| Source | Continuous synthetic eight-pump telemetry with stable, transient, and degradation signals | Complete |
| Source | Recoverable, versioned scenario-command leases with bounded retries | Complete |
| Operational ingestion | Kafka-to-Lakebase standalone Spark Real-Time Mode on dedicated classic compute | Complete |
| Governed ingestion | Continuous SQL Lakeflow Declarative Pipeline on separate classic compute | Complete |
| Unity Catalog | Bronze, Silver, quarantine, comments, expectations, lineage, and curated telemetry view | Complete |
| Unity Catalog | Existing-group grants restricted to `pump_telemetry_history` | Complete |
| Lakebase | Live telemetry, current state, commands, incidents, runs, activities, work orders, identities, and heartbeats | Complete |
| Lakebase | Concurrency-safe ordering, approval replay lease, and immutable governance receipts | Complete |
| Autonomous AI | Alert-generated prompts and concurrent code-defined agent runs | Complete |
| Autonomous AI | Single `databricks-claude-sonnet-4-5` model for the complete workflow | Complete |
| Governance | One configured MCP service with runtime `tools/list` discovery | Complete |
| Governance | Source MCP restricted to the App service principal | Complete |
| Governance | Reconciled selectors and native `ASK` policy with non-executing probe | Complete |
| Governance | Restart isolation and single-flight exact-request replay | Complete |
| Genie | Reproducible telemetry-only Fleet telemetry analyst definition | Complete |
| Genie | OBO Conversation API adapter and App drawer with SQL/results/trust states | Complete |
| App | Live fleet, alerting, autonomous activity, history, and incident conversation/timeline | Complete |
| App | End-to-end health from connectivity, telemetry, producer, RTM, and evaluator freshness | Complete |
| Deployment | Checked manual deployment entry point and build provenance generation | Complete |
| Documentation | Architecture, demo, deployment, debt, and evidence plan reconciled | Complete |

## Validation state

The integrated repository must pass this gate before deployment:

```bash
uv run ruff check backend tests src scripts
uv run pytest
npm --prefix frontend run typecheck
npm --prefix frontend run build
databricks bundle validate --target dev --profile fe_vm_rtm
```

The feature commits on which this documentation is based include:

- governed SQL pipeline and reproducible Genie Agent;
- OBO Genie App integration;
- governed runtime and deployment reconciliation hardening;
- complete single-model consolidation.

## Deployment state

The earlier command-center build is deployed, with producer job `952037348530266` and RTM job
`17156207332698`. The integrated governed-history, Genie, single-model, and hardening changes must
not be marked deployed until `scripts/deploy.py --execute` and the current
[live acceptance checklist](docs/DEPLOYMENT.md) complete against the merged commit.

After that acceptance, replace this section with the actual:

- merged Git commit and App deployment ID;
- producer and RTM parent/task run IDs;
- governed-history pipeline ID and update ID;
- Genie space ID and successful conversation/message IDs;
- scenario, incident, work-order, and governance-receipt IDs;
- App health/build-info output and observed latency;
- link to the committed one-time text [evidence package](docs/EVIDENCE.md).

## Remaining work before validator submission

1. Merge all approved feature branches into `main` and rerun the complete local gate.
2. Run checked preflight, review the selected workspace/user/commit, then execute deployment.
3. Complete live App, pipeline, Genie OBO, MCP denial, native approval, and restart validation.
4. Capture and commit the one-time sanitized text evidence package.
5. Confirm repository access separately; presentation-deck work is intentionally outside this
   technical implementation scope.
