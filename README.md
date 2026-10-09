# Asset Reliability Command Center

A working Databricks prototype for a process-manufacturing reliability problem: detect pump
bearing degradation early, autonomously assess the risk of failure before the next maintenance
window, and govern the resulting maintenance action with human approval.

The repository implements one integrated data and AI journey:

```text
Synthetic pump telemetry → Kafka
    ├─ Spark Real-Time Mode → Lakebase → Databricks App command center
    └─ SQL Lakeflow Declarative Pipeline → Unity Catalog → Genie Agent

Command-center alert → code-defined Sonnet agent → UC Skill → Unity Gateway MCP
    → native ASK approval → governed work order
```

The two Kafka consumers serve different needs. Standalone Spark Real-Time Mode writes the
low-latency operational state used by the App into Lakebase. A continuous SQL Lakeflow
Declarative Pipeline writes durable Bronze, Silver, and quarantine datasets into Unity Catalog
and publishes the curated `pump_telemetry_history` view used exclusively by Genie.

## What is in the build

- A continuous synthetic eight-pump producer on classic compute.
- Standalone Kafka-to-Lakebase Spark Real-Time Mode ingestion.
- A documented SQL Lakeflow Declarative Pipeline with data-quality expectations and quarantine.
- Unity Catalog tables, view, lineage, comments, a governed Skill, MCP service, and policy.
- Lakebase operational state, command, incident, agent, approval, and audit records.
- A code-defined autonomous agent using only `databricks-claude-sonnet-4-5`.
- Unity Gateway tool discovery and native `ASK` immediately before work-order submission.
- A React/FastAPI Databricks App with live fleet state, incident review, and agent activity.
- An OBO **Fleet telemetry analyst** drawer backed by a telemetry-only Genie Agent.
- Recoverable command leases, restart isolation, single-flight approval replay, immutable
  governance receipts, concurrency-safe timeline ordering, and end-to-end freshness health.

All operational data is synthetic. No customer data belongs in this repository.

## Documentation

- [Business demo flow](DEMO.md)
- [Technical architecture](docs/ARCHITECTURE.md)
- [Manual deployment and live acceptance](docs/DEPLOYMENT.md)
- [One-time text evidence package](docs/EVIDENCE.md)
- [Technical debt and accepted tradeoffs](docs/TECHNICAL_DEBT.md)
- [Current implementation status](TASKS.md)
- [Contribution and branching model](CONTRIBUTING.md)
- [Instructions for coding agents](AGENTS.md)

## Local development

Prerequisites are `uv`, Node.js/npm, and Databricks CLI 1.17 or later.

```bash
uv sync --locked
npm --prefix frontend ci
./scripts/dev.sh
```

Open `http://127.0.0.1:5173`. `APP_ADAPTER=local` provides a deterministic command-center
fixture without Kafka, Lakebase, model serving, or Unity Gateway. Genie is intentionally a
deployed OBO integration and is unavailable without a configured Genie space and forwarded
Databricks user token.

## Validation

```bash
uv run ruff check backend tests src scripts
uv run pytest
npm --prefix frontend run typecheck
npm --prefix frontend run build
uv run python scripts/deploy.py \
  --profile fe_vm_rtm \
  --target dev \
  --expected-host https://fevm-classic-stable-realtime-demos.cloud.databricks.com \
  --access-group asset-reliability-command-center-operator \
  --catalog classic_stable_realtime_demos_catalog \
  --schema asset_reliability_command_center \
  --app-name asset-reliability-center
```

The deployment preflight also stages the runtime-only App source and validates the bundle without
changing the workspace. Deployment remains manual. Use only the checked entry point documented in
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md); a Git push or merge never changes the workspace.
