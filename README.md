# Asset Reliability Command Center

A live Databricks App command center demonstrating autonomous reliability review and
runtime-driven agent governance. See [DEMO.md](DEMO.md) for the approved narrative.

## Documentation

- [Technical architecture](docs/ARCHITECTURE.md)
- [Interactive architecture and agent governance explainer](docs/interactive-architecture.html)
- [Manual deployment runbook](docs/DEPLOYMENT.md)
- [Contribution and branching model](CONTRIBUTING.md)
- [Repository instructions for coding agents](AGENTS.md)

## Local run

Prerequisites: `uv`, Node.js, and npm.

```bash
uv sync
npm --prefix frontend install
./scripts/dev.sh
```

Open `http://127.0.0.1:5173`. The default `APP_ADAPTER=local` runs a deterministic eight-pump
telemetry and agent-event adapter so the complete interface can be exercised without
Lakebase, Kafka, model serving, or Gateway infrastructure.

## Tests and build

```bash
uv run pytest
uv run ruff check backend tests
npm --prefix frontend run typecheck
npm --prefix frontend run build
uv export --no-dev --format requirements-txt --output-file requirements.txt
```

After the frontend build, FastAPI serves `frontend/dist` on port 8000. The server contracts
isolate operational persistence and agent event emission so the deployment build can wire
Lakebase, SSE notifications, and the server-hosted agent without changing the UI.

Deployment is intentionally manual; the repository has no CI/CD integration. Follow
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) from a validated `feat/{feature_name}` branch.
