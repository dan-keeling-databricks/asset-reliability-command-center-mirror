# One-time validator evidence package

The repository now contains the curated, sanitized [`evidence/`](../evidence/README.md) capture
from the accepted integrated run on 2026-10-12. The validator reads text and cannot infer
execution from source code, screenshots, or a narrated video, so these committed outputs are the
primary proof that the build ran.

## Rules

- Capture outputs from the exact commit returned by `/api/build-info`.
- Save UTF-8 text, JSON, CSV, or Markdown. A screenshot may be supplemental only.
- Remove OAuth tokens, cookies, client secrets, DSNs, private keys, and personal identifiers not
  required to understand execution.
- Keep workspace/resource IDs when they prove a run, but label the environment and capture time.
- Use synthetic pump records only. Never include customer data.
- Do not rewrite raw outputs to make a failed run appear successful. Add a short interpretation
  beside the original sanitized output.

## Committed layout

```text
evidence/
├── README.md
├── 01_build_and_deployment.txt
├── 02_streaming_jobs.json
├── 03_lakeflow_pipeline.json
├── 04_unity_catalog_queries.md
├── 05_lakebase_queries.md
├── 06_agent_run.json
├── 07_gateway_approval.json
├── 08_genie_conversation.json
└── 09_app_health.json
```

### Evidence index

The committed `evidence/README.md` records:

- Git commit and App deployment ID;
- workspace and UTC capture time;
- producer, RTM, and pipeline run/update IDs;
- synthetic scenario, incident, work-order, and governance-receipt IDs;
- Genie conversation/message IDs;
- a statement that all records are synthetic and outputs were sanitized.

### Build and platform execution

`01_build_and_deployment.txt` contains the successful quality-gate summary, bundle validation,
generated build-info response, and governance reconciliation output.

`02_streaming_jobs.json` contains sanitized job/run output proving both the producer and RTM tasks
reached `RUNNING`, plus live producer heartbeat and Lakebase sink records. The continuous Jobs API
returned no notebook output while the tasks were running; the evidence records that limitation
explicitly instead of fabricating log text.

`03_lakeflow_pipeline.json` contains the pipeline definition/update status, Lakeflow event-log
events, and live Bronze/Silver/quarantine/view counts.

### Governed and operational data

`04_unity_catalog_queries.md` includes the SQL and textual results for:

- Bronze, Silver, quarantine, and curated-view row counts;
- recent validated telemetry with event and ingestion timestamps;
- expectation results or event-log quality metrics;
- `SHOW GRANTS` proving the existing group can select the view but not Bronze/Silver/quarantine;
- the curated view schema proving scenario and broker-control fields are absent.

`05_lakebase_queries.md` includes sanitized SQL and results for recent telemetry, the three
incident outcomes, the governed work order, component heartbeats, and the immutable receipt.

### AI and governance execution

`06_agent_run.json` contains the real autonomous prompt, actual Sonnet model outputs,
runtime-discovered Skill/MCP/tools, tool calls/results, total tokens, cost, and final outcome.

`07_gateway_approval.json` proves:

- `submit_work_order` returned native `ASK` with `execution=not executed`;
- the human approval subject and elicitation/request identifiers;
- the exact request hash resumed once;
- one resulting work order and immutable receipt;
- a human direct-source `/mcp` call was denied.

`08_genie_conversation.json` contains one OBO telemetry question, status transitions,
generated SQL, returned columns/rows, source identity, and the final answer. Do not include the
forwarded user token.

`09_app_health.json` contains `/api/health`, `/api/build-info`, and a sanitized dashboard
snapshot showing connected/fresh component state and measured latency.

## Automated review

`tests/test_evidence_package.py` checks:

1. All ten promised files exist and are nonempty.
2. Every JSON capture parses.
3. No template placeholders remain.
4. Identifiers agree across the App, Lakebase, Gateway, and deployment records.
5. No JWT, bearer credential, client secret, private key, password-bearing DSN, or personal
   Databricks email appears in the package.
