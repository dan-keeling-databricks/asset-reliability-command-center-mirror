# One-time validator evidence package

The validator reads text and cannot infer execution from source code, screenshots, or a narrated
video. After the final integrated deployment passes acceptance, create one curated, sanitized
`evidence/` package and commit it once. This is not a recurring evidence service.

## Rules

- Capture outputs from the exact commit returned by `/api/build-info`.
- Save UTF-8 text, JSON, CSV, or Markdown. A screenshot may be supplemental only.
- Remove OAuth tokens, cookies, client secrets, DSNs, private keys, and personal identifiers not
  required to understand execution.
- Keep workspace/resource IDs when they prove a run, but label the environment and capture time.
- Use synthetic pump records only. Never include customer data.
- Do not rewrite raw outputs to make a failed run appear successful. Add a short interpretation
  beside the original sanitized output.

## Required layout

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

### `README.md`

Record:

- Git commit and App deployment ID;
- workspace and UTC capture time;
- producer, RTM, and pipeline run/update IDs;
- synthetic scenario, incident, work-order, and governance-receipt IDs;
- Genie conversation/message IDs;
- a statement that all records are synthetic and outputs were sanitized.

### Build and platform execution

`01_build_and_deployment.txt` should contain the successful quality-gate summary, bundle
validation, generated build-info response, and governance reconciliation/policy-probe output.

`02_streaming_jobs.json` should contain sanitized `jobs list-runs`/run output proving both the
producer and RTM tasks reached `RUNNING`, plus notebook log excerpts showing broker publication,
RTM progress, and Lakebase sink activity.

`03_lakeflow_pipeline.json` should contain the pipeline definition/update status and Lakeflow
event-log output proving Bronze/Silver/quarantine datasets were created and processed records.

### Governed and operational data

`04_unity_catalog_queries.md` should include the SQL and textual results for:

- Bronze, Silver, quarantine, and curated-view row counts;
- recent validated telemetry with event and ingestion timestamps;
- expectation results or event-log quality metrics;
- `SHOW GRANTS` proving the existing group can select the view but not Bronze/Silver/quarantine;
- the curated view schema proving scenario and broker-control fields are absent.

`05_lakebase_queries.md` should include sanitized SQL and results for recent telemetry, the three
incident outcomes, the governed work order, component heartbeats, and the immutable receipt.

### AI and governance execution

`06_agent_run.json` should contain the real autonomous prompt, actual Sonnet model outputs,
runtime-discovered Skill/MCP/tools, tool calls/results, total tokens, cost, and final outcome.

`07_gateway_approval.json` should prove:

- `submit_work_order` returned native `ASK` with `execution=not executed`;
- the human approval subject and elicitation/request identifiers;
- the exact request hash resumed once;
- one resulting work order and immutable receipt;
- a human direct-source `/mcp` call was denied.

`08_genie_conversation.json` should contain one OBO telemetry question, status transitions,
generated SQL, returned columns/rows, source identity, and the final answer. Do not include the
forwarded user token.

`09_app_health.json` should contain `/api/health`, `/api/build-info`, and a sanitized dashboard
snapshot showing connected/fresh component state and measured latency.

## Final review

Before committing the package:

1. Search for token, secret, authorization, password, DSN, and cookie patterns.
2. Confirm every file is readable without rendering an image.
3. Confirm identifiers agree across the App, Lakebase, Gateway, and deployment records.
4. Confirm the commit in evidence matches the repository commit being submitted.
5. Link the package from the evidence README and submission narrative.
