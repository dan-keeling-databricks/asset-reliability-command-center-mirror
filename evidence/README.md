# Validator execution evidence

This package is a sanitized, text-readable capture from a real integrated run in the Databricks
workspace `https://fevm-classic-stable-realtime-demos.cloud.databricks.com` on
2026-10-12 UTC. The deployed App reported Git commit
`940c383fa91f9bd98f5ca0994d370b820396e582` and deployment
`01f1c625d12d1674b68fb5f905d91d5d`.

All pump telemetry and operational records are synthetic. OAuth tokens, cookies, secrets,
connection strings, and personal identifiers have been removed or replaced with explicit
sanitization markers.

## Correlation identifiers

| Resource | Identifier |
|---|---|
| Producer job / run | `952037348530266` / `410215715966481` |
| RTM job / run | `17156207332698` / `371869867772465` |
| Governed-history job / run | `599499477831511` / `585606286966144` |
| Lakeflow pipeline / update | `a0efdccf-440d-479b-8fc2-7cceb0bb3da2` / `66ebb87e-a19a-4d42-b647-4a139bbe155b` |
| Scenario | `3bd336aa-d1a8-40b1-82f5-c175c1046975` |
| P-204 incident | `13c50285-aba5-444c-b5b6-bba810f2a452` |
| P-204 agent run | `74b9404f-f427-477f-a3ee-c0300efe7bdf` |
| Approval elicitation | `945eb42d-df89-43a4-883e-df753209cdf1` |
| Governance receipt | `448ba4ce-f5ff-416a-bc11-e21ed1f1dde9` |
| Work order | `WO-35374` |
| Genie conversation / message | `01f1c62719ad10cea205668c3d97f07a` / `01f1c62719c21683b24828be6f5cbcef` |

## Files

- `01_build_and_deployment.txt`: local quality gates plus live deployment and reconciliation runs.
- `02_streaming_jobs.json`: live continuous-job states and operational progress proof.
- `03_lakeflow_pipeline.json`: running update, dataset events, and live row counts.
- `04_unity_catalog_queries.md`: governed table/view results, schema, and grants.
- `05_lakebase_queries.md`: operational telemetry, incidents, work order, receipt, and heartbeats.
- `06_agent_run.json`: autonomous prompt, runtime discovery, model/tool timeline, and totals.
- `07_gateway_approval.json`: native `ASK`, approval, replay, receipt, and direct-access denial.
- `08_genie_conversation.json`: OBO Genie request, generated SQL, row, and final answer.
- `09_app_health.json`: deployed App health, provenance, command-center state, and latency.

