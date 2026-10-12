# Databricks end-to-end execution evidence

This is the primary validator-facing proof that the Asset Reliability Command Center executed as
an integrated build. It consolidates genuine sanitized output from the accepted run on
2026-10-12 UTC. All pump records are synthetic. No OAuth credential, cookie, connection string,
private key, or unsanitized human identity is included.

## Build provenance and deployed Databricks App response

```text
Workspace: https://fevm-classic-stable-realtime-demos.cloud.databricks.com
App deployment: 01f1c625d12d1674b68fb5f905d91d5d
Deployment status: SUCCEEDED
App status: RUNNING
Compute status: ACTIVE
```

```http
GET /api/health -> 200
{"status":"ok","adapter":"lakebase","detail":"Lakebase - live operational state"}

GET /api/build-info -> 200
{"git_commit":"940c383fa91f9bd98f5ca0994d370b820396e582"}
```

The dashboard response reported a connected Lakebase operational state, 57 ms Kafka-to-Lakebase
latency, 33 ms Lakebase-to-App latency, and 90 ms end-to-end latency. P-204 showed
`Maintenance scheduled` with `WO-35374 scheduled`; P-202 and P-207 both showed
`Reviewed - no action`.

## Lakeflow pipeline run output showing Bronze, Silver, and quarantine execution

```text
Pipeline ID: a0efdccf-440d-479b-8fc2-7cceb0bb3da2
Update ID: 66ebb87e-a19a-4d42-b647-4a139bbe155b
Pipeline state: RUNNING
Update state: RUNNING
Update cause: JOB_TASK

2026-10-12T10:14:43.382Z Flow telemetry_bronze is RUNNING.
2026-10-12T10:14:45.172Z Flow telemetry_silver is RUNNING.
2026-10-12T10:14:47.208Z Flow telemetry_quarantine is RUNNING.
```

The following live SQL completed as statement
`01f1c626-d929-126b-9256-5813b00c6ab5`:

```sql
SELECT 'telemetry_bronze' AS dataset, COUNT(*) AS row_count
FROM classic_stable_realtime_demos_catalog.asset_reliability_command_center.telemetry_bronze
UNION ALL
SELECT 'telemetry_silver', COUNT(*)
FROM classic_stable_realtime_demos_catalog.asset_reliability_command_center.telemetry_silver
UNION ALL
SELECT 'telemetry_quarantine', COUNT(*)
FROM classic_stable_realtime_demos_catalog.asset_reliability_command_center.telemetry_quarantine
UNION ALL
SELECT 'pump_telemetry_history', COUNT(*)
FROM classic_stable_realtime_demos_catalog.asset_reliability_command_center.pump_telemetry_history;
```

```text
telemetry_bronze       3,884,768
telemetry_silver       3,884,704
telemetry_quarantine           0
pump_telemetry_history 3,884,704
```

This proves the SQL Lakeflow Declarative Pipeline created and ran all three persisted flows and
published the validated history used by Genie. An empty quarantine is the expected result for
the current valid synthetic feed.

## Unity Catalog query results

A separate live query returned the most recent validated records:

```text
pump_id event_time                  ingested_at                vibration_mm_s bearing_temperature_c
P-208   2026-10-12 10:22:47.523242 2026-10-12 10:22:48.016  2.998            57.3
P-207   2026-10-12 10:22:47.523127 2026-10-12 10:22:48.016  2.809            58.8
P-204   2026-10-12 10:22:47.522317 2026-10-12 10:22:48.016  8.040            76.2
P-202   2026-10-12 10:22:47.521989 2026-10-12 10:22:48.016  1.396            57.7
```

`SHOW GRANTS` confirmed that `asset-reliability-command-center-operator` has `SELECT` on
`pump_telemetry_history`. The group has no captured `SELECT` grant on Bronze, Silver, or
quarantine. `DESCRIBE pump_telemetry_history` returned only telemetry fields: event time, pump,
flow, pressure, vibration, bearing temperature, motor current, source, and ingestion time.

## Lakebase Real-Time Mode sink and operational query results

Both continuous jobs were live:

```text
Producer job/run: 952037348530266 / 410215715966481  RUNNING
RTM sink job/run: 17156207332698 / 371869867772465   RUNNING
Producer heartbeat: healthy, phase=streaming, messages_produced=12568
```

Lakebase contained the corresponding P-204 operational record:

```text
scenario_id: 3bd336aa-d1a8-40b1-82f5-c175c1046975
pump_id: P-204
sequence: 812
event_time: 2026-10-12T10:23:45.036551Z
vibration_mm_s: 8.011
bearing_temperature_c: 75.7
kafka_partition: 1
kafka_offset: 7274394
ingested_at: 2026-10-12T10:23:45.059100Z
```

## Genie conversation transcript, generated SQL, query result, and answer

```text
Execution mode: on_behalf_of_user
OBO ready: true
Conversation: 01f1c62719ad10cea205668c3d97f07a
Message: 01f1c62719c21683b24828be6f5cbcef
Statuses: SUBMITTED -> ASKING_AI -> COMPLETED
```

Question:

```text
Which pump had the highest average vibration over the last 10 minutes,
and what was its average vibration?
```

Generated SQL:

```sql
WITH avg_vibration AS (
  SELECT pump_id, ROUND(AVG(vibration_mm_s), 1) AS average_vibration_mm_s
  FROM classic_stable_realtime_demos_catalog.asset_reliability_command_center.pump_telemetry_history
  WHERE event_time >= current_timestamp() - INTERVAL 10 MINUTES
    AND pump_id IS NOT NULL
    AND vibration_mm_s IS NOT NULL
  GROUP BY pump_id
), ranked AS (
  SELECT pump_id, average_vibration_mm_s,
         RANK() OVER (ORDER BY average_vibration_mm_s DESC) AS rnk
  FROM avg_vibration
)
SELECT pump_id, average_vibration_mm_s
FROM ranked
WHERE rnk <= 1
ORDER BY pump_id;
```

Query result and final answer:

```text
pump_id | average_vibration_mm_s
P-204   | 7.6

For the last 10.0 minutes, pump P-204 had the highest average vibration at 7.6 mm/s.
```

## Autonomous agent incident-review and work-order execution log

The alert evaluator generated this prompt without presenter input:

```text
Asset P-204 crossed the governed vibration warning threshold: 4.53 mm/s against 4.4 mm/s.
Assess failure risk before the next maintenance window and take the appropriate governed action.
```

```text
Scenario: 3bd336aa-d1a8-40b1-82f5-c175c1046975
Incident: 13c50285-aba5-444c-b5b6-bba810f2a452
Agent run: 74b9404f-f427-477f-a3ee-c0300efe7bdf
Model: databricks-claude-sonnet-4-5
Skill status: Loaded
MCP server: asset_reliability_mcp
Discovered tools: assess_failure_risk, get_asset_telemetry, prepare_work_order, submit_work_order
```

```text
get_asset_telemetry result: Vibration is 5.845 mm/s with correlated operating context.
assess_failure_risk result: Failure risk assessed at 96% (high).
prepare_work_order result: BRG-6312-C3 is available; estimated duration is 3.5 h.
submit_work_order decision: ASK
submit_work_order execution: Not executed
human decision: ALLOW
replayed submit_work_order execution: Executed once
work order: WO-35374 scheduled for the next available maintenance window
```

Final totals:

```text
Input tokens: 9,715
Output tokens: 892
Estimated cost: $0.042525
Tool calls: 4
Outcome: Maintenance scheduled
```

## Unity Gateway human approval execution log

```text
Elicitation: 945eb42d-df89-43a4-883e-df753209cdf1
Request SHA-256: 620b94e78c0ccf84189d042cb921d91b8f79e7ff1941051f193b29e857f7d2a5
Initial decision: ASK
Initial execution: Not executed
Approving subject: [SANITIZED_HUMAN_SUBJECT]
Approved decision: ALLOW
Replay execution count: 1
Work orders created: WO-35374
Immutable receipt: 448ba4ce-f5ff-416a-bc11-e21ed1f1dde9
```

A separate direct-source probe authenticated as the human App user returned:

```http
POST /mcp -> 403
{"detail":"Gateway source principal required"}
```

## Evidence locations

- `README.md`: concise proof in the validator's primary entry point.
- `notebooks/DATABRICKS_END_TO_END_EXECUTION_EVIDENCE.ipynb`: visible committed notebook outputs.
- `src/pipelines/LAKEFLOW_PIPELINE_RUN_OUTPUT_AND_ROW_COUNTS.md`: Lakeflow proof beside pipeline code.
- `genie/GENIE_CONVERSATION_GENERATED_SQL_AND_RESULTS.md`: Genie proof beside its definition.
- `backend/AUTONOMOUS_AGENT_WORK_ORDER_EXECUTION_LOG.md`: agent proof beside runtime code.
- `backend/UNITY_GATEWAY_HUMAN_APPROVAL_EXECUTION_LOG.md`: approval proof beside Gateway code.
- `frontend/DEPLOYED_DATABRICKS_APP_API_RESPONSES.md`: deployed response beside App UI code.
- `sql/UNITY_CATALOG_AND_LAKEBASE_QUERY_RESULTS.md`: data-plane results beside SQL assets.
- `evidence/`: detailed machine-readable archive retained for audit and cross-checking.

