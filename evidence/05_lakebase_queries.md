# Lakebase operational execution results

Captured from the live `asset_reliability` PostgreSQL schema on 2026-10-12 UTC. Identity values
have been sanitized; synthetic asset and correlation identifiers are retained.

## Recent telemetry written by Spark Real-Time Mode

```sql
SELECT pump_id, sequence, event_time, vibration_mm_s, bearing_temperature_c,
       kafka_partition, kafka_offset, ingested_at
FROM asset_reliability.telemetry_events
WHERE scenario_id = '3bd336aa-d1a8-40b1-82f5-c175c1046975'
ORDER BY ingested_at DESC
LIMIT 8;
```

| pump_id | sequence | event_time | vibration_mm_s | bearing_temperature_c | partition | offset | ingested_at |
|---|---:|---|---:|---:|---:|---:|---|
| P-208 | 812 | 2026-10-12 10:23:45.036865+00 | 3.046 | 57.0 | 0 | 1818598 | 2026-10-12 10:23:45.064192+00 |
| P-207 | 812 | 2026-10-12 10:23:45.036798+00 | 2.712 | 58.5 | 1 | 7274395 | 2026-10-12 10:23:45.059347+00 |
| P-202 | 812 | 2026-10-12 10:23:45.036305+00 | 1.524 | 58.0 | 1 | 7274392 | 2026-10-12 10:23:45.059270+00 |
| P-203 | 812 | 2026-10-12 10:23:45.036464+00 | 1.738 | 56.8 | 1 | 7274393 | 2026-10-12 10:23:45.059203+00 |
| P-204 | 812 | 2026-10-12 10:23:45.036551+00 | 8.011 | 75.7 | 1 | 7274394 | 2026-10-12 10:23:45.059100+00 |

## Three autonomous incident outcomes

| incident_id | pump | status | observed / threshold mm/s | outcome |
|---|---|---|---|---|
| `956180f3-1c67-4baf-9a85-58d0f393dcf1` | P-207 | resolved_no_action | 4.485 / 4.4 | Signal returned to its expected operating envelope; no maintenance action required. |
| `9fed51fd-27f5-45ff-99e9-542da2dd412a` | P-202 | resolved_no_action | 4.571 / 4.4 | Signal returned to its expected operating envelope; no maintenance action required. |
| `13c50285-aba5-444c-b5b6-bba810f2a452` | P-204 | maintenance_scheduled | 4.530 / 4.4 | WO-35374 scheduled to replace the drive-end bearing. |

The P-202 and P-207 evaluations ran concurrently. Their agent runs started at
`10:17:03.470879Z` and `10:17:02.362910Z` and both finished around `10:17:20Z`.

## Governed work order

| work_order_id | incident_id | pump | action | status | submitted_by |
|---|---|---|---|---|---|
| WO-35374 | `13c50285-aba5-444c-b5b6-bba810f2a452` | P-204 | replace bearing | scheduled | governed-agent |

## Immutable governance receipt

| receipt_id | run_id | elicitation_id | request_sha256 | approving_subject | decision |
|---|---|---|---|---|---|
| `448ba4ce-f5ff-416a-bc11-e21ed1f1dde9` | `74b9404f-f427-477f-a3ee-c0300efe7bdf` | `945eb42d-df89-43a4-883e-df753209cdf1` | `620b94e78c0ccf84189d042cb921d91b8f79e7ff1941051f193b29e857f7d2a5` | `[SANITIZED_HUMAN_SUBJECT]` | ALLOW |

Requested `2026-10-12T10:18:02.270128Z`, decided
`2026-10-12T10:20:09.982808Z`, and completed
`2026-10-12T10:20:10.333199Z`. The schema installs a trigger that rejects updates or deletes on
the receipt table; the committed row is the resulting immutable audit record.

## Component heartbeats

| component | observed_at | status | details |
|---|---|---|---|
| alert-evaluator | 2026-10-12 10:23:45.136683+00 | healthy | scenario `3bd336aa-d1a8-40b1-82f5-c175c1046975` |
| telemetry-producer | 2026-10-12 10:23:43.550502+00 | healthy | phase `streaming`; messages produced `12568` |

