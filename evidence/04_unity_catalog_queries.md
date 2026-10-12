# Unity Catalog execution results

Captured 2026-10-12 UTC through SQL warehouse `2162b643ec0f802a`. All statements completed against
the live target workspace.

## Governed dataset counts

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

Statement `01f1c626-d929-126b-9256-5813b00c6ab5` returned `SUCCEEDED`:

| dataset | row_count |
|---|---:|
| telemetry_bronze | 3,884,768 |
| telemetry_silver | 3,884,704 |
| telemetry_quarantine | 0 |
| pump_telemetry_history | 3,884,704 |

## Recent validated telemetry

```sql
SELECT pump_id, event_time, ingested_at,
       ROUND(vibration_mm_s, 3) AS vibration_mm_s,
       ROUND(bearing_temperature_c, 1) AS bearing_temperature_c,
       ROUND(flow_m3h, 1) AS flow_m3h
FROM classic_stable_realtime_demos_catalog.asset_reliability_command_center.pump_telemetry_history
ORDER BY event_time DESC
LIMIT 8;
```

| pump_id | event_time | ingested_at | vibration_mm_s | bearing_temperature_c | flow_m3h |
|---|---|---|---:|---:|---:|
| P-208 | 2026-10-12 10:22:47.523242 | 2026-10-12 10:22:48.016 | 2.998 | 57.3 | 183.5 |
| P-207 | 2026-10-12 10:22:47.523127 | 2026-10-12 10:22:48.016 | 2.809 | 58.8 | 180.8 |
| P-206 | 2026-10-12 10:22:47.522995 | 2026-10-12 10:22:48.016 | 2.523 | 56.4 | 186.1 |
| P-205 | 2026-10-12 10:22:47.522437 | 2026-10-12 10:22:48.016 | 2.369 | 57.8 | 178.5 |
| P-204 | 2026-10-12 10:22:47.522317 | 2026-10-12 10:22:48.016 | 8.040 | 76.2 | 173.4 |
| P-203 | 2026-10-12 10:22:47.522188 | 2026-10-12 10:22:48.016 | 1.706 | 56.3 | 183.4 |
| P-202 | 2026-10-12 10:22:47.521989 | 2026-10-12 10:22:48.016 | 1.396 | 57.7 | 177.9 |
| P-201 | 2026-10-12 10:22:47.521633 | 2026-10-12 10:22:48.016 | 1.358 | 57.3 | 184.1 |

## Curated view schema

`DESCRIBE ...pump_telemetry_history` returned:

| column | type |
|---|---|
| event_time | timestamp |
| pump_id | string |
| flow_m3h | double |
| discharge_pressure_bar | double |
| vibration_mm_s | double |
| bearing_temperature_c | double |
| motor_current_a | double |
| source | string |
| ingested_at | timestamp |

The governed view deliberately omits scenario IDs, Kafka partitions/offsets, broker-control
fields, producer command state, and operational workflow records.

## Least-privilege grants

`SHOW GRANTS ON VIEW ...pump_telemetry_history` included:

| Principal | ActionType | ObjectType | ObjectKey |
|---|---|---|---|
| asset-reliability-command-center-operator | SELECT | TABLE | classic_stable_realtime_demos_catalog.asset_reliability_command_center.pump_telemetry_history |

The same principal produced no rows with `SELECT` when the captured `SHOW GRANTS` results were
filtered for `telemetry_bronze`, `telemetry_silver`, or `telemetry_quarantine`. Catalog owners and
unrelated personal principals were removed from this sanitized evidence because they do not prove
the application's least-privilege contract.

## Data-quality interpretation

The Lakeflow event log in `03_lakeflow_pipeline.json` proves the expectation-bearing Silver and
quarantine flows were running. The live counts show 3,884,704 validated rows exposed through the
curated view and zero current quarantine rows. The 64-row Bronze/Silver difference reflects
historical pipeline state across continuous restarts; this package does not claim those rows were
quarantined in the current update.

