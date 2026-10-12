# Unity Catalog and Lakebase executed query results

## Unity Catalog governed history

The SQL warehouse returned these counts:

```text
telemetry_bronze       3,884,768
telemetry_silver       3,884,704
telemetry_quarantine           0
pump_telemetry_history 3,884,704
```

Recent validated history included:

```text
pump_id event_time                  vibration_mm_s bearing_temperature_c
P-208   2026-10-12 10:22:47.523242 2.998            57.3
P-207   2026-10-12 10:22:47.523127 2.809            58.8
P-204   2026-10-12 10:22:47.522317 8.040            76.2
P-202   2026-10-12 10:22:47.521989 1.396            57.7
```

## Lakebase operational state

The standalone Spark Real-Time Mode sink wrote:

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

The same operational schema contained three completed incident outcomes:

```text
P-207 resolved_no_action
P-202 resolved_no_action
P-204 maintenance_scheduled  WO-35374 scheduled to replace the drive-end bearing
```

The complete SQL and result tables are committed in
`evidence/04_unity_catalog_queries.md` and `evidence/05_lakebase_queries.md`.

