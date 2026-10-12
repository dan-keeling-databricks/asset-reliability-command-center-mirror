# Lakeflow pipeline run output and Bronze/Silver/quarantine row counts

This text is genuine output from the continuous SQL Lakeflow Declarative Pipeline defined in
`governed_telemetry_history.sql`, captured on 2026-10-12 UTC.

```text
Pipeline ID: a0efdccf-440d-479b-8fc2-7cceb0bb3da2
Update ID: 66ebb87e-a19a-4d42-b647-4a139bbe155b
Pipeline state: RUNNING
Update state: RUNNING

2026-10-12T10:14:43.382Z Flow telemetry_bronze is RUNNING.
2026-10-12T10:14:45.172Z Flow telemetry_silver is RUNNING.
2026-10-12T10:14:47.208Z Flow telemetry_quarantine is RUNNING.
```

The live count query completed as statement `01f1c626-d929-126b-9256-5813b00c6ab5`:

```text
dataset                    row_count
telemetry_bronze           3,884,768
telemetry_silver           3,884,704
telemetry_quarantine               0
pump_telemetry_history     3,884,704
```

The zero quarantine count is an executed query result for the current valid synthetic feed, not
a placeholder. Full event excerpts and SQL appear in
`DATABRICKS_END_TO_END_EXECUTION_EVIDENCE.md` and `evidence/03_lakeflow_pipeline.json`.

