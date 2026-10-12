# Deployed Databricks App API responses

The deployed App `asset-reliability-center` returned the following text-readable responses on
2026-10-12 UTC.

```http
GET /api/health -> HTTP 200
{"status":"ok","adapter":"lakebase","detail":"Lakebase - live operational state"}

GET /api/build-info -> HTTP 200
{"git_commit":"940c383fa91f9bd98f5ca0994d370b820396e582"}
```

Sanitized `/api/dashboard` fields:

```json
{
  "scenario_id": "3bd336aa-d1a8-40b1-82f5-c175c1046975",
  "platform": {
    "connection": "connected",
    "kafka_to_lakebase_latency_ms": 57,
    "lakebase_to_ui_latency_ms": 33,
    "end_to_end_latency_ms": 90,
    "last_event_at": "2026-10-12T10:24:02.098691Z"
  },
  "pump_204": {
    "state": "warning",
    "state_label": "Maintenance scheduled",
    "vibration_mm_s": 8.042,
    "warning_threshold_mm_s": 4.4,
    "maintenance_label": "WO-35374 scheduled"
  },
  "incident_history": [
    {"pump_id":"P-204","status_label":"Maintenance scheduled"},
    {"pump_id":"P-202","status_label":"Reviewed - no action"},
    {"pump_id":"P-207","status_label":"Reviewed - no action"}
  ]
}
```

The complete sanitized response is committed in `evidence/09_app_health.json`.

