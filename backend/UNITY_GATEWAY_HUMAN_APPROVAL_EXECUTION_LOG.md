# Unity Gateway human-in-the-loop approval execution log

This is the executed governance boundary for P-204 incident
`13c50285-aba5-444c-b5b6-bba810f2a452`.

```text
Tool: submit_work_order
Initial Gateway decision: ASK
Initial execution: Not executed
Elicitation: 945eb42d-df89-43a4-883e-df753209cdf1
Request SHA-256: 620b94e78c0ccf84189d042cb921d91b8f79e7ff1941051f193b29e857f7d2a5
Human decision: ALLOW
Exact-request replay count: 1
Replay execution: Executed
Work order created: WO-35374
Immutable receipt: 448ba4ce-f5ff-416a-bc11-e21ed1f1dde9
```

The App's source-principal enforcement was also executed directly:

```http
POST /mcp -> HTTP 403
{"detail":"Gateway source principal required"}
```

The structured approval and denial outputs are committed in
`evidence/07_gateway_approval.json`.

