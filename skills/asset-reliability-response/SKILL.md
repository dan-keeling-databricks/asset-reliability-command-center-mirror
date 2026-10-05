---
name: asset-reliability-response
description: Investigate automatically triggered rotating-equipment anomalies, assess near-term failure risk, and pursue proportionate maintenance action through governed tools.
---

# Asset reliability response

Use this skill when an alert supplies an asset, threshold condition, observed value, and
maintenance context. Treat the alert as the investigation request; do not ask an operator to
restate it.

## Investigation outcome

Determine whether the observation is a self-resolving operating transient or credible
degradation likely to cause failure before the next maintenance window. Use available tools
to gather enough evidence for that decision. Do not follow a predetermined tool count or
invent evidence that a tool did not return.

Relevant evidence includes:

- recent direction, persistence, and rate of change of the triggered parameter;
- the asset's stable baseline and comparable fleet peers under similar load;
- correlated changes in flow, pressure, motor current, and bearing temperature;
- maintenance timing and the operational consequence of waiting.

A short excursion correlated with a load or pressure transition and returning toward its
baseline normally requires no maintenance. Persistent or accelerating vibration combined
with rising bearing temperature, current, or loss of hydraulic performance is credible
bearing degradation.

## Action

If evidence supports a normal transient, record a concise no-action resolution with the
observations that distinguish it from degradation.

If failure risk before the next maintenance window is high, pursue a work order to replace
the affected bearing at the earliest suitable window. Populate the work order only from
tool-returned asset, maintenance, and risk evidence.

All business actions must use the available governed tool surface. If policy returns `ASK`
or approval-required, stop before the action and report that the run is waiting for human
authorization. Do not claim the work order was submitted. Resume only after the Gateway
authorizes the pending call, preserving the original request. If policy denies the action,
report it as denied rather than as an execution failure.

## Response style

Keep the operator narrative brief and decision-oriented: risk, evidence, action or no-action,
and governance status. Emit runtime metadata for every skill, model, MCP server, policy
decision, and tool event so the command center can render the actual execution path.
