# Asset Reliability Command Center

## Audience and outcome

This ten-minute Data + AI World Tour demo is designed for Energy, Utilities, and
Manufacturing leaders. It shows a live fleet of comparable pumps, autonomous review of
operating anomalies, and a governed transition from diagnosis to maintenance action. The
business outcome is earlier intervention on Pump P-204 without asking an operator to notice
the anomaly, formulate an investigation, or role-play an approval process.

## Demo flow

1. The command center opens on eight pumps streaming live operating telemetry. Lakebase
   connectivity, live operational state, and measured app-to-Lakebase latency appear in the
   upper-right status area.
2. A restarted scenario immediately produces two normal transients on other pumps. Each
   threshold crossing creates a visible alert and launches an autonomous agent evaluation.
3. The agent reviews both events concurrently and judges them to be normal operating
   transients. Their alerts resolve without maintenance, and the invocation history preserves
   the decisions and evidence.
4. The presenter chooses **Trigger P-204 excursion**. P-204's continuous feed transitions
   naturally from stable operation into a growing bearing-vibration excursion.
5. The app's one-second evaluator detects the threshold crossing, creates an incident, and
   autonomously starts the agent with the pump identity and observed condition. No presenter
   prompt is required.
6. Selecting the alert opens an incident-review overlay. The left panel contains the agent's
   narrative and a follow-up input; the right panel shows every model, skill, MCP server,
   tool call, Gateway decision, and result as it is actually emitted.
7. The agent assesses a high risk of failure before the next maintenance window and proceeds
   toward a bearing-replacement work order. Unity Gateway returns `ASK` immediately before
   the governed action can execute, so the agent visibly stops.
8. The presenter follows the real Gateway handoff and approves once. The governed call then
   executes, the work order is scheduled, and the incident review completes.
9. Closing the overlay returns to the live command center. P-204 remains in warning state but
   now shows maintenance scheduled; the two normal events remain visible in history as
   reviewed with no action required.

## Experience principles

- The command center, not chat, is the primary screen.
- Alerts start agent evaluations; the presenter does not type the initial instruction.
- The UI never predicts or hardcodes a skill, model, MCP server, tool sequence, business
  stage, or tool-call limit. It renders runtime events only after the agent emits them.
- Every tool call is a distinct activity event. The incident footer shows only total tokens,
  estimated cost, and tool calls.
- Independent incident evaluations can run concurrently.
- Unity Gateway's native approval flow is the sole approval authority.
- **Clear and restart** begins a new scenario, clears demo history, returns all pumps to their
  initial state, and immediately schedules the two normal transients. P-204 has one
  presenter-triggered excursion per scenario; repeating it requires a scenario restart.

## Solution architecture

### Control plane and synthetic telemetry

The Databricks App does not connect to Kafka. App controls write durable, idempotent commands
to a Lakebase `scenario_commands` table. A continuously running producer on classic compute
polls and claims those commands, maintains the simulation state for all eight pumps, and
publishes telemetry continuously to a dedicated asset-reliability Kafka topic.

`restart_scenario` creates a fresh scenario identifier and schedules the two normal
transients immediately. `start_p204_excursion` changes P-204 from its current stable signal
into the issue-onset state without interrupting the stream. Command identifiers, scenario
identifiers, and compare-and-set status transitions prevent duplicate execution after
producer or App retries.

This extra hop is deliberate: the Grid Digital Twin broker is reachable from classic
compute in its VPC, while Databricks Apps should not be assumed to have that network path.
The App retains presenter control, but broker connectivity stays on classic compute.

### Streaming and operational state

A standalone Spark Structured Streaming RTM query on fixed classic compute reads Kafka,
decodes and validates the telemetry envelope, and writes it to Lakebase. The implementation
uses standalone RTM because the native Lakebase streaming sink is not supported inside
Lakeflow Declarative Pipelines RTM. Target runtime is DBR 18.3 or later with RTM enabled,
Photon/autoscaling/spot disabled, `update` output mode, a unique checkpoint in a Unity
Catalog volume, and an idempotent event key.

Lakebase contains the telemetry event log, current pump state, scenario commands, incidents,
agent runs, activity events, work-order state, and audit records. Database triggers maintain
the latest pump state and issue `LISTEN/NOTIFY` signals after telemetry writes. The App uses
those notifications to push coalesced display updates over SSE. Indexed, per-pump top-N
queries keep the dashboard bounded as the event log grows, and best-effort batch retention
keeps 24 hours of raw telemetry without blocking ingestion. The App independently queries
current state every second to evaluate alert rules and launch agents. RTM performs ingestion
only; it does not own alert logic or agent orchestration.

### App and agent runtime

The Databricks App hosts the React/AppKit command center and a server-side, code-defined
agent runtime. Each newly created incident supplies a machine-generated prompt containing
the pump, threshold rule, observed value, timestamp, and scenario context. The runtime loads
available skills and governed MCP bindings, lets the agent choose its workflow, and persists
each observed activity event. A background task per incident permits concurrent evaluations.

Two Databricks-hosted models are routed by task purpose. Model and routing metadata are shown
only after a call occurs. Business tool calls pass through the real Unity Gateway using the
approved user identity model. If policy returns `ASK`, the run becomes waiting-for-approval
and resumes only after the native Gateway approval flow authorizes the call.

## Revised build plan

1. **Confirm deployment inputs.** Obtain the target workspace, assign a dedicated Kafka topic,
   verify classic-compute access to the existing broker, and confirm the chosen compute can
   reach the target Lakebase endpoint.
2. **Replace the old implementation.** Preserve only useful deployment references, then build
   a fresh AppKit/React frontend and server application around typed runtime contracts.
3. **Create the Lakebase schema.** Add scenario commands and runs, telemetry events and live
   state, alert rules, incidents, agent runs, generic activity events, work orders, and audit
   records. Add uniqueness constraints, indexes, state-maintenance triggers, notifications,
   and reset semantics.
4. **Build the classic-compute producer.** Continuously emit believable telemetry for eight
   comparable pumps, claim Lakebase commands safely, schedule the two restart transients, and
   apply P-204 issue onset without breaking stream continuity.
5. **Build standalone RTM ingestion.** Read the dedicated Kafka topic, validate the event
   schema, write idempotently to Lakebase, configure persistent checkpoints, and expose
   source, processing, sink, and end-to-end latency metrics.
6. **Build the command-center data path.** Maintain a Lakebase pool and notification listener,
   send UI updates over SSE, poll once per second for trigger evaluation, calculate observed
   app-to-Lakebase latency, and make incident creation race-safe.
7. **Build the code-defined agent.** Load skills and MCP bindings at runtime, generate the
   autonomous incident prompt, support concurrent runs, implement two-model routing, and
   emit a generic event stream with tokens and estimated cost.
8. **Complete governed execution.** Bind real tools through Unity Gateway, use the approved
   identity handoff, persist `ASK` state, open the native approval flow, and resume only from
   the authorized Gateway result.
9. **Build the UI.** Implement the eight-pump live view, agent-active visualization, alerts,
   incident-review overlay, follow-up input, generic activity timeline, invocation history,
   scenario controls, and maintenance-scheduled close state.
10. **Validate locally and remotely.** Test command idempotency, continuous telemetry,
    normal-transient outcomes, controlled P-204 onset, duplicate alert suppression, parallel
    evaluations, dynamic event rendering, real approval, restart behavior, failure recovery,
    latency display, and the complete ten-minute presenter path.

## Known external inputs

- Kafka broker: `63.186.81.193:443`
- Kafka topic: `asset-reliability-pump-telemetry`
- Target Databricks workspace: `fevm-classic-stable-realtime-demos`
- Unity Catalog namespace: `classic_stable_realtime_demos_catalog.asset_reliability_command_center`
- Lakebase project: `asset-reliability-command-center`
- Databricks App: `asset-reliability-center`
