# Asset Reliability Command Center demo

## Audience and business outcome

This ten-minute demo is for process-manufacturing, energy, and utilities leaders responsible for
asset availability. It shows how Databricks continuously ingests equipment telemetry, identifies
a bearing-risk condition without waiting for an operator prompt, and turns that insight into a
governed maintenance action before an avoidable outage.

The executive outcome is protected uptime and production throughput. The reliability owner sees
faster detection, fewer emergency repairs, and a complete audit trail from measurement to action.

## Presenter flow

1. Open the command center on eight comparable pumps streaming live operating measurements.
   Point out the live operational state and measured Kafka-to-Lakebase, Lakebase-to-App, and
   end-to-end latency. The status is based on real freshness and component heartbeats.

2. After **Clear & restart**, P-202 and P-207 immediately produce normal transients. Each
   threshold crossing creates a visible alert and autonomously launches an independent agent
   evaluation. The presenter does not type a prompt.

3. Open one reviewed event. The incident conversation shows the machine-authored initiating
   prompt and every runtime-emitted model, Skill, MCP discovery, tool, result, and policy event.
   Both transients resolve with no maintenance action and remain in invocation history.

4. Choose **Trigger P-204 excursion**. P-204 moves smoothly from a stable feed into rising
   bearing vibration. The one-second App evaluator detects the threshold crossing and launches
   the same autonomous workflow.

5. The single Claude Sonnet 4.5 agent uses the governed response Skill, discovers tools on the
   configured `asset_reliability_mcp` service, and assesses a high likelihood of failure before
   the next maintenance window. Tool choice and call count are runtime decisions, not scripted
   UI stages.

6. Immediately before `submit_work_order`, Unity Gateway returns native `ASK`. The activity card
   says the action was not executed, and the agent stops. Follow the real Gateway handoff and
   approve as the current operator; no supervisor role-play or simulated App approval is needed.

7. The exact pending request resumes once. A work order to replace the bearing is created, an
   immutable governance receipt is recorded, and the agent produces its final finding, evidence,
   risk, and action summary. Close the review to return to the live command center with P-204
   showing maintenance scheduled and both no-action histories retained.

8. Open **Ask fleet telemetry**. Explain that this is a distinct, user-governed Genie Agent over
   the continuous Unity Catalog history—not the incident-response agent. Ask a telemetry question
   such as “Which pumps had the highest average vibration in the last 15 minutes?” Show the answer,
   result table, generated SQL, source, and current OBO identity.

9. Close on the complete journey: one Kafka stream supports both a low-latency Lakebase command
   center and a governed SQL Lakeflow/Unity Catalog history; autonomous action is possible, but
   the consequential write still stops at a policy-controlled human decision.

## Experience invariants

- The command center—not chat—is the primary screen.
- Alerts formulate the initiating prompt and start agent evaluations automatically.
- The UI never predicts a model call, Skill, MCP server, tool, tool order, or tool-call count.
- Every actual tool call appears as its own activity event.
- The incident footer shows only total tokens, estimated cost, and tool calls.
- Independent incidents can execute concurrently.
- Unity Gateway is the sole approval authority.
- Genie uses OBO user permissions and reads only continuous telemetry history.
- Genie conversations survive command-center scenario restarts and expose no scenario concepts.
- **Clear & restart** cancels old agent activity before creating the new scenario.
