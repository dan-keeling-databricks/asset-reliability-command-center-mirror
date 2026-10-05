# Build and deployment status

Target workspace: `fevm-classic-stable-realtime-demos` (`fe_vm_rtm`). The project uses the
existing `classic_stable_realtime_demos_catalog` and the dedicated
`asset_reliability_command_center` schema; it does not create another catalog.

| Phase | Task | Status |
| --- | --- | --- |
| Foundation | Build the React command center, FastAPI service, and typed runtime contracts | Complete |
| Lakebase | Deploy scenario, command, telemetry, live-state, incident, agent, work-order, identity, heartbeat, and audit tables | Complete |
| Lakebase | Add idempotency constraints, indexes, state triggers, `LISTEN/NOTIFY`, and restart semantics | Complete |
| Producer | Stream continuous eight-pump telemetry to Kafka from an isolated classic-compute job | Complete |
| Producer | Support immediate P-202/P-207 transients and one presenter-controlled P-204 issue onset per scenario | Complete |
| Producer | Make broker delivery and sequence recovery safe across continuous-job restarts | Complete |
| RTM | Run standalone Kafka-to-Lakebase Spark Real-Time Mode ingestion on dedicated DBR 19 compute | Complete |
| RTM | Persist checkpoints in the project Unity Catalog volume and use idempotent event keys | Complete |
| App data | Use Lakebase pooling, notification-driven SSE, and independent one-second alert evaluation | Complete |
| Agent | Host the code-defined agent in the Databricks App with runtime skill/MCP discovery | Complete |
| Agent | Run concurrent incident evaluations and route GPT-5.1 to Sonnet 4.5 for high-risk reasoning | Complete |
| Governance | Expose four business tools through Unity Gateway MCP | Complete |
| Governance | Attach native `ASK` policy to `submit_work_order` and preserve the real approval handoff | Complete |
| Governance | Keep `ASK` as a pure pause and emit one conclusion only after successful replay | Complete |
| Agent | Require prospective, tool-specific decision rationales before execution | Complete |
| Agent | Fall back safely when model rationale formatting remains unusable | Complete |
| UI | Build the live fleet command center, agent-active cues, alerts, and invocation history | Complete |
| UI | Build the two-pane incident overlay, follow-up input, dynamic activity timeline, and footer totals | Complete |
| Quality | Pass Ruff, six pytest tests, TypeScript checking, and the Vite production build | Complete |
| Quality | Validate desktop and 1280×800 remote layouts with no console or network errors | Complete |
| Quality | Validate concurrent normal-transient reviews and retained no-action history | Complete |
| Quality | Validate P-204 high-risk assessment, native `ASK`, approval replay, and scheduled work order | Complete |
| Performance | Replace full-scenario sparkline scan with indexed per-pump top-N reads | Complete |
| Performance | Coalesce SSE notifications and remove full snapshots from agent telemetry tools | Complete |
| Performance | Add non-blocking 24-hour batched telemetry retention | Complete |
| UI | Add presentation typography controls and second-round incident conversation/Gantt refinements | Complete on `feat/ui_feedback_round_two` |
| UI feedback round 1 | Simplify fleet navigation, latency, controls, and consolidated history | In review |
| UI feedback round 1 | Unify incident conversation, prompt context, event icons, and execution timeline | In review |
| Multi-user control | Persist one shared shift operator with audited, concurrency-safe handover | Complete on `feat/shared-shift-operator` |
| Repository | Document architecture, manual deployment, and multi-agent worktree conventions | Complete |
| Repository | Add a detailed technical architecture and agent-loop HTML reference | Complete on `feat/interactive_architecture` |
| Repository | Publish the repository and feature branch to GitHub | Complete |

## Deployed resources

- App: `asset-reliability-center`
- Lakebase project: `asset-reliability-command-center`
- Producer job: `952037348530266`
- RTM job: `17156207332698`
- Kafka topic: `asset-reliability-pump-telemetry`
- Unity Catalog schema: `classic_stable_realtime_demos_catalog.asset_reliability_command_center`
- Lakebase schema: `asset_reliability`
- GitHub: `https://github.com/dan-keeling_data/asset-reliability-command-center`
- Feature branch: `feat/lakebase_latency`

## Acceptance run

Scenario `07f74382-8a4c-4e6e-9a93-c66473ce2009` passed at 1440×900. P-202 and
P-207 were reviewed concurrently and resolved with no action. P-204 was assessed at 96%
failure risk; Unity Gateway returned native `ASK` before `submit_work_order` executed. After
approval, the exact pending request resumed without increasing the four-call total and
created `WO-54921`. The overlay closed back to the command center with maintenance scheduled,
both no-action histories retained, and no browser console or network errors.

## Performance acceptance

Deployment `01f1c0aaad0919f59baa96d6c657c119` was validated against the live Lakebase
event log after it had grown beyond 4.9 million rows. Seven settled SSE updates measured
20, 23, 24, 20, 20, 24, and 25 ms: 23 ms median versus the 10,149 ms pre-fix query plan,
approximately 441 times faster. Telemetry advanced throughout, both supported layouts were
clean, Clear & restart launched P-202/P-207 concurrently, and all browser requests succeeded
with an empty console. The bounded retention delete plan was also exercised transactionally:
it selected exactly 1,000 expired events through the composite index and completed below its
two-second statement timeout. The first deployed maintenance cycle then pruned approximately
34,000 expired rows without interrupting ingestion; PostgreSQL autovacuum completed
immediately afterward.
