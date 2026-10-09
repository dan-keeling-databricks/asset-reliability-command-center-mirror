# Technical debt register

## Scope and method

This register reviews the application end to end at repository commit `a29eb56`: React UI,
FastAPI composition, local and production runtimes, the agent loop, identity, MCP source,
Unity Gateway transport and deployment, Lakebase schema/repository, Kafka producer, Spark
Real-Time Mode ingestion, tests, dependencies, Asset Bundle, and deployment runbook.

The review separates defects from intentional ten-minute-demo tradeoffs. Findings are ordered
by remediation priority rather than implementation convenience.

Baseline on 2026-10-09:

- Ruff passed.
- 37 Python tests passed with two upstream deprecation warnings.
- TypeScript checking and the Vite production build passed after `npm ci`.
- Asset Bundle validation passed, with only absent-local-artifact warnings before the build.
- `npm audit --omit=dev` reported one high-severity `source-map-js` advisory.
- No frontend test runner, producer tests, RTM tests, real PostgreSQL integration tests, or
  deployed-policy contract tests exist.

After the documentation pass, the suite contains 56 passing tests; 19 of those enforce Python
module docstrings and Markdown-before-code structure for notebook-source assets.

## P0 — governance and scenario isolation

### TD-001: App users can bypass Gateway policy at the source MCP route

**Evidence:** `backend/main.py::mcp` forwards any valid JSON-RPC request directly to
`AssetReliabilityMCP`; `backend/mcp.py::call_tool` can execute `submit_work_order`. The route
does not verify that the caller is the MCP connection service principal.

**Impact:** a user with App access may call `/mcp` directly and avoid Unity Gateway's `ASK`
policy. This undermines the central governance claim of the demo.

**Remediation:** restrict the source route to the configured connection principal, or host
write-capable tools behind a separately authenticated source. Add a negative test proving an
ordinary App user cannot execute a source tool directly.

### TD-002: Clear/restart does not terminate old agent executions

**Evidence:** `ProductionCommandCenterRuntime.restart` clears identity state and archives the
scenario through `restart_scenario`, but it does not cancel or await `_run_tasks`. Operator
handover already performs this cancellation.

**Impact:** a task from an archived scenario can continue with a previously captured token and
may reach a governed write after the presenter has cleared the demo.

**Remediation:** serialize restart under `_operator_change_lock`; transactionally fail or cancel
active runs and approvals; cancel and await their asyncio tasks; clear scenario-bound identity;
then create and bind the fresh scenario. Add a restart-versus-agent race test.

### TD-003: Maintenance prerequisites are prompt-enforced rather than server-enforced

**Evidence:** the model may call any discovered tool in any order. `submit_work_order` validates
the asset but does not require a persisted high-risk assessment, prepared proposal, incident
match, or proposal version. Existing runtime tests can submit immediately.

**Impact:** a model error can request an approved work order without durable evidence that the
asset is high risk or that the proposal belongs to the current incident.

**Remediation:** persist a versioned incident action proposal containing the assessment evidence;
make submission consume that proposal transactionally; reject mismatched asset, incident,
proposal version, or risk state independently of the prompt.

### TD-004: Governance deployment does not reconcile existing resources

**Evidence:** `scripts/deploy_governance.py::ensure_connection` and `ensure_service` return
existing objects unchanged. Tool selectors, source identity, and policy handler drift are not
compared or updated.

**Impact:** deployment can report success while the active service omits the `ASK` policy or
exposes a stale tool set.

**Remediation:** make deployment convergent: compare desired and effective configuration,
update or safely recreate drifted objects, rotate stale credentials, and run a post-deploy probe
that verifies `submit_work_order` returns native `ASK` before source execution.

## P1 — correctness, recovery, and design drift

### TD-005: Approved single-model design is not on `main`

**Evidence:** `app.yaml`, `databricks.yml`, `backend/settings.py`, and
`backend/production_runtime.py` still configure primary and fast endpoints and route high risk
from GPT-5.1 to Claude Sonnet 4.5. The single-model implementation remains isolated on
`feat/single_model_runtime`.

**Impact:** deployed behavior differs from the approved design and complicates the planned
per-model workflow benchmark because a single evaluation run is not attributable to one model.

**Remediation:** rebase the single-model branch on current `main`, revalidate agent behavior,
merge it, remove obsolete resource bindings/settings, and update architecture and demo docs.

### TD-006: Approval replay is not single-flight or tool-generic

**Evidence:** `_retry_pending` replays without atomically claiming the pending request and always
follows the work-order completion path. It does not validate `pending.tool_name`.

**Impact:** concurrent approval retries may replay a write more than once. Adding `ASK` to any
other tool would falsely record work-order completion.

**Remediation:** claim replay with a compare-and-set transition; validate tool and request ID;
make completion handlers tool-specific; preserve idempotency at the source write; test double
clicks and multiple App instances.

### TD-007: Producer commands have no claim lease or stale-claim recovery

**Evidence:** the producer commits `status='claimed'` before applying a command. No lease expiry,
owner-checked completion, or reclaim query exists.

**Impact:** a producer crash after claim can permanently strand a restart or P-204 excursion.

**Remediation:** store lease expiry and attempt count, reclaim expired claims, require matching
owner/version on completion, and expose failed or exhausted commands in health state.

### TD-008: Activity and conversation sequence allocation can race

**Evidence:** `append_activity` and `append_conversation` allocate ordering with
`MAX(sequence) + 1` under normal transactions.

**Impact:** simultaneous agent, approval, and follow-up writers can choose the same sequence and
hit a uniqueness violation, leaving UI history incomplete.

**Remediation:** lock the owning run/incident row, maintain an atomic next-sequence counter, or
retry unique conflicts. Add concurrent writer tests against PostgreSQL.

### TD-009: Work-order governance evidence is not durably recorded

**Evidence:** the source tool receives part and planned-window data, but `create_work_order`
does not persist those fields. `submitted_by` defaults to `governed-agent` and
`governance_receipt` defaults to an empty object.

**Impact:** Lakebase cannot answer who authorized which exact request, which Gateway elicitation
was satisfied, or what part/window was approved.

**Remediation:** persist operator subject, elicitation/request ID, Gateway decision, serialized
request hash, part, planned window, and completion timestamps as an immutable receipt.

### TD-010: Manual deployment can publish stale frontend assets

**Evidence:** the bundle syncs `frontend/dist/**`, but deployment has no build hook or artifact
freshness check. A deploy from a worktree whose source was merged after its last frontend build
published the old card renderer during the MCP discovery feature.

**Impact:** backend and frontend versions can diverge even though the Git commit and deployment
command appear correct.

**Remediation:** provide one checked deployment script that runs `npm ci`, typecheck, build,
tests, bundle validation, and an asset/source freshness assertion before `apps deploy`. Record the
source commit in an App health/build-info endpoint.

### TD-011: Operational status can remain healthy during pipeline failure

**Evidence:** the platform connection model defaults to connected; snapshots do not derive
readiness from telemetry age or component heartbeats; listener/evaluator retry loops suppress
errors without exporting structured health.

**Impact:** producer, RTM, notification, or evaluator failure may be invisible while the UI says
the system is live.

**Remediation:** calculate readiness from telemetry freshness and producer/RTM/App heartbeats;
surface degraded components and last errors; separate liveness from readiness.

### TD-012: The agent loop has no internal execution budget

**Evidence:** `_run_agent` intentionally permits an unbounded number of model turns and tool
calls.

**Impact:** a repeated model/tool cycle can consume unbounded time, tokens, money, and activity
events even though the UI no longer imposes an artificial four-call cap.

**Remediation:** retain autonomous tool choice but add generous wall-clock, turn, cost, and call
budgets. Exceeding a budget should produce an explicit failed outcome and audit event.

## P2 — observability, portability, and maintainability

### TD-013: Critical paths lack representative integration and UI tests

**Evidence:** Lakebase tests largely inspect SQL strings. There are no real PostgreSQL migration
or transaction tests, MCP source authorization tests, producer/RTM tests, restart/approval race
tests, or frontend test runner.

**Remediation:** prioritize tests for TD-001 through TD-010, using PostgreSQL integration tests,
contract tests for Gateway/source authorization, deterministic producer state tests, Spark schema
tests, and focused React component/accessibility tests.

### TD-014: Invalid or lost Kafka data is not made observable

**Evidence:** RTM uses `failOnDataLoss=false`; malformed payloads and null identity/time fields are
filtered without a quarantine stream or counter.

**Impact:** source gaps or schema drift can silently reduce telemetry completeness.

**Remediation:** publish parse/drop metrics, retain invalid envelopes in a quarantine Delta table
or topic, and document why broker data-loss tolerance is acceptable for the synthetic demo.

### TD-015: Custom Lakebase schemas break telemetry notifications

**Evidence:** migration setup globally replaces `asset_reliability`, including the telemetry
channel name, while `_listen_loop` always subscribes to `asset_reliability_telemetry`.

**Impact:** a non-default schema loses notification-driven telemetry updates.

**Remediation:** keep a stable channel name or derive the same validated channel in both SQL and
listener code; avoid global textual migration replacement.

### TD-016: Core modules combine too many responsibilities

**Evidence:** `lakebase_repository.py` is approximately 1,160 lines,
`production_runtime.py` 878, `runtime.py` 781, and `App.tsx` 502. Persistence, migrations,
queries, lifecycle, policy pause, formatting, and presentation logic are tightly co-located.

**Remediation:** decompose by stable boundaries: migrations/health/queries/commands for Lakebase;
incident runner/approval/model formatting for the agent; hooks and focused components for the UI.
Do this behind existing protocols and contract tests, not as a single rewrite.

### TD-017: Frontend dependency and test hygiene is incomplete

**Evidence:** production dependencies are declared as `latest`; the lock contains a high-severity
`source-map-js` advisory; there is no lint or test script.

**Impact:** clean installs can drift when the lock is refreshed, and regressions rely on manual
browser acceptance.

**Remediation:** pin supported major/minor ranges, update the vulnerable transitive dependency,
add ESLint plus Vitest/Testing Library, and automate audit review without blindly applying major
upgrades.

### TD-018: Incident polling can race and accessibility is incomplete

**Evidence:** the selected incident is refetched on every dashboard revision without cancellation
or a stale-response guard. Clickable pump cards are non-semantic articles, and modal focus is not
fully trapped or restored.

**Remediation:** cancel superseded requests, discard stale responses, use semantic controls, and
add keyboard/focus tests for the incident dialog.

### TD-019: Environment ownership is embedded in source configuration

**Evidence:** workspace host, broker, warehouse, catalog/schema, App, compute types, and a personal
Lakebase principal are committed in `app.yaml` and `databricks.yml`.

**Impact:** reuse, handover, and environment separation require source edits and risk accidental
deployment to the wrong owner or workspace.

**Remediation:** keep safe demo defaults but move identity and environment-specific values to
target variables; add a production target and preflight ownership checks.

### TD-020: Documentation and recorded status have drifted

**Evidence:** `DEMO.md` still describes a two-pane overlay, while the implemented UI uses a
unified conversation/timeline view. Architecture and demo documents still encode two-model
routing rather than the approved single-model direction.

**Remediation:** update narrative and architecture artifacts after the single-model decision is
merged and make the deployment script the single source for quality-gate commands.

## Intentional demo tradeoffs

These should remain documented and tested but are not defects by themselves:

- fixed eight-pump synthetic fleet and deterministic transient timing;
- one P-204 excursion per scenario, reset through Clear/restart;
- process-local OBO tokens, provided restart recovery limitations remain explicit;
- classic compute for broker reachability and dedicated RTM requirements;
- manual deployment while CI/CD is unavailable;
- deterministic local runtime as a UI fixture rather than a governance simulator;
- one configured MCP service with dynamic `tools/list` discovery.

## Recommended remediation order

1. Close the source-MCP authorization bypass and add a deployed negative test.
2. Make restart cancellation and approval replay atomic and single-flight.
3. Enforce evidence/proposal prerequisites at the source write boundary.
4. Make governance deployment convergent and policy-probed.
5. Merge the approved single-model runtime and remove obsolete bindings.
6. Add producer command leases and sequence-allocation safety.
7. Make deployment builds reproducible and expose build identity.
8. Add operational health derived from real freshness/heartbeats.
9. Expand integration/UI tests before decomposing the large modules.
