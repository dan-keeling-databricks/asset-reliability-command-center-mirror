# Technical debt and accepted tradeoffs

This register reflects the integrated validator-ready architecture. It distinguishes resolved
risks from explicitly accepted demo choices and remaining maintainability work. A resolved item
should not be reopened merely because the implementation differs from a hypothetical production
platform; remaining claims must match current source and tests.

## Resolved in the validator-readiness work

### Source MCP policy bypass

**Resolved.** `/mcp` now fails closed unless trusted Databricks proxy identity headers identify
the App service principal configured for the Gateway source connection. Human App users cannot
call the write-capable source directly. Positive and negative API tests cover the boundary.

### Scenario restart leakage

**Resolved.** Restart uses the operator-change lock, archives active database runs, cancels and
awaits in-memory agent tasks, clears scenario-bound identity, and only then creates the next
scenario. Database predicates also reject delayed activity, conversation, completion, failure,
and work-order writes from another replica after its scenario is archived.

### Approval replay race and missing governance evidence

**Resolved.** Pending approval is claimed with a recoverable UUID/version-fenced single-flight
lease. Release and finalization require the exact claim. Successful execution atomically records
an immutable receipt and completed run containing elicitation ID, request hash, approving subject,
decision, and timestamps; database uniqueness remains the final work-order idempotency guard.

### Stranded producer commands

**Resolved.** Producer claims carry expiry, owner/version, and attempt count. Expired claims are
reclaimed or moved to a visible failed state after the bounded retry limit.

### Timeline sequence race

**Resolved.** Activity allocation locks the owning run and conversation allocation locks the
owning incident before calculating the next sequence.

### Governance configuration drift

**Resolved.** Deployment compares and reconciles MCP connection routing, tool selectors, service
policy, Skill files/finalization, and App source permission. A post-deployment probe requires exact
tool discovery, a successful benign source read, and native `ASK` with no write execution.

### Two-model design drift

**Resolved.** Agent reasoning, tool choice, and final response use only
`databricks-claude-sonnet-4-5`. Obsolete endpoint resources and routing settings were removed.

### Untruthful operational health

**Resolved.** App readiness derives from Lakebase access, latest telemetry, producer heartbeat,
RTM's durable ingestion effect, and evaluator heartbeat. Stale components produce a degraded
state with an explicit label.

### Silent invalid data in the governed analytical path

**Resolved for the validator path.** The SQL Lakeflow pipeline preserves raw broker records,
applies named Silver expectations, and writes invalid records with reasons to quarantine. The
separate low-latency RTM path still filters operational envelopes and should gain equivalent
drop metrics before production use.

### Stale frontend deployments and unknown build identity

**Resolved.** `scripts/deploy.py` performs locked installs, the complete test/build gate, frontend
freshness verification, clean-tree validation, and deterministic build-info generation before
the App snapshot is deployed. It readiness-gates the producer, RTM, and governed-history
continuous jobs, grants their service principal verified read access to the synced bundle source
directory, and exposes the deployed commit through `/api/build-info`.

### Architecture and runbook drift

**Resolved.** README, demo narrative, architecture, deployment, task, debt, and evidence documents
now describe the SQL pipeline, telemetry-only OBO Genie, single model, strengthened governance,
existing group, and repository-first deployment path.

## Accepted design decisions

These decisions were explicitly reviewed and should not be “fixed” without revisiting the demo
requirements.

### Model-directed business-tool sequence

The Skill guides evidence gathering and work-order preparation, but the MCP source does not impose
a durable proposal state machine or require a persisted risk assessment before submission. Unity
Gateway still authorizes the consequential action. This preserves a visibly agentic workflow for
the prototype; production use should reconsider server-enforced business prerequisites.

### No fixed tool-call or turn cap

The runtime does not impose a presentation-driven four-call limit or hard-code four business
stages. The model chooses the required work, and every actual call is rendered. A production
service would normally add generous wall-clock, token, cost, and loop safety budgets.

### Two intentional Kafka consumers

Operational RTM-to-Lakebase and governed Lakeflow-to-Unity-Catalog paths consume the same topic
independently. No continuous cross-store reconciliation service is required for this demo.

### Genie is telemetry-only

Genie reads the curated continuous telemetry view and has no scenario, restart, incident,
approval, agent, or work-order context. Genie conversations are independent of command-center
restarts.

### Process-local OBO credentials

Forwarded user tokens remain only in App memory. An App restart during a pending approval requires
a fresh authenticated request before replay. Persisting or distributing bearer tokens would be a
worse security tradeoff for this prototype.

### Deterministic demo data and manual deployment

The eight-pump fleet, transient timing, and one P-204 excursion per scenario are controlled demo
fixtures. Deployment is manual because no CI/CD system is available, but the checked entry point
makes the manual process reproducible.

## Remaining debt

### TD-101: Core modules remain large

`lakebase_repository.py`, `production_runtime.py`, `runtime.py`, and `App.tsx` still combine
multiple stable concerns. The broader decomposition was deliberately deferred to avoid regression
risk during validator-readiness work.

**Future direction:** separate migration/health/query/command persistence, incident/approval/model
services, and focused frontend hooks/components behind the current protocols and contract tests.

### TD-102: Representative integration and frontend tests are incomplete

The suite now covers source authorization, approval/restart races, command leases, ordering,
governance reconciliation, data-plane definitions, Genie API isolation, and deployment contracts.
It still lacks disposable real-PostgreSQL transaction tests, Spark execution tests, and a
Vitest/Testing Library accessibility suite.

### TD-103: Frontend dependency hygiene

The reviewed `source-map-js` advisory is resolved in the lock file and `npm audit` currently
reports no vulnerabilities. Several direct dependencies still use `latest`, and the frontend has
no lint/test scripts. Pin supported ranges and add ESLint plus component tests without applying
blind major upgrades.

### TD-104: Incident request cancellation and modal accessibility

Incident detail refresh can still return out of order, clickable pump cards are not fully
semantic, and modal focus trapping/restoration needs keyboard tests. Add abort/stale-response
guards and complete accessible dialog behavior.

### TD-105: Non-default Lakebase schema notification channel

The migration can replace schema identifiers while the notification listener assumes the default
channel. Use one validated channel derivation in both SQL and Python before supporting arbitrary
Lakebase schemas.

### TD-106: Environment portability is partial

Most deployment inputs are bundle variables or checked command arguments, and personal identity
has been removed from App configuration. Some demo defaults and the governance helper's resource
names remain workspace-specific. Move the remaining names to one validated deployment-settings
contract before reuse in another workspace.

### TD-107: RTM operational rejection metrics

The governed Lakeflow path has quarantine and expectation metrics. The RTM-to-Lakebase path uses
filtering appropriate for low-latency serving but does not yet publish a rejected-record count or
reason. Add operational drop metrics before treating it as a production ingestion service.

## Deferred remediation order

1. Add real PostgreSQL concurrency/migration tests and frontend accessibility tests.
2. Add request cancellation and accessible modal behavior.
3. Pin frontend dependencies and resolve reviewed advisories.
4. Unify schema/channel configuration and complete environment portability.
5. Add RTM rejection metrics.
6. Decompose large modules incrementally behind established interfaces.
