from __future__ import annotations

import asyncio
import math
from collections.abc import Coroutine
from datetime import timedelta
from typing import Any, Protocol

from backend.models import (
    ActiveRun,
    ActivityDatum,
    AlertSummary,
    ConversationTurn,
    DashboardSnapshot,
    IncidentDetail,
    InvocationSummary,
    PlatformStatus,
    PumpState,
    RuntimeAction,
    RuntimeActivity,
    ScenarioCommand,
    ScenarioState,
    utc_now,
)
from backend.repository import CommandCenterRepository
from backend.settings import Settings


class CommandCenterRuntime(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def restart(self) -> str: ...
    async def trigger_excursion(self) -> str: ...
    def capture_identity(self, scenario_id: str, token: str | None) -> None: ...
    async def approve(self, incident_id: str) -> None: ...
    async def follow_up(self, incident_id: str, message: str) -> None: ...


PUMP_DEFINITIONS = [
    ("P-201", "Cooling Water Pump P-201", "Cooling loop A", 1.20, 182.0, 42.0),
    ("P-202", "Cooling Water Pump P-202", "Cooling loop A", 1.45, 179.0, 41.5),
    ("P-203", "Cooling Water Pump P-203", "Cooling loop A", 1.70, 185.0, 42.5),
    ("P-204", "Cooling Water Pump P-204", "Cooling loop A", 1.95, 181.0, 42.0),
    ("P-205", "Cooling Water Pump P-205", "Cooling loop B", 2.20, 177.0, 41.0),
    ("P-206", "Cooling Water Pump P-206", "Cooling loop B", 2.45, 184.0, 42.4),
    ("P-207", "Cooling Water Pump P-207", "Cooling loop B", 2.70, 180.0, 41.8),
    ("P-208", "Cooling Water Pump P-208", "Cooling loop B", 2.95, 183.0, 42.1),
]


def initial_snapshot() -> DashboardSnapshot:
    pumps = [
        PumpState(
            asset_id=asset_id,
            name=name,
            unit=unit,
            vibration_mm_s=vibration,
            flow_m3_h=flow,
            current_a=current,
            trend=[round(vibration - 0.08, 2), vibration, round(vibration + 0.05, 2)],
        )
        for asset_id, name, unit, vibration, flow, current in PUMP_DEFINITIONS
    ]
    return DashboardSnapshot(
        scenario=ScenarioState(),
        platform=PlatformStatus(),
        pumps=pumps,
    )


class LocalCommandCenterRuntime:
    """Deterministic local adapter that exercises the full UI event contract."""

    def __init__(self, repository: CommandCenterRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self._telemetry_task: asyncio.Task[None] | None = None
        self._evaluator_task: asyncio.Task[None] | None = None
        self._run_tasks: set[asyncio.Task[None]] = set()
        self._tick = 0
        self._excursion_assets: set[str] = set()

    async def start(self) -> None:
        await self.repository.start()
        await self.restart()
        self._telemetry_task = asyncio.create_task(self._telemetry_loop())
        self._evaluator_task = asyncio.create_task(self._evaluator_loop())

    def capture_identity(self, scenario_id: str, token: str | None) -> None:
        del scenario_id, token

    async def stop(self) -> None:
        tasks = [*self._run_tasks]
        if self._telemetry_task:
            tasks.append(self._telemetry_task)
        if self._evaluator_task:
            tasks.append(self._evaluator_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._run_tasks.clear()
        await self.repository.close()

    def _spawn(self, operation: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(operation)
        self._run_tasks.add(task)
        task.add_done_callback(self._run_tasks.discard)

    async def restart(self) -> str:
        for task in list(self._run_tasks):
            task.cancel()
        if self._run_tasks:
            await asyncio.gather(*self._run_tasks, return_exceptions=True)
        self._run_tasks.clear()
        self._excursion_assets.clear()
        fresh = initial_snapshot()

        def reset(snapshot: DashboardSnapshot, incidents: dict[str, IncidentDetail]) -> None:
            incidents.clear()
            snapshot.scenario = fresh.scenario
            snapshot.platform = fresh.platform
            snapshot.pumps = fresh.pumps
            snapshot.alerts.clear()
            snapshot.invocations.clear()
            snapshot.active_runs.clear()

        snapshot = await self.repository.mutate(reset)
        self._spawn(self._run_normal_transient("P-202", 0.45, "Startup recirculation"))
        self._spawn(self._run_normal_transient("P-207", 0.8, "Valve transition"))
        return snapshot.scenario.id

    async def trigger_excursion(self) -> str:
        snapshot = await self.repository.snapshot()
        command = ScenarioCommand(
            scenario_id=snapshot.scenario.id,
            kind="start_p204_excursion",
        )
        await self.repository.enqueue_command(command)
        return command.id

    async def _open_p204_incident(self) -> None:
        snapshot = await self.repository.snapshot()
        if any(alert.asset_id == "P-204" for alert in snapshot.alerts):
            return
        incident, invocation, alert = self._new_incident(
            snapshot.scenario.id,
            "P-204",
            "Drive-end bearing vibration excursion",
            "Vibration crossed 4.4 mm/s and continues to rise",
        )

        def create(state: DashboardSnapshot, incidents: dict[str, IncidentDetail]) -> None:
            pump = self._pump(state, "P-204")
            pump.state = "warning"
            pump.state_label = "Warning"
            pump.active_run_id = invocation.id
            state.alerts.insert(0, alert)
            state.invocations.insert(0, invocation)
            state.active_runs.append(
                ActiveRun(
                    invocation_id=invocation.id,
                    incident_id=incident.id,
                    asset_id="P-204",
                    label="Autonomous review",
                    current_activity="Incident context received",
                )
            )
            incidents[incident.id] = incident

        await self.repository.mutate(create)
        self._spawn(self._run_p204_incident(incident.id, invocation.id, alert.id))

    async def _evaluator_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.alert_evaluator_interval_seconds)
            for command in await self.repository.claim_commands():
                error = None
                try:
                    snapshot = await self.repository.snapshot()
                    if command.scenario_id != snapshot.scenario.id:
                        raise ValueError("Command does not target the active scenario")

                    def apply_command(
                        state: DashboardSnapshot, incidents: dict[str, IncidentDetail]
                    ) -> None:
                        del incidents
                        state.scenario.p204_triggered = True

                    await self.repository.mutate(apply_command)
                    self._excursion_assets.add("P-204")
                except Exception as exc:  # noqa: BLE001
                    error = str(exc)
                await self.repository.finish_command(command.id, error)
            snapshot = await self.repository.snapshot()
            p204 = self._pump(snapshot, "P-204")
            if p204.vibration_mm_s >= p204.warning_threshold_mm_s:
                await self._open_p204_incident()

    async def approve(self, incident_id: str) -> None:
        incident = await self.repository.incident(incident_id)
        if not incident or incident.status != "waiting":
            raise ValueError("Incident is not waiting for approval")

        def resolve(snapshot: DashboardSnapshot, incidents: dict[str, IncidentDetail]) -> None:
            current = incidents[incident_id]
            waiting = next(
                (
                    activity
                    for activity in reversed(current.activities)
                    if activity.status == "waiting" and activity.action
                ),
                None,
            )
            if not waiting:
                raise ValueError("Approval activity was not found")
            waiting.status = "complete"
            waiting.status_label = "APPROVED · ALLOW"
            waiting.completed_at = utc_now()
            waiting.summary = "Approval confirmed; the governed action executed."
            for detail in waiting.details:
                if detail.label == "Decision":
                    detail.value = "ALLOW"
                elif detail.label == "Execution":
                    detail.value = "Executed after approval"
            waiting.action = None
            current.activities.append(
                RuntimeActivity(
                    category="tool_result",
                    title="Maintenance action completed",
                    actor="Enterprise maintenance service",
                    status="complete",
                    status_label="Completed",
                    summary="Work order WO-10482 scheduled for the next available outage.",
                    details=[
                        ActivityDatum(label="Work order", value="WO-10482"),
                        ActivityDatum(label="Scope", value="Replace drive-end bearing"),
                    ],
                )
            )
            current.narrative.append(
                "Approval was granted once. Work order WO-10482 is now scheduled."
            )
            current.status = "completed"
            current.status_label = "Maintenance scheduled"
            current.updated_at = utc_now()
            alert = next(item for item in snapshot.alerts if item.incident_id == incident_id)
            alert.status = "maintenance_scheduled"
            alert.status_label = "Maintenance scheduled"
            alert.resolution = "WO-10482 scheduled"
            invocation = next(
                item for item in snapshot.invocations if item.incident_id == incident_id
            )
            invocation.status = "completed"
            invocation.status_label = "Completed"
            invocation.completed_at = utc_now()
            invocation.outcome = "Maintenance scheduled"
            pump = self._pump(snapshot, "P-204")
            pump.maintenance_label = "WO-10482 scheduled"
            pump.active_run_id = None
            snapshot.active_runs = [
                run for run in snapshot.active_runs if run.incident_id != incident_id
            ]

        await self.repository.mutate(resolve)

    async def follow_up(self, incident_id: str, message: str) -> None:
        incident = await self.repository.incident(incident_id)
        if not incident:
            raise LookupError("Incident not found")
        if incident.status == "waiting":
            answer = (
                "The operational write is still blocked. Unity Gateway has not authorized "
                "the action, so no work order has been submitted."
            )
        elif incident.status == "completed" and incident.asset_id == "P-204":
            answer = (
                "WO-10482 is scheduled to replace the drive-end bearing. The pump remains in "
                "warning state until maintenance is performed."
            )
        else:
            answer = (
                "The evidence supports a short-lived operating transient. The signal returned "
                "to its normal envelope and no maintenance action was created."
            )

        def respond(snapshot: DashboardSnapshot, incidents: dict[str, IncidentDetail]) -> None:
            current = incidents[incident_id]
            current.conversation.extend(
                [
                    ConversationTurn(role="operator", content=message),
                    ConversationTurn(role="agent", content=answer),
                ]
            )
            current.activities.append(
                RuntimeActivity(
                    category="model_call",
                    title="Follow-up response",
                    actor="databricks-claude-sonnet-4-5",
                    status="complete",
                    status_label="Completed",
                    summary="Answered using persisted incident context.",
                    details=[ActivityDatum(label="Output tokens", value="96")],
                )
            )
            current.totals.input_tokens += 284
            current.totals.output_tokens += 96
            current.totals.estimated_cost_usd = round(current.totals.estimated_cost_usd + 0.0023, 4)
            current.updated_at = utc_now()

        await self.repository.mutate(respond)

    async def _telemetry_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.telemetry_interval_seconds)
            self._tick += 1

            def tick(snapshot: DashboardSnapshot, incidents: dict[str, IncidentDetail]) -> None:
                del incidents
                for index, pump in enumerate(snapshot.pumps):
                    baseline = PUMP_DEFINITIONS[index][3]
                    wobble = math.sin((self._tick + index) / 2.3) * 0.09
                    if pump.asset_id in self._excursion_assets:
                        next_value = min(7.2, pump.vibration_mm_s + 0.38)
                    elif pump.state == "transient":
                        next_value = max(4.48, pump.vibration_mm_s + wobble)
                    else:
                        next_value = baseline + wobble
                    pump.vibration_mm_s = round(next_value, 2)
                    pump.flow_m3_h = round(
                        PUMP_DEFINITIONS[index][4] + math.sin(self._tick / 3) * 0.8, 1
                    )
                    pump.current_a = round(
                        PUMP_DEFINITIONS[index][5]
                        + (3.2 if pump.asset_id in self._excursion_assets else 0)
                        + math.sin(self._tick / 3) * 0.2,
                        1,
                    )
                    pump.trend = [*pump.trend[-19:], pump.vibration_mm_s]
                    pump.updated_at = utc_now()
                snapshot.platform.kafka_to_lakebase_latency_ms = 5 + (self._tick % 4)
                snapshot.platform.lakebase_to_ui_latency_ms = 9 + (self._tick % 7)
                snapshot.platform.end_to_end_latency_ms = (
                    snapshot.platform.kafka_to_lakebase_latency_ms
                    + snapshot.platform.lakebase_to_ui_latency_ms
                )
                snapshot.platform.latency_ms = snapshot.platform.lakebase_to_ui_latency_ms
                snapshot.platform.last_event_at = utc_now()

            await self.repository.mutate(tick)

    async def _run_normal_transient(self, asset_id: str, delay: float, cause: str) -> None:
        await asyncio.sleep(delay)
        snapshot = await self.repository.snapshot()
        incident, invocation, alert = self._new_incident(
            snapshot.scenario.id,
            asset_id,
            f"{cause} vibration transient",
            "Vibration briefly crossed the operating warning threshold",
        )

        def create(state: DashboardSnapshot, incidents: dict[str, IncidentDetail]) -> None:
            if state.scenario.id != snapshot.scenario.id:
                return
            pump = self._pump(state, asset_id)
            pump.state = "transient"
            pump.state_label = "Reviewing transient"
            pump.vibration_mm_s = 4.58
            pump.active_run_id = invocation.id
            state.alerts.insert(0, alert)
            state.invocations.insert(0, invocation)
            state.active_runs.append(
                ActiveRun(
                    invocation_id=invocation.id,
                    incident_id=incident.id,
                    asset_id=asset_id,
                    label="Autonomous review",
                    current_activity="Classifying threshold event",
                )
            )
            incidents[incident.id] = incident

        await self.repository.mutate(create)
        await self._sleep()
        await self._append_activity(
            incident.id,
            RuntimeActivity(
                category="model_call",
                title="Classify operating event",
                actor="databricks-gpt-5-mini",
                status="complete",
                status_label="Completed",
                summary="Selected the transient-review workflow from current context.",
                details=[
                    ActivityDatum(label="Input tokens", value="412"),
                    ActivityDatum(label="Output tokens", value="71"),
                ],
            ),
            invocation.id,
            tokens=(412, 71, 0.0023),
        )
        await self._sleep()
        await self._append_activity(
            incident.id,
            RuntimeActivity(
                category="tool_call",
                title="Compare operating envelope",
                actor="asset-context · governed MCP",
                status="complete",
                status_label="ALLOW",
                summary="Signal returned inside its normal envelope within 38 seconds.",
                details=[
                    ActivityDatum(label="Decision", value="ALLOW"),
                    ActivityDatum(label="Result", value="Normal transient pattern"),
                ],
            ),
            invocation.id,
            tool_call=True,
        )
        await self._sleep()

        def complete(state: DashboardSnapshot, incidents: dict[str, IncidentDetail]) -> None:
            if incident.id not in incidents:
                return
            current = incidents[incident.id]
            current.status = "completed"
            current.status_label = "Reviewed · no action"
            current.narrative.extend(
                [
                    "The threshold crossing aligned with a short operating transition.",
                    "Vibration returned to the expected envelope without a persistent rise. "
                    "No maintenance action is required.",
                ]
            )
            current.conversation.append(
                ConversationTurn(
                    role="agent",
                    content=(
                        "**Finding:** Normal operating transient.\n"
                        "**Evidence:** Vibration returned to its expected envelope within 38 "
                        "seconds.\n**Risk:** No evidence of developing mechanical failure.\n"
                        "**Action:** No maintenance action required."
                    ),
                )
            )
            current.updated_at = utc_now()
            alert_item = next(item for item in state.alerts if item.id == alert.id)
            alert_item.status = "resolved"
            alert_item.status_label = "Reviewed · no action"
            alert_item.resolution = "Normal operating transient"
            run = next(item for item in state.invocations if item.id == invocation.id)
            run.status = "completed"
            run.status_label = "Completed"
            run.completed_at = utc_now()
            run.outcome = "No action required"
            pump = self._pump(state, asset_id)
            pump.state = "normal"
            pump.state_label = "Normal"
            pump.active_run_id = None
            state.active_runs = [
                item for item in state.active_runs if item.invocation_id != invocation.id
            ]

        await self.repository.mutate(complete)

    async def _run_p204_incident(self, incident_id: str, invocation_id: str, alert_id: str) -> None:
        steps = [
            (
                RuntimeActivity(
                    category="model_call",
                    title="Route reliability investigation",
                    actor="databricks-gpt-5-mini",
                    status="complete",
                    status_label="Completed",
                    summary="Selected an asset-risk investigation from the observed condition.",
                    details=[
                        ActivityDatum(label="Input tokens", value="536"),
                        ActivityDatum(label="Output tokens", value="82"),
                    ],
                ),
                (536, 82, 0.0031),
                False,
            ),
            (
                RuntimeActivity(
                    category="skill",
                    title="Load rotating-equipment analysis",
                    actor="reliability-investigation",
                    status="complete",
                    status_label="Loaded",
                    summary="Applied the current governed reliability procedure.",
                ),
                None,
                False,
            ),
            (
                RuntimeActivity(
                    category="tool_call",
                    title="Read recent telemetry and maintenance context",
                    actor="asset-context · governed MCP",
                    status="complete",
                    status_label="ALLOW",
                    summary="Persistent rise confirmed; no open maintenance order exists.",
                    details=[
                        ActivityDatum(label="Decision", value="ALLOW"),
                        ActivityDatum(label="Observed", value="5.2 mm/s and rising"),
                    ],
                ),
                None,
                True,
            ),
            (
                RuntimeActivity(
                    category="model_call",
                    title="Assess failure risk",
                    actor="databricks-claude-sonnet-4-5",
                    status="complete",
                    status_label="Completed",
                    summary="High likelihood of failure before the next maintenance window.",
                    details=[
                        ActivityDatum(label="Input tokens", value="1,184"),
                        ActivityDatum(label="Output tokens", value="214"),
                        ActivityDatum(label="Risk estimate", value="87%"),
                    ],
                ),
                (1184, 214, 0.0068),
                False,
            ),
            (
                RuntimeActivity(
                    category="tool_call",
                    title="Prepare bearing replacement plan",
                    actor="maintenance-planning · governed MCP",
                    status="complete",
                    status_label="ALLOW",
                    summary=(
                        "Compatible bearing is available; estimated work duration is 3.5 hours."
                    ),
                    details=[
                        ActivityDatum(label="Decision", value="ALLOW"),
                        ActivityDatum(label="Part", value="BRG-6312-C3"),
                    ],
                ),
                None,
                True,
            ),
        ]
        for activity, tokens, tool_call in steps:
            await self._sleep()
            await self._append_activity(
                incident_id,
                activity,
                invocation_id,
                tokens=tokens,
                tool_call=tool_call,
            )
        await self._sleep()
        waiting = RuntimeActivity(
            category="tool_call",
            title="Schedule maintenance work order",
            actor="maintenance-actions · governed MCP",
            status="waiting",
            status_label="ASK · Not executed",
            summary="Policy requires human approval before this operational write can execute.",
            details=[
                ActivityDatum(label="Decision", value="ASK"),
                ActivityDatum(label="Execution", value="Not executed"),
                ActivityDatum(label="Scope", value="Replace P-204 drive-end bearing"),
            ],
            action=RuntimeAction(
                label="Approve once",
                kind="approval",
                endpoint=f"/api/incidents/{incident_id}/approve",
            ),
        )
        await self._append_activity(incident_id, waiting, invocation_id, tool_call=True)

        def pause(state: DashboardSnapshot, incidents: dict[str, IncidentDetail]) -> None:
            current = incidents[incident_id]
            current.status = "waiting"
            current.status_label = "Waiting for approval"
            current.narrative.extend(
                [
                    "The evidence indicates an 87% likelihood of failure before the next "
                    "maintenance window.",
                    "The agent prepared a bearing-replacement action, but Unity Gateway stopped "
                    "execution because an operational write requires human approval.",
                ]
            )
            current.conversation.append(
                ConversationTurn(
                    role="agent",
                    content=(
                        "**Finding:** Persistent drive-end bearing vibration excursion.\n"
                        "**Evidence:** Vibration reached 5.2 mm/s and continued rising.\n"
                        "**Risk:** 87% likelihood of failure before the next maintenance window.\n"
                        "**Action:** Bearing-replacement work order prepared; submission is "
                        "waiting for approval."
                    ),
                )
            )
            alert = next(item for item in state.alerts if item.id == alert_id)
            alert.status = "action_required"
            alert.status_label = "Approval required"
            run = next(item for item in state.invocations if item.id == invocation_id)
            run.status = "waiting"
            run.status_label = "Waiting for approval"

        await self.repository.mutate(pause)

    async def _append_activity(
        self,
        incident_id: str,
        activity: RuntimeActivity,
        invocation_id: str,
        tokens: tuple[int, int, float] | None = None,
        tool_call: bool = False,
    ) -> None:
        def append(snapshot: DashboardSnapshot, incidents: dict[str, IncidentDetail]) -> None:
            if incident_id not in incidents:
                return
            current = incidents[incident_id]
            completed_at = utc_now()
            duration_ms = {"model_call": 180, "skill": 100, "tool_call": 140}.get(
                activity.category, 80
            )
            activity.emitted_at = completed_at
            activity.started_at = completed_at - timedelta(milliseconds=duration_ms)
            activity.completed_at = None if activity.status == "waiting" else completed_at
            current.activities.append(activity)
            current.updated_at = utc_now()
            if tokens:
                current.totals.input_tokens += tokens[0]
                current.totals.output_tokens += tokens[1]
                current.totals.estimated_cost_usd = round(
                    current.totals.estimated_cost_usd + tokens[2], 4
                )
            if tool_call:
                current.totals.tool_calls += 1
            active = next(
                (run for run in snapshot.active_runs if run.invocation_id == invocation_id),
                None,
            )
            if active:
                active.current_activity = activity.title

        await self.repository.mutate(append)

    def _new_incident(
        self,
        scenario_id: str,
        asset_id: str,
        title: str,
        condition: str,
    ) -> tuple[IncidentDetail, InvocationSummary, AlertSummary]:
        incident = IncidentDetail(
            scenario_id=scenario_id,
            asset_id=asset_id,
            title=title,
            condition=condition,
            trigger_prompt=(
                f"Autonomous alert for {asset_id}: {condition}. Assess failure risk before "
                "the next maintenance window and pursue the governed response warranted by "
                "the evidence."
            ),
            narrative=[
                f"An automated threshold rule opened this review for {asset_id}. "
                "The agent received the observed condition and scenario context."
            ],
        )
        invocation = InvocationSummary(
            incident_id=incident.id,
            asset_id=asset_id,
            title=title,
        )
        alert = AlertSummary(
            incident_id=incident.id,
            asset_id=asset_id,
            title=title,
            condition=condition,
            status="evaluating",
            status_label="Agent evaluating",
        )
        return incident, invocation, alert

    @staticmethod
    def _pump(snapshot: DashboardSnapshot, asset_id: str) -> PumpState:
        return next(item for item in snapshot.pumps if item.asset_id == asset_id)

    async def _sleep(self) -> None:
        await asyncio.sleep(self.settings.local_event_step_seconds)
