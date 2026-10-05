from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


class PlatformStatus(BaseModel):
    connection: Literal["connected", "degraded", "offline"] = "connected"
    source_label: str = "Lakebase local adapter"
    latency_ms: int = 12
    kafka_to_lakebase_latency_ms: int = 6
    lakebase_to_ui_latency_ms: int = 12
    end_to_end_latency_ms: int = 18
    last_event_at: datetime = Field(default_factory=utc_now)


class PumpState(BaseModel):
    asset_id: str
    name: str
    unit: str
    state: Literal["normal", "transient", "warning"] = "normal"
    state_label: str = "Normal"
    vibration_mm_s: float
    warning_threshold_mm_s: float = 4.4
    flow_m3_h: float
    current_a: float
    trend: list[float] = Field(default_factory=list)
    maintenance_label: str | None = None
    active_run_id: str | None = None
    updated_at: datetime = Field(default_factory=utc_now)


class AlertSummary(BaseModel):
    id: str = Field(default_factory=lambda: new_id("alert"))
    incident_id: str
    asset_id: str
    title: str
    condition: str
    status: Literal["evaluating", "resolved", "action_required", "maintenance_scheduled"]
    status_label: str
    observed_at: datetime = Field(default_factory=utc_now)
    resolution: str | None = None


class InvocationSummary(BaseModel):
    id: str = Field(default_factory=lambda: new_id("run"))
    incident_id: str
    asset_id: str
    title: str
    status: Literal["running", "waiting", "completed", "failed"] = "running"
    status_label: str = "Running"
    started_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None
    outcome: str | None = None


class ActiveRun(BaseModel):
    invocation_id: str
    incident_id: str
    asset_id: str
    label: str
    current_activity: str
    started_at: datetime = Field(default_factory=utc_now)


class RuntimeAction(BaseModel):
    label: str
    kind: Literal["approval", "link", "command"]
    endpoint: str | None = None
    href: str | None = None


class ActivityDatum(BaseModel):
    label: str
    value: str


class RuntimeActivity(BaseModel):
    id: str = Field(default_factory=lambda: new_id("activity"))
    category: str
    title: str
    actor: str | None = None
    status: Literal["running", "complete", "waiting", "failed", "info"] = "info"
    status_label: str
    summary: str | None = None
    details: list[ActivityDatum] = Field(default_factory=list)
    action: RuntimeAction | None = None
    emitted_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None


class ConversationTurn(BaseModel):
    id: str = Field(default_factory=lambda: new_id("message"))
    role: Literal["agent", "operator", "system"]
    content: str
    created_at: datetime = Field(default_factory=utc_now)


class IncidentTotals(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    estimated_cost_usd: float = 0.0
    tool_calls: int = 0


class PendingApproval(BaseModel):
    serialized_request: str
    elicitation_id: str
    approval_url: str
    message: str
    tool_name: str
    created_at: datetime = Field(default_factory=utc_now)


class IncidentDetail(BaseModel):
    id: str = Field(default_factory=lambda: new_id("incident"))
    scenario_id: str
    asset_id: str
    title: str
    condition: str
    trigger_prompt: str
    status: Literal["running", "waiting", "completed", "failed"] = "running"
    status_label: str = "Agent evaluating"
    narrative: list[str] = Field(default_factory=list)
    conversation: list[ConversationTurn] = Field(default_factory=list)
    activities: list[RuntimeActivity] = Field(default_factory=list)
    totals: IncidentTotals = Field(default_factory=IncidentTotals)
    pending_approval: PendingApproval | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class ScenarioState(BaseModel):
    id: str = Field(default_factory=lambda: new_id("scenario"))
    label: str = "Live reliability scenario"
    p204_triggered: bool = False
    started_at: datetime = Field(default_factory=utc_now)


class ScenarioCommand(BaseModel):
    id: str = Field(default_factory=lambda: new_id("command"))
    scenario_id: str
    kind: Literal["start_p204_excursion"]
    status: Literal["pending", "running", "completed", "failed"] = "pending"
    created_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None
    error: str | None = None


class DashboardSnapshot(BaseModel):
    revision: int = 0
    scenario: ScenarioState = Field(default_factory=ScenarioState)
    platform: PlatformStatus = Field(default_factory=PlatformStatus)
    pumps: list[PumpState] = Field(default_factory=list)
    alerts: list[AlertSummary] = Field(default_factory=list)
    invocations: list[InvocationSummary] = Field(default_factory=list)
    active_runs: list[ActiveRun] = Field(default_factory=list)


class FollowUpRequest(BaseModel):
    message: str = Field(min_length=1, max_length=1000)


class CommandResponse(BaseModel):
    status: Literal["accepted"] = "accepted"
    scenario_id: str
    incident_id: str | None = None
    command_id: str | None = None


class AgentEmission(BaseModel):
    """Production agent runtimes publish this generic envelope to the application."""

    incident_id: str
    activity: RuntimeActivity
    usage: dict[str, Any] | None = None
