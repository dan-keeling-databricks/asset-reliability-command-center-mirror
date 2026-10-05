from typing import Any

import pytest

from backend.model_client import ModelResult, ModelTurn
from backend.models import IncidentDetail, RuntimeActivity
from backend.production_runtime import (
    ProductionCommandCenterRuntime,
    _canonical_decision_rationale,
    _canonical_terminal_response,
    _valid_decision_rationale,
    _valid_terminal_response,
)
from backend.settings import Settings


def test_tool_selection_protocol_accepts_only_decision_rationale() -> None:
    assert _valid_decision_rationale(
        "Decision rationale: Current telemetry is required before assessing failure risk."
    )
    assert not _valid_decision_rationale("I will inspect telemetry.")
    assert not _valid_decision_rationale("Decision rationale: First. Second. Third.")
    assert not _valid_decision_rationale("Decision rationale: First.\nExtra prose")


def test_terminal_protocol_requires_exactly_four_ordered_markdown_bullets() -> None:
    valid = (
        "- **Finding:** Normal operating transient.\n"
        "- **Evidence:** Vibration returned to baseline.\n"
        "- **Risk:** Low near-term failure risk.\n"
        "- **Action:** No maintenance required."
    )

    assert _valid_terminal_response(valid)
    assert not _valid_terminal_response(valid.removeprefix("- "))
    assert not _valid_terminal_response(f"Conclusion\n{valid}")
    assert not _valid_terminal_response(valid.replace("Evidence", "Risk", 1))


def test_decision_rationale_is_canonicalized_without_discarding_model_wording() -> None:
    assert _canonical_decision_rationale(
        "```text\n**Decision rationale:**\nTelemetry is required first. "
        "The risk tool will then assess persistence. Extra prose must be removed.\n```"
    ) == (
        "Decision rationale: Telemetry is required first. "
        "The risk tool will then assess persistence."
    )
    assert _canonical_decision_rationale("I will inspect governed telemetry first.") == (
        "Decision rationale: I will inspect governed telemetry first."
    )
    assert _canonical_decision_rationale("") is None


def test_terminal_response_is_canonicalized_without_discarding_model_wording() -> None:
    wrapped = (
        "```markdown\nFinding: Persistent vibration.\n"
        "**Evidence:** Rising vibration and temperature.\n"
        "- **Risk:** High failure risk.\n"
        "* **Action:** Work order awaits approval.\n```"
    )
    assert _canonical_terminal_response(wrapped) == (
        "- **Finding:** Persistent vibration.\n"
        "- **Evidence:** Rising vibration and temperature.\n"
        "- **Risk:** High failure risk.\n"
        "- **Action:** Work order awaits approval."
    )


class FakeRepository:
    def __init__(self, incident: IncidentDetail) -> None:
        self.current_incident = incident
        self.activities: list[RuntimeActivity] = []
        self.conversation: list[tuple[str, str]] = []
        self.completed_work_order: str | None = None
        self.failure: str | None = None

    async def incident(self, incident_id: str) -> IncidentDetail | None:
        if incident_id != self.current_incident.id:
            return None
        return self.current_incident

    async def append_activity(
        self,
        incident_id: str,
        activity: RuntimeActivity,
        tokens: tuple[int, int, float] | None = None,
        tool_call: bool = False,
    ) -> None:
        del incident_id, tokens, tool_call
        self.activities.append(activity)

    async def append_conversation(self, incident_id: str, role: str, content: str) -> None:
        del incident_id
        self.conversation.append((role, content))

    async def complete_incident(self, incident_id: str, work_order_id: str) -> None:
        del incident_id
        self.current_incident.status = "completed"
        self.completed_work_order = work_order_id

    async def fail_incident(self, incident_id: str, error: str) -> None:
        del incident_id
        self.current_incident.status = "failed"
        self.failure = error


class FakeGateway:
    tools = [
        {"name": "assess_failure_risk", "inputSchema": {"type": "object"}},
        {"name": "prepare_work_order", "inputSchema": {"type": "object"}},
        {"name": "submit_work_order", "inputSchema": {"type": "object"}},
    ]

    async def list_tools(self, token: str) -> list[dict[str, Any]]:
        assert token == "obo-token"
        return self.tools

    async def call_tool(self, name: str, arguments: dict[str, Any], token: str) -> dict[str, Any]:
        del arguments
        assert token == "obo-token"
        results = {
            "assess_failure_risk": {
                "failure_probability_pct": 92,
                "risk_band": "high",
            },
            "prepare_work_order": {"status": "prepared"},
            "submit_work_order": {"work_order_id": "WO-TEST-1"},
        }
        return {"result": {"structuredContent": results[name]}}


class FakeSkillGateway:
    async def call_tool(self, name: str, arguments: dict[str, Any], token: str) -> dict[str, Any]:
        del arguments
        assert name == "governed-reliability-skill"
        assert token == "obo-token"
        return {
            "result": {
                "structuredContent": {
                    "result": "Assess risk, prepare warranted work, and submit through Gateway."
                }
            }
        }


class FakeModel:
    def __init__(self) -> None:
        self.endpoints: list[str] = []
        self.turns = [
            self._tool_turn("risk", "assess_failure_risk"),
            self._tool_turn("prepare", "prepare_work_order"),
            self._tool_turn("submit", "submit_work_order"),
        ]

    @staticmethod
    def _tool_turn(call_id: str, name: str) -> ModelTurn:
        return ModelTurn(
            message={
                "role": "assistant",
                "content": f"Decision rationale: Evidence warrants calling {name}.",
                "tool_calls": [
                    {
                        "id": call_id,
                        "type": "function",
                        "function": {"name": name, "arguments": "{}"},
                    }
                ],
            },
            input_tokens=100,
            output_tokens=20,
        )

    async def turn(
        self,
        endpoint: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        token: str | None = None,
    ) -> ModelTurn:
        del messages, tools, token
        self.endpoints.append(endpoint)
        return self.turns.pop(0)

    async def chat(
        self,
        endpoint: str,
        messages: list[dict[str, str]],
        token: str | None = None,
    ) -> ModelResult:
        del messages, token
        self.endpoints.append(endpoint)
        return ModelResult(
            content="The bearing replacement is scheduled.", input_tokens=80, output_tokens=12
        )


def runtime_settings(**overrides: Any) -> Settings:
    return Settings(
        _env_file=None,
        app_adapter="lakebase",
        databricks_host="https://workspace.example",
        governed_mcp_endpoint="https://workspace.example/mcp",
        governed_skill_endpoint="https://workspace.example/skills",
        governed_skill_tool="governed-reliability-skill",
        **overrides,
    )


def test_settings_default_to_one_sonnet_endpoint() -> None:
    settings = Settings(_env_file=None)

    assert settings.model_endpoint == "databricks-claude-sonnet-4-5"


@pytest.mark.asyncio
async def test_agent_flow_and_follow_up_use_one_configured_model_endpoint() -> None:
    incident = IncidentDetail(
        id="incident-1",
        scenario_id="scenario-1",
        asset_id="P-204",
        title="Bearing vibration excursion",
        condition="Vibration crossed 4.4 mm/s",
        trigger_prompt="Assess the P-204 threshold excursion autonomously.",
    )
    repository = FakeRepository(incident)
    model = FakeModel()
    settings = runtime_settings(model_endpoint="databricks-custom-agent-model")
    runtime = ProductionCommandCenterRuntime(
        repository, settings, gateway=FakeGateway(), model=model
    )
    runtime.skill_gateway = FakeSkillGateway()
    runtime.capture_identity("scenario-1", "obo-token")

    await runtime._run_agent("incident-1")
    await runtime.follow_up("incident-1", "What was scheduled?")

    assert repository.failure is None
    assert repository.completed_work_order == "WO-TEST-1"
    assert model.endpoints == ["databricks-custom-agent-model"] * 4
    assert {
        activity.actor for activity in repository.activities if activity.category == "model_call"
    } == {"databricks-custom-agent-model"}
    assert repository.conversation[-1] == (
        "agent",
        "The bearing replacement is scheduled.",
    )
