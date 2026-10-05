from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from backend.gateway import GatewayAsk, GatewayElicitation
from backend.production_runtime import (
    ProductionCommandCenterRuntime,
    _canonical_decision_rationale,
    _canonical_terminal_response,
    _valid_decision_rationale,
    _valid_terminal_response,
)


@pytest.mark.asyncio
async def test_gateway_ask_waits_for_explicit_approval_replay() -> None:
    runtime = ProductionCommandCenterRuntime.__new__(ProductionCommandCenterRuntime)
    runtime.repository = SimpleNamespace(
        append_activity=AsyncMock(),
        set_waiting=AsyncMock(),
    )
    runtime._spawn = Mock()
    ask = GatewayAsk(
        '{"method":"tools/call"}',
        GatewayElicitation(
            elicitation_id="approval-1",
            url="https://workspace.example/approval-1",
            message="Approval required",
        ),
    )

    await runtime._pause_for_gateway(
        "incident-1", "submit_work_order", ask, started_at=datetime.now(UTC)
    )

    activity = runtime.repository.append_activity.await_args.args[1]
    assert activity.action.href == "https://workspace.example/approval-1"
    assert activity.action.endpoint == "/api/incidents/incident-1/approve"
    runtime._spawn.assert_not_called()


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
