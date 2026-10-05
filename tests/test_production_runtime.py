import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from backend.gateway import GatewayAsk, GatewayElicitation
from backend.identity import OboIdentityRegistry
from backend.models import IncidentDetail, PendingApproval
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


@pytest.mark.asyncio
async def test_repeated_gateway_ask_leaves_waiting_state_unchanged() -> None:
    runtime = ProductionCommandCenterRuntime.__new__(ProductionCommandCenterRuntime)
    incident = SimpleNamespace(status="waiting", scenario_id="scenario-1")
    runtime.repository = SimpleNamespace(
        incident=AsyncMock(return_value=incident),
        append_activity=AsyncMock(),
        append_conversation=AsyncMock(),
        complete_incident=AsyncMock(),
    )
    runtime.identities = OboIdentityRegistry()
    runtime.identities.capture("scenario-1", "live-token")
    repeated = GatewayAsk(
        '{"method":"tools/call"}',
        GatewayElicitation("approval-1", "https://example.test/approval", "Still required"),
    )
    runtime.gateway = SimpleNamespace(replay=AsyncMock(side_effect=repeated))
    pending = PendingApproval(
        serialized_request='{"method":"tools/call"}',
        elicitation_id="approval-1",
        approval_url="https://example.test/approval",
        message="Approval required",
        tool_name="submit_work_order",
    )

    assert await runtime._retry_pending("incident-1", pending) is False
    runtime.repository.append_activity.assert_not_awaited()
    runtime.repository.append_conversation.assert_not_awaited()
    runtime.repository.complete_incident.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_approval_model_failure_uses_one_strict_fallback() -> None:
    runtime = ProductionCommandCenterRuntime.__new__(ProductionCommandCenterRuntime)
    runtime.settings = SimpleNamespace(primary_model_endpoint="primary-model")
    runtime.model = SimpleNamespace(turn=AsyncMock(side_effect=RuntimeError("model unavailable")))
    runtime.repository = SimpleNamespace(append_conversation=AsyncMock())
    incident = IncidentDetail(
        scenario_id="scenario-1",
        asset_id="P-204",
        title="Bearing risk",
        condition="Persistent drive-end vibration.",
        trigger_prompt="Assess the alert.",
    )

    await runtime._generate_post_approval_response(
        "incident-1",
        incident,
        {"result": {"structuredContent": {"work_order_id": "WO-10482"}}},
    )

    runtime.repository.append_conversation.assert_awaited_once()
    role, content = runtime.repository.append_conversation.await_args.args[1:]
    assert role == "agent"
    assert _valid_terminal_response(content)
    assert "WO-10482" in content


def test_tool_selection_protocol_accepts_only_decision_rationale() -> None:
    tools = ["get_asset_telemetry"]
    assert _valid_decision_rationale(
        "Decision rationale: I will call get_asset_telemetry because current telemetry is "
        "required before assessing failure risk.",
        tools,
    )
    assert not _valid_decision_rationale("I will inspect telemetry.", tools)
    for verb in ("called", "ran", "returned", "completed", "executed", "found"):
        assert not _valid_decision_rationale(
            f"Decision rationale: I will call get_asset_telemetry because it {verb} evidence.",
            tools,
        )
    assert _valid_decision_rationale(
        "Decision rationale: I will call get_asset_telemetry and assess_failure_risk because "
        "the threshold crossing needs telemetry and a risk estimate.",
        ["get_asset_telemetry", "assess_failure_risk"],
    )


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
        "Extra prose must be removed.\n```",
        ["get_asset_telemetry"],
    ) == (
        "Decision rationale: I will call get_asset_telemetry because telemetry is required first."
    )
    assert _canonical_decision_rationale(
        "The tool returned a high-risk result.", ["prepare_work_order"]
    ) == (
        "Decision rationale: I will call prepare_work_order because the available incident "
        "evidence warrants this governed action."
    )
    assert _canonical_decision_rationale("", ["get_asset_telemetry"]) is None


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


def runtime_for_handover(repository) -> ProductionCommandCenterRuntime:
    runtime = ProductionCommandCenterRuntime.__new__(ProductionCommandCenterRuntime)
    runtime.repository = repository
    runtime.identities = OboIdentityRegistry()
    runtime._user_identities = OboIdentityRegistry()
    runtime._run_tasks = set()
    runtime._operator_change_lock = asyncio.Lock()
    runtime._identity_scenario_id = "old-scenario"
    runtime._last_subject = None
    return runtime


@pytest.mark.asyncio
async def test_handover_binds_cached_selected_operator_token_to_fresh_scenario() -> None:
    repository = SimpleNamespace(handover_operator=AsyncMock(return_value="fresh-scenario"))
    runtime = runtime_for_handover(repository)
    runtime.cache_user_identity("bob@databricks.com", "bob-live-token")
    old_review = asyncio.create_task(asyncio.sleep(10))
    runtime._run_tasks.add(old_review)

    scenario_id = await runtime.handover_operator(
        3, "bob@databricks.com", "alice@databricks.com"
    )

    assert scenario_id == "fresh-scenario"
    assert runtime.identities.get("fresh-scenario").token == "bob-live-token"
    assert runtime._identity_scenario_id == "fresh-scenario"
    assert old_review.cancelled()


@pytest.mark.asyncio
async def test_handover_without_cached_operator_token_leaves_evaluator_waiting() -> None:
    repository = SimpleNamespace(handover_operator=AsyncMock(return_value="fresh-scenario"))
    runtime = runtime_for_handover(repository)

    await runtime.handover_operator(3, "bob@databricks.com", "alice@databricks.com")

    assert runtime.identities.get("fresh-scenario") is None
    assert runtime._identity_scenario_id is None


@pytest.mark.asyncio
async def test_rejected_stale_handover_does_not_cancel_active_reviews() -> None:
    repository = SimpleNamespace(
        handover_operator=AsyncMock(side_effect=ValueError("stale operator version"))
    )
    runtime = runtime_for_handover(repository)
    active_review = asyncio.create_task(asyncio.sleep(10))
    runtime._run_tasks.add(active_review)

    with pytest.raises(ValueError, match="stale operator version"):
        await runtime.handover_operator(2, "bob@databricks.com", "alice@databricks.com")

    assert not active_review.cancelled()
    assert not active_review.done()
    assert runtime._identity_scenario_id == "old-scenario"
    active_review.cancel()
    await asyncio.gather(active_review, return_exceptions=True)
