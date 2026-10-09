"""API contract tests for identity, operator control, scenarios, SSE, and MCP routing."""

import time
from contextlib import contextmanager

from fastapi.testclient import TestClient

import backend.main as main_module
from backend.main import create_app
from backend.repository import LocalCommandCenterRepository
from backend.runtime import LocalCommandCenterRuntime, initial_snapshot
from backend.settings import Settings


@contextmanager
def local_client():
    settings = Settings(
        app_adapter="local",
        telemetry_interval_seconds=0.05,
        alert_evaluator_interval_seconds=0.02,
        local_event_step_seconds=0.01,
    )
    repository = LocalCommandCenterRepository(initial_snapshot())
    runtime = LocalCommandCenterRuntime(repository, settings)
    with TestClient(create_app(settings, repository, runtime)) as client:
        yield client


def wait_for(client: TestClient, predicate, timeout: float = 2.0):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = client.get("/api/dashboard").json()
        if predicate(latest):
            return latest
        time.sleep(0.01)
    raise AssertionError(f"Dashboard did not reach expected state: {latest}")


def test_dashboard_exposes_eight_live_pumps_and_platform_status() -> None:
    with local_client() as client:
        assert client.get("/api/health").json() == {
            "status": "ok",
            "adapter": "local",
        }
        dashboard = client.get("/api/dashboard").json()
        assert len(dashboard["pumps"]) == 8
        assert {pump["asset_id"] for pump in dashboard["pumps"]} == {
            "P-201",
            "P-202",
            "P-203",
            "P-204",
            "P-205",
            "P-206",
            "P-207",
            "P-208",
        }
        assert all(pump["warning_threshold_mm_s"] == 4.4 for pump in dashboard["pumps"])
        assert all("flow_m3_h" in pump and "current_a" in pump for pump in dashboard["pumps"])
        assert dashboard["platform"]["connection"] == "connected"
        assert dashboard["platform"]["latency_ms"] > 0
        assert dashboard["platform"]["kafka_to_lakebase_latency_ms"] > 0
        assert dashboard["platform"]["lakebase_to_ui_latency_ms"] > 0
        assert dashboard["platform"]["end_to_end_latency_ms"] == (
            dashboard["platform"]["kafka_to_lakebase_latency_ms"]
            + dashboard["platform"]["lakebase_to_ui_latency_ms"]
        )


def test_trigger_runs_generic_activity_stream_and_waits_for_approval() -> None:
    with local_client() as client:
        response = client.post("/api/scenario/trigger-p204")
        assert response.status_code == 202
        assert response.json()["incident_id"] is None
        assert response.json()["command_id"].startswith("command_")
        immediate = client.get("/api/dashboard").json()
        assert not any(alert["asset_id"] == "P-204" for alert in immediate["alerts"])
        dashboard = wait_for(
            client,
            lambda state: any(
                alert["asset_id"] == "P-204" and alert["status"] == "action_required"
                for alert in state["alerts"]
            ),
        )
        incident_id = next(
            alert["incident_id"] for alert in dashboard["alerts"] if alert["asset_id"] == "P-204"
        )
        p204 = next(pump for pump in dashboard["pumps"] if pump["asset_id"] == "P-204")
        assert p204["state"] == "warning"
        assert any(run["incident_id"] == incident_id for run in dashboard["active_runs"])

        incident = client.get(f"/api/incidents/{incident_id}").json()
        assert incident["trigger_prompt"].startswith("Autonomous alert for P-204")
        assert incident["status"] == "waiting"
        assert incident["totals"]["tool_calls"] == 3
        assert incident["totals"]["input_tokens"] > 0
        waiting = next(
            activity for activity in incident["activities"] if activity["status"] == "waiting"
        )
        assert waiting["action"]["endpoint"].endswith("/approve")
        assert incident["conversation"] == []
        tool_titles = {
            activity["title"]
            for activity in incident["activities"]
            if activity["category"] == "tool_call"
        }
        assert tool_titles == {
            "get_asset_telemetry",
            "prepare_work_order",
            "submit_work_order",
        }
        model_activities = [
            activity for activity in incident["activities"] if activity["category"] == "model_call"
        ]
        assert {activity["title"] for activity in model_activities} == {"Model execution"}
        assert all(
            any(detail["label"] == "Model output" for detail in activity["details"])
            for activity in model_activities
        )
        assert all(
            "Generated terminal response" not in activity["summary"]
            for activity in model_activities
        )

        repeated = client.post("/api/scenario/trigger-p204")
        assert repeated.status_code == 409


def test_approval_and_follow_up_complete_incident() -> None:
    with local_client() as client:
        response = client.post("/api/scenario/trigger-p204").json()
        assert response["incident_id"] is None
        dashboard = wait_for(
            client,
            lambda state: any(
                alert["asset_id"] == "P-204" and alert["status"] == "action_required"
                for alert in state["alerts"]
            ),
        )
        incident_id = next(
            alert["incident_id"] for alert in dashboard["alerts"] if alert["asset_id"] == "P-204"
        )
        approved = client.post(f"/api/incidents/{incident_id}/approve")
        assert approved.status_code == 202
        incident = client.get(f"/api/incidents/{incident_id}").json()
        assert incident["status"] == "completed"
        submit = next(
            activity
            for activity in incident["activities"]
            if activity["title"] == "submit_work_order" and activity["category"] == "tool_call"
        )
        assert submit["status_label"] == "APPROVED · ALLOW"
        assert any(activity["category"] == "tool_result" for activity in incident["activities"])
        conclusions = [
            activity["summary"]
            for activity in incident["activities"]
            if activity["category"] == "model_call"
            and activity["summary"].startswith("- **Finding:**")
        ]
        assert len(conclusions) == 1
        assert conclusions[0].splitlines() == [
            "- **Finding:** Persistent drive-end bearing vibration excursion.",
            "- **Evidence:** Vibration reached 5.2 mm/s and continued rising.",
            "- **Risk:** 87% likelihood of failure before the next maintenance window.",
            "- **Action:** Work order WO-10482 scheduled for the next available outage.",
        ]
        terminal = conclusions[0]
        assert sum(activity["summary"] == terminal for activity in incident["activities"]) == 1
        assert all(
            detail["value"] != terminal
            for activity in incident["activities"]
            for detail in activity["details"]
        )

        tokens_before_follow_up = (
            incident["totals"]["input_tokens"] + incident["totals"]["output_tokens"]
        )
        follow_up = client.post(
            f"/api/incidents/{incident_id}/follow-up",
            json={"message": "What was scheduled?"},
        )
        assert follow_up.status_code == 202
        incident = client.get(f"/api/incidents/{incident_id}").json()
        assert [turn["role"] for turn in incident["conversation"]] == ["operator"]
        follow_up_activity = next(
            activity
            for activity in reversed(incident["activities"])
            if activity["category"] == "model_call"
        )
        assert follow_up_activity["title"] == "Model execution"
        assert "WO-10482" in follow_up_activity["summary"]
        assert (
            incident["totals"]["input_tokens"] + incident["totals"]["output_tokens"]
            > tokens_before_follow_up
        )


def test_restart_is_idempotent_and_clears_scenario_history() -> None:
    with local_client() as client:
        first = client.get("/api/dashboard").json()["scenario"]["id"]
        client.post("/api/scenario/trigger-p204")
        restarted = client.post("/api/scenario/restart")
        assert restarted.status_code == 202
        second = restarted.json()["scenario_id"]
        assert first != second
        state = client.get("/api/dashboard").json()
        assert state["scenario"]["p204_triggered"] is False
        assert state["alerts"] == []
        assert state["invocations"] == []
        assert all(pump["state"] == "normal" for pump in state["pumps"])


def test_local_fleet_has_stable_widely_spaced_normal_baselines() -> None:
    snapshot = initial_snapshot()
    vibrations = [pump.vibration_mm_s for pump in snapshot.pumps]

    assert vibrations == sorted(vibrations)
    assert min(vibrations) == 1.2
    assert max(vibrations) == 2.95
    assert all(value < 4.4 for value in vibrations)


def test_active_threshold_review_is_not_preclassified_as_transient() -> None:
    with local_client() as client:
        dashboard = wait_for(client, lambda state: bool(state["active_runs"]))
        active_assets = {run["asset_id"] for run in dashboard["active_runs"]}
        active_pumps = [pump for pump in dashboard["pumps"] if pump["asset_id"] in active_assets]

        assert active_pumps
        assert all(pump["state_label"] == "Agent reviewing" for pump in active_pumps)


def test_shared_operator_handover_resets_scenario_and_enforces_version() -> None:
    settings = Settings(
        app_adapter="local",
        telemetry_interval_seconds=0.05,
        alert_evaluator_interval_seconds=0.02,
        local_event_step_seconds=0.01,
    )
    repository = LocalCommandCenterRepository(initial_snapshot())
    runtime = LocalCommandCenterRuntime(repository, settings)
    alice = {
        "X-Forwarded-Email": "alice@databricks.com",
        "X-Forwarded-Preferred-Username": "Alice",
    }
    bob = {
        "X-Forwarded-Email": "bob@databricks.com",
        "X-Forwarded-Preferred-Username": "Bob",
    }

    with TestClient(create_app(settings, repository, runtime)) as client:
        first = client.get("/api/dashboard", headers=alice).json()
        original_scenario = first["scenario"]["id"]
        assert first["viewer"]["is_operator"] is True
        assert first["shift_operator"]["operator"]["email"] == "alice@databricks.com"

        bob_view = client.get("/api/dashboard", headers=bob).json()
        assert bob_view["viewer"]["is_operator"] is False
        assert client.post("/api/scenario/trigger-p204", headers=bob).status_code == 403

        handed_over = client.post(
            "/api/operator/handover",
            headers=bob,
            json={
                "operator_email": "bob@databricks.com",
                "expected_version": bob_view["shift_operator"]["version"],
                "confirmed": True,
            },
        )
        assert handed_over.status_code == 202
        assert handed_over.json()["scenario_id"] != original_scenario

        after = client.get("/api/dashboard", headers=bob).json()
        assert after["viewer"]["is_operator"] is True
        assert after["shift_operator"]["operator"]["name"] == "Bob"
        assert after["revision"] > bob_view["revision"]
        assert after["alerts"] == []
        assert repository._operator_audit[-1]["previous_operator"] == "alice@databricks.com"
        assert repository._operator_audit[-1]["initiated_by"] == "bob@databricks.com"

        stale = client.post(
            "/api/operator/handover",
            headers=alice,
            json={
                "operator_email": "alice@databricks.com",
                "expected_version": bob_view["shift_operator"]["version"],
                "confirmed": True,
            },
        )
        assert stale.status_code == 409


def test_operator_handover_requires_explicit_confirmation() -> None:
    with local_client() as client:
        dashboard = client.get("/api/dashboard").json()
        response = client.post(
            "/api/operator/handover",
            json={
                "operator_email": dashboard["shift_operator"]["operator"]["email"],
                "expected_version": dashboard["shift_operator"]["version"],
                "confirmed": False,
            },
        )
        assert response.status_code == 400


def test_mcp_source_rejects_human_identity(monkeypatch) -> None:
    monkeypatch.setenv("MCP_SOURCE_PRINCIPAL", "app-service-principal")
    with local_client() as client:
        response = client.post(
            "/mcp",
            headers={"X-Forwarded-Email": "operator@databricks.com"},
            json={"jsonrpc": "2.0", "id": "1", "method": "tools/list"},
        )

    assert response.status_code == 403


def test_mcp_source_accepts_configured_app_service_principal(monkeypatch) -> None:
    monkeypatch.setenv("MCP_SOURCE_PRINCIPAL", "app-service-principal")
    with local_client() as client:
        response = client.post(
            "/mcp",
            headers={"X-Forwarded-Service-Principal": "app-service-principal"},
            json={"jsonrpc": "2.0", "id": "1", "method": "tools/list"},
        )

    assert response.status_code == 200
    assert {tool["name"] for tool in response.json()["result"]["tools"]} == {
        "get_asset_telemetry",
        "assess_failure_risk",
        "prepare_work_order",
        "submit_work_order",
    }


def test_build_info_is_text_readable(monkeypatch) -> None:
    monkeypatch.setenv("APP_BUILD_COMMIT", "abc123")
    with local_client() as client:
        assert client.get("/api/build-info").json() == {"git_commit": "abc123"}


def test_build_info_uses_deployment_artifact_when_env_is_absent(monkeypatch, tmp_path) -> None:
    build_info = tmp_path / ".build-info.json"
    build_info.write_text('{"git_commit":"def456"}', encoding="utf-8")
    monkeypatch.delenv("APP_BUILD_COMMIT", raising=False)
    monkeypatch.setattr(main_module, "BUILD_INFO_PATH", build_info)

    with local_client() as client:
        assert client.get("/api/build-info").json() == {"git_commit": "def456"}
