import time
from contextlib import contextmanager

from fastapi.testclient import TestClient

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
        assert incident["activities"][-1]["status"] == "waiting"
        assert incident["activities"][-1]["action"]["endpoint"].endswith("/approve")
        assert incident["conversation"][-1]["content"].startswith("**Finding:**")

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
        assert incident["activities"][-2]["status_label"] == "APPROVED · ALLOW"
        assert incident["activities"][-1]["category"] == "tool_result"

        follow_up = client.post(
            f"/api/incidents/{incident_id}/follow-up",
            json={"message": "What was scheduled?"},
        )
        assert follow_up.status_code == 202
        incident = client.get(f"/api/incidents/{incident_id}").json()
        assert [turn["role"] for turn in incident["conversation"][-2:]] == [
            "operator",
            "agent",
        ]
        assert "WO-10482" in incident["conversation"][-1]["content"]


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
