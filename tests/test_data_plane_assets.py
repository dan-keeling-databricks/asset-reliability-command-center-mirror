"""Static contracts for the governed telemetry pipeline and Genie definition."""

from __future__ import annotations

import json
import re
from argparse import Namespace
from pathlib import Path

from scripts import deploy_data_plane

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "src" / "pipelines" / "governed_telemetry_history.sql"
GENIE = ROOT / "genie" / "fleet_telemetry_analyst.geniespace.json"


def test_pipeline_is_documented_sql_with_required_datasets() -> None:
    source = PIPELINE.read_text()

    assert source.startswith("-- Databricks notebook source")
    assert "STREAM read_kafka(" in source
    assert "CREATE OR REFRESH STREAMING TABLE telemetry_bronze" in source
    assert "CREATE OR REFRESH STREAMING TABLE telemetry_silver" in source
    assert "CREATE OR REFRESH STREAMING TABLE telemetry_quarantine" in source
    assert "CREATE VIEW pump_telemetry_history" in source
    assert source.count("CONSTRAINT ") >= 9

    # Every executable notebook cell must be introduced by a Markdown cell. This keeps the
    # pipeline readable as text in the repository and after workspace deployment.
    cells = source.split("-- COMMAND ----------")
    executable_cells = [cell for cell in cells if re.search(r"\bCREATE\b", cell)]
    for cell in executable_cells:
        cell_index = cells.index(cell)
        assert cell_index > 0
        assert "-- MAGIC %md" in cells[cell_index - 1]


def test_curated_view_excludes_control_and_broker_fields() -> None:
    source = PIPELINE.read_text()
    view_sql = source.split("CREATE VIEW pump_telemetry_history", maxsplit=1)[1]

    for excluded in (
        "scenario_id",
        "sequence",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    ):
        assert excluded not in view_sql

    for included in (
        "event_time",
        "pump_id",
        "flow_m3h",
        "vibration_mm_s",
        "bearing_temperature_c",
        "motor_current_a",
    ):
        assert included in view_sql


def test_genie_is_limited_to_the_curated_telemetry_view() -> None:
    definition = json.loads(GENIE.read_text())
    tables = definition["data_sources"]["tables"]

    assert len(tables) == 1
    assert "${" not in tables[0]["identifier"]
    assert tables[0]["identifier"].endswith(".pump_telemetry_history")
    assert [item["column_name"] for item in tables[0]["column_configs"]] == sorted(
        item["column_name"] for item in tables[0]["column_configs"]
    )

    serialized = json.dumps(definition).lower()
    for excluded_concept in ("scenario", "restart", "incident", "work order"):
        assert excluded_concept not in serialized


def test_genie_id_keyed_arrays_are_sorted_and_well_formed() -> None:
    definition = json.loads(GENIE.read_text())
    collections = (
        definition["config"]["sample_questions"],
        definition["instructions"]["example_question_sqls"],
        definition["instructions"]["text_instructions"],
    )

    all_ids: list[str] = []
    for collection in collections:
        ids = [item["id"] for item in collection]
        assert ids == sorted(ids)
        assert all(re.fullmatch(r"[0-9a-f]{32}", item_id) for item_id in ids)
        all_ids.extend(ids)
    assert len(all_ids) == len(set(all_ids))


def test_bundle_uses_classic_pipeline_native_genie_and_obo_scope() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    app = (ROOT / "app.yaml").read_text()

    assert "governed_telemetry_history:" in bundle
    assert 'databricks_cli_version: ">=1.17.0"' in bundle
    assert "serverless: false" in bundle
    assert "continuous: true" not in bundle
    assert "governed_telemetry_history_continuous:" in bundle
    assert "pipeline_id: ${resources.pipelines.governed_telemetry_history.id}" in bundle
    assert "pause_status: UNPAUSED" in bundle
    assert "level: CAN_RUN" in bundle
    assert "node_type_id: ${var.telemetry_pipeline_node_type}" in bundle
    assert "fleet_telemetry_analyst:" in bundle
    assert "default: /Shared" in bundle
    assert "space_id: ${resources.genie_spaces.fleet_telemetry_analyst.id}" in bundle
    assert bundle.count("service_principal_name: ${var.app_service_principal_client_id}") == 6
    assert "asset_reliability_workloads:" in bundle
    assert '"minValue": 0' in bundle
    assert '"maxValue": 1' in bundle
    assert '"value": "i3.xlarge"' in bundle
    assert '"value": "ON_DEMAND"' in bundle
    assert bundle.count(
        "policy_id: ${resources.cluster_policies.asset_reliability_workloads.id}"
    ) == 3
    assert "- dashboards.genie" in bundle
    assert "DATABRICKS_GENIE_SPACE_ID" in app
    assert "GENIE_SOURCE_LABEL" in app
    assert "valueFrom: fleet-telemetry-genie" in app
    assert "dan.keeling@" not in app


def test_access_grant_exposes_only_the_curated_view() -> None:
    grants = (ROOT / "sql" / "grant_telemetry_history.sql").read_text()

    assert "GRANT USE CATALOG" in grants
    assert "GRANT USE SCHEMA" in grants
    assert "GRANT SELECT" in grants
    assert "pump_telemetry_history" in grants
    assert "telemetry_bronze" not in grants
    assert "telemetry_silver" not in grants
    assert "telemetry_quarantine" not in grants


def test_runtime_principal_grant_is_minimal_and_idempotent() -> None:
    grants = (ROOT / "sql" / "grant_runtime_principal.sql").read_text()

    assert "GRANT USE CATALOG" in grants
    assert "GRANT USE SCHEMA, CREATE TABLE" in grants
    assert "GRANT READ VOLUME, WRITE VOLUME" in grants
    assert grants.count("SHOW GRANTS") == 3
    assert "CREATE USER" not in grants
    assert "CREATE GROUP" not in grants
    assert "ALL PRIVILEGES" not in grants


def test_deployment_helper_orders_identity_pipeline_grant_and_genie(monkeypatch) -> None:
    events: list[tuple[object, ...]] = []
    args = Namespace(
        profile="explicit-profile",
        target="dev",
        access_group="existing-group",
        app_service_principal_client_id="existing-app-sp",
        catalog="catalog",
        schema="schema",
        timeout_seconds=60,
        poll_seconds=0,
    )

    monkeypatch.setattr(
        deploy_data_plane,
        "selective_deploy",
        lambda _args, *resources: events.append(("select", *resources)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "run",
        lambda command, **_kwargs: events.append(("run", *command)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "current_pipeline_update_ids",
        lambda _args: events.append(("capture_updates",)) or {"old-update"},
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "verify_compute_policy_access",
        lambda _args: events.append(("verify_policy",)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "restart_continuous_job",
        lambda _args, key: events.append(("restart", key)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "wait_for_pipeline_publication",
        lambda _args, updates: events.append(("wait", *sorted(updates))),
    )

    deploy_data_plane.deploy(args)

    labels = [event[0] for event in events]
    assert labels == [
        "select",
        "run",
        "verify_policy",
        "capture_updates",
        "select",
        "restart",
        "restart",
        "restart",
        "wait",
        "run",
        "run",
    ]
    assert events[0] == (
        "select",
        "cluster_policies.asset_reliability_workloads",
        "jobs.setup_runtime_principal_access",
    )
    assert events[5:8] == [
        ("restart", "telemetry_producer"),
        ("restart", "telemetry_rtm"),
        ("restart", "governed_telemetry_history_continuous"),
    ]
    assert "setup_telemetry_access" in events[9]
    assert events[-1][-1] == "--auto-approve"


def test_publication_wait_requires_new_healthy_update_and_view(monkeypatch) -> None:
    args = Namespace(
        profile="explicit-profile",
        catalog="catalog",
        schema="schema",
        timeout_seconds=1,
        poll_seconds=0,
    )
    monkeypatch.setattr(deploy_data_plane, "resource_id", lambda *_args: "pipeline-id")
    monkeypatch.setattr(
        deploy_data_plane,
        "run_json",
        lambda _command: {
            "latest_updates": [{"update_id": "new-update", "state": "RUNNING"}]
        },
    )
    monkeypatch.setattr(deploy_data_plane, "view_exists", lambda _args: True)

    deploy_data_plane.wait_for_pipeline_publication(args, {"old-update"})
