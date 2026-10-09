"""Static contracts for the governed telemetry pipeline and Genie definition."""

from __future__ import annotations

import json
import re
from pathlib import Path

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
    assert "serverless: false" in bundle
    assert "node_type_id: ${var.telemetry_pipeline_node_type}" in bundle
    assert "fleet_telemetry_analyst:" in bundle
    assert "space_id: ${resources.genie_spaces.fleet_telemetry_analyst.id}" in bundle
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
