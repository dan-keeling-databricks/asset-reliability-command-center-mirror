"""Static and unit contracts for the checked manual deployment entry point."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from scripts.deploy import (
    DeploymentInputs,
    assert_frontend_fresh,
    deployment_steps,
    write_build_info,
)


def inputs() -> DeploymentInputs:
    return DeploymentInputs(
        profile="explicit-profile",
        target="dev",
        expected_host="https://workspace.example",
        access_group="existing-group",
        catalog="demo_catalog",
        schema="asset_reliability",
        app_name="asset-reliability-center",
        app_service_principal_client_id="existing-app-sp",
        table_timeout_seconds=900,
    )


def test_deployment_steps_are_explicit_and_dependency_ordered() -> None:
    steps = deployment_steps(inputs())
    labels = [step.label for step in steps]

    assert labels == [
        "Deploy the governed data plane in readiness-gated phases",
        "Deploy the App snapshot after Genie and telemetry are ready",
        "Create or update the Unity Gateway policy function",
        "Reconcile the Skill, MCP source, service, selectors, and ASK policy",
    ]
    for step in steps:
        if step.command[0] == "databricks":
            assert "--profile" in step.command
            assert "explicit-profile" in step.command
    assert steps[3].environment == {"DATABRICKS_CONFIG_PROFILE": "explicit-profile"}
    assert "--app-service-principal-client-id" in steps[0].command
    assert "existing-app-sp" in steps[0].command
    assert "--timeout-seconds" in steps[0].command
    assert "900" in steps[0].command


def test_build_info_contains_only_the_selected_commit(tmp_path: Path) -> None:
    output = tmp_path / ".build-info.json"
    write_build_info("abc123", output)

    assert json.loads(output.read_text()) == {"git_commit": "abc123"}
    assert output.read_text().endswith("\n")


def test_frontend_freshness_rejects_source_newer_than_build(tmp_path: Path) -> None:
    frontend = tmp_path / "frontend"
    source = frontend / "src" / "App.tsx"
    index = frontend / "dist" / "index.html"
    asset = frontend / "dist" / "assets" / "index.js"
    for path in (source, index, asset):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(path.name)

    assert_frontend_fresh(tmp_path)
    time.sleep(0.001)
    source.write_text("new source")
    with pytest.raises(RuntimeError, match="older than frontend source"):
        assert_frontend_fresh(tmp_path)
