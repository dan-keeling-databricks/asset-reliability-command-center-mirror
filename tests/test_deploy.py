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
    stage_app_source,
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
    assert steps[3].command == [
        "uv",
        "run",
        "python",
        "-m",
        "scripts.deploy_governance",
    ]
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


def _write_minimal_runtime(root: Path) -> None:
    """Create only the files that a deployed FastAPI/React App reads at runtime."""

    files = {
        "backend/main.py": "app = object()\n",
        "backend/__pycache__/main.pyc": "cache",
        "backend/.env": "SECRET=must-not-be-staged\n",
        "frontend/dist/index.html": "<main>command center</main>\n",
        "frontend/dist/assets/index.js": "export {};\n",
        "sql/lakebase_schema.sql": "SELECT 1;\n",
        "app.yaml": "command: [uvicorn, backend.main:app]\n",
        "pyproject.toml": "[project]\nname='test'\nversion='0.0.0'\n",
        "uv.lock": "version = 1\n",
        "requirements.txt": "fastapi==1.0.0\n",
        ".build-info.json": '{"git_commit":"abc123"}\n',
        "genie/fleet_telemetry_analyst.geniespace.json": "{}\n",
        "src/streaming/job.py": "raise SystemExit\n",
        "tests/test_example.py": "def test_example(): pass\n",
    }
    for relative, contents in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)


def test_app_staging_contains_runtime_allowlist_and_excludes_bundle_resources(
    tmp_path: Path,
) -> None:
    _write_minimal_runtime(tmp_path)

    staged = stage_app_source(tmp_path)

    assert (staged / "backend/main.py").is_file()
    assert (staged / "frontend/dist/index.html").is_file()
    assert (staged / "sql/lakebase_schema.sql").is_file()
    assert json.loads((staged / ".build-info.json").read_text()) == {
        "git_commit": "abc123"
    }
    assert not (staged / "backend/__pycache__").exists()
    assert not (staged / "backend/.env").exists()
    assert not (staged / "genie").exists()
    assert not (staged / "src").exists()
    assert not (staged / "tests").exists()
    assert not list(staged.rglob("*.geniespace.json"))


def test_app_staging_removes_stale_files_before_rebuild(tmp_path: Path) -> None:
    _write_minimal_runtime(tmp_path)
    staged = stage_app_source(tmp_path)
    stale = staged / "stale-secret.env"
    stale.write_text("must not survive\n")

    stage_app_source(tmp_path)

    assert not stale.exists()


def test_bundle_uses_dedicated_app_snapshot_without_moving_genie_definition() -> None:
    bundle = (Path(__file__).resolve().parents[1] / "databricks.yml").read_text()

    assert "source_code_path: ./.artifacts/app" in bundle
    assert "file_path: ./genie/fleet_telemetry_analyst.geniespace.json" in bundle
    assert "- .artifacts/app/**" in bundle
