"""Checked, dependency-ordered manual deployment for the complete demo.

The project intentionally has no CI/CD service.  This entry point gives a human deployer one
repeatable path that validates a clean source revision, produces current frontend assets,
injects build provenance, reconciles bundle and governance resources, restarts continuous
workloads, and verifies the live platform state.

Remote mutations are disabled unless ``--execute`` is supplied.  No credential or generated
secret is read, printed, or written by this script; Databricks CLI authentication is selected
only through the required explicit profile.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BUILD_INFO = ROOT / ".build-info.json"
MINIMUM_CLI_VERSION = (1, 17, 0)


@dataclass(frozen=True)
class DeploymentInputs:
    """Explicit environment identity required by every remote deployment command."""

    profile: str
    target: str
    expected_host: str
    access_group: str
    catalog: str
    schema: str
    app_name: str
    app_service_principal_client_id: str
    table_timeout_seconds: int

    @property
    def bundle_flags(self) -> list[str]:
        return [
            "--target",
            self.target,
            "--profile",
            self.profile,
            "--var",
            f"app_access_group={self.access_group}",
            "--var",
            f"app_service_principal_client_id={self.app_service_principal_client_id}",
            "--var",
            f"catalog={self.catalog}",
            "--var",
            f"schema={self.schema}",
        ]


@dataclass(frozen=True)
class Step:
    """One ordered remote mutation with an optional environment override."""

    label: str
    command: list[str]
    environment: dict[str, str] | None = None


class Runner:
    """Checked subprocess boundary that never invokes a shell."""

    def run(
        self,
        command: list[str],
        *,
        capture: bool = False,
        environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        print(f"+ {shlex.join(command)}", flush=True)
        env = os.environ.copy()
        if environment:
            env.update(environment)
        return subprocess.run(  # noqa: S603
            command,
            cwd=ROOT,
            check=True,
            text=True,
            capture_output=capture,
            env=env,
        )

    def json(self, command: list[str]) -> dict[str, Any]:
        result = self.run(command, capture=True)
        return json.loads(result.stdout)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="Databricks CLI profile.")
    parser.add_argument("--target", required=True, help="Bundle target.")
    parser.add_argument(
        "--expected-host",
        required=True,
        help="Exact workspace URL expected from the selected profile.",
    )
    parser.add_argument(
        "--access-group",
        required=True,
        help="Existing group receiving curated telemetry and Genie access.",
    )
    parser.add_argument("--catalog", required=True, help="Project Unity Catalog catalog.")
    parser.add_argument("--schema", required=True, help="Project Unity Catalog schema.")
    parser.add_argument("--app-name", required=True, help="Databricks App name.")
    parser.add_argument(
        "--app-service-principal-client-id",
        required=True,
        help="Existing Databricks App service-principal application ID.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply remote mutations after all local and remote preflight checks pass.",
    )
    parser.add_argument(
        "--table-timeout-seconds",
        type=int,
        default=600,
        help="Maximum wait for the pipeline to publish the curated view.",
    )
    return parser.parse_args()


def current_commit(runner: Runner) -> str:
    """Return the exact immutable revision selected for deployment."""

    return runner.run(["git", "rev-parse", "HEAD"], capture=True).stdout.strip()


def assert_clean_head(runner: Runner) -> None:
    """Reject tracked, staged, or untracked source changes before artifact generation."""

    status = runner.run(
        ["git", "status", "--porcelain", "--untracked-files=all"], capture=True
    ).stdout
    unexpected = [
        line
        for line in status.splitlines()
        if not line.endswith(" .build-info.json")
    ]
    if unexpected:
        raise RuntimeError(
            "Deployment requires a clean HEAD; commit or remove these changes:\n"
            + "\n".join(unexpected)
        )


def write_build_info(commit: str, path: Path = BUILD_INFO) -> None:
    """Materialize source provenance consumed by the App's build-info endpoint."""

    payload = {"git_commit": commit}
    path.write_text(json.dumps(payload, sort_keys=True) + "\n")
    if json.loads(path.read_text()) != payload:
        raise RuntimeError(f"Unable to verify generated build metadata at {path}")


def assert_cli_version(runner: Runner) -> None:
    """Enforce the repository's minimum supported Databricks CLI version."""

    output = runner.run(["databricks", "version"], capture=True).stdout.strip()
    numeric = output.rsplit(" ", maxsplit=1)[-1].removeprefix("v").split("-")[0]
    try:
        version = tuple(int(part) for part in numeric.split(".")[:3])
    except ValueError as error:
        raise RuntimeError(f"Cannot parse Databricks CLI version: {output}") from error
    if version < MINIMUM_CLI_VERSION:
        required = ".".join(str(part) for part in MINIMUM_CLI_VERSION)
        raise RuntimeError(f"Databricks CLI {required} or later is required; found {numeric}")


def verify_profile(runner: Runner, inputs: DeploymentInputs) -> dict[str, Any]:
    """Confirm workspace, human group membership, and the existing App workload identity."""

    auth = runner.json(
        ["databricks", "auth", "describe", "--profile", inputs.profile, "-o", "json"]
    )
    actual_host = auth.get("details", {}).get("host", "").rstrip("/")
    if actual_host != inputs.expected_host.rstrip("/"):
        raise RuntimeError(
            f"Profile {inputs.profile!r} resolves to {actual_host!r}, not "
            f"{inputs.expected_host!r}"
        )

    user = runner.json(
        ["databricks", "current-user", "me", "--profile", inputs.profile, "-o", "json"]
    )
    groups = {
        value
        for group in user.get("groups", [])
        for value in (group.get("display"), group.get("value"))
        if value
    }
    if inputs.access_group not in groups:
        raise RuntimeError(
            f"Deploying user is not a member of existing group {inputs.access_group!r}"
        )
    app = runner.json(
        [
            "databricks",
            "apps",
            "get",
            inputs.app_name,
            "--profile",
            inputs.profile,
            "-o",
            "json",
        ]
    )
    actual_principal = app.get("service_principal_client_id")
    if actual_principal != inputs.app_service_principal_client_id:
        raise RuntimeError(
            f"App {inputs.app_name!r} uses service principal {actual_principal!r}, not the "
            "explicit deployment input"
        )
    return user


def assert_requirements_current(runner: Runner) -> None:
    """Ensure the committed App requirements are a deterministic export of uv.lock."""

    exported = runner.run(
        [
            "uv",
            "export",
            "--locked",
            "--no-dev",
            "--no-hashes",
            "--format",
            "requirements-txt",
        ],
        capture=True,
    ).stdout
    committed = (ROOT / "requirements.txt").read_text()
    # uv embeds the exact invocation in the two-line generated header.  Compare the package
    # body so stdout export and the committed ``-o requirements.txt`` export are equivalent.
    if exported.splitlines()[2:] != committed.splitlines()[2:]:
        raise RuntimeError(
            "requirements.txt is stale; regenerate it with the uv export command in the runbook"
        )


def assert_frontend_fresh(root: Path = ROOT) -> None:
    """Prove the ignored production bundle is newer than every frontend source input."""

    frontend = root / "frontend"
    index = frontend / "dist" / "index.html"
    assets = list((frontend / "dist" / "assets").glob("*"))
    if not index.is_file() or not assets:
        raise RuntimeError("frontend/dist is incomplete; run the production frontend build")
    inputs = [
        path
        for path in frontend.rglob("*")
        if path.is_file()
        and "dist" not in path.parts
        and "node_modules" not in path.parts
        and not path.name.endswith(".tsbuildinfo")
    ]
    newest_input = max(path.stat().st_mtime_ns for path in inputs)
    oldest_output = min(path.stat().st_mtime_ns for path in [index, *assets])
    if oldest_output < newest_input:
        raise RuntimeError("frontend/dist is older than frontend source inputs")


def run_quality_gate(runner: Runner, inputs: DeploymentInputs) -> None:
    """Install locked dependencies, run all tests, build assets, and validate the bundle."""

    commands = [
        ["uv", "sync", "--locked"],
        ["npm", "--prefix", "frontend", "ci"],
        ["uv", "run", "ruff", "check", "backend", "tests", "src", "scripts"],
        ["uv", "run", "pytest"],
        ["npm", "--prefix", "frontend", "run", "typecheck"],
        ["npm", "--prefix", "frontend", "run", "build"],
    ]
    for command in commands:
        runner.run(command)
    assert_requirements_current(runner)
    assert_frontend_fresh()
    runner.run(["databricks", "bundle", "validate", *inputs.bundle_flags])
    runner.run(["git", "diff", "--exit-code"])


def deployment_steps(inputs: DeploymentInputs) -> list[Step]:
    """Return remote mutations in their required dependency order."""

    flags = inputs.bundle_flags
    return [
        Step(
            "Deploy the governed data plane in readiness-gated phases",
            [
                "uv",
                "run",
                "python",
                "scripts/deploy_data_plane.py",
                "--profile",
                inputs.profile,
                "--target",
                inputs.target,
                "--access-group",
                inputs.access_group,
                "--app-service-principal-client-id",
                inputs.app_service_principal_client_id,
                "--catalog",
                inputs.catalog,
                "--schema",
                inputs.schema,
                "--timeout-seconds",
                str(inputs.table_timeout_seconds),
                "--deploy",
            ],
        ),
        Step(
            "Deploy the App snapshot after Genie and telemetry are ready",
            ["databricks", "apps", "deploy", *flags, "--auto-approve"],
        ),
        Step(
            "Create or update the Unity Gateway policy function",
            ["databricks", "bundle", "run", *flags, "setup_unity_governance"],
        ),
        Step(
            "Reconcile the Skill, MCP source, service, selectors, and ASK policy",
            ["uv", "run", "python", "scripts/deploy_governance.py"],
            environment={"DATABRICKS_CONFIG_PROFILE": inputs.profile},
        ),
    ]


def ensure_app_user_scopes(
    runner: Runner, inputs: DeploymentInputs, *, timeout_seconds: int = 600
) -> None:
    """Restart App compute only when newly declared OBO scopes are not yet effective."""

    get_command = [
        "databricks",
        "apps",
        "get",
        inputs.app_name,
        "--profile",
        inputs.profile,
        "-o",
        "json",
    ]
    required = {"ai-gateway", "dashboards.genie"}
    app = runner.json(get_command)
    effective = set(app.get("effective_user_api_scopes", []))
    if required <= effective:
        return

    runner.run(
        ["databricks", "apps", "stop", inputs.app_name, "--profile", inputs.profile]
    )
    runner.run(
        ["databricks", "apps", "start", inputs.app_name, "--profile", inputs.profile]
    )
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        app = runner.json(get_command)
        effective = set(app.get("effective_user_api_scopes", []))
        state = (app.get("app_status") or {}).get("state")
        if state == "RUNNING" and required <= effective:
            return
        time.sleep(5)
    raise RuntimeError(
        "App did not become RUNNING with ai-gateway and dashboards.genie OBO scopes"
    )


def verify_live_resources(runner: Runner, inputs: DeploymentInputs, commit: str) -> None:
    """Run read-only platform checks after every dependency has been reconciled."""

    runner.run(
        ["databricks", "apps", "get", inputs.app_name, "--profile", inputs.profile]
    )
    runner.run(["databricks", "bundle", "summary", *inputs.bundle_flags])
    runner.run(
        [
            "databricks",
            "tables",
            "get",
            f"{inputs.catalog}.{inputs.schema}.pump_telemetry_history",
            "--profile",
            inputs.profile,
        ]
    )
    print(
        f"Deployment completed for commit {commit}. Finish the authenticated App and "
        "end-to-end acceptance checklist in docs/DEPLOYMENT.md before recording evidence."
    )


def main() -> None:
    args = parse_args()
    inputs = DeploymentInputs(
        profile=args.profile,
        target=args.target,
        expected_host=args.expected_host,
        access_group=args.access_group,
        catalog=args.catalog,
        schema=args.schema,
        app_name=args.app_name,
        app_service_principal_client_id=args.app_service_principal_client_id,
        table_timeout_seconds=args.table_timeout_seconds,
    )
    runner = Runner()

    assert_clean_head(runner)
    commit = current_commit(runner)
    write_build_info(commit)
    assert_cli_version(runner)
    user = verify_profile(runner, inputs)
    print(
        f"Preflight target: {inputs.expected_host} as "
        f"{user.get('userName', user.get('id', 'unknown user'))}; commit {commit}"
    )
    run_quality_gate(runner, inputs)

    if not args.execute:
        print("Preflight passed. Re-run with --execute to apply the ordered deployment.")
        return

    for step in deployment_steps(inputs):
        print(f"\n==> {step.label}")
        runner.run(step.command, environment=step.environment)
        if step.label.startswith("Deploy the App snapshot"):
            ensure_app_user_scopes(runner, inputs)
    verify_live_resources(runner, inputs, commit)


if __name__ == "__main__":
    main()
