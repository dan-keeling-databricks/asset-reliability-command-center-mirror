"""Validate and dependency-order deployment of the governed telemetry data plane.

The helper deliberately performs three deployment phases. First it grants the existing App
service principal the minimum privileges required by the workloads and runs the idempotent
Lakebase migration as that schema-owning identity. It then deploys and restarts the continuous
data-plane jobs and waits for a *new* pipeline update to publish the curated view. Only after
the view exists does it grant human read access and deploy the Genie space and App binding.
This ordering prevents both first-deployment and schema-upgrade races.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ACCESS_GROUP = "asset-reliability-command-center-operator"
DEFAULT_TIMEOUT_SECONDS = 1_200
PIPELINE_KEY = "governed_telemetry_history"
PIPELINE_RUNNER_KEY = "governed_telemetry_history_continuous"
COMPUTE_POLICY_KEY = "asset_reliability_workloads"
LAKEBASE_MIGRATION_KEY = "migrate_lakebase_schema"
CURATED_VIEW = "pump_telemetry_history"
FAILED_UPDATE_STATES = {"CANCELED", "FAILED"}
READY_UPDATE_STATES = {"COMPLETED", "RUNNING"}


def run(
    command: list[str], *, capture: bool = False, check: bool = True
) -> subprocess.CompletedProcess[str]:
    """Run one command from the bundle root without invoking a shell."""

    return subprocess.run(  # noqa: S603
        command,
        cwd=REPOSITORY_ROOT,
        check=check,
        text=True,
        capture_output=capture,
    )


def run_json(command: list[str]) -> dict[str, Any]:
    """Run a checked CLI command and decode its JSON object response."""

    result = run(command, capture=True)
    payload = json.loads(result.stdout)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a JSON object from {' '.join(command)}")
    return payload


def bundle_command(action: str, args: argparse.Namespace, *extra: str) -> list[str]:
    """Build a bundle command with an explicit profile, target, and access group."""

    return [
        "databricks",
        "bundle",
        action,
        "--target",
        args.target,
        "--profile",
        args.profile,
        "--var",
        f"app_access_group={args.access_group}",
        "--var",
        f"app_service_principal_client_id={args.app_service_principal_client_id}",
        "--var",
        f"catalog={args.catalog}",
        "--var",
        f"schema={args.schema}",
        *extra,
    ]


def workspace_command(args: argparse.Namespace, *parts: str) -> list[str]:
    """Build a non-bundle Databricks CLI command with an explicit profile."""

    return ["databricks", *parts, "--profile", args.profile]


def bundle_summary(args: argparse.Namespace) -> dict[str, Any]:
    """Return remote-aware bundle output used to resolve generated resource IDs."""

    return run_json(bundle_command("summary", args, "--force-pull", "--output", "json"))


def resource_id(args: argparse.Namespace, kind: str, key: str) -> str:
    """Resolve a deployed bundle resource ID and fail clearly if deployment was incomplete."""

    resource = bundle_summary(args).get("resources", {}).get(kind, {}).get(key, {})
    identifier = resource.get("id")
    if not identifier:
        raise RuntimeError(f"Bundle resource {kind}.{key} has no deployed ID")
    return str(identifier)


def bundle_source_directory(args: argparse.Namespace) -> tuple[str, str]:
    """Resolve the synced bundle directory and its workspace object ID.

    Bundle resource definitions point jobs and pipelines at notebooks below
    ``workspace.file_path``.  A workload running as the App service principal therefore
    needs explicit read access to that directory; access inherited by the human deployer is
    not sufficient.  Resolving the path from bundle metadata keeps this helper portable
    across users, targets, and workspaces.
    """

    summary = bundle_summary(args)
    source_path = summary.get("workspace", {}).get("file_path")
    if not source_path:
        raise RuntimeError("Bundle summary does not expose workspace.file_path")
    status = run_json(
        workspace_command(
            args,
            "workspace",
            "get-status",
            str(source_path),
            "--output",
            "json",
        )
    )
    object_id = status.get("object_id")
    if not object_id:
        raise RuntimeError(f"Workspace source directory {source_path} has no object ID")
    return str(source_path), str(object_id)


def grant_bundle_source_read(args: argparse.Namespace) -> None:
    """Idempotently grant the workload identity read access to synced bundle sources."""

    source_path, object_id = bundle_source_directory(args)
    access_control = {
        "access_control_list": [
            {
                "service_principal_name": args.app_service_principal_client_id,
                "permission_level": "CAN_READ",
            }
        ]
    }
    run_json(
        workspace_command(
            args,
            "workspace",
            "update-permissions",
            "directories",
            object_id,
            "--json",
            json.dumps(access_control, sort_keys=True),
            "--output",
            "json",
        )
    )

    permissions = run_json(
        workspace_command(
            args,
            "workspace",
            "get-permissions",
            "directories",
            object_id,
            "--output",
            "json",
        )
    )
    matching = [
        entry
        for entry in permissions.get("access_control_list", [])
        if entry.get("service_principal_name") == args.app_service_principal_client_id
    ]
    can_read = any(
        permission.get("permission_level") in {"CAN_READ", "CAN_RUN", "CAN_EDIT", "CAN_MANAGE"}
        for entry in matching
        for permission in entry.get("all_permissions", [])
    )
    if not can_read:
        raise RuntimeError("App service principal does not have read access to bundle sources")

    print(
        json.dumps(
            {
                "bundle_source_path": source_path,
                "principal_suffix": args.app_service_principal_client_id[-6:],
                "permission": "CAN_READ",
                "verified": True,
            }
        )
    )


def current_pipeline_update_ids(args: argparse.Namespace) -> set[str]:
    """Return update IDs already known before this deployment, or an empty set initially."""

    try:
        pipeline_id = resource_id(args, "pipelines", PIPELINE_KEY)
        pipeline = run_json(
            workspace_command(args, "pipelines", "get", pipeline_id, "--output", "json")
        )
    except (RuntimeError, subprocess.CalledProcessError, json.JSONDecodeError):
        return set()
    return {
        str(update["update_id"])
        for update in pipeline.get("latest_updates", [])
        if update.get("update_id")
    }


def view_exists(args: argparse.Namespace) -> bool:
    """Check for the curated UC view without treating absence during startup as an error."""

    view_name = f"{args.catalog}.{args.schema}.{CURATED_VIEW}"
    result = run(
        workspace_command(args, "tables", "get", view_name, "--output", "json"),
        capture=True,
        check=False,
    )
    return result.returncode == 0


def verify_compute_policy_access(args: argparse.Namespace) -> None:
    """Verify the App SP has only the required CAN_USE binding on the managed policy."""

    policy_id = resource_id(args, "cluster_policies", COMPUTE_POLICY_KEY)
    permissions = run_json(
        workspace_command(
            args,
            "permissions",
            "get",
            "cluster-policies",
            policy_id,
            "--output",
            "json",
        )
    )
    matching = [
        entry
        for entry in permissions.get("access_control_list", [])
        if entry.get("service_principal_name") == args.app_service_principal_client_id
    ]
    can_use = any(
        permission.get("permission_level") == "CAN_USE"
        for entry in matching
        for permission in entry.get("all_permissions", [])
    )
    if not can_use:
        raise RuntimeError("App service principal does not have CAN_USE on compute policy")

    # The application ID is an identifier rather than a secret, but truncation keeps routine
    # deployment output free of complete identity values.
    print(
        json.dumps(
            {
                "compute_policy": policy_id,
                "principal_suffix": args.app_service_principal_client_id[-6:],
                "permission": "CAN_USE",
                "verified": True,
            }
        )
    )


def wait_for_pipeline_publication(
    args: argparse.Namespace, previous_update_ids: set[str]
) -> None:
    """Wait for a new healthy update and the curated view, failing fast on update failure."""

    pipeline_id = resource_id(args, "pipelines", PIPELINE_KEY)
    deadline = time.monotonic() + args.timeout_seconds
    last_state = "not started"

    while time.monotonic() < deadline:
        pipeline = run_json(
            workspace_command(args, "pipelines", "get", pipeline_id, "--output", "json")
        )
        new_updates = [
            update
            for update in pipeline.get("latest_updates", [])
            if update.get("update_id") not in previous_update_ids
        ]
        if new_updates:
            newest = new_updates[0]
            last_state = str(newest.get("state", "UNKNOWN"))
            if last_state in FAILED_UPDATE_STATES:
                raise RuntimeError(
                    f"Pipeline update {newest.get('update_id')} ended in {last_state}"
                )
            if last_state in READY_UPDATE_STATES and view_exists(args):
                return
        else:
            last_state = str(pipeline.get("state", "waiting for a new update"))
        time.sleep(args.poll_seconds)

    raise TimeoutError(
        f"Timed out after {args.timeout_seconds}s waiting for a new pipeline update and "
        f"{args.catalog}.{args.schema}.{CURATED_VIEW}; last state was {last_state}"
    )


def selective_deploy(args: argparse.Namespace, *resources: str) -> None:
    """Deploy an explicit resource phase and its referenced dependencies."""

    run(
        bundle_command(
            "deploy",
            args,
            "--auto-approve",
            "--select",
            ",".join(resources),
        )
    )


def restart_continuous_job(args: argparse.Namespace, resource_key: str) -> None:
    """Restart a continuous job so its next run uses the deployed source and identity."""

    run(bundle_command("run", args, resource_key, "--restart", "--no-wait"))


def deploy(args: argparse.Namespace) -> None:
    """Execute the ordered, idempotent data-plane deployment protocol."""

    # Phase 1 runs as the deployer because this setup job intentionally has no run_as block.
    # Continuous workloads retain their previous identity until all prerequisite grants exist.
    selective_deploy(
        args,
        "cluster_policies.asset_reliability_workloads",
        "jobs.setup_runtime_principal_access",
        f"jobs.{LAKEBASE_MIGRATION_KEY}",
    )
    run(bundle_command("run", args, "setup_runtime_principal_access"))
    verify_compute_policy_access(args)
    grant_bundle_source_read(args)
    # This checked run is the hard schema barrier. The producer and RTM restart calls below
    # are unreachable if App-SP authentication, migration DDL, or contract verification fails.
    run(bundle_command("run", args, LAKEBASE_MIGRATION_KEY))

    previous_update_ids = current_pipeline_update_ids(args)

    # Phase 2 can now switch the continuous workloads to the App SP. The pipeline runner is a
    # continuous Lakeflow Job because modern bundles drop the deprecated pipeline-level flag.
    selective_deploy(
        args,
        "pipelines.governed_telemetry_history",
        "jobs.governed_telemetry_history_continuous",
        "jobs.setup_telemetry_access",
        "jobs.telemetry_producer",
        "jobs.telemetry_rtm",
    )
    grant_bundle_source_read(args)
    restart_continuous_job(args, "telemetry_producer")
    restart_continuous_job(args, "telemetry_rtm")
    restart_continuous_job(args, PIPELINE_RUNNER_KEY)
    wait_for_pipeline_publication(args, previous_update_ids)

    # Human OBO access is applied only after publication. The final full deployment can then
    # safely create/update Genie and bind it to the App without a missing data source.
    run(bundle_command("run", args, "setup_telemetry_access"))
    run(bundle_command("deploy", args, "--auto-approve"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="Explicit Databricks CLI profile.")
    parser.add_argument("--target", default="dev", help="Bundle target (default: dev).")
    parser.add_argument(
        "--access-group",
        default=DEFAULT_ACCESS_GROUP,
        help="Existing workspace group; this helper never creates groups.",
    )
    parser.add_argument(
        "--app-service-principal-client-id",
        default="bd5178cb-8774-4d77-bc63-b4f491716d0b",
        help="Existing Databricks App service-principal application ID.",
    )
    parser.add_argument(
        "--catalog",
        default="classic_stable_realtime_demos_catalog",
        help="Catalog containing the governed telemetry view.",
    )
    parser.add_argument(
        "--schema",
        default="asset_reliability_command_center",
        help="Schema containing the governed telemetry view.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Maximum wait for a new pipeline update and curated view.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=10.0,
        help="Pipeline publication polling interval.",
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Execute the full ordered deployment; without this flag only validation runs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(bundle_command("validate", args))
    if args.deploy:
        deploy(args)


if __name__ == "__main__":
    main()
