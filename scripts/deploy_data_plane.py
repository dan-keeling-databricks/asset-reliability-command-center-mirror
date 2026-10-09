"""Validate and idempotently deploy the governed telemetry data plane.

The Databricks bundle owns both the Lakeflow pipeline and Genie space, so repeated bundle
deployments reconcile those resources rather than creating duplicates. Starting the
continuous pipeline and applying the view grant are explicit flags because the grant can run
only after the pipeline has published its datasets.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ACCESS_GROUP = "asset-reliability-command-center-operator"


def run(command: list[str]) -> None:
    """Run one checked command from the bundle root without invoking a shell."""

    subprocess.run(command, cwd=REPOSITORY_ROOT, check=True)  # noqa: S603


def bundle_command(action: str, args: argparse.Namespace, *extra: str) -> list[str]:
    """Build a Databricks bundle command with explicit target, profile, and group."""

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
        *extra,
    ]


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
        "--deploy",
        action="store_true",
        help="Apply the validated bundle, including create/update of the Genie space.",
    )
    parser.add_argument(
        "--start-pipeline",
        action="store_true",
        help="Start the continuous pipeline without waiting for termination.",
    )
    parser.add_argument(
        "--apply-view-grant",
        action="store_true",
        help="Run the idempotent UC grant job after pump_telemetry_history exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(bundle_command("validate", args))

    if args.deploy:
        run(bundle_command("deploy", args, "--auto-approve"))
    if args.start_pipeline:
        run(
            bundle_command(
                "run", args, "governed_telemetry_history", "--no-wait", "--refresh-all"
            )
        )
    if args.apply_view_grant:
        run(bundle_command("run", args, "setup_telemetry_access"))


if __name__ == "__main__":
    main()
