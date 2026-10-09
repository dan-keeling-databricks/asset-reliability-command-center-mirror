"""Focused tests for bounded Lakebase query construction and repository invariants."""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backend.lakebase_repository import LakebaseCommandCenterRepository


def test_schema_persists_versioned_operator_and_handover_audit() -> None:
    schema = (Path(__file__).resolve().parents[1] / "sql" / "lakebase_schema.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS asset_reliability.app_users" in schema
    assert "CREATE TABLE IF NOT EXISTS asset_reliability.shift_operator" in schema
    assert "CREATE TABLE IF NOT EXISTS asset_reliability.shift_operator_audit" in schema
    assert "previous_version BIGINT NOT NULL" in schema
    assert "new_version BIGINT NOT NULL" in schema


def test_schema_persists_command_leases_and_immutable_governance_receipts() -> None:
    schema = (Path(__file__).resolve().parents[1] / "sql" / "lakebase_schema.sql").read_text()

    assert "lease_expires_at TIMESTAMPTZ" in schema
    assert "claim_version BIGINT NOT NULL DEFAULT 0" in schema
    assert "attempt_count INTEGER NOT NULL DEFAULT 0" in schema
    assert "CREATE TABLE IF NOT EXISTS asset_reliability.governance_receipts" in schema
    assert "request_sha256 TEXT NOT NULL" in schema
    assert "governance_receipts_are_immutable" in schema


def test_schema_repairs_legacy_leases_and_adds_versioned_approval_claims() -> None:
    schema = (Path(__file__).resolve().parents[1] / "sql" / "lakebase_schema.sql").read_text()

    assert "status = 'claimed' AND lease_expires_at IS NULL" in schema
    assert "approval_claim_id UUID" in schema
    assert "approval_claim_version BIGINT NOT NULL DEFAULT 0" in schema
    assert "agent_runs_approval_replay_state_check" in schema
    assert "VALIDATE CONSTRAINT agent_runs_approval_replay_state_check" in schema


def test_runtime_mutations_are_fenced_by_active_scenario_and_approval_claim() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "backend" / "lakebase_repository.py"
    ).read_text()

    assert "scenario.status = 'active'" in source
    assert "run.approval_claim_id = %s::uuid" in source
    assert "run.approval_claim_version = %s" in source
    assert "run.approval_replay_state = 'claimed'" in source
    assert "Governed work order rejected because its scenario or " in source
    assert '"approval claim is stale"' in source


def wait_repository(*, revision: int = 0, coalesce_seconds: float = 0.01):
    repository = LakebaseCommandCenterRepository.__new__(LakebaseCommandCenterRepository)
    repository._revision = revision
    repository._change_condition = asyncio.Condition()
    repository._change_counter = 0
    repository._handled_change_counter = 0
    repository._sse_coalesce_seconds = coalesce_seconds
    return repository


async def notify(repository: LakebaseCommandCenterRepository) -> None:
    async with repository._change_condition:
        repository._change_counter += 1
        repository._change_condition.notify_all()


@pytest.mark.asyncio
async def test_wait_for_change_does_not_snapshot_before_notification() -> None:
    repository = wait_repository()
    repository.snapshot = AsyncMock(return_value=SimpleNamespace(revision=1))

    waiting = asyncio.create_task(repository.wait_for_change(0, timeout=0.5))
    await asyncio.sleep(0.01)
    repository.snapshot.assert_not_awaited()

    await notify(repository)
    result = await waiting

    assert result.revision == 1
    repository.snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_wait_for_change_coalesces_notification_burst_into_one_snapshot() -> None:
    repository = wait_repository(coalesce_seconds=0.02)
    repository.snapshot = AsyncMock(return_value=SimpleNamespace(revision=1))

    waiting = asyncio.create_task(repository.wait_for_change(0, timeout=0.5))
    await asyncio.sleep(0)
    await notify(repository)
    await asyncio.sleep(0.005)
    await notify(repository)
    await asyncio.sleep(0.005)
    await notify(repository)

    result = await waiting

    assert result.revision == 1
    repository.snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_wait_for_change_consumes_notification_pending_before_wait() -> None:
    repository = wait_repository(coalesce_seconds=0)
    repository._change_counter = 1
    repository.snapshot = AsyncMock(return_value=SimpleNamespace(revision=1))

    result = await asyncio.wait_for(repository.wait_for_change(0, timeout=10), timeout=0.1)

    assert result.revision == 1
    repository.snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_wait_for_change_timeout_snapshots_once_for_keep_alive() -> None:
    repository = wait_repository()
    repository.snapshot = AsyncMock(return_value=SimpleNamespace(revision=4))

    result = await repository.wait_for_change(4, timeout=0.01)

    assert result.revision == 4
    repository.snapshot.assert_awaited_once()


@pytest.mark.asyncio
async def test_wait_for_change_snapshots_immediately_for_advanced_revision() -> None:
    repository = wait_repository(revision=5)
    repository.snapshot = AsyncMock(return_value=SimpleNamespace(revision=5))

    result = await asyncio.wait_for(repository.wait_for_change(4, timeout=10), timeout=0.1)

    assert result.revision == 5
    repository.snapshot.assert_awaited_once()


def test_snapshot_uses_indexed_lateral_top_twenty_probe() -> None:
    repository = LakebaseCommandCenterRepository.__new__(LakebaseCommandCenterRepository)
    repository.schema = "asset_reliability"

    query = repository._pump_snapshot_query().as_string().upper()

    assert "LEFT JOIN LATERAL" in query
    assert "TELEMETRY.SCENARIO_ID = %S" in query
    assert "TELEMETRY.PUMP_ID = P.PUMP_ID" in query
    assert "ORDER BY TELEMETRY.EVENT_TIME DESC" in query
    assert "LIMIT 20" in query
    assert "ROW_NUMBER" not in query


def test_retention_delete_is_bounded_and_index_addressable() -> None:
    repository = LakebaseCommandCenterRepository.__new__(LakebaseCommandCenterRepository)
    repository.schema = "asset_reliability"

    query = repository._retention_delete_query().as_string().upper()

    assert "SCENARIO_ID = %S AND PUMP_ID = %S" in query
    assert "EVENT_TIME <" in query
    assert "LIMIT %S" in query
    assert "FOR UPDATE SKIP LOCKED" in query


def test_retention_series_discovery_never_scans_telemetry() -> None:
    repository = LakebaseCommandCenterRepository.__new__(LakebaseCommandCenterRepository)
    repository.schema = "asset_reliability"

    query = repository._retention_series_query().as_string().upper()

    assert "SCENARIO_RUNS" in query
    assert "CROSS JOIN" in query
    assert "PUMPS" in query
    assert "TELEMETRY_EVENTS" not in query
    assert "EXISTS" not in query


def test_threshold_evaluation_is_scoped_to_identity_scenario() -> None:
    repository = LakebaseCommandCenterRepository.__new__(LakebaseCommandCenterRepository)
    repository.schema = "asset_reliability"

    query = repository._threshold_candidates_query().as_string().upper()

    assert "SCENARIO.SCENARIO_ID = %S::UUID" in query
    assert "SCENARIO.STATUS = 'ACTIVE'" in query


def test_sequence_allocation_locks_parent_rows() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "backend" / "lakebase_repository.py"
    ).read_text()

    assert "SELECT run.run_id FROM {} run" in source
    assert "FOR UPDATE OF run" in source
    assert "SELECT incident.incident_id FROM {} incident" in source
    assert "FOR UPDATE OF incident" in source


def test_producer_commands_use_versioned_recoverable_leases() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "src" / "producer" / "pump_telemetry_producer.py"
    ).read_text()

    assert "lease_expires_at < now()" in source
    assert "attempt_count < %s" in source
    assert "claimed_by = %s AND claim_version = %s" in source
    assert "Lost command lease before completion" in source


class FakeResult:
    def __init__(self, rows: list[dict[str, float]]) -> None:
        self._rows = rows

    async def fetchall(self) -> list[dict[str, float]]:
        return self._rows


class FakeConnection:
    def __init__(self, rows: list[dict[str, float]]) -> None:
        self.rows = rows
        self.query = ""
        self.parameters = None

    async def execute(self, query, parameters=None) -> FakeResult:
        self.query = query.as_string()
        self.parameters = parameters
        return FakeResult(self.rows)


class FakeConnectionContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, *_args) -> None:
        return None


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection_instance = connection

    def connection(self) -> FakeConnectionContext:
        return FakeConnectionContext(self.connection_instance)


@pytest.mark.asyncio
async def test_telemetry_window_queries_active_scenario_without_snapshot() -> None:
    rows = [
        {
            "vibration_mm_s": 4.8,
            "flow_m3h": 181.0,
            "motor_current_a": 42.0,
            "bearing_temperature_c": 65.0,
        },
        {
            "vibration_mm_s": 4.7,
            "flow_m3h": 181.0,
            "motor_current_a": 42.0,
            "bearing_temperature_c": 64.0,
        },
    ]
    connection = FakeConnection(rows)
    repository = LakebaseCommandCenterRepository.__new__(LakebaseCommandCenterRepository)
    repository.schema = "asset_reliability"
    repository._pool = FakePool(connection)
    repository.snapshot = AsyncMock(side_effect=AssertionError("snapshot must not be called"))

    result = await repository.telemetry_window("P-204", limit=2)

    assert [row["vibration_mm_s"] for row in result] == [4.7, 4.8]
    assert 'FROM "asset_reliability"."scenario_runs"' in connection.query
    assert "status = 'active'" in connection.query
    assert connection.parameters == ("P-204", 2)
    repository.snapshot.assert_not_awaited()
