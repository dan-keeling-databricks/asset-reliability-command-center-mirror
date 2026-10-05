from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from backend.models import (
    ActiveRun,
    AlertSummary,
    ConversationTurn,
    DashboardSnapshot,
    IncidentDetail,
    IncidentTotals,
    InvocationSummary,
    PendingApproval,
    PlatformStatus,
    PumpState,
    RuntimeActivity,
    ScenarioCommand,
    ScenarioState,
)


class OAuthAsyncConnection(psycopg.AsyncConnection[Any]):
    """Mint a Lakebase database credential for each new pooled connection."""

    @classmethod
    async def connect(cls, conninfo: str = "", **kwargs: Any) -> OAuthAsyncConnection:
        password = os.getenv("PGPASSWORD")
        if not password:
            from databricks.sdk import WorkspaceClient

            endpoint = os.environ["LAKEBASE_ENDPOINT"]
            credential = await asyncio.to_thread(
                WorkspaceClient().postgres.generate_database_credential,
                endpoint=endpoint,
            )
            password = credential.token
        kwargs["password"] = password
        return await super().connect(conninfo, **kwargs)


class LakebaseCommandCenterRepository:
    """Repository over the canonical relational asset-reliability Lakebase schema."""

    def __init__(
        self,
        dsn: str,
        schema: str = "asset_reliability",
        job_principal: str | None = None,
        migrate_on_start: bool = True,
        sse_coalesce_seconds: float = 0.05,
        telemetry_retention_seconds: int = 86_400,
        telemetry_retention_interval_seconds: float = 300.0,
        telemetry_retention_batch_size: int = 1_000,
    ) -> None:
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", schema):
            raise ValueError("Lakebase schema must be a simple PostgreSQL identifier")
        self.schema = schema
        self._dsn = dsn
        self.job_principal = job_principal
        self._migrate_on_start = migrate_on_start
        self._sse_coalesce_seconds = max(0.0, sse_coalesce_seconds)
        self._telemetry_retention_seconds = max(1, telemetry_retention_seconds)
        self._telemetry_retention_interval_seconds = max(0.1, telemetry_retention_interval_seconds)
        self._telemetry_retention_batch_size = max(1, telemetry_retention_batch_size)
        self._pool = AsyncConnectionPool(
            conninfo=dsn,
            connection_class=OAuthAsyncConnection,
            min_size=1,
            max_size=8,
            max_lifetime=2700,
            open=False,
            kwargs={"row_factory": dict_row},
        )
        self._revision = 0
        self._fingerprint = ""
        self._revision_lock = asyncio.Lock()
        self._change_condition = asyncio.Condition()
        self._change_counter = 0
        self._handled_change_counter = 0
        self._listener_task: asyncio.Task[None] | None = None
        self._retention_task: asyncio.Task[None] | None = None

    def table(self, name: str) -> sql.Composed:
        return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(name))

    async def start(self) -> None:
        await self._pool.open(wait=True, timeout=30)
        if self._migrate_on_start:
            await self._migrate()
        await self._ensure_active_scenario()
        self._listener_task = asyncio.create_task(self._listen_loop())
        self._retention_task = asyncio.create_task(self._retention_loop())

    async def close(self) -> None:
        tasks = [task for task in (self._listener_task, self._retention_task) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self._pool.close()

    async def _migrate(self) -> None:
        migration_path = Path(__file__).resolve().parents[1] / "sql" / "lakebase_schema.sql"
        migration = migration_path.read_text()
        if self.schema != "asset_reliability":
            migration = migration.replace("asset_reliability", self.schema)
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(migration)
                await connection.execute(
                    sql.SQL(
                        """CREATE TABLE IF NOT EXISTS {} (
                        run_id UUID PRIMARY KEY,
                        scenario_id UUID NOT NULL,
                        identity_subject TEXT,
                        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"""
                    ).format(self.table("run_identities"))
                )
                await connection.execute(
                    sql.SQL(
                        """CREATE OR REPLACE FUNCTION {}.notify_app_change()
                        RETURNS TRIGGER LANGUAGE plpgsql AS $$
                        BEGIN
                          PERFORM pg_notify('asset_reliability_app', TG_TABLE_NAME);
                          RETURN COALESCE(NEW, OLD);
                        END; $$"""
                    ).format(sql.Identifier(self.schema))
                )
                for table_name in (
                    "scenario_runs",
                    "scenario_commands",
                    "incidents",
                    "agent_runs",
                    "activity_events",
                    "conversation_messages",
                    "work_orders",
                ):
                    await connection.execute(
                        sql.SQL("DROP TRIGGER IF EXISTS notify_app_change ON {}").format(
                            self.table(table_name)
                        )
                    )
                    await connection.execute(
                        sql.SQL(
                            "CREATE TRIGGER notify_app_change AFTER INSERT OR UPDATE OR DELETE "
                            "ON {} FOR EACH STATEMENT EXECUTE FUNCTION {}.notify_app_change()"
                        ).format(self.table(table_name), sql.Identifier(self.schema))
                    )
                if self.job_principal:
                    schema = sql.Identifier(self.schema)
                    principal = sql.Identifier(self.job_principal)
                    await connection.execute(
                        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, principal)
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {} TO {}"
                        ).format(schema, principal)
                    )
                    await connection.execute(
                        sql.SQL(
                            "ALTER DEFAULT PRIVILEGES IN SCHEMA {} "
                            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}"
                        ).format(schema, principal)
                    )

    async def _ensure_active_scenario(self) -> None:
        async with self._pool.connection() as connection:
            async with connection.transaction():
                active = await (
                    await connection.execute(
                        sql.SQL("SELECT scenario_id FROM {} WHERE status = 'active'").format(
                            self.table("scenario_runs")
                        )
                    )
                ).fetchone()
                if active:
                    return
                scenario_id = uuid4()
                await connection.execute(
                    sql.SQL(
                        "INSERT INTO {} (scenario_id, status, requested_by) "
                        "VALUES (%s, 'active', 'app-startup')"
                    ).format(self.table("scenario_runs")),
                    (scenario_id,),
                )

    async def _listen_loop(self) -> None:
        while True:
            try:
                async with await OAuthAsyncConnection.connect(
                    self._dsn, autocommit=True
                ) as connection:
                    await connection.execute("LISTEN asset_reliability_telemetry")
                    await connection.execute("LISTEN asset_reliability_app")
                    async for _notification in connection.notifies():
                        async with self._change_condition:
                            self._change_counter += 1
                            self._change_condition.notify_all()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                await asyncio.sleep(1)

    async def _retention_loop(self) -> None:
        while True:
            await asyncio.sleep(self._telemetry_retention_interval_seconds)
            try:
                await self._run_retention_pass()
            except asyncio.CancelledError:
                raise
            except psycopg.Error:
                # Retention is best-effort maintenance and must not affect live reads.
                continue

    async def _run_retention_pass(self) -> None:
        async with self._pool.connection() as connection:
            series = await (
                await connection.execute(self._retention_series_query())
            ).fetchall()

        for row in series:
            try:
                async with self._pool.connection() as connection:
                    async with connection.transaction():
                        await connection.execute("SET LOCAL lock_timeout = '250ms'")
                        await connection.execute("SET LOCAL statement_timeout = '2s'")
                        await connection.execute(
                            self._retention_delete_query(),
                            (
                                row["scenario_id"],
                                row["pump_id"],
                                self._telemetry_retention_seconds,
                                self._telemetry_retention_batch_size,
                            ),
                        )
            except psycopg.Error:
                continue

    def _retention_series_query(self) -> sql.Composed:
        # The cross product is tiny (scenarios × eight pumps). Do not add an EXISTS probe:
        # PostgreSQL can decorrelate it into a full telemetry scan before applying LIMIT.
        return sql.SQL(
            """SELECT scenario.scenario_id::text, pump.pump_id
            FROM {} scenario CROSS JOIN {} pump
            ORDER BY scenario.started_at, pump.pump_id"""
        ).format(self.table("scenario_runs"), self.table("pumps"))

    def _retention_delete_query(self) -> sql.Composed:
        return sql.SQL(
            """WITH expired AS (
              SELECT event_id FROM {}
              WHERE scenario_id = %s AND pump_id = %s
                AND event_time < NOW() - make_interval(secs => %s)
              ORDER BY event_time
              LIMIT %s
              FOR UPDATE SKIP LOCKED
            )
            DELETE FROM {} telemetry USING expired
            WHERE telemetry.event_id = expired.event_id"""
        ).format(self.table("telemetry_events"), self.table("telemetry_events"))

    def _pump_snapshot_query(self) -> sql.Composed:
        return sql.SQL(
            """SELECT p.pump_id, p.display_name, p.area_name,
            live.event_time, live.ingested_at, live.flow_m3h,
            live.vibration_mm_s, live.motor_current_a,
            trend.values AS trend_values,
            incident.incident_id::text, incident.status AS incident_status,
            run.run_id::text, run.status AS run_status,
            event.title AS current_activity, work.work_order_id
            FROM {} p
            LEFT JOIN {} live ON live.pump_id = p.pump_id
              AND live.scenario_id = %s
            LEFT JOIN LATERAL (
              SELECT array_agg(sample.vibration_mm_s ORDER BY sample.event_time) AS values
              FROM (
                SELECT telemetry.vibration_mm_s, telemetry.event_time
                FROM {} telemetry
                WHERE telemetry.scenario_id = %s
                  AND telemetry.pump_id = p.pump_id
                ORDER BY telemetry.event_time DESC
                LIMIT 20
              ) sample
            ) trend ON TRUE
            LEFT JOIN {} incident ON incident.pump_id = p.pump_id
              AND incident.scenario_id = %s::uuid
            LEFT JOIN LATERAL (
              SELECT * FROM {} candidate
              WHERE candidate.incident_id = incident.incident_id
              ORDER BY candidate.started_at DESC NULLS LAST LIMIT 1
            ) run ON TRUE
            LEFT JOIN LATERAL (
              SELECT title FROM {} candidate WHERE candidate.run_id = run.run_id
              ORDER BY sequence DESC LIMIT 1
            ) event ON TRUE
            LEFT JOIN {} work ON work.incident_id = incident.incident_id
            ORDER BY p.pump_id"""
        ).format(
            self.table("pumps"),
            self.table("pump_live_state"),
            self.table("telemetry_events"),
            self.table("incidents"),
            self.table("agent_runs"),
            self.table("activity_events"),
            self.table("work_orders"),
        )

    async def snapshot(self) -> DashboardSnapshot:
        query_started = time.perf_counter()
        async with self._change_condition:
            snapshot_change_counter = self._change_counter
        async with self._pool.connection() as connection:
            scenario = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT scenario_id::text, started_at, p204_excursion_started_at "
                        "FROM {} WHERE status = 'active' ORDER BY started_at DESC LIMIT 1"
                    ).format(self.table("scenario_runs"))
                )
            ).fetchone()
            if not scenario:
                raise RuntimeError("No active asset-reliability scenario")
            pumps = await (
                await connection.execute(
                    self._pump_snapshot_query(),
                    (
                        scenario["scenario_id"],
                        scenario["scenario_id"],
                        scenario["scenario_id"],
                    ),
                )
            ).fetchall()
            incident_rows = await (
                await connection.execute(
                    sql.SQL(
                        """SELECT incident.incident_id::text, incident.pump_id, incident.status,
                        incident.observed_value, incident.threshold_value, incident.triggered_at,
                        incident.resolution_summary, run.run_id::text, run.status AS run_status,
                        run.started_at, run.completed_at
                        FROM {} incident LEFT JOIN {} run
                          ON run.incident_id = incident.incident_id
                        WHERE incident.scenario_id = %s::uuid
                        ORDER BY incident.triggered_at DESC"""
                    ).format(self.table("incidents"), self.table("agent_runs")),
                    (scenario["scenario_id"],),
                )
            ).fetchall()
            pending_command = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT 1 FROM {} WHERE scenario_id = %s::uuid "
                        "AND command_type = 'start_p204_excursion' LIMIT 1"
                    ).format(self.table("scenario_commands")),
                    (scenario["scenario_id"],),
                )
            ).fetchone()
        query_latency_ms = max(1, round((time.perf_counter() - query_started) * 1000))

        pump_models: list[PumpState] = []
        active_runs: list[ActiveRun] = []
        latest_ingested: datetime | None = None
        kafka_to_lakebase_latency_ms = 0
        baselines = {
            "P-201": (182.0, 42.0, 1.35),
            "P-202": (179.0, 41.5, 1.45),
            "P-203": (185.0, 42.5, 1.30),
            "P-204": (181.0, 42.0, 1.55),
            "P-205": (177.0, 41.0, 1.40),
            "P-206": (184.0, 42.4, 1.25),
            "P-207": (180.0, 41.8, 1.50),
            "P-208": (183.0, 42.1, 1.32),
        }
        for row in pumps:
            flow, current, vibration = baselines[row["pump_id"]]
            incident_status = row["incident_status"]
            state = "normal"
            state_label = "Normal"
            if incident_status in {"reviewing", "waiting_approval"}:
                state = "warning" if row["pump_id"] == "P-204" else "transient"
                state_label = "Warning" if state == "warning" else "Reviewing transient"
            elif incident_status == "maintenance_scheduled":
                state, state_label = "warning", "Maintenance scheduled"
            ingested = row["ingested_at"]
            if ingested and (latest_ingested is None or ingested > latest_ingested):
                latest_ingested = ingested
                if row["event_time"]:
                    kafka_to_lakebase_latency_ms = max(
                        0, round((ingested - row["event_time"]).total_seconds() * 1000)
                    )
            pump_models.append(
                PumpState(
                    asset_id=row["pump_id"],
                    name=row["display_name"],
                    unit=row["area_name"],
                    state=state,
                    state_label=state_label,
                    vibration_mm_s=float(row["vibration_mm_s"] or vibration),
                    flow_m3_h=float(row["flow_m3h"] or flow),
                    current_a=float(row["motor_current_a"] or current),
                    trend=list(row["trend_values"] or [vibration]),
                    maintenance_label=(
                        f"{row['work_order_id']} scheduled" if row["work_order_id"] else None
                    ),
                    active_run_id=(
                        row["run_id"]
                        if row["run_status"] in {"queued", "running", "waiting_approval"}
                        else None
                    ),
                    updated_at=row["event_time"] or scenario["started_at"],
                )
            )
            if row["run_status"] in {"queued", "running", "waiting_approval"}:
                active_runs.append(
                    ActiveRun(
                        invocation_id=row["run_id"],
                        incident_id=row["incident_id"],
                        asset_id=row["pump_id"],
                        label="Autonomous review",
                        current_activity=row["current_activity"] or "Incident context received",
                    )
                )
        alerts = [self._alert_from_row(row) for row in incident_rows]
        invocations = [self._invocation_from_row(row) for row in incident_rows]
        snapshot = DashboardSnapshot(
            scenario=ScenarioState(
                id=scenario["scenario_id"],
                p204_triggered=bool(scenario["p204_excursion_started_at"] or pending_command),
                started_at=scenario["started_at"],
            ),
            platform=PlatformStatus(
                source_label="Lakebase · live operational state",
                latency_ms=query_latency_ms,
                kafka_to_lakebase_latency_ms=kafka_to_lakebase_latency_ms,
                lakebase_to_ui_latency_ms=query_latency_ms,
                end_to_end_latency_ms=kafka_to_lakebase_latency_ms + query_latency_ms,
                last_event_at=latest_ingested or scenario["started_at"],
            ),
            pumps=pump_models,
            alerts=alerts,
            invocations=invocations,
            active_runs=active_runs,
        )
        snapshot = await self._with_revision(snapshot)
        async with self._change_condition:
            self._handled_change_counter = max(
                self._handled_change_counter, snapshot_change_counter
            )
        return snapshot

    async def _with_revision(self, snapshot: DashboardSnapshot) -> DashboardSnapshot:
        stable = snapshot.model_copy(deep=True)
        stable.revision = 0
        stable.platform.latency_ms = 0
        stable.platform.kafka_to_lakebase_latency_ms = 0
        stable.platform.lakebase_to_ui_latency_ms = 0
        stable.platform.end_to_end_latency_ms = 0
        fingerprint = stable.model_dump_json()
        async with self._revision_lock:
            if fingerprint != self._fingerprint:
                self._fingerprint = fingerprint
                self._revision += 1
            snapshot.revision = self._revision
        return snapshot

    @staticmethod
    def _alert_from_row(row: dict[str, Any]) -> AlertSummary:
        status_map = {
            "reviewing": ("evaluating", "Agent evaluating"),
            "waiting_approval": ("action_required", "Approval required"),
            "resolved_no_action": ("resolved", "Reviewed · no action"),
            "maintenance_scheduled": ("maintenance_scheduled", "Maintenance scheduled"),
            "failed": ("action_required", "Review failed"),
        }
        status, label = status_map[row["status"]]
        return AlertSummary(
            incident_id=row["incident_id"],
            asset_id=row["pump_id"],
            title="Drive-end bearing vibration excursion",
            condition=(
                f"Vibration reached {float(row['observed_value']):.2f} mm/s "
                f"against a {float(row['threshold_value']):.1f} mm/s warning threshold"
            ),
            status=status,
            status_label=label,
            observed_at=row["triggered_at"],
            resolution=row["resolution_summary"],
        )

    @staticmethod
    def _invocation_from_row(row: dict[str, Any]) -> InvocationSummary:
        status_map = {
            "queued": ("running", "Queued"),
            "running": ("running", "Running"),
            "waiting_approval": ("waiting", "Waiting for approval"),
            "completed": ("completed", "Completed"),
            "failed": ("failed", "Failed"),
        }
        run_status = row["run_status"] or "queued"
        status, label = status_map[run_status]
        return InvocationSummary(
            id=row["run_id"] or row["incident_id"],
            incident_id=row["incident_id"],
            asset_id=row["pump_id"],
            title="Drive-end bearing vibration excursion",
            status=status,
            status_label=label,
            started_at=row["started_at"] or row["triggered_at"],
            completed_at=row["completed_at"],
            outcome=row["resolution_summary"],
        )

    async def incident(self, incident_id: str) -> IncidentDetail | None:
        async with self._pool.connection() as connection:
            row = await (
                await connection.execute(
                    sql.SQL(
                        """SELECT incident.*, run.run_id::text, run.status AS run_status,
                        run.pending_approval_url, run.pending_tool_request, run.trigger_prompt,
                        run.total_input_tokens, run.total_output_tokens,
                        run.estimated_cost_usd, run.tool_call_count,
                        run.started_at, run.completed_at
                        FROM {} incident LEFT JOIN {} run ON run.incident_id = incident.incident_id
                        WHERE incident.incident_id = %s::uuid"""
                    ).format(self.table("incidents"), self.table("agent_runs")),
                    (incident_id,),
                )
            ).fetchone()
            if not row:
                return None
            activities = await (
                await connection.execute(
                    sql.SQL("SELECT * FROM {} WHERE run_id = %s::uuid ORDER BY sequence").format(
                        self.table("activity_events")
                    ),
                    (row["run_id"],),
                )
            ).fetchall()
            messages = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE incident_id = %s::uuid ORDER BY sequence"
                    ).format(self.table("conversation_messages")),
                    (incident_id,),
                )
            ).fetchall()
        status_map = {
            "reviewing": ("running", "Agent evaluating"),
            "waiting_approval": ("waiting", "Waiting for approval"),
            "resolved_no_action": ("completed", "Reviewed · no action"),
            "maintenance_scheduled": ("completed", "Maintenance scheduled"),
            "failed": ("failed", "Agent failed"),
        }
        status, label = status_map[row["status"]]
        pending_data = row["pending_tool_request"] or None
        pending = None
        if pending_data:
            pending = PendingApproval.model_validate(pending_data)
        activity_models = []
        for event in activities:
            metadata = event["runtime_metadata"] or {}
            if "id" in metadata:
                activity_models.append(RuntimeActivity.model_validate(metadata))
            else:
                activity_models.append(
                    RuntimeActivity(
                        category=event["event_type"],
                        title=event["title"],
                        status=event["status"] or "info",
                        status_label=event["status"] or "Info",
                        summary=event["detail"],
                        emitted_at=event["occurred_at"],
                        started_at=event["occurred_at"],
                        completed_at=event["occurred_at"],
                    )
                )
        conversation = [
            ConversationTurn(
                id=str(message["message_id"]),
                role=message["role"],
                content=message["content"],
                created_at=message["created_at"],
            )
            for message in messages
        ]
        narrative = [
            f"An automated 4.4 mm/s threshold rule opened this review for {row['pump_id']}."
        ]
        if row["resolution_summary"]:
            narrative.append(row["resolution_summary"])
        elif status == "waiting":
            narrative.append(
                "The agent found high failure risk, but Unity Gateway stopped the operational "
                "write and requested human approval."
            )
        return IncidentDetail(
            id=str(row["incident_id"]),
            scenario_id=str(row["scenario_id"]),
            asset_id=row["pump_id"],
            title="Drive-end bearing vibration excursion",
            condition=(
                f"Vibration reached {float(row['observed_value']):.2f} mm/s and "
                f"crossed the {float(row['threshold_value']):.1f} mm/s warning threshold"
            ),
            trigger_prompt=row["trigger_prompt"],
            status=status,
            status_label=label,
            narrative=narrative,
            conversation=conversation,
            activities=activity_models,
            totals=IncidentTotals(
                input_tokens=row["total_input_tokens"],
                output_tokens=row["total_output_tokens"],
                estimated_cost_usd=float(row["estimated_cost_usd"]),
                tool_calls=row["tool_call_count"],
            ),
            pending_approval=pending,
            created_at=row["triggered_at"],
            updated_at=row["resolved_at"] or row["triggered_at"],
        )

    async def wait_for_change(self, after_revision: int, timeout: float) -> DashboardSnapshot:
        if self._revision > after_revision:
            return await self.snapshot()

        notified = False
        async with self._change_condition:
            if self._revision > after_revision:
                notified = True
            elif self._change_counter > self._handled_change_counter:
                notified = True
            else:
                change_cursor = self._change_counter
                try:
                    await asyncio.wait_for(
                        self._change_condition.wait_for(
                            lambda: self._change_counter > change_cursor
                        ),
                        timeout=timeout,
                    )
                    notified = True
                except TimeoutError:
                    pass

        if notified and self._revision <= after_revision:
            await self._coalesce_notifications()
        return await self.snapshot()

    async def _coalesce_notifications(self) -> None:
        if self._sse_coalesce_seconds <= 0:
            return
        while True:
            async with self._change_condition:
                change_cursor = self._change_counter
                try:
                    await asyncio.wait_for(
                        self._change_condition.wait_for(
                            lambda: self._change_counter > change_cursor
                        ),
                        timeout=self._sse_coalesce_seconds,
                    )
                except TimeoutError:
                    return

    async def restart_scenario(self, requested_by: str | None) -> str:
        scenario_id, command_id = uuid4(), uuid4()
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status = 'archived', ended_at = NOW() "
                        "WHERE status = 'active'"
                    ).format(self.table("scenario_runs"))
                )
                await connection.execute(
                    sql.SQL(
                        "INSERT INTO {} (scenario_id, status, requested_by) "
                        "VALUES (%s, 'active', %s)"
                    ).format(self.table("scenario_runs")),
                    (scenario_id, requested_by),
                )
                await connection.execute(
                    sql.SQL(
                        "INSERT INTO {} (command_id, scenario_id, command_type, requested_by) "
                        "VALUES (%s, %s, 'restart_scenario', %s)"
                    ).format(self.table("scenario_commands")),
                    (command_id, scenario_id, requested_by),
                )
        return str(scenario_id)

    async def enqueue_command(self, command: ScenarioCommand) -> None:
        try:
            async with self._pool.connection() as connection:
                await connection.execute(
                    sql.SQL(
                        "INSERT INTO {} (command_id, scenario_id, command_type) "
                        "VALUES (%s::uuid, %s::uuid, %s)"
                    ).format(self.table("scenario_commands")),
                    (command.id, command.scenario_id, command.kind),
                )
                await connection.commit()
        except psycopg.errors.UniqueViolation as exc:
            raise ValueError("P-204 excursion already queued for this scenario") from exc

    async def evaluate_thresholds(self) -> list[tuple[str, str]]:
        created: list[tuple[str, str]] = []
        async with self._pool.connection() as connection:
            async with connection.transaction():
                candidates = await (
                    await connection.execute(
                        sql.SQL(
                            """SELECT scenario.scenario_id, live.pump_id, live.vibration_mm_s,
                            live.event_time, rule.rule_id, rule.warning_threshold
                            FROM {} scenario JOIN {} live
                              ON live.scenario_id = scenario.scenario_id::text
                            JOIN {} rule ON rule.rule_id = 'bearing-vibration-warning'
                            WHERE scenario.status = 'active' AND rule.enabled
                              AND live.vibration_mm_s >= rule.warning_threshold"""
                        ).format(
                            self.table("scenario_runs"),
                            self.table("pump_live_state"),
                            self.table("alert_rules"),
                        )
                    )
                ).fetchall()
                for candidate in candidates:
                    incident_id, run_id = uuid4(), uuid4()
                    inserted = await connection.execute(
                        sql.SQL(
                            """INSERT INTO {} (incident_id, scenario_id, pump_id, rule_id, status,
                            observed_value, threshold_value, triggered_at)
                            VALUES (%s, %s, %s, %s, 'reviewing', %s, %s, %s)
                            ON CONFLICT (scenario_id, pump_id, rule_id) DO NOTHING"""
                        ).format(self.table("incidents")),
                        (
                            incident_id,
                            candidate["scenario_id"],
                            candidate["pump_id"],
                            candidate["rule_id"],
                            candidate["vibration_mm_s"],
                            candidate["warning_threshold"],
                            candidate["event_time"],
                        ),
                    )
                    if inserted.rowcount != 1:
                        continue
                    prompt = (
                        f"Asset {candidate['pump_id']} crossed the governed vibration warning "
                        f"threshold: {candidate['vibration_mm_s']:.2f} mm/s against "
                        f"{candidate['warning_threshold']:.1f} mm/s. Assess failure risk before "
                        "the next maintenance window and take the appropriate governed action."
                    )
                    await connection.execute(
                        sql.SQL(
                            "INSERT INTO {} "
                            "(run_id, incident_id, status, trigger_prompt, started_at) "
                            "VALUES (%s, %s, 'running', %s, NOW())"
                        ).format(self.table("agent_runs")),
                        (run_id, incident_id, prompt),
                    )
                    created.append((str(incident_id), str(run_id)))
        return created

    async def append_activity(
        self,
        incident_id: str,
        activity: RuntimeActivity,
        tokens: tuple[int, int, float] | None = None,
        tool_call: bool = False,
    ) -> None:
        async with self._pool.connection() as connection:
            async with connection.transaction():
                run = await (
                    await connection.execute(
                        sql.SQL("SELECT run_id FROM {} WHERE incident_id = %s::uuid").format(
                            self.table("agent_runs")
                        ),
                        (incident_id,),
                    )
                ).fetchone()
                await connection.execute(
                    sql.SQL(
                        "INSERT INTO {} (event_id, run_id, sequence, event_type, title, detail, "
                        "status, runtime_metadata) SELECT %s, %s, COALESCE(MAX(sequence), 0) + 1, "
                        "%s, %s, %s, %s, %s FROM {} WHERE run_id = %s"
                    ).format(self.table("activity_events"), self.table("activity_events")),
                    (
                        uuid4(),
                        run["run_id"],
                        activity.category,
                        activity.title,
                        activity.summary,
                        activity.status,
                        Jsonb(activity.model_dump(mode="json")),
                        run["run_id"],
                    ),
                )
                if tokens or tool_call:
                    input_tokens, output_tokens, cost = tokens or (0, 0, 0.0)
                    await connection.execute(
                        sql.SQL(
                            "UPDATE {} SET total_input_tokens = total_input_tokens + %s, "
                            "total_output_tokens = total_output_tokens + %s, "
                            "estimated_cost_usd = estimated_cost_usd + %s, "
                            "tool_call_count = tool_call_count + %s WHERE run_id = %s"
                        ).format(self.table("agent_runs")),
                        (input_tokens, output_tokens, cost, 1 if tool_call else 0, run["run_id"]),
                    )

    async def set_waiting(self, incident_id: str, pending: PendingApproval) -> None:
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status = 'waiting_approval' WHERE incident_id = %s::uuid"
                    ).format(self.table("incidents")),
                    (incident_id,),
                )
                await connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status = 'waiting_approval', pending_approval_url = %s, "
                        "pending_tool_request = %s WHERE incident_id = %s::uuid"
                    ).format(self.table("agent_runs")),
                    (pending.approval_url, Jsonb(pending.model_dump(mode="json")), incident_id),
                )

    async def complete_incident(self, incident_id: str, work_order_id: str) -> None:
        summary = f"{work_order_id} scheduled to replace the drive-end bearing."
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status = 'maintenance_scheduled', resolved_at = NOW(), "
                        "resolution_summary = %s WHERE incident_id = %s::uuid"
                    ).format(self.table("incidents")),
                    (summary, incident_id),
                )
                await connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status = 'completed', completed_at = NOW(), "
                        "pending_approval_url = NULL, pending_tool_request = NULL "
                        "WHERE incident_id = %s::uuid"
                    ).format(self.table("agent_runs")),
                    (incident_id,),
                )
        await self.append_conversation(
            incident_id,
            "agent",
            f"Approval was granted through Unity Gateway. {summary}",
        )

    async def resolve_no_action(self, incident_id: str) -> None:
        summary = (
            "Signal returned to its expected operating envelope; no maintenance action required."
        )
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status = 'resolved_no_action', resolved_at = NOW(), "
                        "resolution_summary = %s WHERE incident_id = %s::uuid"
                    ).format(self.table("incidents")),
                    (summary, incident_id),
                )
                await connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status = 'completed', completed_at = NOW() "
                        "WHERE incident_id = %s::uuid"
                    ).format(self.table("agent_runs")),
                    (incident_id,),
                )
        await self.append_conversation(incident_id, "agent", summary)

    async def fail_incident(self, incident_id: str, error: str) -> None:
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    sql.SQL("UPDATE {} SET status = 'failed' WHERE incident_id = %s::uuid").format(
                        self.table("incidents")
                    ),
                    (incident_id,),
                )
                await connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status = 'failed', completed_at = NOW(), error = %s "
                        "WHERE incident_id = %s::uuid"
                    ).format(self.table("agent_runs")),
                    (error, incident_id),
                )

    async def append_conversation(self, incident_id: str, role: str, content: str) -> None:
        async with self._pool.connection() as connection:
            await connection.execute(
                sql.SQL(
                    "INSERT INTO {} (message_id, incident_id, sequence, role, content) "
                    "SELECT %s, %s::uuid, COALESCE(MAX(sequence), 0) + 1, %s, %s "
                    "FROM {} WHERE incident_id = %s::uuid"
                ).format(self.table("conversation_messages"), self.table("conversation_messages")),
                (uuid4(), incident_id, role, content, incident_id),
            )
            await connection.commit()

    async def record_run_identity(self, run_id: str, scenario_id: str, subject: str | None) -> None:
        async with self._pool.connection() as connection:
            await connection.execute(
                sql.SQL(
                    "INSERT INTO {} (run_id, scenario_id, identity_subject) VALUES (%s, %s, %s) "
                    "ON CONFLICT (run_id) DO UPDATE SET "
                    "identity_subject = EXCLUDED.identity_subject"
                ).format(self.table("run_identities")),
                (run_id, scenario_id, subject),
            )
            await connection.commit()

    async def create_work_order(
        self, work_order_id: str, incident_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        async with self._pool.connection() as connection:
            await connection.execute(
                sql.SQL(
                    "INSERT INTO {} (work_order_id, incident_id, pump_id, action, status, "
                    "submitted_at, submitted_by, governance_receipt) VALUES "
                    "(%s, %s::uuid, %s, %s, 'scheduled', NOW(), %s, %s) "
                    "ON CONFLICT (work_order_id) DO NOTHING"
                ).format(self.table("work_orders")),
                (
                    work_order_id,
                    incident_id,
                    payload["asset_id"],
                    payload["action"],
                    payload.get("submitted_by", "governed-agent"),
                    Jsonb(payload.get("governance_receipt", {})),
                ),
            )
            await connection.commit()
            row = await (
                await connection.execute(
                    sql.SQL("SELECT * FROM {} WHERE work_order_id = %s").format(
                        self.table("work_orders")
                    ),
                    (work_order_id,),
                )
            ).fetchone()
        return {
            "work_order_id": row["work_order_id"],
            "incident_id": str(row["incident_id"]),
            "asset_id": row["pump_id"],
            "action": row["action"],
            "status": row["status"],
            "created_at": row["created_at"].isoformat(),
        }

    async def telemetry_window(self, asset_id: str, limit: int = 12) -> list[dict[str, Any]]:
        async with self._pool.connection() as connection:
            rows = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT vibration_mm_s, flow_m3h, motor_current_a, bearing_temperature_c "
                        "FROM {} WHERE scenario_id = ("
                        "SELECT scenario_id::text FROM {} WHERE status = 'active' "
                        "ORDER BY started_at DESC LIMIT 1) AND pump_id = %s "
                        "ORDER BY event_time DESC LIMIT %s"
                    ).format(self.table("telemetry_events"), self.table("scenario_runs")),
                    (asset_id, limit),
                )
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    async def mutate(self, operation) -> DashboardSnapshot:
        del operation
        raise RuntimeError("Production writes use canonical transactional repository methods")

    async def claim_commands(self, limit: int = 10) -> list[ScenarioCommand]:
        del limit
        raise RuntimeError("The classic telemetry producer owns production command claiming")

    async def finish_command(self, command_id: str, error: str | None = None) -> None:
        del command_id, error
        raise RuntimeError("The classic telemetry producer owns production command completion")


def lakebase_conninfo() -> str:
    required = {name: os.getenv(name) for name in ("PGHOST", "PGDATABASE", "PGUSER")}
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Missing Lakebase environment: {', '.join(missing)}")
    return (
        f"host={required['PGHOST']} port={os.getenv('PGPORT', '5432')} "
        f"dbname={required['PGDATABASE']} user={required['PGUSER']} "
        f"sslmode={os.getenv('PGSSLMODE', 'require')}"
    )
