# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous pump telemetry producer
# MAGIC
# MAGIC This classic-compute job maintains a continuous eight-pump simulation, reads durable
# MAGIC scenario commands from Lakebase, and publishes telemetry to Kafka. The Databricks App
# MAGIC controls the scenario through Lakebase and never requires network access to the broker.
# MAGIC
# MAGIC **Reliability contract:** Kafka delivery is acknowledged before the producer heartbeat
# MAGIC advances, event identifiers are deterministic, and a restarted job resumes beyond both
# MAGIC persisted and wall-clock sequence floors.

# COMMAND ----------

"""Continuous synthetic pump telemetry producer.

This process deliberately runs on classic compute. It polls Lakebase for low-volume
scenario commands and publishes the eight-pump telemetry stream to the VPC-reachable
Kafka broker. The Databricks App never connects to Kafka directly.
"""

from __future__ import annotations

import json
import math
import random
import re
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
from databricks.sdk import WorkspaceClient
from kafka import KafkaProducer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime configuration
# MAGIC
# MAGIC Widgets keep environment-specific endpoints and rates outside the simulation logic.
# MAGIC Identifiers and rates are validated before any external connection is opened.

# COMMAND ----------

dbutils.widgets.text("kafka_bootstrap", "63.186.81.193:443")
dbutils.widgets.text("kafka_topic", "asset-reliability-pump-telemetry")
dbutils.widgets.text(
    "lakebase_endpoint",
    "projects/asset-reliability-command-center/branches/production/endpoints/primary",
)
dbutils.widgets.text("lakebase_database", "databricks_postgres")
dbutils.widgets.text("lakebase_schema", "asset_reliability")
dbutils.widgets.text("rate_hz", "2")
dbutils.widgets.text("run_seconds", "0")

KAFKA_BOOTSTRAP = dbutils.widgets.get("kafka_bootstrap")
KAFKA_TOPIC = dbutils.widgets.get("kafka_topic")
LAKEBASE_ENDPOINT = dbutils.widgets.get("lakebase_endpoint")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
LAKEBASE_SCHEMA = dbutils.widgets.get("lakebase_schema")
RATE_HZ = float(dbutils.widgets.get("rate_hz"))
RUN_SECONDS = int(dbutils.widgets.get("run_seconds"))

if not re.fullmatch(r"[a-z_][a-z0-9_]*", LAKEBASE_SCHEMA):
    raise ValueError("lakebase_schema must be a simple PostgreSQL identifier")
if RATE_HZ <= 0:
    raise ValueError("rate_hz must be positive")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fleet and simulation state
# MAGIC
# MAGIC Pump profiles define stable operating baselines. `SimulationState` carries the durable
# MAGIC scenario identity and monotonically increasing sequence used to derive idempotent event
# MAGIC identifiers. P-204 onset is optional because that excursion is presenter-controlled.

# COMMAND ----------


@dataclass(frozen=True)
class PumpProfile:
    """Stable operating baseline for one comparable pump."""

    pump_id: str
    flow_m3h: float
    pressure_bar: float
    vibration_mm_s: float
    bearing_temp_c: float
    motor_current_a: float


@dataclass
class SimulationState:
    """Mutable state needed to resume one deterministic scenario stream."""

    scenario_id: str
    started_monotonic: float
    sequence: int = 0
    p204_onset_monotonic: float | None = None


PUMPS = (
    PumpProfile("P-201", 182.0, 8.4, 1.20, 57.0, 42.0),
    PumpProfile("P-202", 179.0, 8.2, 1.45, 58.0, 41.5),
    PumpProfile("P-203", 185.0, 8.5, 1.70, 56.5, 42.5),
    PumpProfile("P-204", 181.0, 8.3, 1.95, 59.0, 42.0),
    PumpProfile("P-205", 177.0, 8.1, 2.20, 57.5, 41.0),
    PumpProfile("P-206", 184.0, 8.4, 2.45, 56.0, 42.4),
    PumpProfile("P-207", 180.0, 8.2, 2.70, 58.5, 41.8),
    PumpProfile("P-208", 183.0, 8.3, 2.95, 57.0, 42.1),
)

NAMESPACE = uuid.UUID("39deea2a-8ef5-4fb9-94b6-5ce34c505855")
PRODUCER_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
COMMAND_LEASE_SECONDS = 30
COMMAND_MAX_ATTEMPTS = 3
workspace = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase control plane
# MAGIC
# MAGIC These helpers mint short-lived database credentials, claim scenario commands with
# MAGIC `FOR UPDATE SKIP LOCKED`, persist command outcomes, and publish producer health. Database
# MAGIC commits are explicit so command ownership and heartbeat state have clear transaction
# MAGIC boundaries.

# COMMAND ----------


def lakebase_connect() -> psycopg.Connection[Any]:
    endpoint = workspace.postgres.get_endpoint(name=LAKEBASE_ENDPOINT)
    credential = workspace.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
    return psycopg.connect(
        host=endpoint.status.hosts.host,
        port=5432,
        dbname=LAKEBASE_DATABASE,
        user=workspace.current_user.me().user_name,
        password=credential.token,
        sslmode="require",
        connect_timeout=10,
        autocommit=False,
    )


def active_scenario(conn: psycopg.Connection[Any]) -> dict[str, Any] | None:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT scenario_id::text, started_at, p204_excursion_started_at
            FROM {LAKEBASE_SCHEMA}.scenario_runs
            WHERE status = 'active'
            ORDER BY started_at DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
    conn.commit()
    if not row:
        return None
    return {
        "scenario_id": row[0],
        "started_at": row[1],
        "p204_excursion_started_at": row[2],
    }


def claim_commands(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    """Claim new or abandoned commands with a versioned, bounded lease.

    A producer that dies after claiming cannot strand a command indefinitely. Expired leases
    are reclaimed until the bounded attempt limit is reached; completion is fenced by owner
    and claim version so a delayed former owner cannot finish a newer producer's claim.
    """

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {LAKEBASE_SCHEMA}.scenario_commands
            SET status = 'failed', completed_at = now(),
                error = 'Command lease expired after maximum retry attempts'
            WHERE status = 'claimed' AND lease_expires_at < now()
              AND attempt_count >= %s
            """,
            (COMMAND_MAX_ATTEMPTS,),
        )
        cursor.execute(
            f"""
            WITH claimable AS (
                SELECT command_id
                FROM {LAKEBASE_SCHEMA}.scenario_commands
                WHERE (status = 'pending'
                       OR (status = 'claimed' AND lease_expires_at < now()))
                  AND attempt_count < %s
                ORDER BY requested_at
                FOR UPDATE SKIP LOCKED
                LIMIT 10
            )
            UPDATE {LAKEBASE_SCHEMA}.scenario_commands AS command
            SET status = 'claimed', claimed_at = now(), claimed_by = %s,
                lease_expires_at = now() + (%s * interval '1 second'),
                attempt_count = command.attempt_count + 1,
                claim_version = command.claim_version + 1,
                error = NULL
            FROM claimable
            WHERE command.command_id = claimable.command_id
            RETURNING command.command_id::text, command.scenario_id::text,
                      command.command_type, command.payload, command.claim_version,
                      command.attempt_count
            """,
            (COMMAND_MAX_ATTEMPTS, PRODUCER_ID, COMMAND_LEASE_SECONDS),
        )
        rows = cursor.fetchall()
    conn.commit()
    return [
        {
            "command_id": row[0],
            "scenario_id": row[1],
            "command_type": row[2],
            "payload": row[3] or {},
            "claim_version": row[4],
            "attempt_count": row[5],
        }
        for row in rows
    ]


def complete_command(
    conn: psycopg.Connection[Any], command_id: str, claim_version: int, *, error: str | None = None
) -> None:
    """Finish only the claim still owned by this producer and version."""

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {LAKEBASE_SCHEMA}.scenario_commands
            SET status = %s, completed_at = now(), error = %s,
                lease_expires_at = NULL
            WHERE command_id = %s::uuid AND status = 'claimed'
              AND claimed_by = %s AND claim_version = %s
            """,
            (
                "failed" if error else "completed",
                error,
                command_id,
                PRODUCER_ID,
                claim_version,
            ),
        )
        if cursor.rowcount != 1:
            conn.rollback()
            raise RuntimeError(
                f"Lost command lease before completion: {command_id} v{claim_version}"
            )
    conn.commit()


def record_p204_onset(conn: psycopg.Connection[Any], scenario_id: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {LAKEBASE_SCHEMA}.scenario_runs
            SET p204_excursion_started_at = COALESCE(p204_excursion_started_at, now())
            WHERE scenario_id = %s::uuid AND status = 'active'
            """,
            (scenario_id,),
        )
    conn.commit()


def heartbeat(
    conn: psycopg.Connection[Any], produced: int, *, phase: str = "streaming"
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {LAKEBASE_SCHEMA}.component_heartbeats
                (component_name, observed_at, status, details)
            VALUES ('telemetry-producer', now(), 'healthy', %s::jsonb)
            ON CONFLICT (component_name) DO UPDATE
            SET observed_at = EXCLUDED.observed_at,
                status = EXCLUDED.status,
                details = EXCLUDED.details
            """,
            (
                json.dumps(
                    {
                        "producer_id": PRODUCER_ID,
                        "messages_produced": produced,
                        "phase": phase,
                    }
                ),
            ),
        )
    conn.commit()


def seconds_since(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return max(0.0, (datetime.now(UTC) - timestamp).total_seconds())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Signal generation and restart recovery
# MAGIC
# MAGIC Normal transients on P-202 and P-207 deliberately cross the shared vibration threshold
# MAGIC but decay. P-204 remains stable until commanded, then develops correlated vibration,
# MAGIC temperature, current, flow, and pressure changes. Seeded noise and UUIDv5 identifiers
# MAGIC make a `(scenario, sequence, pump)` sample reproducible and idempotent.

# COMMAND ----------


def next_sequence(conn: psycopg.Connection[Any], scenario_id: str) -> int:
    """Resume after the last persisted event when a continuous job restarts."""
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT COALESCE(MAX(sequence), -1) + 1
            FROM {LAKEBASE_SCHEMA}.telemetry_events
            WHERE scenario_id = %s
            """,
            (scenario_id,),
        )
        row = cursor.fetchone()
    conn.commit()
    return int(row[0])


def state_from_scenario(
    conn: psycopg.Connection[Any], scenario: dict[str, Any]
) -> SimulationState:
    elapsed = seconds_since(scenario["started_at"]) or 0.0
    onset_elapsed = seconds_since(scenario["p204_excursion_started_at"])
    now = time.monotonic()
    # The wall-clock floor protects against records that were acknowledged by Kafka but
    # have not reached Lakebase yet when this process is replaced during a deployment.
    resume_sequence = max(
        next_sequence(conn, scenario["scenario_id"]),
        int(elapsed * RATE_HZ),
    )
    return SimulationState(
        scenario_id=scenario["scenario_id"],
        started_monotonic=now - elapsed,
        sequence=resume_sequence,
        p204_onset_monotonic=(now - onset_elapsed if onset_elapsed is not None else None),
    )


def transient_shape(elapsed: float, duration: float) -> float:
    if elapsed < 0 or elapsed > duration:
        return 0.0
    return math.sin(math.pi * elapsed / duration) ** 2


def telemetry_for(
    profile: PumpProfile, state: SimulationState, now_monotonic: float
) -> dict[str, Any]:
    """Generate one reproducible, correlated telemetry envelope for a pump tick."""

    elapsed = now_monotonic - state.started_monotonic
    seed = f"{state.scenario_id}:{state.sequence}:{profile.pump_id}"
    noise = random.Random(seed)
    pump_index = int(profile.pump_id[-1])
    load_wave = math.sin((elapsed / (12.0 + pump_index * 1.7)) + pump_index * 0.73)
    vibration_wave = math.sin((elapsed / (4.0 + pump_index * 0.55)) + pump_index * 1.11)

    flow = profile.flow_m3h + 2.0 * load_wave + noise.uniform(-0.7, 0.7)
    pressure = profile.pressure_bar + 0.08 * load_wave + noise.uniform(-0.03, 0.03)
    vibration = (
        profile.vibration_mm_s
        + 0.10 * load_wave
        + 0.07 * vibration_wave
        + noise.uniform(-0.04, 0.04)
    )
    temperature = profile.bearing_temp_c + 0.35 * load_wave + noise.uniform(-0.12, 0.12)
    current = profile.motor_current_a + 0.5 * load_wave + noise.uniform(-0.2, 0.2)

    # Two short, correlated load transients begin immediately after every scenario restart.
    # They cross the same warning threshold as the failure case, but decay without the
    # temperature/vibration persistence that indicates bearing degradation.
    if profile.pump_id == "P-202":
        pulse = transient_shape(elapsed, 10.0)
        vibration += 3.35 * pulse
        flow += 18.0 * pulse
        current += 5.5 * pulse
        pressure += 0.25 * pulse
    elif profile.pump_id == "P-207":
        pulse = transient_shape(elapsed, 11.0)
        vibration += 3.20 * pulse
        flow -= 15.0 * pulse
        current += 4.8 * pulse
        pressure += 0.55 * pulse

    # P-204 remains stable until the presenter command is observed. Its issue onset then
    # ramps smoothly from the current signal rather than replacing or pausing the feed.
    if profile.pump_id == "P-204" and state.p204_onset_monotonic is not None:
        issue_age = max(0.0, now_monotonic - state.p204_onset_monotonic)
        severity = min(1.0, issue_age / 18.0)
        vibration += 6.1 * severity
        temperature += 17.0 * severity
        current += 5.0 * severity
        flow -= 8.0 * severity
        pressure -= 0.35 * severity

    event_time = datetime.now(UTC)
    event_id = uuid.uuid5(
        NAMESPACE, f"{state.scenario_id}:{state.sequence}:{profile.pump_id}"
    )
    return {
        "event_id": str(event_id),
        "scenario_id": state.scenario_id,
        "pump_id": profile.pump_id,
        "sequence": state.sequence,
        "event_time": event_time.isoformat().replace("+00:00", "Z"),
        "flow_m3h": round(flow, 3),
        "discharge_pressure_bar": round(pressure, 3),
        "vibration_mm_s": round(vibration, 3),
        "bearing_temperature_c": round(temperature, 3),
        "motor_current_a": round(current, 3),
        "source": "synthetic-classic-producer",
    }


def apply_command(
    conn: psycopg.Connection[Any], command: dict[str, Any], state: SimulationState | None
) -> SimulationState | None:
    """Apply a durable scenario command to the producer's in-memory state."""

    command_type = command["command_type"]
    scenario_id = command["scenario_id"]
    if command_type == "restart_scenario":
        return SimulationState(scenario_id=scenario_id, started_monotonic=time.monotonic())
    if command_type == "start_p204_excursion":
        if state is None or state.scenario_id != scenario_id:
            scenario = active_scenario(conn)
            if scenario is None or scenario["scenario_id"] != scenario_id:
                raise RuntimeError("P-204 excursion command does not target the active scenario")
            state = state_from_scenario(conn, scenario)
        if state.p204_onset_monotonic is None:
            state.p204_onset_monotonic = time.monotonic()
            record_p204_onset(conn, scenario_id)
        return state
    raise ValueError(f"Unsupported scenario command: {command_type}")


def open_database_with_retry() -> psycopg.Connection[Any]:
    while True:
        try:
            connection = lakebase_connect()
            print("Connected to Lakebase control plane")
            return connection
        except Exception as exc:  # noqa: BLE001
            print(f"Lakebase connection not ready: {exc!r}; retrying in 5 seconds")
            time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous publishing loop
# MAGIC
# MAGIC Each tick first applies control commands, then publishes one sample per pump. `flush`
# MAGIC makes the heartbeat represent broker-acknowledged delivery rather than records merely
# MAGIC accepted into the client buffer. Recoverable control-plane failures reconnect to
# MAGIC Lakebase; unrecoverable broker failures escape so the continuous Databricks job restarts.

# COMMAND ----------


database = open_database_with_retry()
heartbeat(database, 0, phase="lakebase-connected")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda value: value.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
    linger_ms=20,
    acks=1,
    retries=5,
)
scenario = active_scenario(database)
simulation = state_from_scenario(database, scenario) if scenario else None

print(
    f"Producer {PRODUCER_ID} publishing {len(PUMPS)} pumps at {RATE_HZ} Hz "
    f"to {KAFKA_BOOTSTRAP}/{KAFKA_TOPIC}"
)

started = time.monotonic()
last_heartbeat = 0.0
last_scenario_check = 0.0
messages_produced = 0
tick_interval = 1.0 / RATE_HZ

try:
    while RUN_SECONDS == 0 or time.monotonic() - started < RUN_SECONDS:
        tick_started = time.monotonic()
        try:
            for command in claim_commands(database):
                try:
                    simulation = apply_command(database, command, simulation)
                    complete_command(
                        database, command["command_id"], command["claim_version"]
                    )
                    print(f"Applied {command['command_type']} for {command['scenario_id']}")
                except Exception as exc:  # noqa: BLE001
                    database.rollback()
                    complete_command(
                        database,
                        command["command_id"],
                        command["claim_version"],
                        error=str(exc),
                    )
                    print(f"Command {command['command_id']} failed: {exc!r}")

            if tick_started - last_scenario_check >= 5.0:
                latest = active_scenario(database)
                last_scenario_check = tick_started
                if latest and (
                    simulation is None or latest["scenario_id"] != simulation.scenario_id
                ):
                    simulation = state_from_scenario(database, latest)

            if simulation is not None:
                for pump in PUMPS:
                    payload = telemetry_for(pump, simulation, tick_started)
                    producer.send(KAFKA_TOPIC, key=pump.pump_id, value=payload)
                    messages_produced += 1
                # A heartbeat must represent broker-acknowledged delivery rather than only
                # records accepted into the local producer buffer. This also makes a lost
                # broker connection fail visibly so the continuous job can restart.
                producer.flush(timeout=10)
                simulation.sequence += 1

            if tick_started - last_heartbeat >= 5.0:
                heartbeat(database, messages_produced)
                last_heartbeat = tick_started
        except (psycopg.Error, OSError) as exc:
            print(f"Control-plane connection failed: {exc!r}; reconnecting")
            try:
                database.close()
            except Exception:  # noqa: BLE001
                pass
            database = open_database_with_retry()

        elapsed = time.monotonic() - tick_started
        time.sleep(max(0.0, tick_interval - elapsed))
finally:
    producer.flush(timeout=10)
    producer.close(timeout=10)
    database.close()

print(json.dumps({"messages_produced": messages_produced, "topic": KAFKA_TOPIC}))
