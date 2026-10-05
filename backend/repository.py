from __future__ import annotations

import asyncio
from typing import Any, Protocol

from backend.models import DashboardSnapshot, IncidentDetail, ScenarioCommand, utc_now


class Mutation(Protocol):
    def __call__(
        self, snapshot: DashboardSnapshot, incidents: dict[str, IncidentDetail]
    ) -> None: ...


class CommandCenterRepository(Protocol):
    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def snapshot(self) -> DashboardSnapshot: ...
    async def incident(self, incident_id: str) -> IncidentDetail | None: ...
    async def mutate(self, operation: Mutation) -> DashboardSnapshot: ...
    async def wait_for_change(self, after_revision: int, timeout: float) -> DashboardSnapshot: ...
    async def enqueue_command(self, command: ScenarioCommand) -> None: ...
    async def claim_commands(self, limit: int = 10) -> list[ScenarioCommand]: ...
    async def finish_command(self, command_id: str, error: str | None = None) -> None: ...
    async def record_run_identity(
        self, run_id: str, scenario_id: str, subject: str | None
    ) -> None: ...
    async def create_work_order(
        self, work_order_id: str, incident_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]: ...
    async def telemetry_window(self, asset_id: str, limit: int = 12) -> list[dict[str, Any]]: ...


class LocalCommandCenterRepository:
    """Deterministic in-memory adapter implementing the production persistence contract."""

    def __init__(self, initial: DashboardSnapshot) -> None:
        self._snapshot = initial
        self._incidents: dict[str, IncidentDetail] = {}
        self._commands: dict[str, ScenarioCommand] = {}
        self._identities: dict[str, dict[str, Any]] = {}
        self._work_orders: dict[str, dict[str, Any]] = {}
        self._condition = asyncio.Condition()

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def snapshot(self) -> DashboardSnapshot:
        async with self._condition:
            return self._snapshot.model_copy(deep=True)

    async def incident(self, incident_id: str) -> IncidentDetail | None:
        async with self._condition:
            incident = self._incidents.get(incident_id)
            return incident.model_copy(deep=True) if incident else None

    async def mutate(self, operation: Mutation) -> DashboardSnapshot:
        async with self._condition:
            operation(self._snapshot, self._incidents)
            self._snapshot.revision += 1
            self._condition.notify_all()
            return self._snapshot.model_copy(deep=True)

    async def wait_for_change(self, after_revision: int, timeout: float) -> DashboardSnapshot:
        async with self._condition:
            if self._snapshot.revision <= after_revision:
                try:
                    await asyncio.wait_for(
                        self._condition.wait_for(lambda: self._snapshot.revision > after_revision),
                        timeout=timeout,
                    )
                except TimeoutError:
                    pass
            return self._snapshot.model_copy(deep=True)

    async def enqueue_command(self, command: ScenarioCommand) -> None:
        async with self._condition:
            duplicate = any(
                item.scenario_id == command.scenario_id
                and item.kind == command.kind
                and item.status != "failed"
                for item in self._commands.values()
            )
            if duplicate:
                raise ValueError("P-204 excursion already queued for this scenario")
            self._commands[command.id] = command.model_copy(deep=True)

    async def claim_commands(self, limit: int = 10) -> list[ScenarioCommand]:
        async with self._condition:
            claimed: list[ScenarioCommand] = []
            for command in self._commands.values():
                if command.status != "pending":
                    continue
                command.status = "running"
                claimed.append(command.model_copy(deep=True))
                if len(claimed) >= limit:
                    break
            return claimed

    async def finish_command(self, command_id: str, error: str | None = None) -> None:
        async with self._condition:
            command = self._commands[command_id]
            command.status = "failed" if error else "completed"
            command.error = error
            command.completed_at = utc_now()

    async def record_run_identity(self, run_id: str, scenario_id: str, subject: str | None) -> None:
        async with self._condition:
            self._identities[run_id] = {
                "scenario_id": scenario_id,
                "subject": subject,
                "captured_at": utc_now(),
            }

    async def create_work_order(
        self, work_order_id: str, incident_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        async with self._condition:
            existing = self._work_orders.get(work_order_id)
            if existing:
                return dict(existing)
            record = {
                "work_order_id": work_order_id,
                "incident_id": incident_id,
                "status": "scheduled",
                **payload,
                "created_at": utc_now().isoformat(),
            }
            self._work_orders[work_order_id] = record
            return dict(record)

    async def telemetry_window(self, asset_id: str, limit: int = 12) -> list[dict[str, Any]]:
        async with self._condition:
            pump = next(item for item in self._snapshot.pumps if item.asset_id == asset_id)
            values = pump.trend[-limit:]
            return [
                {
                    "vibration_mm_s": value,
                    "flow_m3h": pump.flow_m3_h,
                    "motor_current_a": pump.current_a,
                    "bearing_temperature_c": 59.0 + max(0, value - 4.4) * 3,
                }
                for value in values
            ]
