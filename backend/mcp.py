from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any

from backend.repository import CommandCenterRepository

TOOLS: list[dict[str, Any]] = [
    {
        "name": "get_asset_telemetry",
        "description": "Read current and recent operating telemetry for one asset.",
        "inputSchema": {
            "type": "object",
            "properties": {"asset_id": {"type": "string"}},
            "required": ["asset_id"],
        },
    },
    {
        "name": "assess_failure_risk",
        "description": "Estimate failure risk before the next maintenance window.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "asset_id": {"type": "string"},
                "maintenance_window_days": {"type": "integer", "default": 21},
            },
            "required": ["asset_id"],
        },
    },
    {
        "name": "prepare_work_order",
        "description": "Prepare a non-binding maintenance plan and verify parts availability.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "asset_id": {"type": "string"},
                "action": {"type": "string"},
                "incident_id": {"type": "string"},
            },
            "required": ["asset_id", "action", "incident_id"],
        },
    },
    {
        "name": "submit_work_order",
        "description": "Submit the prepared maintenance work order as an operational write.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "asset_id": {"type": "string"},
                "action": {"type": "string"},
                "incident_id": {"type": "string"},
                "planned_window": {"type": "string"},
            },
            "required": ["asset_id", "action", "incident_id", "planned_window"],
        },
    },
]


class AssetReliabilityMCP:
    def __init__(self, repository: CommandCenterRepository) -> None:
        self.repository = repository

    async def handle(self, request: dict[str, Any]) -> dict[str, Any] | None:
        request_id = request.get("id")
        method = request.get("method")
        if method == "notifications/initialized":
            return None
        if method == "initialize":
            return self._result(
                request_id,
                {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {
                        "name": "asset-reliability-command-center",
                        "version": "0.3.0",
                    },
                },
            )
        if method == "ping":
            return self._result(request_id, {})
        if method == "tools/list":
            return self._result(request_id, {"tools": TOOLS})
        if method == "tools/call":
            params = request.get("params", {})
            try:
                value = await self.call_tool(params.get("name", ""), params.get("arguments", {}))
            except (KeyError, ValueError) as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32602, "message": str(exc)},
                }
            return self._result(
                request_id,
                {
                    "content": [{"type": "text", "text": json.dumps(value)}],
                    "structuredContent": value,
                    "isError": False,
                },
            )
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": f"Unsupported method: {method}"},
        }

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if name == "get_asset_telemetry":
            pump = await self._pump(arguments["asset_id"])
            return {
                "asset_id": pump.asset_id,
                "vibration_mm_s": pump.vibration_mm_s,
                "warning_threshold_mm_s": pump.warning_threshold_mm_s,
                "flow_m3_h": pump.flow_m3_h,
                "current_a": pump.current_a,
                "trend": pump.trend,
                "updated_at": pump.updated_at.isoformat(),
            }
        if name == "assess_failure_risk":
            await asyncio.sleep(6)
            pump = await self._pump(arguments["asset_id"])
            window = await self.repository.telemetry_window(arguments["asset_id"], limit=12)
            recent = window[-6:]
            if len(recent) >= 2:
                vibration_change = recent[-1]["vibration_mm_s"] - recent[0]["vibration_mm_s"]
                current_change = recent[-1]["motor_current_a"] - recent[0]["motor_current_a"]
                temperature_change = (
                    recent[-1]["bearing_temperature_c"] - recent[0]["bearing_temperature_c"]
                )
            else:
                vibration_change = current_change = temperature_change = 0.0
            threshold_samples = sum(
                item["vibration_mm_s"] >= pump.warning_threshold_mm_s for item in recent
            )
            sustained = len(recent) >= 4 and threshold_samples >= max(3, len(recent) - 1)
            correlated_level = bool(
                recent
                and (
                    recent[-1]["motor_current_a"] >= 45.0
                    or recent[-1]["bearing_temperature_c"] >= 65.0
                )
            )
            correlated_direction = current_change > 0.1 or temperature_change > 0.2
            persistent = (
                pump.vibration_mm_s >= pump.warning_threshold_mm_s
                and (
                    (sustained and correlated_level)
                    or (vibration_change > 0.12 and correlated_direction)
                )
            )
            probability = (
                min(96, round(82 + max(0, pump.vibration_mm_s - 4.4) * 20)) if persistent else 18
            )
            return {
                "asset_id": pump.asset_id,
                "failure_probability_pct": probability,
                "risk_band": "high" if persistent else "low",
                "maintenance_window_days": arguments.get("maintenance_window_days", 21),
                "evidence": (
                    "Persistent vibration rise with elevated current or bearing temperature"
                    if persistent
                    else "Short excursion is decaying with the correlated operating transient"
                ),
                "recent_vibration_change_mm_s": round(vibration_change, 3),
                "recent_current_change_a": round(current_change, 3),
                "recent_temperature_change_c": round(temperature_change, 3),
                "recent_threshold_samples": threshold_samples,
                "sustained_threshold_exceedance": sustained,
            }
        if name == "prepare_work_order":
            await self._pump(arguments["asset_id"])
            return {
                "asset_id": arguments["asset_id"],
                "incident_id": arguments["incident_id"],
                "action": arguments["action"],
                "part": "BRG-6312-C3",
                "part_available": True,
                "estimated_duration_hours": 3.5,
                "planned_window": "next available maintenance window",
            }
        if name == "submit_work_order":
            await self._pump(arguments["asset_id"])
            digest = hashlib.sha256(arguments["incident_id"].encode()).hexdigest()
            work_order_id = f"WO-{int(digest[:8], 16) % 100000:05d}"
            return await self.repository.create_work_order(
                work_order_id,
                arguments["incident_id"],
                {
                    "asset_id": arguments["asset_id"],
                    "action": arguments["action"],
                    "planned_window": arguments["planned_window"],
                    "part": "BRG-6312-C3",
                },
            )
        raise ValueError(f"Unknown tool: {name}")

    async def _pump(self, asset_id: str):
        snapshot = await self.repository.snapshot()
        pump = next((item for item in snapshot.pumps if item.asset_id == asset_id), None)
        if not pump:
            raise ValueError(f"Unknown asset: {asset_id}")
        return pump

    @staticmethod
    def _result(request_id: Any, value: dict[str, Any]) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "result": value}
