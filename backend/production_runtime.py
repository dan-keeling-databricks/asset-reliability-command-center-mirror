from __future__ import annotations

import asyncio
import json
from collections.abc import Coroutine
from typing import Any
from uuid import uuid4

from backend.gateway import GatewayAsk, GovernedGatewayClient
from backend.identity import OboIdentityRegistry
from backend.lakebase_repository import LakebaseCommandCenterRepository
from backend.model_client import DatabricksModelClient
from backend.models import (
    ActivityDatum,
    PendingApproval,
    RuntimeAction,
    RuntimeActivity,
    ScenarioCommand,
)
from backend.settings import Settings


class ProductionCommandCenterRuntime:
    """Concurrent code-defined agent runtime over canonical Lakebase state."""

    def __init__(
        self,
        repository: LakebaseCommandCenterRepository,
        settings: Settings,
        gateway: GovernedGatewayClient | None = None,
        model: DatabricksModelClient | None = None,
    ) -> None:
        if not settings.governed_mcp_endpoint:
            raise RuntimeError("GOVERNED_MCP_ENDPOINT is required in production")
        if not settings.databricks_host:
            raise RuntimeError("DATABRICKS_HOST is required in production")
        self.repository = repository
        self.settings = settings
        self.gateway = gateway or GovernedGatewayClient(settings.governed_mcp_endpoint)
        self.skill_gateway = (
            GovernedGatewayClient(settings.governed_skill_endpoint)
            if settings.governed_skill_endpoint
            else None
        )
        self.model = model or DatabricksModelClient(settings.databricks_host)
        self.identities = OboIdentityRegistry()
        self._evaluator_task: asyncio.Task[None] | None = None
        self._run_tasks: set[asyncio.Task[None]] = set()
        self._last_subject: str | None = None

    async def start(self) -> None:
        await self.repository.start()
        self._evaluator_task = asyncio.create_task(self._evaluator_loop())

    async def stop(self) -> None:
        tasks = [*self._run_tasks]
        if self._evaluator_task:
            tasks.append(self._evaluator_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.gateway.close()
        if self.skill_gateway:
            await self.skill_gateway.close()
        await self.model.close()
        await self.repository.close()

    def _spawn(self, operation: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(operation)
        self._run_tasks.add(task)
        task.add_done_callback(self._run_tasks.discard)

    def capture_identity(self, scenario_id: str, token: str | None) -> None:
        identity = self.identities.capture(scenario_id, token)
        if identity:
            self._last_subject = identity.subject

    async def restart(self) -> str:
        self.identities.clear()
        return await self.repository.restart_scenario(self._last_subject)

    async def trigger_excursion(self) -> str:
        snapshot = await self.repository.snapshot()
        command = ScenarioCommand(
            id=str(uuid4()),
            scenario_id=snapshot.scenario.id,
            kind="start_p204_excursion",
        )
        await self.repository.enqueue_command(command)
        return command.id

    async def approve(self, incident_id: str) -> None:
        incident = await self.repository.incident(incident_id)
        if not incident or not incident.pending_approval:
            raise ValueError("Incident is not waiting for Gateway approval")
        completed = await self._retry_pending(incident_id, incident.pending_approval)
        if not completed:
            raise ValueError("Unity Gateway approval is still pending")

    async def follow_up(self, incident_id: str, message: str) -> None:
        incident = await self.repository.incident(incident_id)
        if not incident:
            raise LookupError("Incident not found")
        await self.repository.append_conversation(incident_id, "operator", message)
        result = await self.model.chat(
            self.settings.primary_model_endpoint,
            [
                {
                    "role": "system",
                    "content": "Answer concisely from the supplied governed incident record.",
                },
                {
                    "role": "user",
                    "content": f"Incident: {incident.model_dump_json()}\nQuestion: {message}",
                },
            ],
            token=None,
        )
        await self.repository.append_conversation(incident_id, "agent", result.content)
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="model_call",
                title="Follow-up response",
                actor=self.settings.primary_model_endpoint,
                status="complete",
                status_label="Completed",
                summary="Answered using persisted incident context.",
                details=[ActivityDatum(label="Output tokens", value=str(result.output_tokens))],
            ),
            tokens=(
                result.input_tokens,
                result.output_tokens,
                self._model_cost(result.input_tokens, result.output_tokens),
            ),
        )

    async def _evaluator_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.alert_evaluator_interval_seconds)
            try:
                for incident_id, run_id in await self.repository.evaluate_thresholds():
                    incident = await self.repository.incident(incident_id)
                    if not incident:
                        continue
                    identity = self.identities.get(incident.scenario_id)
                    await self.repository.record_run_identity(
                        run_id,
                        incident.scenario_id,
                        identity.subject if identity else None,
                    )
                    self._spawn(self._run_agent(incident_id))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                # The next one-second evaluation retries; a transient database failure must not
                # terminate autonomous monitoring.
                continue

    async def _run_agent(self, incident_id: str) -> None:
        try:
            incident = await self.repository.incident(incident_id)
            if not incident:
                return
            identity = self.identities.get(incident.scenario_id)
            if not identity:
                raise RuntimeError("No in-memory OBO identity is available for this scenario")

            if not self.skill_gateway or not self.settings.governed_skill_tool:
                raise RuntimeError("Governed Skill Gateway configuration is missing")
            skill_response = await self.skill_gateway.call_tool(
                self.settings.governed_skill_tool, {}, identity.token
            )
            skill_data = _structured(skill_response)
            skill_instructions = str(skill_data.get("result") or json.dumps(skill_data))
            await self.repository.append_activity(
                incident_id,
                RuntimeActivity(
                    category="skill",
                    title="Load governed reliability response skill",
                    actor=self.settings.governed_skill_tool,
                    status="complete",
                    status_label="Loaded",
                    summary="Runtime loaded the current Unity Catalog response procedure.",
                ),
            )
            tools = await self.gateway.list_tools(identity.token)
            available = {tool["name"]: tool for tool in tools}
            await self.repository.append_activity(
                incident_id,
                RuntimeActivity(
                    category="mcp_server",
                    title="Discover governed maintenance capabilities",
                    actor=self.settings.governed_mcp_endpoint,
                    status="complete",
                    status_label="Connected",
                    summary=f"Unity Gateway exposed {len(available)} governed tools at runtime.",
                    details=[ActivityDatum(label="Available", value=", ".join(sorted(available)))],
                ),
            )
            openai_tools = [
                {
                    "type": "function",
                    "function": {
                        "name": tool["name"],
                        "description": tool.get("description", ""),
                        "parameters": tool.get("inputSchema", {"type": "object"}),
                    },
                }
                for tool in tools
            ]
            messages: list[dict[str, Any]] = [
                {
                    "role": "system",
                    "content": (
                        "You are a code-defined asset reliability agent. Follow the governed "
                        "Skill below. Choose tools from the capabilities discovered at runtime; "
                        "do not assume a fixed sequence or call count. Never claim an action ran "
                        "when a tool was blocked. Finish with one concise operator response using "
                        "four bullet-led sections: Finding, Evidence, Risk, and Action.\n\n"
                        f"GOVERNED SKILL:\n{skill_instructions}"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Autonomous alert for {incident.asset_id}: {incident.condition}. "
                        "Assess failure risk before the next maintenance window and pursue the "
                        "governed response warranted by the evidence. The incident id is "
                        f"{incident_id}."
                    ),
                },
            ]
            endpoint = self.settings.fast_model_endpoint
            last_risk: dict[str, Any] | None = None
            action_continuation_requested = False
            while True:
                turn = await self.model.turn(endpoint, messages, tools=openai_tools)
                tool_calls = turn.message.get("tool_calls", [])
                summary = str(turn.message.get("content") or "")
                if not summary:
                    summary = f"Selected {len(tool_calls)} governed capability call(s)."
                await self.repository.append_activity(
                    incident_id,
                    RuntimeActivity(
                        category="model_call",
                        title="Choose next reliability action",
                        actor=endpoint,
                        status="complete",
                        status_label="Completed",
                        summary=summary,
                        details=[
                            ActivityDatum(label="Input tokens", value=str(turn.input_tokens)),
                            ActivityDatum(label="Output tokens", value=str(turn.output_tokens)),
                        ],
                    ),
                    tokens=(
                        turn.input_tokens,
                        turn.output_tokens,
                        self._model_cost(turn.input_tokens, turn.output_tokens),
                    ),
                )
                messages.append(turn.message)
                if not tool_calls:
                    if (
                        last_risk
                        and last_risk.get("risk_band") == "high"
                        and not action_continuation_requested
                    ):
                        action_continuation_requested = True
                        messages.append(
                            {
                                "role": "user",
                                "content": (
                                    "High risk is established, but no governed action has been "
                                    "attempted. Continue the Skill workflow by invoking the "
                                    "available governed action warranted by the evidence. Do not "
                                    "predict the policy decision; let Unity Gateway decide."
                                ),
                            }
                        )
                        continue
                    if last_risk and last_risk.get("risk_band") != "high":
                        if turn.message.get("content"):
                            await self.repository.append_conversation(
                                incident_id, "agent", str(turn.message["content"])
                            )
                        await self.repository.resolve_no_action(incident_id)
                        return
                    raise RuntimeError(
                        "Agent ended without resolving the incident through governed evidence"
                    )

                for tool_call in tool_calls:
                    function = tool_call["function"]
                    tool_name = function["name"]
                    if tool_name not in available:
                        raise RuntimeError(f"Model selected unavailable tool: {tool_name}")
                    raw_arguments = function.get("arguments") or "{}"
                    arguments = (
                        json.loads(raw_arguments)
                        if isinstance(raw_arguments, str)
                        else raw_arguments
                    )
                    try:
                        result = await self.gateway.call_tool(tool_name, arguments, identity.token)
                    except GatewayAsk as ask:
                        await self._pause_for_gateway(incident_id, tool_name, ask)
                        return
                    data = _structured(result)
                    await self.repository.append_activity(
                        incident_id,
                        RuntimeActivity(
                            category="tool_call",
                            title=_tool_title(tool_name),
                            actor=f"Unity Gateway · {tool_name}",
                            status="complete",
                            status_label="ALLOW",
                            summary=_result_summary(data),
                            details=[ActivityDatum(label="Decision", value="ALLOW")],
                        ),
                        tool_call=True,
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call["id"],
                            "name": tool_name,
                            "content": json.dumps(data),
                        }
                    )
                    if tool_name == "assess_failure_risk":
                        last_risk = data
                        if data.get("risk_band") == "high":
                            endpoint = self.settings.primary_model_endpoint
                    if tool_name == "submit_work_order":
                        await self._complete_governed_action(incident_id, result)
                        return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            await self.repository.append_activity(
                incident_id,
                RuntimeActivity(
                    category="runtime",
                    title="Agent run failed",
                    status="failed",
                    status_label="Failed",
                    summary=str(exc),
                ),
            )
            await self.repository.fail_incident(incident_id, str(exc))

    async def _pause_for_gateway(self, incident_id: str, tool_name: str, ask: GatewayAsk) -> None:
        pending = PendingApproval(
            serialized_request=ask.serialized_request,
            elicitation_id=ask.elicitation.elicitation_id,
            approval_url=ask.elicitation.url,
            message=ask.elicitation.message,
            tool_name=tool_name,
        )
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="tool_call",
                title=_tool_title(tool_name),
                actor=f"Unity Gateway · {tool_name}",
                status="waiting",
                status_label="ASK · Not executed",
                summary=ask.elicitation.message,
                details=[
                    ActivityDatum(label="Decision", value="ASK"),
                    ActivityDatum(label="Execution", value="Not executed"),
                ],
                action=RuntimeAction(
                    label="Review approval request",
                    kind="link",
                    href=ask.elicitation.url,
                ),
            ),
            tool_call=True,
        )
        await self.repository.set_waiting(incident_id, pending)
        self._spawn(self._resume_when_approved(incident_id, pending))

    def _model_cost(self, input_tokens: int, output_tokens: int) -> float:
        return (
            input_tokens * self.settings.model_input_cost_per_million
            + output_tokens * self.settings.model_output_cost_per_million
        ) / 1_000_000

    async def _resume_when_approved(self, incident_id: str, pending: PendingApproval) -> None:
        while True:
            await asyncio.sleep(self.settings.approval_poll_seconds)
            if await self._retry_pending(incident_id, pending):
                return

    async def _retry_pending(self, incident_id: str, pending: PendingApproval) -> bool:
        incident = await self.repository.incident(incident_id)
        if not incident or incident.status != "waiting":
            return True
        identity = self.identities.get(incident.scenario_id)
        if not identity:
            return False
        try:
            result = await self.gateway.replay(pending.serialized_request, identity.token)
        except GatewayAsk:
            return False
        await self._complete_governed_action(incident_id, result)
        return True

    async def _complete_governed_action(self, incident_id: str, result: dict[str, Any]) -> None:
        data = _structured(result)
        work_order_id = data.get("work_order_id", "Work order")
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="tool_result",
                title="Maintenance action completed",
                actor="Unity Gateway · submit_work_order",
                status="complete",
                status_label="APPROVED · ALLOW",
                summary=f"{work_order_id} scheduled for the next available maintenance window.",
                details=[
                    ActivityDatum(label="Work order", value=str(work_order_id)),
                    ActivityDatum(label="Decision", value="ALLOW"),
                ],
            ),
        )
        await self.repository.complete_incident(incident_id, str(work_order_id))


def _structured(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("result", {})
    if isinstance(result.get("structuredContent"), dict):
        return result["structuredContent"]
    for item in result.get("content", []):
        if item.get("type") == "text":
            try:
                value = json.loads(item["text"])
                if isinstance(value, dict):
                    return value
            except json.JSONDecodeError:
                return {"result": item["text"]}
    return result if isinstance(result, dict) else {"result": result}


def _result_summary(data: dict[str, Any]) -> str:
    if "failure_probability_pct" in data:
        return f"Failure risk assessed at {data['failure_probability_pct']}% ({data['risk_band']})."
    if "vibration_mm_s" in data:
        return f"Vibration is {data['vibration_mm_s']} mm/s with correlated operating context."
    if data.get("part_available"):
        return (
            f"{data.get('part')} is available; estimated duration is "
            f"{data.get('estimated_duration_hours')} h."
        )
    return "Governed tool completed successfully."


def _tool_title(name: str) -> str:
    return {
        "get_asset_telemetry": "Read recent asset telemetry",
        "assess_failure_risk": "Assess failure risk before the maintenance window",
        "prepare_work_order": "Prepare bearing replacement work order",
        "submit_work_order": "Submit maintenance work order",
    }.get(name, name.replace("_", " ").title())
