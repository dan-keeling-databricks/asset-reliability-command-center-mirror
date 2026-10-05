from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Coroutine
from datetime import datetime
from typing import Any
from uuid import uuid4

from backend.gateway import GatewayAsk, GovernedGatewayClient
from backend.identity import OboIdentity, OboIdentityRegistry
from backend.lakebase_repository import LakebaseCommandCenterRepository
from backend.model_client import DatabricksModelClient
from backend.models import (
    ActivityDatum,
    PendingApproval,
    RuntimeAction,
    RuntimeActivity,
    ScenarioCommand,
    utc_now,
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
        self._user_identities = OboIdentityRegistry()
        self._evaluator_task: asyncio.Task[None] | None = None
        self._run_tasks: set[asyncio.Task[None]] = set()
        self._last_subject: str | None = None
        self._identity_scenario_id: str | None = None
        self._operator_change_lock = asyncio.Lock()

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
            self._identity_scenario_id = scenario_id

    def cache_user_identity(self, user_email: str, token: str | None) -> None:
        self._user_identities.capture(user_email.lower(), token)

    def bind_operator_identity(self, scenario_id: str, operator_email: str) -> bool:
        cached = self._user_identities.get(operator_email.lower())
        if not cached:
            self._identity_scenario_id = None
            return False
        self.identities.capture(scenario_id, cached.token)
        self._last_subject = cached.subject
        self._identity_scenario_id = scenario_id
        return True

    async def restart(self) -> str:
        self.identities.clear()
        self._identity_scenario_id = None
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

    async def handover_operator(
        self, expected_version: int, new_operator_email: str, initiated_by: str
    ) -> str:
        async with self._operator_change_lock:
            scenario_id = await self.repository.handover_operator(
                expected_version, new_operator_email, initiated_by
            )
            for task in list(self._run_tasks):
                task.cancel()
            if self._run_tasks:
                await asyncio.gather(*self._run_tasks, return_exceptions=True)
            self._run_tasks.clear()
            self.identities.clear()
            self._identity_scenario_id = None
            self.bind_operator_identity(scenario_id, new_operator_email)
            return scenario_id

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
        started_at = utc_now()
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
        completed_at = utc_now()
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
                started_at=started_at,
                completed_at=completed_at,
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
                scenario_id = self._identity_scenario_id
                if not scenario_id or not self.identities.get(scenario_id):
                    continue
                async with self._operator_change_lock:
                    for incident_id, run_id in await self.repository.evaluate_thresholds(
                        scenario_id
                    ):
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
        run_started_at = utc_now()
        try:
            incident = await self.repository.incident(incident_id)
            if not incident:
                return
            identity = await self._wait_for_identity(incident.scenario_id)
            if not identity:
                raise RuntimeError("No in-memory OBO identity is available for this scenario")

            if not self.skill_gateway or not self.settings.governed_skill_tool:
                raise RuntimeError("Governed Skill Gateway configuration is missing")
            skill_started_at = utc_now()
            skill_response = await self.skill_gateway.call_tool(
                self.settings.governed_skill_tool, {}, identity.token
            )
            skill_completed_at = utc_now()
            skill_data = _structured(skill_response)
            skill_instructions = str(skill_data.get("result") or json.dumps(skill_data))
            await self.repository.append_activity(
                incident_id,
                RuntimeActivity(
                    category="skill",
                    title=self.settings.governed_skill_tool,
                    actor=self.settings.governed_skill_tool,
                    status="complete",
                    status_label="Loaded",
                    summary="Runtime loaded the current Unity Catalog response procedure.",
                    started_at=skill_started_at,
                    completed_at=skill_completed_at,
                ),
            )
            discovery_started_at = utc_now()
            tools = await self.gateway.list_tools(identity.token)
            discovery_completed_at = utc_now()
            available = {tool["name"]: tool for tool in tools}
            await self.repository.append_activity(
                incident_id,
                RuntimeActivity(
                    category="mcp",
                    title="Discover governed maintenance capabilities",
                    actor=self.settings.governed_mcp_endpoint,
                    status="complete",
                    status_label="Connected",
                    summary=f"Unity Gateway exposed {len(available)} governed tools at runtime.",
                    details=[ActivityDatum(label="Available", value=", ".join(sorted(available)))],
                    started_at=discovery_started_at,
                    completed_at=discovery_completed_at,
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
                        "when a tool was blocked. OUTPUT PROTOCOL IS STRICT: whenever you select "
                        "tools, assistant content must be exactly `Decision rationale: <one or "
                        "two concise sentences>` with no other prose. A terminal response must "
                        "contain exactly four Markdown bullets, in this exact order and with no "
                        "other prose: `- **Finding:** ...`, `- **Evidence:** ...`, "
                        "`- **Risk:** ...`, `- **Action:** ...`.\n\n"
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
                turn_started_at = utc_now()
                turn = await self.model.turn(endpoint, messages, tools=openai_tools)
                turn_completed_at = utc_now()
                tool_calls = turn.message.get("tool_calls", [])
                summary = str(turn.message.get("content") or "")
                input_tokens = turn.input_tokens
                output_tokens = turn.output_tokens
                canonical_rationale = _canonical_decision_rationale(summary) if tool_calls else None
                if tool_calls and not canonical_rationale:
                    rationale = await self.model.turn(
                        endpoint,
                        [
                            {
                                "role": "system",
                                "content": (
                                    "Return exactly `Decision rationale: <one or two concise "
                                    "sentences>` with no heading, bullets, or additional prose."
                                ),
                            },
                            {
                                "role": "user",
                                "content": (
                                    "Explain the evidence-based reason for the selected governed "
                                    "tool call(s). Selected calls: "
                                    f"{json.dumps(tool_calls)}. Recent context: "
                                    f"{json.dumps(messages[-3:])}"
                                ),
                            },
                        ],
                    )
                    summary = str(rationale.message.get("content") or "")
                    input_tokens += rationale.input_tokens
                    output_tokens += rationale.output_tokens
                    turn_completed_at = utc_now()
                    canonical_rationale = _canonical_decision_rationale(summary)
                    if not canonical_rationale:
                        raise RuntimeError(
                            "Model did not return a usable tool-selection rationale after "
                            "correction"
                        )
                if canonical_rationale:
                    summary = canonical_rationale
                await self.repository.append_activity(
                    incident_id,
                    RuntimeActivity(
                        category="model_call",
                        title="Model execution",
                        actor=endpoint,
                        status="complete",
                        status_label="Completed",
                        summary=summary,
                        details=[
                            ActivityDatum(label="Input tokens", value=str(input_tokens)),
                            ActivityDatum(label="Output tokens", value=str(output_tokens)),
                            ActivityDatum(label="Model output", value=summary),
                        ],
                        started_at=turn_started_at,
                        completed_at=turn_completed_at,
                    ),
                    tokens=(
                        input_tokens,
                        output_tokens,
                        self._model_cost(input_tokens, output_tokens),
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
                        final_content = await self._ensure_terminal_response(
                            incident_id, endpoint, messages, summary
                        )
                        await self.repository.append_conversation(
                            incident_id, "agent", final_content
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
                    tool_started_at = utc_now()
                    try:
                        result = await self.gateway.call_tool(tool_name, arguments, identity.token)
                    except GatewayAsk as ask:
                        await self._pause_for_gateway(
                            incident_id, tool_name, ask, started_at=tool_started_at
                        )
                        final_content = await self._ensure_terminal_response(
                            incident_id,
                            endpoint,
                            [
                                {
                                    "role": "system",
                                    "content": (
                                        "Return exactly four Markdown bullets in this order, "
                                        "with no other prose: `- **Finding:** ...`, "
                                        "`- **Evidence:** ...`, `- **Risk:** ...`, "
                                        "`- **Action:** ...`."
                                    ),
                                },
                                {
                                    "role": "user",
                                    "content": (
                                        f"Summarize incident {incident_id} for "
                                        f"{incident.asset_id}. "
                                        f"Observed condition: {incident.condition}. Risk evidence: "
                                        f"{json.dumps(last_risk or {})}. Unity Gateway returned "
                                        f"ASK for `{tool_name}`: {ask.elicitation.message}. The "
                                        "action has not executed and is waiting for human approval."
                                    ),
                                },
                            ],
                            "",
                        )
                        await self.repository.append_conversation(
                            incident_id, "agent", final_content
                        )
                        return
                    tool_completed_at = utc_now()
                    data = _structured(result)
                    await self.repository.append_activity(
                        incident_id,
                        RuntimeActivity(
                            category="tool_call",
                            title=tool_name,
                            actor=f"Unity Gateway · {tool_name}",
                            status="complete",
                            status_label="ALLOW",
                            summary=_result_summary(data),
                            details=[
                                ActivityDatum(label="Decision", value="ALLOW"),
                                ActivityDatum(label="Execution", value="Executed"),
                            ],
                            started_at=tool_started_at,
                            completed_at=tool_completed_at,
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
                        await self._complete_governed_action(
                            incident_id,
                            result,
                            started_at=tool_started_at,
                            completed_at=tool_completed_at,
                        )
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
                    started_at=run_started_at,
                    completed_at=utc_now(),
                ),
            )
            await self.repository.fail_incident(incident_id, str(exc))

    async def _wait_for_identity(self, scenario_id: str) -> OboIdentity | None:
        """Allow a local hot reload's next dashboard poll to restore process-local OBO state."""
        for _ in range(60):
            identity = self.identities.get(scenario_id)
            if identity:
                return identity
            await asyncio.sleep(0.5)
        return None

    async def _pause_for_gateway(
        self, incident_id: str, tool_name: str, ask: GatewayAsk, *, started_at: datetime
    ) -> None:
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
                title=tool_name,
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
                    endpoint=f"/api/incidents/{incident_id}/approve",
                ),
                started_at=started_at,
                completed_at=None,
            ),
            tool_call=True,
        )
        await self.repository.set_waiting(incident_id, pending)

    def _model_cost(self, input_tokens: int, output_tokens: int) -> float:
        return (
            input_tokens * self.settings.model_input_cost_per_million
            + output_tokens * self.settings.model_output_cost_per_million
        ) / 1_000_000

    async def _retry_pending(self, incident_id: str, pending: PendingApproval) -> bool:
        incident = await self.repository.incident(incident_id)
        if not incident or incident.status != "waiting":
            return True
        identity = self.identities.get(incident.scenario_id)
        if not identity:
            return False
        try:
            replay_started_at = utc_now()
            result = await self.gateway.replay(pending.serialized_request, identity.token)
        except GatewayAsk:
            return False
        replay_completed_at = utc_now()
        await self._complete_governed_action(
            incident_id,
            result,
            started_at=replay_started_at,
            completed_at=replay_completed_at,
        )
        return True

    async def _complete_governed_action(
        self,
        incident_id: str,
        result: dict[str, Any],
        *,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        data = _structured(result)
        work_order_id = data.get("work_order_id", "Work order")
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="tool_result",
                title="submit_work_order",
                actor="Unity Gateway · submit_work_order",
                status="complete",
                status_label="APPROVED · ALLOW",
                summary=f"{work_order_id} scheduled for the next available maintenance window.",
                details=[
                    ActivityDatum(label="Work order", value=str(work_order_id)),
                    ActivityDatum(label="Decision", value="ALLOW"),
                    ActivityDatum(label="Execution", value="Executed"),
                ],
                started_at=started_at,
                completed_at=completed_at,
            ),
        )
        await self.repository.complete_incident(incident_id, str(work_order_id))

    async def _ensure_terminal_response(
        self,
        incident_id: str,
        endpoint: str,
        messages: list[dict[str, Any]],
        content: str,
    ) -> str:
        canonical = _canonical_terminal_response(content)
        if canonical:
            return canonical
        correction_messages = [
            *messages,
            {
                "role": "user",
                "content": (
                    "Reformat the conclusion only. Return exactly four Markdown bullets in "
                    "this order, with no heading or other prose: `- **Finding:** ...`, "
                    "`- **Evidence:** ...`, `- **Risk:** ...`, `- **Action:** ...`."
                ),
            },
        ]
        started_at = utc_now()
        corrected = await self.model.turn(endpoint, correction_messages)
        completed_at = utc_now()
        corrected_content = str(corrected.message.get("content") or "")
        canonical = _canonical_terminal_response(corrected_content)
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="model_call",
                title="Model execution",
                actor=endpoint,
                status="complete",
                status_label="Completed",
                summary=canonical or corrected_content,
                details=[
                    ActivityDatum(label="Input tokens", value=str(corrected.input_tokens)),
                    ActivityDatum(label="Output tokens", value=str(corrected.output_tokens)),
                    ActivityDatum(label="Model output", value=canonical or corrected_content),
                ],
                started_at=started_at,
                completed_at=completed_at,
            ),
            tokens=(
                corrected.input_tokens,
                corrected.output_tokens,
                self._model_cost(corrected.input_tokens, corrected.output_tokens),
            ),
        )
        if not canonical:
            raise RuntimeError(
                "Model terminal output violated the response protocol after reformat"
            )
        return canonical


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


def _valid_decision_rationale(content: str) -> bool:
    if not content.startswith("Decision rationale: ") or "\n" in content:
        return False
    rationale = content.removeprefix("Decision rationale: ").strip()
    return bool(rationale) and sum(rationale.count(mark) for mark in ".!?") <= 2


def _canonical_decision_rationale(content: str) -> str | None:
    """Preserve model wording while enforcing the presentation-safe rationale envelope."""
    cleaned = re.sub(r"^```(?:markdown|text)?\s*|\s*```$", "", content.strip(), flags=re.I)
    if not cleaned:
        return None
    match = re.search(r"decision\s+rationale\s*:\s*", cleaned, flags=re.I)
    rationale = cleaned[match.end() :] if match else cleaned
    rationale = re.sub(r"^[\s#>*-]+", "", rationale)
    rationale = re.sub(r"\*\*|__|`", "", rationale)
    rationale = " ".join(rationale.split())
    if not rationale:
        return None
    sentences = re.split(r"(?<=[.!?])\s+", rationale)
    concise = " ".join(sentences[:2]).strip()
    return f"Decision rationale: {concise}" if concise else None


def _valid_terminal_response(content: str) -> bool:
    lines = content.splitlines()
    labels = ("Finding", "Evidence", "Risk", "Action")
    return len(lines) == 4 and all(
        line.startswith(f"- **{label}:** ") and bool(line.removeprefix(f"- **{label}:** ").strip())
        for line, label in zip(lines, labels, strict=True)
    )


def _canonical_terminal_response(content: str) -> str | None:
    """Extract the four required fields and render the exact presentation contract."""
    cleaned = re.sub(r"^```(?:markdown|text)?\s*|\s*```$", "", content.strip(), flags=re.I)
    if not cleaned:
        return None
    labels = ("Finding", "Evidence", "Risk", "Action")
    values: list[str] = []
    for index, label in enumerate(labels):
        next_label = labels[index + 1] if index + 1 < len(labels) else None
        boundary = (
            rf"(?=\s*(?:[-*]\s*)?\*{{0,2}}{next_label}:\*{{0,2}})"
            if next_label
            else r"$"
        )
        match = re.search(
            rf"(?:[-*]\s*)?\*{{0,2}}{label}:\*{{0,2}}\s*(.*?){boundary}",
            cleaned,
            flags=re.I | re.S,
        )
        if not match:
            return None
        value = " ".join(match.group(1).strip().split())
        if not value:
            return None
        values.append(value)
    return "\n".join(
        f"- **{label}:** {value}" for label, value in zip(labels, values, strict=True)
    )
