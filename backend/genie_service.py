"""User-scoped adapter for the Databricks Genie Conversation API.

The command center's incident agent and the Fleet telemetry analyst are deliberately
independent.  This module owns only the latter: it sends natural-language questions to one
curated Genie space and normalizes the asynchronous Conversation API into a small browser
contract.

OBO credentials are request-scoped.  A :class:`WorkspaceClient` is constructed inside each
platform call and is never retained by the service or conversation registry.  Conversation
ownership is tracked in memory by authenticated user subject so one App user cannot replay a
conversation identifier observed from another user.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from databricks.sdk import WorkspaceClient
from pydantic import BaseModel, Field

TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED"}
STATUS_LABELS = {
    "SUBMITTED": "Submitted to Genie",
    "FILTERING_CONTEXT": "Selecting governed telemetry context",
    "ASKING_AI": "Interpreting the telemetry question",
    "EXECUTING_QUERY": "Running the generated SQL",
    "COMPLETED": "Answer ready",
    "FAILED": "Genie could not answer",
    "CANCELLED": "Request cancelled",
}


class GenieQuestion(BaseModel):
    """A new telemetry question or a follow-up in an owned conversation."""

    question: str = Field(min_length=1, max_length=2_000)
    conversation_id: str | None = Field(default=None, min_length=1, max_length=256)


class GenieSubmission(BaseModel):
    """Identifiers returned immediately after Genie accepts a question."""

    conversation_id: str
    message_id: str
    status: str = "SUBMITTED"
    status_label: str = STATUS_LABELS["SUBMITTED"]


class GenieQuery(BaseModel):
    """Inspectable SQL emitted by Genie for one answer attachment."""

    attachment_id: str
    title: str | None = None
    description: str | None = None
    sql: str


class GenieResultTable(BaseModel):
    """Bounded first result chunk returned for one generated query."""

    attachment_id: str
    columns: list[str]
    rows: list[list[str | None]]
    total_rows: int | None = None
    truncated: bool = False


class GenieMessageView(BaseModel):
    """Normalized status, answer, SQL, and result data for the browser."""

    conversation_id: str
    message_id: str
    status: str
    status_label: str
    terminal: bool
    answer: str | None = None
    requires_clarification: bool = False
    queries: list[GenieQuery] = Field(default_factory=list)
    results: list[GenieResultTable] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    suggested_questions: list[str] = Field(default_factory=list)
    error: str | None = None
    source_label: str
    generated_at: datetime | None = None


@dataclass(frozen=True)
class GenieCaller:
    """Stable caller identity derived from trusted Databricks proxy headers."""

    subject: str
    email: str | None
    display_name: str


@dataclass(frozen=True)
class SubmittedMessage:
    """Platform-neutral identifiers from a newly submitted Genie message."""

    conversation_id: str
    message_id: str


class GeniePlatformClient(Protocol):
    """Minimal synchronous platform boundary used by the async service."""

    def start(self, *, token: str, space_id: str, question: str) -> SubmittedMessage: ...

    def follow_up(
        self,
        *,
        token: str,
        space_id: str,
        conversation_id: str,
        question: str,
    ) -> SubmittedMessage: ...

    def get_message(
        self,
        *,
        token: str,
        space_id: str,
        conversation_id: str,
        message_id: str,
    ) -> Any: ...

    def get_query_result(
        self,
        *,
        token: str,
        space_id: str,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
    ) -> Any: ...


class DatabricksGenieClient:
    """Conversation API client that creates a fresh user-scoped SDK client per call."""

    def __init__(
        self,
        host: str,
        workspace_client_factory: Callable[..., WorkspaceClient] = WorkspaceClient,
    ) -> None:
        self._host = host
        self._workspace_client_factory = workspace_client_factory

    def _genie(self, token: str):
        # The returned SDK object lives only for the duration of the enclosing platform call.
        # Neither the token nor the client is placed in shared application state.
        return self._workspace_client_factory(host=self._host, token=token).genie

    def start(self, *, token: str, space_id: str, question: str) -> SubmittedMessage:
        wait = self._genie(token).start_conversation(
            space_id=space_id,
            content=question,
            enable_visualization=False,
        )
        return SubmittedMessage(
            conversation_id=wait.conversation_id,
            message_id=wait.message_id,
        )

    def follow_up(
        self,
        *,
        token: str,
        space_id: str,
        conversation_id: str,
        question: str,
    ) -> SubmittedMessage:
        wait = self._genie(token).create_message(
            space_id=space_id,
            conversation_id=conversation_id,
            content=question,
            enable_visualization=False,
        )
        return SubmittedMessage(
            conversation_id=conversation_id,
            message_id=wait.message_id,
        )

    def get_message(
        self,
        *,
        token: str,
        space_id: str,
        conversation_id: str,
        message_id: str,
    ) -> Any:
        return self._genie(token).get_message(space_id, conversation_id, message_id)

    def get_query_result(
        self,
        *,
        token: str,
        space_id: str,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
    ) -> Any:
        return self._genie(token).get_message_attachment_query_result(
            space_id,
            conversation_id,
            message_id,
            attachment_id,
        )


class ConversationRegistry:
    """Bounded in-memory ownership map for opaque Genie conversation identifiers.

    Databricks Apps currently runs this service as one Uvicorn process.  The bounded map avoids
    treating browser-provided conversation IDs as authorization while preventing unbounded
    growth across a long-running demo.  It intentionally stores identities and IDs only—never
    forwarded OAuth credentials.
    """

    def __init__(self, max_conversations: int = 512) -> None:
        self._max_conversations = max_conversations
        self._owners: OrderedDict[str, str] = OrderedDict()
        self._lock = asyncio.Lock()

    async def register(self, conversation_id: str, subject: str) -> None:
        async with self._lock:
            owner = self._owners.get(conversation_id)
            if owner is not None and owner != subject:
                raise PermissionError("Conversation belongs to another signed-in user")
            self._owners[conversation_id] = subject
            self._owners.move_to_end(conversation_id)
            while len(self._owners) > self._max_conversations:
                self._owners.popitem(last=False)

    async def require_owner(self, conversation_id: str, subject: str) -> None:
        async with self._lock:
            if self._owners.get(conversation_id) != subject:
                raise PermissionError("Conversation is not available to this signed-in user")
            self._owners.move_to_end(conversation_id)


class GenieService:
    """Async orchestration and response normalization for one telemetry-only Genie space."""

    def __init__(
        self,
        *,
        space_id: str,
        client: GeniePlatformClient,
        source_label: str = "Unity Catalog · pump_telemetry_history",
        registry: ConversationRegistry | None = None,
    ) -> None:
        if not space_id:
            raise ValueError("A Genie space ID is required")
        self._space_id = space_id
        self._client = client
        self._source_label = source_label
        self._registry = registry or ConversationRegistry()

    async def submit(
        self,
        *,
        caller: GenieCaller,
        token: str,
        question: GenieQuestion,
    ) -> GenieSubmission:
        """Submit a first question or an owned conversational follow-up."""

        content = question.question.strip()
        if not content:
            raise ValueError("Question cannot be blank")
        if question.conversation_id:
            await self._registry.require_owner(question.conversation_id, caller.subject)
            submitted = await asyncio.to_thread(
                self._client.follow_up,
                token=token,
                space_id=self._space_id,
                conversation_id=question.conversation_id,
                question=content,
            )
        else:
            submitted = await asyncio.to_thread(
                self._client.start,
                token=token,
                space_id=self._space_id,
                question=content,
            )
            await self._registry.register(submitted.conversation_id, caller.subject)
        return GenieSubmission(
            conversation_id=submitted.conversation_id,
            message_id=submitted.message_id,
        )

    async def message(
        self,
        *,
        caller: GenieCaller,
        token: str,
        conversation_id: str,
        message_id: str,
    ) -> GenieMessageView:
        """Poll a message and, once complete, retrieve bounded query result chunks."""

        await self._registry.require_owner(conversation_id, caller.subject)
        message = await asyncio.to_thread(
            self._client.get_message,
            token=token,
            space_id=self._space_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )
        status = _enum_value(getattr(message, "status", None)) or "SUBMITTED"
        queries, answer, clarification, suggestions = _attachments(message)
        results: list[GenieResultTable] = []
        warnings: list[str] = []
        if status == "COMPLETED":
            for query in queries:
                try:
                    result = await asyncio.to_thread(
                        self._client.get_query_result,
                        token=token,
                        space_id=self._space_id,
                        conversation_id=conversation_id,
                        message_id=message_id,
                        attachment_id=query.attachment_id,
                    )
                    results.append(_result_table(query.attachment_id, result))
                except Exception:
                    # A text answer and generated SQL remain useful if the optional result
                    # attachment cannot be fetched.  Never turn that partial success into a
                    # blank drawer or discard the inspectable query.
                    query_label = query.title or "a query"
                    warnings.append(
                        f"Tabular results are temporarily unavailable for {query_label}."
                    )

        error = _error_text(getattr(message, "error", None))
        return GenieMessageView(
            conversation_id=conversation_id,
            message_id=message_id,
            status=status,
            status_label=STATUS_LABELS.get(status, status.replace("_", " ").title()),
            terminal=status in TERMINAL_STATUSES,
            answer=answer,
            requires_clarification=clarification,
            queries=queries,
            results=results,
            warnings=warnings,
            suggested_questions=suggestions,
            error=error,
            source_label=self._source_label,
            generated_at=_timestamp(getattr(message, "last_updated_timestamp", None)),
        )


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    raw = getattr(value, "value", value)
    return str(raw)


def _attachments(message: Any) -> tuple[list[GenieQuery], str | None, bool, list[str]]:
    queries: list[GenieQuery] = []
    answer_parts: list[str] = []
    clarification_parts: list[str] = []
    suggestions: list[str] = []
    for attachment in getattr(message, "attachments", None) or []:
        attachment_id = getattr(attachment, "attachment_id", None)
        query = getattr(attachment, "query", None)
        sql = getattr(query, "query", None) if query else None
        if attachment_id and sql:
            queries.append(
                GenieQuery(
                    attachment_id=attachment_id,
                    title=getattr(query, "title", None),
                    description=getattr(query, "description", None),
                    sql=sql,
                )
            )
        text = getattr(attachment, "text", None)
        content = getattr(text, "content", None) if text else None
        purpose = _enum_value(getattr(text, "purpose", None)) if text else None
        if content:
            if purpose == "FOLLOW_UP_QUESTION":
                clarification_parts.append(content)
            else:
                answer_parts.append(content)
        suggested = getattr(attachment, "suggested_questions", None)
        suggestions.extend(getattr(suggested, "questions", None) or [])

    clarification = bool(clarification_parts)
    combined = answer_parts or clarification_parts
    return queries, "\n\n".join(combined) or None, clarification, suggestions


def _result_table(attachment_id: str, response: Any) -> GenieResultTable:
    statement = getattr(response, "statement_response", None)
    manifest = getattr(statement, "manifest", None) if statement else None
    schema = getattr(manifest, "schema", None) if manifest else None
    columns = [
        getattr(column, "name", None) or f"column_{index + 1}"
        for index, column in enumerate(getattr(schema, "columns", None) or [])
    ]
    data = getattr(statement, "result", None) if statement else None
    rows = [list(row) for row in (getattr(data, "data_array", None) or [])]
    total_rows = getattr(manifest, "total_row_count", None) if manifest else None
    truncated = bool(getattr(manifest, "truncated", False)) if manifest else False
    if total_rows is not None and len(rows) < total_rows:
        truncated = True
    return GenieResultTable(
        attachment_id=attachment_id,
        columns=columns,
        rows=rows,
        total_rows=total_rows,
        truncated=truncated,
    )


def _error_text(error: Any) -> str | None:
    if error is None:
        return None
    return getattr(error, "error", None) or getattr(error, "message", None) or str(error)


def _timestamp(milliseconds: int | None) -> datetime | None:
    if milliseconds is None:
        return None
    return datetime.fromtimestamp(milliseconds / 1_000, tz=UTC)
