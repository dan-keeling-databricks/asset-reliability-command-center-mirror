"""Focused tests for OBO Genie routing, response normalization, and user isolation."""

from __future__ import annotations

import logging
from enum import Enum
from types import SimpleNamespace

from databricks.sdk.errors import DatabricksError
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.genie_router import create_genie_router
from backend.genie_service import DatabricksGenieClient, SubmittedMessage


class Status(Enum):
    SUBMITTED = "SUBMITTED"
    COMPLETED = "COMPLETED"


class Purpose(Enum):
    ANSWER = "TEXT_ATTACHMENT_PURPOSE_ANSWER"


class FakeGenieClient:
    """Records request-scoped tokens without placing them in application state."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.conversation_index = 0

    def start(self, *, token: str, space_id: str, question: str) -> SubmittedMessage:
        self.calls.append(("start", token))
        self.conversation_index += 1
        return SubmittedMessage(
            conversation_id=f"conversation-{self.conversation_index}",
            message_id=f"message-{self.conversation_index}",
        )

    def follow_up(
        self,
        *,
        token: str,
        space_id: str,
        conversation_id: str,
        question: str,
    ) -> SubmittedMessage:
        self.calls.append(("follow_up", token))
        return SubmittedMessage(conversation_id=conversation_id, message_id="message-follow-up")

    def get_message(
        self,
        *,
        token: str,
        space_id: str,
        conversation_id: str,
        message_id: str,
    ):
        self.calls.append(("get_message", token))
        return SimpleNamespace(
            conversation_id=conversation_id,
            message_id=message_id,
            status=Status.COMPLETED,
            last_updated_timestamp=1_782_000_000_000,
            error=None,
            attachments=[
                SimpleNamespace(
                    attachment_id="query-1",
                    query=SimpleNamespace(
                        title="Peak vibration",
                        description="Ranks pumps by maximum vibration.",
                        query="SELECT pump_id, max(vibration_mm_s) FROM telemetry GROUP BY 1",
                    ),
                    text=None,
                    suggested_questions=None,
                ),
                SimpleNamespace(
                    attachment_id="text-1",
                    query=None,
                    text=SimpleNamespace(
                        content="P-204 recorded the highest vibration.",
                        purpose=Purpose.ANSWER,
                    ),
                    suggested_questions=SimpleNamespace(
                        questions=["Compare P-204 with the fleet average."]
                    ),
                ),
            ],
        )

    def get_query_result(
        self,
        *,
        token: str,
        space_id: str,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
    ):
        self.calls.append(("get_query_result", token))
        return SimpleNamespace(
            statement_response=SimpleNamespace(
                manifest=SimpleNamespace(
                    schema=SimpleNamespace(
                        columns=[SimpleNamespace(name="pump_id"), SimpleNamespace(name="peak")]
                    ),
                    total_row_count=2,
                    truncated=False,
                ),
                result=SimpleNamespace(data_array=[["P-204", "5.2"], ["P-207", "4.6"]]),
            )
        )


def genie_client(fake: FakeGenieClient) -> TestClient:
    app = FastAPI()
    app.include_router(
        create_genie_router(
            space_id="space-telemetry",
            host="https://example.cloud.databricks.com",
            source_label="Unity Catalog · catalog.schema.pump_telemetry_history",
            platform_client=fake,
        )
    )
    return TestClient(app)


def user_headers(email: str, token: str = "obo-token") -> dict[str, str]:
    return {
        "X-Forwarded-User": f"subject:{email}",
        "X-Forwarded-Email": email,
        "X-Forwarded-Preferred-Username": email.split("@", 1)[0].title(),
        "X-Forwarded-Access-Token": token,
    }


def test_genie_sdk_client_selects_only_forwarded_token_auth() -> None:
    """App OAuth environment credentials must not compete with the user's OBO token."""

    calls: list[dict[str, str]] = []
    genie_api = object()

    def workspace_client_factory(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(genie=genie_api)

    client = DatabricksGenieClient(
        "https://example.cloud.databricks.com",
        workspace_client_factory=workspace_client_factory,
    )

    assert client._genie("request-scoped-obo-token") is genie_api
    assert calls == [
        {
            "host": "https://example.cloud.databricks.com",
            "token": "request-scoped-obo-token",
            "auth_type": "pat",
        }
    ]


def test_genie_requires_obo_token_and_reports_signed_in_identity() -> None:
    fake = FakeGenieClient()
    with genie_client(fake) as client:
        identity = client.get(
            "/api/genie/whoami",
            headers=user_headers("alice@databricks.com"),
        )
        assert identity.status_code == 200
        assert identity.json() == {
            "subject": "subject:alice@databricks.com",
            "email": "alice@databricks.com",
            "display_name": "Alice",
            "execution_mode": "on_behalf_of_user",
            "obo_ready": True,
        }

        without_token = user_headers("alice@databricks.com")
        without_token.pop("X-Forwarded-Access-Token")
        response = client.post(
            "/api/genie/messages",
            headers=without_token,
            json={"question": "Highest vibration?"},
        )
        assert response.status_code == 401
        assert fake.calls == []


def test_genie_returns_answer_query_results_and_source_provenance() -> None:
    fake = FakeGenieClient()
    headers = user_headers("alice@databricks.com", "alice-secret-token")
    with genie_client(fake) as client:
        submitted = client.post(
            "/api/genie/messages",
            headers=headers,
            json={"question": "Which pump had the highest vibration?"},
        )
        assert submitted.status_code == 202
        identifiers = submitted.json()

        completed = client.get(
            f"/api/genie/messages/{identifiers['message_id']}",
            params={"conversation_id": identifiers["conversation_id"]},
            headers=headers,
        )
        assert completed.status_code == 200
        body = completed.json()
        assert body["status"] == "COMPLETED"
        assert body["answer"] == "P-204 recorded the highest vibration."
        assert body["queries"][0]["sql"].startswith("SELECT pump_id")
        assert body["results"][0]["columns"] == ["pump_id", "peak"]
        assert body["results"][0]["rows"] == [["P-204", "5.2"], ["P-207", "4.6"]]
        assert body["suggested_questions"] == ["Compare P-204 with the fleet average."]
        assert body["source_label"].endswith("pump_telemetry_history")
        assert body["generated_at"] is not None
        assert fake.calls == [
            ("start", "alice-secret-token"),
            ("get_message", "alice-secret-token"),
            ("get_query_result", "alice-secret-token"),
        ]


def test_conversation_ids_are_isolated_by_authenticated_user() -> None:
    fake = FakeGenieClient()
    alice = user_headers("alice@databricks.com", "alice-token")
    bob = user_headers("bob@databricks.com", "bob-token")
    with genie_client(fake) as client:
        first = client.post(
            "/api/genie/messages",
            headers=alice,
            json={"question": "Fleet average vibration?"},
        ).json()

        bob_poll = client.get(
            f"/api/genie/messages/{first['message_id']}",
            params={"conversation_id": first["conversation_id"]},
            headers=bob,
        )
        assert bob_poll.status_code == 403

        bob_follow_up = client.post(
            "/api/genie/messages",
            headers=bob,
            json={
                "question": "Break that down by pump.",
                "conversation_id": first["conversation_id"],
            },
        )
        assert bob_follow_up.status_code == 403
        assert all(token != "bob-token" for _, token in fake.calls)


def test_follow_up_reuses_owned_conversation_while_new_question_starts_another() -> None:
    fake = FakeGenieClient()
    headers = user_headers("alice@databricks.com")
    with genie_client(fake) as client:
        first = client.post(
            "/api/genie/messages",
            headers=headers,
            json={"question": "Fleet vibration today?"},
        ).json()
        follow_up = client.post(
            "/api/genie/messages",
            headers=headers,
            json={
                "question": "Now compare bearing temperature.",
                "conversation_id": first["conversation_id"],
            },
        ).json()
        second = client.post(
            "/api/genie/messages",
            headers=headers,
            json={"question": "Motor current this week?"},
        ).json()

        assert follow_up["conversation_id"] == first["conversation_id"]
        assert second["conversation_id"] != first["conversation_id"]
        assert [call for call, _ in fake.calls] == ["start", "follow_up", "start"]


def test_answer_remains_available_when_tabular_result_fetch_is_partial() -> None:
    class PartialClient(FakeGenieClient):
        def get_query_result(self, **kwargs):
            raise RuntimeError("temporary result endpoint failure")

    fake = PartialClient()
    headers = user_headers("alice@databricks.com")
    with genie_client(fake) as client:
        submitted = client.post(
            "/api/genie/messages",
            headers=headers,
            json={"question": "Which pump had the highest vibration?"},
        ).json()
        completed = client.get(
            f"/api/genie/messages/{submitted['message_id']}",
            params={"conversation_id": submitted["conversation_id"]},
            headers=headers,
        )

        assert completed.status_code == 200
        body = completed.json()
        assert body["answer"] == "P-204 recorded the highest vibration."
        assert body["queries"]
        assert body["results"] == []
        assert body["warnings"] == [
            "Tabular results are temporarily unavailable for Peak vibration."
        ]


def test_genie_failure_logs_error_metadata_and_scopes_without_token(caplog) -> None:
    """Runtime diagnostics expose the platform response but never the OBO credential."""

    class FailingClient(FakeGenieClient):
        def start(self, **kwargs):
            token = kwargs["token"]
            raise DatabricksError(
                f"scope rejected for Bearer {token}",
                error_code="INSUFFICIENT_SCOPE",
            )

    token = (
        "eyJhbGciOiJub25lIn0."
        "eyJzY29wZSI6ImdlbmllIGlhbS5jdXJyZW50LXVzZXI6cmVhZCJ9."
        "diagnostic-signature"
    )
    with caplog.at_level(logging.ERROR, logger="backend.genie_router"):
        with genie_client(FailingClient()) as client:
            response = client.post(
                "/api/genie/messages",
                headers=user_headers("alice@databricks.com", token),
                json={"question": "Highest vibration?"},
            )

    assert response.status_code == 502
    assert "operation=submit_message" in caplog.text
    assert "exception_type=DatabricksError" in caplog.text
    assert "error_code=INSUFFICIENT_SCOPE" in caplog.text
    assert "genie" in caplog.text
    assert "iam.current-user:read" in caplog.text
    assert token not in caplog.text
    assert "diagnostic-signature" not in caplog.text
