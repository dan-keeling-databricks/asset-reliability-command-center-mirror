"""Integrity checks for the one-time, text-readable validator evidence package."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence"
EXPECTED_FILES = {
    "README.md",
    "01_build_and_deployment.txt",
    "02_streaming_jobs.json",
    "03_lakeflow_pipeline.json",
    "04_unity_catalog_queries.md",
    "05_lakebase_queries.md",
    "06_agent_run.json",
    "07_gateway_approval.json",
    "08_genie_conversation.json",
    "09_app_health.json",
}
DEPLOYED_COMMIT = "940c383fa91f9bd98f5ca0994d370b820396e582"
SCENARIO_ID = "3bd336aa-d1a8-40b1-82f5-c175c1046975"
INCIDENT_ID = "13c50285-aba5-444c-b5b6-bba810f2a452"
RUN_ID = "74b9404f-f427-477f-a3ee-c0300efe7bdf"
ELICITATION_ID = "945eb42d-df89-43a4-883e-df753209cdf1"
WORK_ORDER_ID = "WO-35374"


def evidence_text() -> str:
    """Return the complete committed package for cross-file checks."""

    return "\n".join(path.read_text(encoding="utf-8") for path in EVIDENCE.iterdir())


def test_required_evidence_files_are_present_and_nonempty() -> None:
    """The validator must never receive a missing or empty promised artifact."""

    assert {path.name for path in EVIDENCE.iterdir() if path.is_file()} == EXPECTED_FILES
    for path in EVIDENCE.iterdir():
        if path.is_file():
            assert len(path.read_text(encoding="utf-8").strip()) >= 100, path.name


def test_json_evidence_is_valid() -> None:
    """Machine-readable captures must remain parseable after documentation edits."""

    for path in EVIDENCE.glob("*.json"):
        assert json.loads(path.read_text(encoding="utf-8")), path.name


def test_package_contains_no_template_placeholders() -> None:
    """Reject the future-tense placeholders that caused the original validator failure."""

    text = evidence_text()
    for marker in ("TODO", "TBD", "CAPTURE LATER", "REPLACE_ME", "<INSERT"):
        assert marker not in text.upper()


def test_correlation_identifiers_are_consistent() -> None:
    """The build, App, agent, Gateway, and data captures must describe one real run."""

    text = evidence_text()
    for value in (
        DEPLOYED_COMMIT,
        SCENARIO_ID,
        INCIDENT_ID,
        RUN_ID,
        ELICITATION_ID,
        WORK_ORDER_ID,
    ):
        assert text.count(value) >= 2, value

    agent = json.loads((EVIDENCE / "06_agent_run.json").read_text(encoding="utf-8"))
    approval = json.loads((EVIDENCE / "07_gateway_approval.json").read_text(encoding="utf-8"))
    assert agent["incident_id"] == approval["incident_id"] == INCIDENT_ID
    assert agent["run_id"] == approval["run_id"] == RUN_ID
    assert approval["initial_tool_response"]["elicitation_id"] == ELICITATION_ID
    assert approval["single_flight_replay"]["work_orders_created"] == [WORK_ORDER_ID]


def test_evidence_contains_no_credential_material_or_personal_email() -> None:
    """Keep proof useful while preventing credentials or personal identities entering Git."""

    text = evidence_text()
    prohibited = {
        "JWT": r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*\b",
        "bearer credential": r"(?i)authorization\s*:\s*bearer\s+[A-Za-z0-9._~-]{16,}",
        "client secret": r"(?i)client_secret\s*[:=]\s*[^\s\]}]+",
        "private key": r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
        "password DSN": r"(?i)postgres(?:ql)?://[^\s:@]+:[^\s@]+@",
        "personal email": r"\b(?!human@)[A-Za-z0-9._%+-]+@databricks\.com\b",
    }
    for label, pattern in prohibited.items():
        assert re.search(pattern, text) is None, label

