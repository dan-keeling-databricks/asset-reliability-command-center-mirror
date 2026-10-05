from backend.production_runtime import _valid_decision_rationale, _valid_terminal_response


def test_tool_selection_protocol_accepts_only_decision_rationale() -> None:
    assert _valid_decision_rationale(
        "Decision rationale: Current telemetry is required before assessing failure risk."
    )
    assert not _valid_decision_rationale("I will inspect telemetry.")
    assert not _valid_decision_rationale("Decision rationale: First. Second. Third.")
    assert not _valid_decision_rationale("Decision rationale: First.\nExtra prose")


def test_terminal_protocol_requires_exactly_four_ordered_markdown_bullets() -> None:
    valid = (
        "- **Finding:** Normal operating transient.\n"
        "- **Evidence:** Vibration returned to baseline.\n"
        "- **Risk:** Low near-term failure risk.\n"
        "- **Action:** No maintenance required."
    )

    assert _valid_terminal_response(valid)
    assert not _valid_terminal_response(valid.removeprefix("- "))
    assert not _valid_terminal_response(f"Conclusion\n{valid}")
    assert not _valid_terminal_response(valid.replace("Evidence", "Risk", 1))
