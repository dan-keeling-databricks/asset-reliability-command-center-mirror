"""Contract tests for reviewed static frontend assets."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_reviewed_architecture_is_the_app_asset() -> None:
    """Keep the approved review artifact identical to the HTML rendered by the App."""

    review_html = (ROOT / "docs/design/architecture-tab-prototype.html").read_text()
    app_html = (ROOT / "frontend/public/architecture-tab.html").read_text()

    assert app_html == review_html
    assert "lakehouse-evidence" not in app_html
    app_source = (ROOT / "frontend/src/App.tsx").read_text()
    assert "/architecture-tab.html?build=${encodeURIComponent(cacheKey)}#embed" in app_source
    assert (
        'jsonRequest<{ git_commit: string }>("/api/build-info", { cache: "no-store" })'
        in app_source
    )
    assert "<ArchitectureView fontScale={fontScale}" in app_source
    assert 'setProperty("--font-scale"' in app_source


def test_submit_work_order_card_hides_redundant_details() -> None:
    """Keep audit details in the API while omitting reviewed card repetition."""

    app_source = (ROOT / "frontend/src/App.tsx").read_text()
    styles = (ROOT / "frontend/src/styles.css").read_text()

    assert '=== "submit_work_order"' in app_source
    assert '...(isSubmitWorkOrder ? ["rationale", "planned window"] : [])' in app_source
    assert ".approval-context + .gateway-policy { margin-top: 14px; }" in styles


def test_architecture_review_defaults_to_130_percent_without_nested_app_frame() -> None:
    """Keep the review artifact faithful to the approved enlarged architecture."""

    app_html = (ROOT / "frontend/public/architecture-tab.html").read_text()

    assert "--font-scale: 1.3;" in app_html
    assert "app-frame-inner" not in app_html
    assert "grid-template-rows: auto auto minmax(0, 1fr);" in app_html
    assert "align-self: center;" in app_html
    assert "Asset Reliability Command Centre" in app_html


def test_architecture_connectors_match_reviewed_data_flow() -> None:
    """Lock the simplified labels and reviewed connector geometry."""

    app_html = (ROOT / "frontend/public/architecture-tab.html").read_text()

    assert "App API" not in app_html
    assert "governed access" not in app_html.lower()
    assert "Lakebase Change Data Feed" not in app_html
    assert 'id="arrow-green"' in app_html
    assert 'orient="auto-start-reverse"' in app_html
    assert '.flow-bidirectional { marker-start: url(#arrow-green); }' in app_html
    assert (
        '<path class="flow flow-green flow-bidirectional" d="M638 295 V400"/>'
        in app_html
    )
    assert '<path class="flow flow-violet" d="M1031 295 V393"/>' in app_html
    assert "M1031 279 V393" not in app_html
    assert "M839 437 H770 V465 H733" not in app_html


def test_reviewed_ui_copy_and_vertical_incident_space_are_preserved() -> None:
    """Lock the British UI copy, emphasized title, and enlarged review modal."""

    app_source = (ROOT / "frontend/src/App.tsx").read_text()
    styles = (ROOT / "frontend/src/styles.css").read_text()
    index_html = (ROOT / "frontend/index.html").read_text()

    assert "Circulatory pump assets" in app_source
    assert "Comparable pump assets" not in app_source
    assert "asset <strong>risk</strong>" in app_source
    assert "autonomous <strong>response</strong>" in app_source
    assert "operational <strong>evidence</strong>" in app_source
    assert "Command Center" not in app_source
    assert "Asset Reliability Command Centre" in index_html
    assert "margin: 0 0 24px 13px" in styles
    assert "font-size: calc(20px * var(--font-scale))" in styles
    assert "height: min(calc(50vh + 410px), calc(100vh - 20px))" in styles
