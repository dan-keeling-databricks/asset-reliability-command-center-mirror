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
    assert 'src="/architecture-tab.html#embed"' in app_source
    assert "<ArchitectureView fontScale={fontScale}" in app_source
    assert 'setProperty("--font-scale"' in app_source
