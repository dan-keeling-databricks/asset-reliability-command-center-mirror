"""Guardrails for Python module and Databricks notebook-source documentation."""

import ast
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = tuple(
    path
    for path in sorted((PROJECT_ROOT / "src").rglob("*.py"))
    if path.read_text().startswith("# Databricks notebook source\n")
)
PYTHON_MODULES = tuple(
    sorted(
        [
            *(PROJECT_ROOT / "backend").rglob("*.py"),
            *(PROJECT_ROOT / "scripts").rglob("*.py"),
            *(PROJECT_ROOT / "src").rglob("*.py"),
        ]
    )
)


@pytest.mark.parametrize(
    "path", PYTHON_MODULES, ids=lambda path: str(path.relative_to(PROJECT_ROOT))
)
def test_python_module_has_explanatory_docstring(path: Path) -> None:
    """Keep architectural intent visible without requiring notebook formatting."""

    module = ast.parse(path.read_text())
    assert ast.get_docstring(module), f"Missing module docstring: {path.relative_to(PROJECT_ROOT)}"


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda path: str(path.relative_to(PROJECT_ROOT)))
def test_notebook_code_cells_are_introduced_by_markdown(path: Path) -> None:
    """Require a descriptive Markdown cell immediately before every notebook code cell."""

    source = path.read_text()
    assert source.startswith("# Databricks notebook source\n")
    cells = source.split("# COMMAND ----------")
    assert "# MAGIC %md" in cells[0], f"Notebook must open with Markdown: {path.name}"

    for index, cell in enumerate(cells[1:], start=1):
        executable_lines = [
            line
            for line in cell.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        if not executable_lines:
            continue
        assert "# MAGIC %md" in cells[index - 1], (
            f"Code cell {index} in {path.relative_to(PROJECT_ROOT)} needs preceding Markdown"
        )
