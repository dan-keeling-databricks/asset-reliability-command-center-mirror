#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

trap 'kill 0' EXIT
uv run uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload &
npm --prefix frontend run dev
