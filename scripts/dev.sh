#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

backend_port="${BACKEND_PORT:-8000}"
export FRONTEND_PORT="${FRONTEND_PORT:-5173}"
export VITE_BACKEND_URL="http://127.0.0.1:$backend_port"

trap 'kill 0' EXIT
uv run uvicorn backend.main:app --host 127.0.0.1 --port "$backend_port" --reload &
npm --prefix frontend run dev
