# Coding agent instructions

These instructions apply to the entire repository.

## Start here

Read `README.md`, `docs/ARCHITECTURE.md`, `docs/DEPLOYMENT.md`, `DEMO.md`, and `TASKS.md`
before changing behavior. Preserve the approved demo narrative and runtime-driven governance
principles unless the task explicitly changes them.

## Parallel work

- Every agent works in a separate Git worktree and a branch named
  `feat/{feature_name}`. Never share a writable worktree between agents.
- Before editing, run `git status --short`, confirm the branch, and agree file ownership with
  the coordinating agent. Split work by component whenever possible.
- Do not discard, reset, rewrite, or amend another agent's changes. Rebase or merge only after
  the owning agent has committed its work.
- Keep commits narrowly scoped. Record cross-component follow-ups in `TASKS.md` rather than
  making opportunistic changes in files owned by another task.

Example:

```bash
git fetch origin
git worktree add ../asset-reliability-command-center-my-feature \
  -b feat/my_feature origin/main
```

## Engineering constraints

- Use `uv` for all Python commands and `npm --prefix frontend` for frontend commands.
- Use Unity Catalog three-part names and the existing project catalog/schema variables.
- Do not use DBFS. Streaming checkpoints belong in the configured Unity Catalog volume.
- Keep Lakebase migrations idempotent and compatible with a live telemetry writer.
- Never hardcode model, skill, MCP server, policy, or tool-call activity in the UI. Render only
  runtime-emitted events.
- Unity Gateway is the sole approval authority. Do not simulate approval in application code.
- Never commit secrets, OAuth tokens, generated credentials, `.env`, local databases, build
  output, Databricks state, or browser artifacts.

## Required validation

Run these before handing work to the coordinating agent:

```bash
uv run ruff check backend tests src scripts
uv run pytest
npm --prefix frontend run typecheck
npm --prefix frontend run build
databricks bundle validate --target dev --profile fe_vm_rtm
```

For UI changes, validate locally and against the deployed Databricks App with browser console
and network inspection. Deployment remains manual and requires explicit task authorization.
