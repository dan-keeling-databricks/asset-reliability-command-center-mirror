# Contributing

## Branching model

`main` is the shared integration branch. All new work—including fixes—is developed on a
branch named `feat/{feature_name}`, using lowercase snake case for the feature name. Do not
develop directly on `main`.

For parallel agent work, create one worktree per branch:

```bash
git fetch origin
git worktree add ../asset-reliability-command-center-latency \
  -b feat/lakebase_latency origin/main
```

Keep branches small enough to review. Avoid two branches editing the same files concurrently;
when that is unavoidable, agree ownership and integration order first.

## Development workflow

1. Read `AGENTS.md`, `docs/ARCHITECTURE.md`, `DEMO.md`, and `TASKS.md`.
2. Create a `feat/{feature_name}` branch and isolated worktree.
3. Implement locally and add focused tests.
4. Run the complete validation suite documented in `AGENTS.md`.
5. Update architecture, deployment, demo, or task documentation when behavior changes.
6. Commit with a concise imperative subject and push the feature branch.
7. Review and merge into `main` only after local and deployed validation evidence is recorded.

## Deployment policy

There is no CI/CD integration. A merge or push does not deploy anything. An authorized
operator follows `docs/DEPLOYMENT.md`, records resource/run identifiers, and validates the
remote App before considering a change complete.
