---
name: torii-development
description: Contributor workflow for dojoengine/torii. Use when implementing or reviewing changes in Torii indexer, processors, GraphQL/gRPC surfaces, SQLite migrations, and CI-aligned tests.
---

# Torii Development

Use this skill to ship focused changes in `dojoengine/torii` with correct fixture handling and CI-like checks.

## Core Workflow

1. Identify impacted crates (`indexer`, `processors`, `sqlite`, `server`, `graphql`, `grpc`, `runner`, `cli`).
2. Prepare fixtures:
   - `bash scripts/extract_test_db.sh`
3. If Cairo artifacts changed, rebuild test assets:
   - `bash scripts/rebuild_test_artifacts.sh sozo katana`
4. Build and test:
   - `cargo build --workspace`
   - `KATANA_RUNNER_BIN=katana cargo nextest run --all-features --workspace`
   - Use `bash scripts/selective_test.sh <base_branch>` for scoped runs
5. Run formatting and lint checks:
   - `bash scripts/rust_fmt.sh --fix`
   - `bash scripts/clippy.sh`
6. If migrations changed, run and verify migration steps for `crates/migrations`.

## PR Checklist

- Include migration impact explicitly when applicable.
- Keep fixture updates in the same PR as related code changes.
- Document validation commands and outcomes.
