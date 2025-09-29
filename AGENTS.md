# Repository Guidelines

## Project Structure & Module Organization
- Code lives in `crates/` with co-located tests under `src/tests` or `#[cfg(test)]`; tooling scripts and runtime assets (Katana DBs, spawn bundles, binaries) sit in `scripts/`, `types-test-db*/`, `spawn-and-move-db*/`, and `bin/`.
- `crates/migrations/` tracks SQLite steps—run `sqlx migrate add <name> --source crates/migrations` and `sqlx migrate run` before committing.
- Docs and example data sit in `examples/`, `migrations/`, `docs/`, and `grafana/`.

## Key Crates Overview
- `indexer`: drives StarkNet block ingestion and dispatch to downstream services.
- `processors`: event processors mapping on-chain updates into records with metrics.
- `sqlite`: storage backend exposing pooled connections, migrations, and query helpers.
- `grpc`: gRPC facade sharing protobuf models from `crates/proto`.
- `server` & `graphql`: HTTP plus GraphQL surfaces, resolvers, and fixture-backed tests for querying indexed data.
- `runner` & `cli`: runtime orchestration plus configuration/bootstrap logic for the `torii` binary.

## Build, Test, and Development Commands
- `cargo build --workspace` or `cargo build --bin torii` use the pinned toolchain in `rust-toolchain.toml`.
- `bash scripts/extract_test_db.sh` inflates Katana fixtures into `tmp/`; after Cairo updates run `bash scripts/rebuild_test_artifacts.sh sozo katana`.
- `KATANA_RUNNER_BIN=katana cargo nextest run --all-features --workspace` mirrors CI; `docker-compose -f docker-compose.grafana.yml up -d` brings up Prometheus/Grafana during QA.

## Coding Style & Naming Conventions
- Run `bash scripts/rust_fmt.sh --fix`; we follow `rustfmt` defaults (4 spaces, trailing commas, grouped imports).
- Guard lints with `bash scripts/clippy.sh`; fix warnings rather than allowing them.
- Keep files snake_case, types UpperCamelCase, constants SCREAMING_SNAKE_CASE, and emit structured logs through `tracing`.

## Testing Guidelines
- Prefer `cargo nextest`; drop to `cargo test -p <crate>` when debugging locally.
- Reset Katana snapshots by clearing `tmp/` and rerunning the rebuild script.
- Name integration tests after behaviors (e.g., `sync_failure.rs`) and store shared payloads beneath `tests/fixtures`.

## Commit & Pull Request Guidelines
- Follow the conventional history style (`feat(scope): summary`, `chore: …`, optional PR number).
- Keep commits single-purpose with tests green; stage only in-scope files.
- Work on a feature branch (`git checkout -b <scope/topic>`), push to `origin`, open the PR via `gh pr create`, and self-review with comments before requesting review.
- Provide a concise description, linked issue, validation notes, and screenshots for Grafana/dashboard tweaks; flag migration or fixture impacts explicitly.

## Security & Configuration Tips
- Stick to versions pinned in `.tool-versions`, keep secrets out of the repo, pass config with env vars or `torii` CLI flags, and front exposed metrics with `metrics_proxy.py` or another proxy.
