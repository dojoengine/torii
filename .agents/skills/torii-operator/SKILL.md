---
name: torii-operator
description: "Run Torii for Dojo/Starknet: configure startup and options, verify endpoints, and query indexed data via SQL, gRPC, GraphQL, MCP, static routes, and metrics."
---

# Torii Operator

Execute this workflow to run and query Torii with source-accurate behavior.

## Workflow

1. Identify the user intent.
- Start or tune indexer runtime.
- Query existing indexed data.
- Debug missing data, schema, endpoint, or performance issues.

2. Choose startup shape.
- Use `references/startup-and-options.md` for bootstrapping, config files, and flags.
- Favor explicit `--db-dir` for persistent state.
- Confirm provider spec compatibility before deep debugging.

3. Validate service surfaces after startup.
- Check HTTP root, GraphQL, SQL (if enabled), MCP, gRPC, and metrics (if enabled).
- Use endpoint checks from `references/querying-and-interfaces.md`.

4. Choose query interface by task.
- Use GraphQL for app-facing queries/subscriptions.
- Use gRPC for typed retrieval/subscription/search/offchain publish.
- Use SQL for direct investigation and schema-first debugging.
- Use MCP when an MCP client needs schema/query tools.

5. Query safely.
- Prefer read-only SQL for diagnostics.
- Use schema discovery before writing complex SQL.
- Treat `ExecuteSql` and `/sql` as privileged surfaces and confirm they are enabled.

6. Escalate to protocol-specific references.
- Startup and options: `references/startup-and-options.md`
- Interface usage and examples: `references/querying-and-interfaces.md`
- Full gRPC RPC map: `references/grpc-rpcs.md`

## Source Of Truth

- CLI and defaults: `crates/cli/src/args.rs`, `crates/cli/src/options.rs`
- Runtime wiring: `crates/runner/src/lib.rs`
- HTTP proxy/handlers: `crates/server/src/proxy.rs`, `crates/server/src/handlers/`
- GraphQL schema/runtime: `crates/graphql/src/schema.rs`, `crates/graphql/src/server.rs`
- gRPC API: `crates/proto/proto/world.proto`, `crates/proto/proto/types.proto`, `crates/grpc/server/src/lib.rs`
- Storage/search behavior: `crates/sqlite/sqlite/src/storage.rs`, `crates/migrations/`
