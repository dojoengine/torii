# Startup And Options

## Contents

1. Build and run
2. Runtime behavior that impacts debugging
3. Endpoint map (default ports)
4. Config file usage and precedence
5. CLI option groups and flags

## Build and run

```bash
cargo build --bin torii
```

Minimal run:

```bash
torii --world 0x... --rpc http://127.0.0.1:5050 --db-dir ./torii-data
```

Useful defaults and behavior:
- `--rpc` defaults to `http://0.0.0.0:5050`.
- `--db-dir` is optional. Without it, Torii uses a temporary DB file.
- `--world` is optional. If set, Torii also appends it to indexed contracts as `WORLD`.

## Runtime behavior that impacts debugging

- Provider spec check:
  Torii validates provider spec `0.9` on startup. If provider is not `v0.9`, startup fails.
- Contract deployment check:
  Enable `--runner.check_contracts` to fail fast when target contracts are undeployed.
- SQL endpoint guard:
  `--http.sql` controls both HTTP `/sql` and gRPC `ExecuteSql` availability.
- TLS options:
  Use `--http.tls_cert_path` and `--http.tls_key_path` for HTTPS.
  `--http.mkcert` can auto-generate local certs if `mkcert` exists.
- Search behavior:
  Search tuning flags are applied in SQLite storage (`max_results`, min query length, snippets, prefix matching).

## Endpoint map (default ports)

- HTTP proxy: `http://127.0.0.1:8080` (`--http.addr`, `--http.port`)
- GraphQL: proxied on `http://127.0.0.1:8080/graphql`
- SQL playground/query: `http://127.0.0.1:8080/sql` (when `--http.sql true`)
- MCP: `http://127.0.0.1:8080/mcp`
- gRPC: `127.0.0.1:50051` (`--grpc.addr`, `--grpc.port`)
- Metrics: `127.0.0.1:9200/metrics` when `--metrics`
- Relay:
  TCP/QUIC `9090`, WebRTC `9091`, WebSocket `9092` by default

## Config file usage and precedence

Use `--config /path/to/torii.toml`.

Rules:
- CLI flags override config file values.
- `--dump-config /path/to/out.toml` writes the effective config and exits through normal startup flow.
- `DOJO_WORLD_ADDRESS` can populate `--world`.

Example minimal config:

```toml
world_address = "0x..."
rpc = "http://127.0.0.1:5050"
db_dir = "./torii-data"

[indexing]
transactions = true

[sql]
historical = ["ns-MyModel"]
```

## CLI option groups and flags

Top-level:
- `--world`
- `--rpc`
- `--db-dir`
- `--config`
- `--dump-config`

Relay options:
- `--relay.port`
- `--relay.webrtc_port`
- `--relay.websocket_port`
- `--relay.local_key_path`
- `--relay.cert_path`
- `--relay.peers`

Indexing options:
- `--indexing.events_chunk_size`
- `--indexing.blocks_chunk_size`
- `--indexing.preconfirmed` (alias: `--indexing.pending`)
- `--indexing.polling_interval`
- `--indexing.max_concurrent_tasks`
- `--indexing.transactions`
- `--indexing.transaction_receipts`
- `--indexing.contracts`
- `--indexing.namespaces`
- `--indexing.models`
- `--indexing.world_block`
- `--indexing.controllers`
- `--indexing.strict_model_reader`
- `--indexing.batch_chunk_size`
- `--indexing.external_contracts`
- `--indexing.external_contract_whitelist`

Events indexing options:
- `--events.raw`

HTTP server options:
- `--http.addr`
- `--http.port`
- `--http.cors_origins`
- `--http.sql`
- `--http.tls_cert_path`
- `--http.tls_key_path`
- `--http.mkcert`

Metrics options:
- `--metrics`
- `--metrics.addr`
- `--metrics.port`

ERC options:
- `--erc.max_metadata_tasks`
- `--erc.token_attributes`
- `--erc.trait_counts`
- `--erc.metadata_updates`
- `--erc.metadata_update_whitelist`
- `--erc.metadata_update_blacklist`
- `--erc.metadata_updates_only_at_head`
- `--erc.async_metadata_updates`

Messaging options:
- `--messaging.max_age`
- `--messaging.future_tolerance`
- `--messaging.require_timestamp`

SQL options:
- `--sql.all_model_indices`
- `--sql.model_indices`
- `--sql.historical`
- `--sql.page_size`
- `--sql.cache_size`
- `--sql.hooks`
- `--sql.migrations`
- `--sql.aggregators`
- `--sql.wal_autocheckpoint`
- `--sql.wal_truncate_size_threshold`
- `--sql.optimize_interval`
- `--sql.busy_timeout`
- `--sql.acquire_timeout`
- `--sql.idle_timeout`
- `--sql.max_connections`
- `--sql.soft_memory_limit`
- `--sql.hard_memory_limit`
- `--sql.shared_cache`
- `--sql.temp_store`
- `--sql.mmap_size`
- `--sql.journal_size_limit`

Activity tracking options:
- `--activity.enabled`
- `--activity.session_timeout`
- `--activity.excluded_entrypoints`

Achievement tracking options:
- `--achievement.registration_model_name`
- `--achievement.progression_model_name`

Search API options:
- `--search.enabled`
- `--search.max_results`
- `--search.min_query_length`
- `--search.return_snippets`
- `--search.snippet_length`
- `--search.prefix_matching`

Snapshot options:
- `--snapshot.url`
- `--snapshot.version`

Runner options:
- `--runner.explorer`
- `--runner.check_contracts`
- `--runner.query_threads`
- `--runner.indexer_threads`
- `--runner.allocation_strategy`

gRPC options:
- `--grpc.addr`
- `--grpc.port`
- `--grpc.subscription_buffer_size`
- `--grpc.optimistic`
- `--grpc.tcp_keepalive_interval`
- `--grpc.http2_keepalive_interval`
- `--grpc.http2_keepalive_timeout`
- `--grpc.max_message_size`

