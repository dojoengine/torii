# Project Memory

- The CLI uses `clap` with `MergeOptions` to merge CLI args and config file settings.
- Prefer explicit flag naming (e.g., `sql_api_enabled` over `raw_sql`) to avoid ambiguity.
- Use `clap::ArgAction::Set` for boolean flags that accept explicit `true`/`false` values; avoid `SetTrue` in those cases.
- When a flag affects multiple transports (HTTP + gRPC), ensure the name/behavior applies consistently and is documented/tested.
