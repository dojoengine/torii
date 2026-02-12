# Querying And Interfaces

## Contents

1. Quick health checks
2. SQL over HTTP
3. GraphQL
4. gRPC
5. MCP endpoint
6. Static and metadata endpoints
7. Metrics

## Quick health checks

```bash
curl -s http://127.0.0.1:8080/
```

Expected shape:
- JSON with `service: "torii"` and `version`.

Check GraphQL route:

```bash
curl -i http://127.0.0.1:8080/graphql
```

Check gRPC reflection:

```bash
grpcurl -plaintext 127.0.0.1:50051 list
grpcurl -plaintext 127.0.0.1:50051 describe world.World
```

## SQL over HTTP

HTTP SQL is available only if `--http.sql true` (default true).

Playground:

```bash
open http://127.0.0.1:8080/sql
```

GET query:

```bash
curl -sG http://127.0.0.1:8080/sql \
  --data-urlencode 'query=SELECT id, world_address, created_at FROM entities LIMIT 5;'
```

POST query body:

```bash
curl -s http://127.0.0.1:8080/sql \
  -X POST \
  --data 'SELECT id, transaction_hash, executed_at FROM events ORDER BY executed_at DESC LIMIT 10;'
```

## GraphQL

Endpoint: `http://127.0.0.1:8080/graphql`

Common root query fields:
- `entity`, `entities`
- `eventMessage`, `eventMessages`
- `event`, `events`
- `model`, `models`
- `transaction`, `transactions`
- `controller`, `controllers`
- `tokenBalances`, `tokenTransfers`, `tokens`
- `metadata`, `metadatas`
- Dynamic model resolvers generated from indexed model schema

Mutation:
- `publishMessage(worldAddress, signature, message)`

Subscriptions:
- `entityUpdated`
- `eventMessageUpdated`
- `eventEmitted`
- `modelRegistered`
- `tokenBalanceUpdated`
- `tokenUpdated`
- `transaction`

Example query:

```bash
curl -s http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  -d '{
    "query":"query { entities(first: 3) { totalCount edges { node { id keys } } } }"
  }'
```

## gRPC

Endpoint: `127.0.0.1:50051`

List and inspect:

```bash
grpcurl -plaintext 127.0.0.1:50051 list world.World
grpcurl -plaintext 127.0.0.1:50051 describe world.World
```

Example calls:

```bash
grpcurl -plaintext -d '{}' 127.0.0.1:50051 world.World/Worlds

grpcurl -plaintext -d '{
  "query": "SELECT id, world_address FROM entities LIMIT 5;"
}' 127.0.0.1:50051 world.World/ExecuteSql

grpcurl -plaintext -d '{
  "query": { "query": "dragon", "limit": 10 }
}' 127.0.0.1:50051 world.World/Search
```

Notes:
- `ExecuteSql` is blocked when `--http.sql false`.
- gRPC supports compressed messages and gRPC-Web in server config.
- Use `torii-grpc-client` or `torii-client` crates for typed Rust integration.

## MCP endpoint

Routes:
- WebSocket/SSE entry: `/mcp`
- SSE message endpoint: `/mcp/message?sessionId=...`

Built-in tools:
- `query` tool: run SQL and return JSON text payload
- `schema` tool: return table/column schema (`sqlite_master` + `pragma_table_info`)

Use MCP for agent-driven DB inspection when raw SQL schema discovery is needed.

## Static and metadata endpoints

Static token/contract image serving:
- `/static/{contract_address}/image`
- `/static/{contract_address}/{token_id}/image`

Optional image sizing query params:
- `?h=...&w=...` (aliases: `height`, `width`)

Metadata reindex endpoint:
- `/metadata/reindex/{contract_address}/{token_id}`

Use metadata reindex to force refresh token metadata from chain and persist updates.

## Metrics

Enable metrics:

```bash
torii --metrics --metrics.addr 0.0.0.0 --metrics.port 9200
```

Query:

```bash
curl -s http://127.0.0.1:9200/metrics | head
```

For Grafana/Prometheus setup, use:
- `docker-compose -f docker-compose.grafana.yml up -d`
- `docs/grafana-setup.md`

