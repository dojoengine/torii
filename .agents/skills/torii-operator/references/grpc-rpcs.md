# gRPC RPC Map

Service: `world.World`

Use this as a quick RPC inventory. For field-level detail, inspect:
- `crates/proto/proto/world.proto`
- `crates/proto/proto/types.proto`

## World metadata and entity/model APIs

- `Worlds`
- `RetrieveEntities`
- `SubscribeEntities`
- `UpdateEntitiesSubscription`
- `RetrieveEventMessages`
- `SubscribeEventMessages`
- `UpdateEventMessagesSubscription`
- `RetrieveEvents`
- `SubscribeEvents`
- `RetrieveControllers`
- `RetrieveContracts`

## Token and transfer APIs

- `RetrieveTokens`
- `SubscribeTokens`
- `UpdateTokensSubscription`
- `RetrieveTokenBalances`
- `SubscribeTokenBalances`
- `UpdateTokenBalancesSubscription`
- `RetrieveTokenTransfers`
- `SubscribeTokenTransfers`
- `UpdateTokenTransfersSubscription`
- `RetrieveTokenContracts`

## Transactions

- `RetrieveTransactions`
- `SubscribeTransactions`

## Aggregations, activity, achievements

- `RetrieveAggregations`
- `SubscribeAggregations`
- `UpdateAggregationsSubscription`
- `RetrieveActivities`
- `SubscribeActivities`
- `UpdateActivitiesSubscription`
- `RetrieveAchievements`
- `RetrievePlayerAchievements`
- `SubscribeAchievementProgressions`
- `UpdateAchievementProgressionsSubscription`

## Search and SQL

- `Search`
- `ExecuteSql`

## Offchain messaging

- `PublishMessage`
- `PublishMessageBatch`

