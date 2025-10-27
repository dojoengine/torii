pub mod error;

use crypto_bigint::U256;
use starknet::core::types::Felt;
use torii_grpc_client::{
    AchievementProgressionUpdateStreaming, ActivityUpdateStreaming, AggregationUpdateStreaming,
    ContractUpdateStreaming, EntityUpdateStreaming, EventUpdateStreaming, TokenBalanceStreaming,
    TokenTransferUpdateStreaming, TokenUpdateStreaming, TransactionUpdateStreaming, WorldClient,
};
use torii_proto::proto::world::{
    RetrieveAchievementsResponse, RetrieveActivitiesResponse, RetrieveAggregationsResponse,
    RetrieveContractsResponse, RetrieveControllersResponse, RetrieveEntitiesResponse,
    RetrieveEventsResponse, RetrievePlayerAchievementsResponse, RetrieveTokenBalancesResponse,
    RetrieveTokenContractsResponse, RetrieveTokenTransfersResponse, RetrieveTokensResponse,
    RetrieveTransactionsResponse,
};
use torii_proto::schema::Entity;
use torii_proto::{
    Achievement, AchievementQuery, Activity, ActivityQuery, AggregationEntry, AggregationQuery,
    Clause, Contract, ContractQuery, Controller, ControllerQuery, Event, EventQuery, KeysClause,
    Message, Page, PlayerAchievementEntry, PlayerAchievementQuery, Query, SearchQuery,
    SearchResponse, SqlRow, Token, TokenBalance, TokenBalanceQuery, TokenContract,
    TokenContractQuery, TokenQuery, TokenTransfer, TokenTransferQuery, Transaction,
    TransactionFilter, TransactionQuery, World,
};

use crate::error::Error;

#[allow(unused)]
#[derive(Debug)]
pub struct Client {
    /// The grpc client.
    inner: WorldClient,
}

impl Client {
    /// Returns a initialized [Client] with default max message size (4MB).
    pub async fn new(torii_url: String) -> Result<Self, Error> {
        let grpc_client = WorldClient::new(torii_url).await?;

        Ok(Self { inner: grpc_client })
    }

    /// Returns a initialized [Client] with custom max message size.
    ///
    /// # Arguments
    /// * `torii_url` - The URL of the Torii server
    /// * `max_message_size` - Maximum size in bytes for gRPC messages (both incoming and outgoing)
    pub async fn new_with_config(
        torii_url: String,
        max_message_size: usize,
    ) -> Result<Self, Error> {
        let grpc_client = WorldClient::new_with_config(torii_url, max_message_size).await?;

        Ok(Self { inner: grpc_client })
    }

    /// Publishes an offchain message to the world.
    /// Returns the entity id of the offchain message.
    pub async fn publish_message(&self, message: Message) -> Result<String, Error> {
        let mut grpc_client = self.inner.clone();
        let entity_id = grpc_client.publish_message(message).await?;
        Ok(entity_id)
    }

    /// Publishes a set of offchain messages to the world.
    /// Returns the entity ids of the offchain messages.
    pub async fn publish_message_batch(
        &self,
        messages: Vec<Message>,
    ) -> Result<Vec<String>, Error> {
        let mut grpc_client = self.inner.clone();
        let entity_ids = grpc_client.publish_message_batch(messages).await?;
        Ok(entity_ids)
    }

    /// Returns a read lock on the World metadata that the client is connected to.
    pub async fn worlds(&self, world_addresses: Vec<Felt>) -> Result<Vec<World>, Error> {
        let mut grpc_client = self.inner.clone();
        let worlds = grpc_client.worlds(world_addresses).await?;
        Ok(worlds)
    }

    /// Retrieves controllers matching contract addresses.
    pub async fn controllers(&self, query: ControllerQuery) -> Result<Page<Controller>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveControllersResponse {
            controllers,
            next_cursor,
        } = grpc_client.retrieve_controllers(query).await?;
        Ok(Page {
            items: controllers
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<Controller>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Retrieves contracts matching the query parameters.
    pub async fn contracts(&self, query: ContractQuery) -> Result<Vec<Contract>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveContractsResponse { contracts } = grpc_client.retrieve_contracts(query).await?;
        Ok(contracts
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<Contract>, _>>()?)
    }

    /// Retrieves tokens matching contract addresses.
    pub async fn tokens(&self, query: TokenQuery) -> Result<Page<Token>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveTokensResponse {
            tokens,
            next_cursor,
        } = grpc_client.retrieve_tokens(query).await?;
        Ok(Page {
            items: tokens
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<Token>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Retrieves token balances for account addresses and contract addresses.
    pub async fn token_balances(
        &self,
        query: TokenBalanceQuery,
    ) -> Result<Page<TokenBalance>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveTokenBalancesResponse {
            balances,
            next_cursor,
        } = grpc_client.retrieve_token_balances(query).await?;
        Ok(Page {
            items: balances
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<TokenBalance>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Retrieves token contracts matching the query parameters.
    pub async fn token_contracts(
        &self,
        query: TokenContractQuery,
    ) -> Result<Page<TokenContract>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveTokenContractsResponse {
            token_contracts,
            next_cursor,
        } = grpc_client.retrieve_token_contracts(query).await?;
        Ok(Page {
            items: token_contracts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<TokenContract>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Retrieves token transfers matching query parameter.
    pub async fn token_transfers(
        &self,
        query: TokenTransferQuery,
    ) -> Result<Page<TokenTransfer>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveTokenTransfersResponse {
            transfers,
            next_cursor,
        } = grpc_client.retrieve_token_transfers(query).await?;
        Ok(Page {
            items: transfers
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<TokenTransfer>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Retrieves transactions matching query parameter.
    pub async fn transactions(&self, query: TransactionQuery) -> Result<Page<Transaction>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveTransactionsResponse {
            transactions,
            next_cursor,
        } = grpc_client.retrieve_transactions(query).await?;
        Ok(Page {
            items: transactions
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<Transaction>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Retrieves aggregations (leaderboards, stats, rankings) matching query parameter.
    pub async fn aggregations(
        &self,
        query: AggregationQuery,
    ) -> Result<Page<AggregationEntry>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveAggregationsResponse {
            entries,
            next_cursor,
        } = grpc_client.retrieve_aggregations(query).await?;
        Ok(Page {
            items: entries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<AggregationEntry>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Retrieves activities (user session tracking) matching query parameter.
    pub async fn activities(&self, query: ActivityQuery) -> Result<Page<Activity>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveActivitiesResponse {
            activities,
            next_cursor,
        } = grpc_client.retrieve_activities(query).await?;
        Ok(Page {
            items: activities
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<Activity>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Subscribe to activity updates (user session tracking).
    /// If no world_addresses are provided, it will subscribe to updates for all worlds.
    /// If no namespaces are provided, it will subscribe to updates for all namespaces.
    /// If no caller_addresses are provided, it will subscribe to updates for all callers.
    pub async fn on_activity_updated(
        &self,
        world_addresses: Vec<Felt>,
        namespaces: Vec<String>,
        caller_addresses: Vec<Felt>,
    ) -> Result<ActivityUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_activities(world_addresses, namespaces, caller_addresses)
            .await?;
        Ok(stream)
    }

    /// Update an activities subscription
    pub async fn update_activity_subscription(
        &self,
        subscription_id: u64,
        world_addresses: Vec<Felt>,
        namespaces: Vec<String>,
        caller_addresses: Vec<Felt>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_activities_subscription(
                subscription_id,
                world_addresses,
                namespaces,
                caller_addresses,
            )
            .await?;
        Ok(())
    }

    /// Retrieves achievements matching query parameter.
    pub async fn achievements(&self, query: AchievementQuery) -> Result<Page<Achievement>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveAchievementsResponse {
            achievements,
            next_cursor,
        } = grpc_client.retrieve_achievements(query).await?;
        Ok(Page {
            items: achievements
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<Achievement>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Retrieves player achievement data matching query parameter.
    pub async fn player_achievements(
        &self,
        query: PlayerAchievementQuery,
    ) -> Result<Page<PlayerAchievementEntry>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrievePlayerAchievementsResponse {
            players,
            next_cursor,
        } = grpc_client.retrieve_player_achievements(query).await?;
        Ok(Page {
            items: players
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<PlayerAchievementEntry>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Subscribe to achievement progression updates.
    /// If no world_addresses are provided, it will subscribe to updates for all worlds.
    /// If no namespaces are provided, it will subscribe to updates for all namespaces.
    /// If no player_addresses are provided, it will subscribe to updates for all players.
    /// If no achievement_ids are provided, it will subscribe to updates for all achievements.
    pub async fn on_achievement_progression_updated(
        &self,
        world_addresses: Vec<Felt>,
        namespaces: Vec<String>,
        player_addresses: Vec<Felt>,
        achievement_ids: Vec<String>,
    ) -> Result<AchievementProgressionUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_achievement_progressions(
                world_addresses,
                namespaces,
                player_addresses,
                achievement_ids,
            )
            .await?;
        Ok(stream)
    }

    /// Update an achievement progressions subscription
    pub async fn update_achievement_progression_subscription(
        &self,
        subscription_id: u64,
        world_addresses: Vec<Felt>,
        namespaces: Vec<String>,
        player_addresses: Vec<Felt>,
        achievement_ids: Vec<String>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_achievement_progressions_subscription(
                subscription_id,
                world_addresses,
                namespaces,
                player_addresses,
                achievement_ids,
            )
            .await?;
        Ok(())
    }

    /// Subscribe to aggregation updates (leaderboards, stats, rankings).
    /// If no aggregator_ids are provided, it will subscribe to updates for all aggregators.
    /// If no entity_ids are provided, it will subscribe to updates for all entities.
    pub async fn on_aggregation_updated(
        &self,
        aggregator_ids: Vec<String>,
        entity_ids: Vec<String>,
    ) -> Result<AggregationUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_aggregations(aggregator_ids, entity_ids)
            .await?;
        Ok(stream)
    }

    /// Update an aggregations subscription
    pub async fn update_aggregation_subscription(
        &self,
        subscription_id: u64,
        aggregator_ids: Vec<String>,
        entity_ids: Vec<String>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_aggregations_subscription(subscription_id, aggregator_ids, entity_ids)
            .await?;
        Ok(())
    }

    /// A direct stream to grpc subscribe transactions
    pub async fn on_transaction(
        &self,
        filter: Option<TransactionFilter>,
    ) -> Result<TransactionUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client.subscribe_transactions(filter).await?;
        Ok(stream)
    }

    /// Retrieves entities matching query parameter.
    ///
    /// The query param includes an optional clause for filtering. Without clause, it fetches ALL
    /// entities, this is less efficient as it requires an additional query for each entity's
    /// model data. Specifying a clause can optimize the query by limiting the retrieval to specific
    /// type of entites matching keys and/or models.
    pub async fn entities(&self, query: Query) -> Result<Page<Entity>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveEntitiesResponse {
            entities,
            next_cursor,
        } = grpc_client.retrieve_entities(query).await?;
        Ok(Page {
            items: entities
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<Entity>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Similary to entities, this function retrieves event messages matching the query parameter.
    pub async fn event_messages(&self, query: Query) -> Result<Page<Entity>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveEntitiesResponse {
            entities,
            next_cursor,
        } = grpc_client.retrieve_event_messages(query).await?;
        Ok(Page {
            items: entities
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<Entity>, _>>()?,
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// Retrieve raw starknet events matching the keys provided.
    /// If the keys are empty, it will return all events.
    pub async fn starknet_events(&self, query: EventQuery) -> Result<Page<Event>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveEventsResponse {
            events,
            next_cursor,
        } = grpc_client.retrieve_events(query).await?;
        Ok(Page {
            items: events.into_iter().map(Event::from).collect::<Vec<Event>>(),
            next_cursor: if next_cursor.is_empty() {
                None
            } else {
                Some(next_cursor)
            },
        })
    }

    /// A direct stream to grpc subscribe entities
    pub async fn on_entity_updated(
        &self,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) -> Result<EntityUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_entities(clause, world_addresses)
            .await?;
        Ok(stream)
    }

    /// Update the entities subscription
    pub async fn update_entity_subscription(
        &self,
        subscription_id: u64,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_entities_subscription(subscription_id, clause, world_addresses)
            .await?;
        Ok(())
    }

    /// A direct stream to grpc subscribe event messages
    pub async fn on_event_message_updated(
        &self,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) -> Result<EntityUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_event_messages(clause, world_addresses)
            .await?;
        Ok(stream)
    }

    /// Update the event messages subscription
    pub async fn update_event_message_subscription(
        &self,
        subscription_id: u64,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_event_messages_subscription(subscription_id, clause, world_addresses)
            .await?;
        Ok(())
    }

    /// A direct stream to grpc subscribe starknet events
    pub async fn on_starknet_event(
        &self,
        keys: Vec<KeysClause>,
    ) -> Result<EventUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client.subscribe_events(keys).await?;
        Ok(stream)
    }

    /// Subscribe to indexer updates for a specific contract address.
    /// If no contract address is provided, it will subscribe to updates for world contract.
    pub async fn on_contract_updated(
        &self,
        contract_address: Option<Felt>,
    ) -> Result<ContractUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_contracts(ContractQuery {
                contract_addresses: contract_address.map(|c| vec![c]).unwrap_or_default(),
                contract_types: vec![],
            })
            .await?;
        Ok(stream)
    }

    /// Subscribes to token balances updates.
    /// If no contract addresses are provided, it will subscribe to updates for all contract
    /// addresses. If no account addresses are provided, it will subscribe to updates for all
    /// account addresses.
    pub async fn on_token_balance_updated(
        &self,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<TokenBalanceStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_token_balances(contract_addresses, account_addresses, token_ids)
            .await?;
        Ok(stream)
    }

    /// Update the token balances subscription
    pub async fn update_token_balance_subscription(
        &self,
        subscription_id: u64,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_token_balances_subscription(
                subscription_id,
                contract_addresses,
                account_addresses,
                token_ids,
            )
            .await?;
        Ok(())
    }

    /// A direct stream to grpc subscribe tokens
    pub async fn on_token_updated(
        &self,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<TokenUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_tokens(contract_addresses, token_ids)
            .await?;
        Ok(stream)
    }

    /// Update the tokens subscription
    pub async fn update_token_subscription(
        &self,
        subscription_id: u64,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_tokens_subscription(subscription_id, contract_addresses, token_ids)
            .await?;
        Ok(())
    }

    /// A direct stream to grpc subscribe token transfers
    pub async fn on_token_transfer_updated(
        &self,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<TokenTransferUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_token_transfers(contract_addresses, account_addresses, token_ids)
            .await?;
        Ok(stream)
    }

    /// Update the token transfers subscription
    pub async fn update_token_transfer_subscription(
        &self,
        subscription_id: u64,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_token_transfers_subscription(
                subscription_id,
                contract_addresses,
                account_addresses,
                token_ids,
            )
            .await?;
        Ok(())
    }

    /// Execute a SQL query against the Torii database.
    ///
    /// # Arguments
    /// * `query` - The SQL query string to execute
    ///
    /// # Returns
    /// A vector of `SqlRow` results
    ///
    /// # Example
    /// ```no_run
    /// # use torii_client::Client;
    /// # use starknet::core::types::Felt;
    /// # use torii_proto::SqlValue;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new("http://localhost:8080".to_string(), Felt::ZERO).await?;
    /// let rows = client.sql("SELECT * FROM entities LIMIT 10".to_string()).await?;
    /// println!("Found {} rows", rows.len());
    /// for row in rows {
    ///     for (column, value) in row.fields {
    ///         match value {
    ///             SqlValue::Text(s) => println!("{}: {}", column, s),
    ///             SqlValue::Integer(i) => println!("{}: {}", column, i),
    ///             SqlValue::Real(r) => println!("{}: {}", column, r),
    ///             SqlValue::Blob(b) => println!("{}: {} bytes", column, b.len()),
    ///             SqlValue::Null => println!("{}: NULL", column),
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn sql(&self, query: String) -> Result<Vec<SqlRow>, Error> {
        let mut grpc_client = self.inner.clone();
        let rows = grpc_client.execute_sql(query).await?;
        Ok(rows)
    }

    /// Perform a full-text search across indexed entities using FTS5.
    ///
    /// # Arguments
    /// * `query` - Search query containing the search text and optional limit
    ///
    /// # Returns
    /// A `SearchResponse` containing results grouped by table with relevance scores
    ///
    /// # Example
    /// ```no_run
    /// # use torii_client::Client;
    /// # use torii_proto::SearchQuery;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new("http://localhost:8080".to_string()).await?;
    /// let results = client.search(SearchQuery {
    ///     query: "dragon".to_string(),
    ///     limit: 10,
    /// }).await?;
    /// println!("Found {} total matches", results.total);
    /// for table_results in results.results {
    ///     println!("Table {}: {} matches", table_results.table, table_results.count);
    ///     for match_result in table_results.matches {
    ///         println!("  ID: {}, Score: {:?}", match_result.id, match_result.score);
    ///         for (field, value) in match_result.fields {
    ///             println!("    {}: {}", field, value);
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search(&self, query: SearchQuery) -> Result<SearchResponse, Error> {
        let mut grpc_client = self.inner.clone();
        let response = grpc_client.search(query).await?;
        Ok(response)
    }
}
