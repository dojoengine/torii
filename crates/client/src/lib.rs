pub mod error;

use crypto_bigint::U256;
use starknet::core::types::Felt;
use torii_grpc_client::{
    AggregationUpdateStreaming, ContractUpdateStreaming, EntityUpdateStreaming,
    EventUpdateStreaming, TokenBalanceStreaming, TokenTransferUpdateStreaming,
    TokenUpdateStreaming, TransactionUpdateStreaming, WorldClient,
};
use torii_proto::proto::world::{
    RetrieveAggregationsResponse, RetrieveContractsResponse, RetrieveControllersResponse,
    RetrieveEntitiesResponse, RetrieveEventsResponse, RetrieveTokenBalancesResponse,
    RetrieveTokenContractsResponse, RetrieveTokenTransfersResponse, RetrieveTokensResponse,
    RetrieveTransactionsResponse,
};
use torii_proto::schema::Entity;
use torii_proto::{
    AggregationEntry, AggregationQuery, Clause, Contract, ContractQuery, Controller,
    ControllerQuery, Event, EventQuery, KeysClause, Message, Page, Query, SqlRow, Token,
    TokenBalance, TokenBalanceQuery, TokenContract, TokenContractQuery, TokenQuery, TokenTransfer,
    TokenTransferQuery, Transaction, TransactionFilter, TransactionQuery, World,
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
    pub async fn new(torii_url: String, world: Felt) -> Result<Self, Error> {
        let grpc_client = WorldClient::new(torii_url, world).await?;

        Ok(Self { inner: grpc_client })
    }

    /// Returns a initialized [Client] with custom max message size.
    ///
    /// # Arguments
    /// * `torii_url` - The URL of the Torii server
    /// * `world` - The world address
    /// * `max_message_size` - Maximum size in bytes for gRPC messages (both incoming and outgoing)
    pub async fn new_with_config(
        torii_url: String,
        world: Felt,
        max_message_size: usize,
    ) -> Result<Self, Error> {
        let grpc_client = WorldClient::new_with_config(torii_url, world, max_message_size).await?;

        Ok(Self { inner: grpc_client })
    }

    /// Publishes an offchain message to the world.
    /// Returns the entity id of the offchain message.
    pub async fn publish_message(&self, message: Message) -> Result<Felt, Error> {
        let mut grpc_client = self.inner.clone();
        let entity_id = grpc_client.publish_message(message).await?;
        Ok(entity_id)
    }

    /// Publishes a set of offchain messages to the world.
    /// Returns the entity ids of the offchain messages.
    pub async fn publish_message_batch(&self, messages: Vec<Message>) -> Result<Vec<Felt>, Error> {
        let mut grpc_client = self.inner.clone();
        let entity_ids = grpc_client.publish_message_batch(messages).await?;
        Ok(entity_ids)
    }

    /// Returns a read lock on the World metadata that the client is connected to.
    pub async fn metadata(&self) -> Result<World, Error> {
        let mut grpc_client = self.inner.clone();
        let world = grpc_client.metadata().await?;
        Ok(world)
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
    ) -> Result<EntityUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client.subscribe_entities(clause).await?;
        Ok(stream)
    }

    /// Update the entities subscription
    pub async fn update_entity_subscription(
        &self,
        subscription_id: u64,
        clause: Option<Clause>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_entities_subscription(subscription_id, clause)
            .await?;
        Ok(())
    }

    /// A direct stream to grpc subscribe event messages
    pub async fn on_event_message_updated(
        &self,
        clause: Option<Clause>,
    ) -> Result<EntityUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client.subscribe_event_messages(clause).await?;
        Ok(stream)
    }

    /// Update the event messages subscription
    pub async fn update_event_message_subscription(
        &self,
        subscription_id: u64,
        clause: Option<Clause>,
    ) -> Result<(), Error> {
        let mut grpc_client = self.inner.clone();
        grpc_client
            .update_event_messages_subscription(subscription_id, clause)
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
}
