pub mod error;

use crypto_bigint::U256;
use starknet::core::types::Felt;
use torii_grpc_client::{
    EntityUpdateStreaming, EventUpdateStreaming, IndexerUpdateStreaming, TokenBalanceStreaming,
    TokenUpdateStreaming, TransactionUpdateStreaming, WorldClient,
};
use torii_proto::proto::world::{
    RetrieveControllersResponse, RetrieveEntitiesResponse, RetrieveEventsResponse,
    RetrieveTokenBalancesResponse, RetrieveTokenCollectionsResponse, RetrieveTokensResponse,
    RetrieveTransactionsResponse,
};
use torii_proto::schema::Entity;
use torii_proto::{
    Clause, Controller, ControllerQuery, Event, EventQuery, KeysClause, Message, Page, Query,
    Token, TokenBalance, TokenBalanceQuery, TokenQuery, Transaction, TransactionFilter,
    TransactionQuery, World,
};

use crate::error::Error;

#[allow(unused)]
#[derive(Debug)]
pub struct Client {
    /// The grpc client.
    inner: WorldClient,
}

impl Client {
    /// Returns a initialized [Client].
    pub async fn new(torii_url: String, world: Felt) -> Result<Self, Error> {
        let grpc_client = WorldClient::new(torii_url, world).await?;

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

    /// Retrieves tokens matching contract addresses.
    pub async fn token_collections(&self, query: TokenBalanceQuery) -> Result<Page<Token>, Error> {
        let mut grpc_client = self.inner.clone();
        let RetrieveTokenCollectionsResponse {
            tokens,
            next_cursor,
        } = grpc_client.retrieve_token_collections(query).await?;
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
    pub async fn on_indexer_updated(
        &self,
        contract_address: Option<Felt>,
    ) -> Result<IndexerUpdateStreaming, Error> {
        let mut grpc_client = self.inner.clone();
        let stream = grpc_client
            .subscribe_indexer(contract_address.unwrap_or_default())
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
}
