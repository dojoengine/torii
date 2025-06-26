use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dojo_types::schema::Ty;
use dojo_world::config::WorldMetadata;
use dojo_world::contracts::abigen::model::Layout;
use starknet::core::types::{Felt, U256};
use std::fmt::Debug;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
};
use torii_math::I256;
use torii_proto::schema::Entity;

use crate::types::{Cursor, ParsedCall};
use torii_proto::{
    Controller, Event, EventQuery, Model, Page, Query, Token, TokenBalance, TokenCollection,
};

pub mod types;
pub mod utils;

pub type StorageError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait ReadOnlyStorage: Send + Sync + Debug {
    /// Returns the cursors for all contracts.
    async fn cursors(&self) -> Result<HashMap<Felt, Cursor>, StorageError>;

    /// Returns the model metadata for the storage.
    async fn model(&self, model: Felt) -> Result<Model, StorageError>;

    /// Returns the models for the storage.
    async fn models(&self) -> Result<Vec<Model>, StorageError>;

    /// Returns the IDs of all the registered tokens
    async fn token_ids(&self) -> Result<HashSet<String>, StorageError>;

    /// Returns the controllers for the storage.
    async fn controllers(
        &self,
        contract_addresses: &[Felt],
        usernames: &[String],
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<Page<Controller>, StorageError>;

    /// Returns the tokens for the storage.
    async fn tokens(
        &self,
        contract_addresses: &[Felt],
        token_ids: &[U256],
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<Page<Token>, StorageError>;

    /// Returns the token balances for the storage.
    async fn token_balances(
        &self,
        account_addresses: &[Felt],
        contract_addresses: &[Felt],
        token_ids: &[U256],
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<Page<TokenBalance>, StorageError>;

    /// Returns the token collections for the storage.
    async fn token_collections(
        &self,
        account_addresses: &[Felt],
        contract_addresses: &[Felt],
        token_ids: &[U256],
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<Page<TokenCollection>, StorageError>;

    /// Returns events for the storage.
    async fn events(&self, query: EventQuery) -> Result<Page<Event>, StorageError>;

    /// Returns entities for the storage.
    async fn entities(&self, query: &Query) -> Result<Page<Entity>, StorageError>;

    /// Returns event messages for the storage.
    async fn event_messages(&self, query: &Query) -> Result<Page<Entity>, StorageError>;
}

#[async_trait]
pub trait Storage: ReadOnlyStorage + Send + Sync + Debug {
    /// Updates the contract cursors with the storage.
    async fn update_cursors(
        &self,
        cursors: HashMap<Felt, Cursor>,
        cursor_transactions: HashMap<Felt, HashSet<Felt>>,
    ) -> Result<(), StorageError>;

    /// Registers a model with the storage, along with its table.
    /// This is also used when a model is upgraded, which should
    /// update the model schema and its table.
    #[allow(clippy::too_many_arguments)]
    async fn register_model(
        &self,
        selector: Felt,
        model: &Ty,
        layout: &Layout,
        class_hash: Felt,
        contract_address: Felt,
        packed_size: u32,
        unpacked_size: u32,
        block_timestamp: u64,
        schema_diff: Option<&Ty>,
        upgrade_diff: Option<&Ty>,
    ) -> Result<(), StorageError>;

    /// Sets an entity with the storage.
    /// It should insert or update the entity if it already exists.
    /// Along with its model state in the model table.
    async fn set_entity(
        &self,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
        entity_id: Felt,
        model_selector: Felt,
        keys: Option<Vec<Felt>>,
    ) -> Result<(), StorageError>;

    /// Sets an event message with the storage.
    /// It should insert or update the event message if it already exists.
    /// Along with its model state in the model table.
    async fn set_event_message(
        &self,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
    ) -> Result<(), StorageError>;

    /// Deletes an entity with the storage.
    /// It should delete the entity from the entity table.
    /// Along with its model state in the model table.
    async fn delete_entity(
        &self,
        entity_id: Felt,
        model_id: Felt,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
    ) -> Result<(), StorageError>;

    /// Sets the metadata for a resource with the storage.
    /// It should insert or update the metadata if it already exists.
    /// Along with its model state in the model table.
    async fn set_metadata(
        &self,
        resource: &Felt,
        uri: &str,
        block_timestamp: u64,
    ) -> Result<(), StorageError>;

    /// Updates the metadata for a resource with the storage.
    /// It should update the metadata if it already exists.
    /// Along with its model state in the model table.
    async fn update_metadata(
        &self,
        resource: &Felt,
        uri: &str,
        metadata: &WorldMetadata,
        icon_img: &Option<String>,
        cover_img: &Option<String>,
    ) -> Result<(), StorageError>;

    /// Stores a transaction with the storage.
    /// It should insert or ignore the transaction if it already exists.
    /// And store all the relevant calls made in the transaction.
    /// Along with the unique models if any used in the transaction.
    #[allow(clippy::too_many_arguments)]
    async fn store_transaction(
        &self,
        transaction_hash: Felt,
        sender_address: Felt,
        calldata: &[Felt],
        max_fee: Felt,
        signature: &[Felt],
        nonce: Felt,
        block_number: u64,
        contract_addresses: &HashSet<Felt>,
        transaction_type: &str,
        block_timestamp: u64,
        calls: &[ParsedCall],
        unique_models: &HashSet<Felt>,
    ) -> Result<(), StorageError>;

    /// Stores an event with the storage.
    async fn store_event(
        &self,
        event_id: &str,
        event: &starknet::core::types::Event,
        transaction_hash: Felt,
        block_timestamp: u64,
    ) -> Result<(), StorageError>;

    /// Adds a controller to the storage.
    async fn add_controller(
        &self,
        username: &str,
        address: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    /// Registers an ERC20 token with the storage.
    async fn register_erc20_token(
        &self,
        contract_address: Felt,
        name: String,
        symbol: String,
        decimals: u8,
    ) -> Result<(), StorageError>;

    /// Registers an NFT (ERC721/ERC1155) token with the storage.
    async fn register_nft_token(
        &self,
        contract_address: Felt,
        token_id: U256,
        metadata: String,
    ) -> Result<(), StorageError>;

    /// Stores a token transfer event with the storage.
    #[allow(clippy::too_many_arguments)]
    async fn store_erc_transfer_event(
        &self,
        contract_address: Felt,
        from: Felt,
        to: Felt,
        amount: U256,
        token_id: Option<U256>,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<(), StorageError>;

    /// Updates NFT metadata for a specific token.
    async fn update_nft_metadata(
        &self,
        contract_address: Felt,
        token_id: U256,
        metadata: String,
    ) -> Result<(), StorageError>;

    /// Applies cached balance differences to the storage.
    async fn apply_balances_diff(
        &self,
        balances_diff: HashMap<String, I256>,
        cursors: HashMap<Felt, Cursor>,
    ) -> Result<(), StorageError>;

    /// Executes pending operations and commits the current transaction.
    async fn execute(&self) -> Result<(), StorageError>;

    /// Rolls back the current transaction and starts a new one.
    async fn rollback(&self) -> Result<(), StorageError>;
}
