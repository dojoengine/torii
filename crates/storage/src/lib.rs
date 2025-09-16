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

use torii_proto::{
    Contract, ContractCursor, ContractQuery, Controller, ControllerQuery, Event, EventQuery, Model,
    Page, Query, Token, TokenBalance, TokenBalanceQuery, TokenContract, TokenContractQuery,
    TokenQuery, TokenTransfer, TokenTransferQuery, Transaction, TransactionCall, TransactionQuery,
};

pub mod utils;

pub use torii_proto as proto;

pub type StorageError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait ReadOnlyStorage: Send + Sync + Debug {
    fn as_read_only(&self) -> &dyn ReadOnlyStorage;

    /// Returns the model metadata for the storage.
    async fn model(&self, model: Felt) -> Result<Model, StorageError>;

    /// Returns the models for the storage.
    /// If selectors is empty, returns all models.
    async fn models(&self, selectors: &[Felt]) -> Result<Vec<Model>, StorageError>;

    /// Returns the IDs of all the registered tokens
    async fn token_ids(&self) -> Result<HashSet<String>, StorageError>;

    /// Returns the controllers for the storage.
    async fn controllers(&self, query: &ControllerQuery) -> Result<Page<Controller>, StorageError>;

    /// Returns the contracts for the storage.
    async fn contracts(&self, query: &ContractQuery) -> Result<Vec<Contract>, StorageError>;

    /// Returns the tokens for the storage.
    async fn tokens(&self, query: &TokenQuery) -> Result<Page<Token>, StorageError>;

    /// Returns the token balances for the storage.
    async fn token_balances(
        &self,
        query: &TokenBalanceQuery,
    ) -> Result<Page<TokenBalance>, StorageError>;

    /// Returns the token contracts for the storage.
    async fn token_contracts(
        &self,
        query: &TokenContractQuery,
    ) -> Result<Page<TokenContract>, StorageError>;

    /// Returns token transfers for the storage.
    async fn token_transfers(
        &self,
        query: &TokenTransferQuery,
    ) -> Result<Page<TokenTransfer>, StorageError>;

    /// Returns transactions for the storage.
    async fn transactions(
        &self,
        query: &TransactionQuery,
    ) -> Result<Page<Transaction>, StorageError>;

    /// Returns events for the storage.
    async fn events(&self, query: EventQuery) -> Result<Page<Event>, StorageError>;

    /// Returns entities for the storage.
    async fn entities(&self, query: &Query) -> Result<Page<Entity>, StorageError>;

    /// Returns event messages for the storage.
    async fn event_messages(&self, query: &Query) -> Result<Page<Entity>, StorageError>;

    /// Returns the model data of an entity.
    async fn entity_model(
        &self,
        entity_id: Felt,
        model_selector: Felt,
    ) -> Result<Option<Ty>, StorageError>;
}

#[async_trait]
pub trait Storage: ReadOnlyStorage + Send + Sync + Debug {
    /// Updates the contract cursors with the storage.
    async fn update_cursors(
        &self,
        cursors: HashMap<Felt, ContractCursor>,
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
        legacy_store: bool,
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
        calls: &[TransactionCall],
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

    /// Registers an ERC token contract with the storage.
    async fn register_token_contract(
        &self,
        contract_address: Felt,
        name: String,
        symbol: String,
        decimals: u8,
        metadata: Option<String>,
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
    async fn update_token_metadata(
        &self,
        contract_address: Felt,
        token_id: Option<U256>,
        metadata: String,
    ) -> Result<(), StorageError>;

    /// Applies cached balance differences to the storage.
    async fn apply_balances_diff(
        &self,
        balances_diff: HashMap<String, I256>,
        total_supply_diff: HashMap<String, I256>,
        cursors: HashMap<Felt, ContractCursor>,
    ) -> Result<(), StorageError>;

    /// Executes pending operations and commits the current transaction.
    async fn execute(&self) -> Result<(), StorageError>;

    /// Rolls back the current transaction and starts a new one.
    async fn rollback(&self) -> Result<(), StorageError>;
}
