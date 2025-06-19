use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dojo_types::schema::Ty;
use dojo_world::config::WorldMetadata;
use dojo_world::contracts::abigen::model::Layout;
use starknet::{core::types::{Event, Felt, U256}, providers::Provider};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
};

use crate::types::{Cursor, ParsedCall};

pub mod types;

pub type StorageError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait Storage: Send + Sync {
    /// Updates the contract cursors with the storage.
    async fn update_cursors(
        &self,
        cursors: HashMap<Felt, Cursor>,
        cursor_transactions: HashMap<Felt, HashSet<Felt>>,
    ) -> Result<(), StorageError>;

    /// Registers a model with the storage, along with its table.
    /// This is also used when a model is upgraded, which should
    /// update the model schema and its table.
    async fn register_model(
        &self,
        namespace: &str,
        model: &Ty,
        layout: Layout,
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
    fn set_metadata(
        &self,
        resource: &Felt,
        uri: &str,
        block_timestamp: u64,
    ) -> Result<(), StorageError>;

    /// Updates the metadata for a resource with the storage.
    /// It should update the metadata if it already exists.
    /// Along with its model state in the model table.
    fn update_metadata(
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
    fn store_transaction(
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
    fn store_event(
        &self,
        event_id: &str,
        event: &Event,
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

    /// Handles ERC20 token transfers, updating balances and registering tokens as needed.
    async fn handle_erc20_transfer<P: Provider + Sync>(
        &self,
        provider: &P,
        contract_address: Felt,
        from_address: Felt,
        to_address: Felt,
        amount: U256,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<(), StorageError>;

    /// Handles NFT (ERC721/ERC1155) token transfers, updating balances and registering tokens as needed.
    async fn handle_nft_transfer<P: Provider + Sync>(
        &self,
        provider: &P,
        contract_address: Felt,
        from_address: Felt,
        to_address: Felt,
        token_id: U256,
        amount: U256,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<(), StorageError>;

    /// Updates NFT metadata for a specific token.
    async fn update_nft_metadata<P: Provider + Sync>(
        &self,
        provider: &P,
        contract_address: Felt,
        token_id: U256,
    ) -> Result<(), StorageError>;

    /// Applies cached balance differences to the storage.
    async fn apply_cache_diff(&self, cursors: HashMap<Felt, Cursor>) -> Result<(), StorageError>;

    /// Executes pending operations and commits the current transaction.
    async fn execute(&self) -> Result<(), StorageError>;

    /// Rolls back the current transaction and starts a new one.
    async fn rollback(&self) -> Result<(), StorageError>;
}
