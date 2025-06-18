use async_trait::async_trait;
use dojo_types::schema::Ty;
use dojo_world::config::WorldMetadata;
use dojo_world::contracts::abigen::model::Layout;
use starknet::core::types::{Event, Felt};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
};

use crate::types::ParsedCall;

pub mod types;

#[async_trait]
pub trait Storage<E: Error + Send + Sync>: Send + Sync {
    /// Registers a model with the storage, along with its table.
    /// This is also used when a model is upgraded, which should
    /// update the model schema and its table.
    async fn register_model(
        &mut self,
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
    ) -> Result<(), E>;

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
        keys_str: Option<&str>,
    ) -> Result<(), E>;

    /// Sets an event message with the storage.
    /// It should insert or update the event message if it already exists.
    /// Along with its model state in the model table.
    async fn set_event_message(
        &mut self,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
    ) -> Result<(), E>;

    /// Deletes an entity with the storage.
    /// It should delete the entity from the entity table.
    /// Along with its model state in the model table.
    async fn delete_entity(
        &mut self,
        entity_id: Felt,
        model_id: Felt,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
    ) -> Result<(), E>;

    /// Sets the metadata for a resource with the storage.
    /// It should insert or update the metadata if it already exists.
    /// Along with its model state in the model table.
    fn set_metadata(
        &mut self,
        resource: &Felt,
        uri: &str,
        block_timestamp: u64,
    ) -> Result<(), E>;

    /// Updates the metadata for a resource with the storage.
    /// It should update the metadata if it already exists.
    /// Along with its model state in the model table.
    fn update_metadata(
        &mut self,
        resource: &Felt,
        uri: &str,
        metadata: &WorldMetadata,
        icon_img: &Option<String>,
        cover_img: &Option<String>,
    ) -> Result<(), E>;

    /// Stores a transaction with the storage.
    /// It should insert or ignore the transaction if it already exists.
    /// And store all the relevant calls made in the transaction.
    /// Along with the unique models if any used in the transaction.
    fn store_transaction(
        &mut self,
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
    ) -> Result<(), E>;

    /// Stores an event with the storage.
    fn store_event(
        &mut self,
        event_id: &str,
        event: &Event,
        transaction_hash: Felt,
        block_timestamp: u64,
    ) -> Result<(), E>;
}
