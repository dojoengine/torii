use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use dojo_world::contracts::world::WorldContractReader;
use starknet::core::types::{Event, Felt, Transaction};
use starknet::providers::Provider;
use torii_cache::ContractClassCache;
use torii_storage::Storage;

pub mod error;
pub mod processors;
pub mod task_manager;
mod erc;
mod fetch;
mod constants;

use crate::error::Error;
use crate::task_manager::TaskId;

pub use processors::Processors;

pub type Result<T> = std::result::Result<T, Error>;

pub struct EventProcessorContext<P: Provider + Sync> {
    pub world: Arc<WorldContractReader<P>>,
    pub storage: Arc<dyn Storage>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub event_id: String,
    pub event: Event,
    pub config: EventProcessorConfig,
}

#[derive(Clone, Debug, Default)]
pub struct EventProcessorConfig {
    pub namespaces: HashSet<String>,
    pub strict_model_reader: bool,
    pub historical_models: HashSet<Felt>,
}

impl EventProcessorConfig {
    pub fn should_index(&self, namespace: &str) -> bool {
        self.namespaces.is_empty() || self.namespaces.contains(namespace)
    }

    pub fn is_historical(&self, selector: &Felt) -> bool {
        self.historical_models.contains(selector)
    }
}

type EventKey = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexingMode {
    Historical,
    Latest(EventKey),
}

#[async_trait]
pub trait EventProcessor<P>: Send + Sync
where
    P: Provider + Sync + Send,
{
    fn event_key(&self) -> String;

    fn event_keys_as_string(&self, event: &Event) -> String {
        event
            .keys
            .iter()
            .map(|i| format!("{:#064x}", i))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn validate(&self, event: &Event) -> bool;

    fn task_identifier(&self, event: &Event) -> TaskId;

    fn task_dependencies(&self, _event: &Event) -> Vec<TaskId> {
        vec![] // Default implementation returns no dependencies
    }

    fn indexing_mode(&self, _event: &Event, _config: &EventProcessorConfig) -> IndexingMode {
        IndexingMode::Historical
    }

    #[allow(clippy::too_many_arguments)]
    async fn process(
        &self,
        ctx: &EventProcessorContext<P>,
    ) -> Result<()>;
}

pub struct BlockProcessorContext<P: Provider + Sync> {
    pub storage: Arc<dyn Storage>,
    pub provider: Arc<P>,
    pub block_number: u64,
    pub block_timestamp: u64,
}

#[async_trait]
pub trait BlockProcessor<P: Provider + Sync>: Send + Sync {
    fn get_block_number(&self) -> String;
    async fn process(
        &self,
        ctx: &BlockProcessorContext<P>,
    ) -> Result<()>;
}

pub struct TransactionProcessorContext<P: Provider + Sync + std::fmt::Debug> {
    pub storage: Arc<dyn Storage>,
    pub provider: Arc<P>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: Felt,
    pub transaction: Transaction,
    pub contract_addresses: HashSet<Felt>,
    pub contract_class_cache: Arc<ContractClassCache<P>>,
    pub unique_models: HashSet<Felt>,
}

#[async_trait]
pub trait TransactionProcessor<P: Provider + Sync + std::fmt::Debug>: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn process(
        &self,
        ctx: &TransactionProcessorContext<P>,
    ) -> Result<()>;
}
