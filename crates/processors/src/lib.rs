use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use starknet::core::types::{Event, Felt, TransactionContent};
use starknet::providers::Provider;
use tokio::sync::Semaphore;
use torii_cache::{Cache, ContractClassCache};
use torii_storage::Storage;

mod constants;
pub mod erc;
pub mod error;
pub mod fetch;
pub mod processors;
pub mod task_manager;

use crate::error::Error;
use crate::task_manager::TaskId;

pub use processors::Processors;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct EventProcessorContext<P: Provider + Sync + Send + 'static> {
    pub storage: Arc<dyn Storage>,
    pub cache: Arc<dyn Cache>,
    pub provider: P,
    pub contract_address: Felt,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub event_id: String,
    pub event: Event,
    pub config: EventProcessorConfig,
    pub nft_metadata_semaphore: Arc<Semaphore>,
    pub is_at_head: bool,
}

#[derive(Clone, Debug)]
pub struct EventProcessorConfig {
    pub namespaces: HashSet<String>,
    pub strict_model_reader: bool,
    pub historical_models: HashSet<Felt>,
    pub max_metadata_tasks: usize,
    pub models: HashSet<String>,
    pub external_contracts: bool,
    pub external_contract_whitelist: HashSet<String>,
    pub metadata_updates: bool,
    pub metadata_update_whitelist: HashSet<Felt>,
    pub metadata_update_blacklist: HashSet<Felt>,
    pub metadata_updates_only_at_head: bool,
    pub async_metadata_updates: bool,
}

impl Default for EventProcessorConfig {
    fn default() -> Self {
        Self {
            namespaces: HashSet::new(),
            strict_model_reader: false,
            historical_models: HashSet::new(),
            max_metadata_tasks: 10,
            models: HashSet::new(),
            external_contracts: true,
            external_contract_whitelist: HashSet::new(),
            metadata_updates: true,
            metadata_update_whitelist: HashSet::new(),
            metadata_update_blacklist: HashSet::new(),
            metadata_updates_only_at_head: false,
            async_metadata_updates: false,
        }
    }
}

impl EventProcessorConfig {
    pub fn should_index(&self, namespace: &str, name: &str) -> bool {
        (self.namespaces.is_empty() || self.namespaces.contains(namespace))
            && (self.models.is_empty()
                || self.models.contains(name)
                || self.models.contains(&format!("{}-{}", namespace, name)))
    }

    pub fn is_historical(&self, selector: &Felt) -> bool {
        self.historical_models.contains(selector)
    }

    pub fn should_index_external_contract(&self, namespace: &str, instance_name: &str) -> bool {
        (self.namespaces.is_empty() || self.namespaces.contains(namespace))
            && (self.external_contracts
                && (self.external_contract_whitelist.is_empty()
                    || self.external_contract_whitelist.contains(instance_name)))
    }

    pub fn should_process_metadata_updates(&self, contract_address: &Felt) -> bool {
        // If metadata updates are globally disabled, return false
        if !self.metadata_updates {
            return false;
        }

        // Check blacklist first (takes precedence)
        if self.metadata_update_blacklist.contains(contract_address) {
            return false;
        }

        // If whitelist is empty, allow all (except blacklisted)
        if self.metadata_update_whitelist.is_empty() {
            return true;
        }

        // Check if address is in whitelist
        self.metadata_update_whitelist.contains(contract_address)
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
    P: Provider + Sync + Send + Clone,
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

    fn should_process(&self, _event: &Event, _config: &EventProcessorConfig) -> bool {
        true // Default implementation allows all events to be processed
    }

    fn task_identifier(&self, event: &Event) -> TaskId;

    fn task_dependencies(&self, _event: &Event) -> Vec<TaskId> {
        vec![] // Default implementation returns no dependencies
    }

    fn indexing_mode(&self, _event: &Event, _config: &EventProcessorConfig) -> IndexingMode {
        IndexingMode::Historical
    }

    #[allow(clippy::too_many_arguments)]
    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<()>;
}

#[derive(Debug)]
pub struct BlockProcessorContext<P: Provider + Sync + Send + Clone> {
    pub storage: Arc<dyn Storage>,
    pub provider: P,
    pub block_number: u64,
    pub block_timestamp: u64,
}

#[async_trait]
pub trait BlockProcessor<P: Provider + Sync + Send + Clone>: Send + Sync {
    fn get_block_number(&self) -> String;
    async fn process(&self, ctx: &BlockProcessorContext<P>) -> Result<()>;
}

#[derive(Debug)]
pub struct TransactionProcessorContext<P: Provider + Sync + Send + Clone + std::fmt::Debug> {
    pub storage: Arc<dyn Storage>,
    pub cache: Arc<ContractClassCache<P>>,
    pub provider: P,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: Felt,
    pub transaction: TransactionContent,
    pub contract_addresses: HashSet<Felt>,
    pub contract_class_cache: Arc<ContractClassCache<P>>,
    pub unique_models: HashSet<Felt>,
}

#[async_trait]
pub trait TransactionProcessor<P: Provider + Sync + Send + Clone + std::fmt::Debug>:
    Send + Sync
{
    #[allow(clippy::too_many_arguments)]
    async fn process(&self, ctx: &TransactionProcessorContext<P>) -> Result<()>;
}
