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
    pub block_number: u64,
    pub block_timestamp: u64,
    pub event_id: String,
    pub event: Event,
    pub config: EventProcessorConfig,
    pub nft_metadata_semaphore: Arc<Semaphore>,
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
