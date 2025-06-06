use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use dojo_world::contracts::world::WorldContractReader;
use starknet::core::types::{Event, Felt, Transaction};
use starknet::providers::Provider;
use torii_sqlite::cache::ContractClassCache;
use torii_sqlite::Sql;

pub mod error;
pub mod processors;
pub mod task_manager;

use crate::error::Error;
use crate::task_manager::TaskId;

pub use processors::Processors;

pub type Result<T> = std::result::Result<T, Error>;

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
        world: Arc<WorldContractReader<P>>,
        db: &mut Sql,
        block_number: u64,
        block_timestamp: u64,
        event_id: &str,
        event: &Event,
        _config: &EventProcessorConfig,
    ) -> Result<()>;
}

#[async_trait]
pub trait BlockProcessor<P: Provider + Sync>: Send + Sync {
    fn get_block_number(&self) -> String;
    async fn process(
        &self,
        db: &mut Sql,
        provider: &P,
        block_number: u64,
        block_timestamp: u64,
    ) -> Result<()>;
}

#[async_trait]
pub trait TransactionProcessor<P: Provider + Sync + std::fmt::Debug>: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn process(
        &self,
        db: &mut Sql,
        provider: &P,
        block_number: u64,
        block_timestamp: u64,
        transaction_hash: Felt,
        contract_addresses: &HashSet<Felt>,
        transaction: &Transaction,
        contract_class_cache: &ContractClassCache<P>,
        unique_models: &HashSet<Felt>,
    ) -> Result<()>;
}
