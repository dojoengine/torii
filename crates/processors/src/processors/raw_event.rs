use std::sync::Arc;

use anyhow::{Error, Result};
use async_trait::async_trait;
use dojo_world::contracts::world::WorldContractReader;
use starknet::core::types::Event;
use starknet::providers::Provider;
use torii_sqlite::Sql;

use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig};

#[derive(Default, Debug)]
pub struct RawEventProcessor;

#[async_trait]
impl<P> EventProcessor<P> for RawEventProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "".to_string()
    }

    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, _event: &Event) -> TaskId {
        0
    }

    async fn process(
        &self,
        _world: Arc<WorldContractReader<P>>,
        _db: &mut Sql,
        _block_number: u64,
        _block_timestamp: u64,
        _event_id: &str,
        _event: &Event,
        _config: &EventProcessorConfig,
    ) -> Result<(), Error> {
        // We can choose to consider them, or not.

        Ok(())
    }
}
