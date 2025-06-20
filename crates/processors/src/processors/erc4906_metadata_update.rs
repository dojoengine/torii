use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use cainome::cairo_serde::{CairoSerde, U256 as U256Cainome};
use dojo_world::contracts::world::WorldContractReader;
use starknet::core::types::{Event, U256};
use starknet::providers::Provider;
use torii_sqlite::Sql;
use tracing::debug;

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig, IndexingMode};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc4906_metadata_update";
#[derive(Default, Debug)]
pub struct Erc4906MetadataUpdateProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc4906MetadataUpdateProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "MetadataUpdate".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // Single token metadata update: [hash(MetadataUpdate), token_id.low, token_id.high]
        event.keys.len() == 3 && event.data.is_empty()
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        event.from_address.hash(&mut hasher);
        let token_id = U256Cainome::cairo_deserialize(&event.keys, 1).unwrap();
        let token_id = U256::from_words(token_id.low, token_id.high);
        token_id.hash(&mut hasher);
        hasher.finish()
    }

    fn task_dependencies(&self, event: &Event) -> Vec<TaskId> {
        let mut hasher = DefaultHasher::new();
        event.from_address.hash(&mut hasher);
        vec![hasher.finish()]
    }

    // We can dedup singular metadata updates. To only keep the latest one.
    fn indexing_mode(&self, event: &Event, _config: &EventProcessorConfig) -> IndexingMode {
        let mut hasher = DefaultHasher::new();
        event.keys[0].hash(&mut hasher);
        IndexingMode::Latest(hasher.finish())
    }

    async fn process(
        &self,
        world: Arc<WorldContractReader<P>>,
        db: &mut Sql,
        _block_number: u64,
        _block_timestamp: u64,
        _event_id: &str,
        event: &Event,
        _config: &EventProcessorConfig,
    ) -> Result<(), Error> {
        let token_address = event.from_address;
        let token_id = U256Cainome::cairo_deserialize(&event.keys, 1)?;
        let token_id = U256::from_words(token_id.low, token_id.high);

        db.update_nft_metadata(world.provider(), token_address, token_id)
            .await?;

        debug!(
            target: LOG_TARGET,
            token_address = ?token_address,
            token_id = ?token_id,
            "NFT metadata updated for single token"
        );

        Ok(())
    }
}
