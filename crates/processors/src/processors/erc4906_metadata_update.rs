use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use cainome::cairo_serde::{CairoSerde, U256 as U256Cainome};
use starknet::core::types::{Event, U256};
use starknet::providers::Provider;
use tracing::debug;

use crate::erc::fetch_token_metadata;
use crate::error::{Error, TokenMetadataError};
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig, EventProcessorContext, IndexingMode};
use torii_proto::TokenId;

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc4906_metadata_update";
#[derive(Default, Debug)]
pub struct Erc4906MetadataUpdateProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc4906MetadataUpdateProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "MetadataUpdate".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // Single token metadata update: [hash(MetadataUpdate), token_id.low, token_id.high]
        event.keys.len() == 3 && event.data.is_empty()
    }

    fn should_process(&self, event: &Event, config: &EventProcessorConfig) -> bool {
        config.should_process_metadata_updates(&event.from_address)
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

    // Deduplicate metadata updates during historical sync, but process all when at head
    fn indexing_mode(&self, event: &Event, config: &EventProcessorConfig) -> IndexingMode {
        // If metadata_updates_only_at_head is disabled, process all updates historically
        if !config.metadata_updates_only_at_head {
            return IndexingMode::Historical;
        }

        // When at head, process all metadata updates as they come
        // This is determined at runtime in the task manager based on ParallelizedEvent.at_head
        // For now, we use Latest mode and the processor will check at_head at runtime
        let mut hasher = DefaultHasher::new();
        event.from_address.hash(&mut hasher);
        event.keys[0].hash(&mut hasher);
        // Include token_id in the hash to deduplicate per token
        if event.keys.len() >= 3 {
            event.keys[1].hash(&mut hasher); // token_id.low
            event.keys[2].hash(&mut hasher); // token_id.high
        }
        IndexingMode::Latest(hasher.finish())
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        let token_address = ctx.event.from_address;
        let token_id = U256Cainome::cairo_deserialize(&ctx.event.keys, 1)?;
        let token_id = U256::from_words(token_id.low, token_id.high);

        // If metadata_updates_only_at_head is enabled and we're not at head, skip processing
        // This defers metadata fetching until we're truly at chain head for fresh data
        if ctx.config.metadata_updates_only_at_head && !ctx.at_head {
            debug!(
                target: LOG_TARGET,
                token_address = ?token_address,
                token_id = ?token_id,
                block_number = ctx.block_number,
                "Deferring metadata update - will process when at head"
            );
            return Ok(());
        }

        debug!(
            target: LOG_TARGET,
            token_address = ?token_address,
            token_id = ?token_id,
            block_number = ctx.block_number,
            at_head = ctx.at_head,
            "Processing NFT metadata update"
        );

        let id = TokenId::Nft(token_address, token_id);
        if !ctx.cache.is_token_registered(&id).await {
            return Ok(());
        }

        let _permit = ctx
            .nft_metadata_semaphore
            .acquire()
            .await
            .map_err(TokenMetadataError::AcquireError)?;

        let metadata = fetch_token_metadata(token_address, token_id, &ctx.provider).await?;

        ctx.storage.update_token_metadata(id, metadata).await?;

        debug!(
            target: LOG_TARGET,
            token_address = ?token_address,
            token_id = ?token_id,
            "NFT metadata updated for single token"
        );

        Ok(())
    }
}
