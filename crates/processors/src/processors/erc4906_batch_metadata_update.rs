use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use cainome::cairo_serde::{CairoSerde, U256 as U256Cainome};
use futures_util::future::try_join_all;
use starknet::core::types::{Event, U256};
use starknet::providers::Provider;
use torii_proto::TokenId;
use tracing::debug;

use crate::erc::fetch_token_metadata;
use crate::error::{Error, TokenMetadataError};
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc4906_metadata_update_batch";

#[derive(Default, Debug)]
pub struct Erc4906BatchMetadataUpdateProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc4906BatchMetadataUpdateProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + Clone + 'static,
{
    fn event_key(&self) -> String {
        "BatchMetadataUpdate".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // Batch metadata update: [hash(BatchMetadataUpdate), from_token_id.low, from_token_id.high,
        // to_token_id.low, to_token_id.high]
        event.keys.len() == 5 && event.data.is_empty()
    }

    fn should_process(&self, event: &Event, config: &crate::EventProcessorConfig) -> bool {
        config.should_process_metadata_updates(&event.from_address)
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        event.from_address.hash(&mut hasher);
        hasher.finish()
    }

    // Deduplicate batch metadata updates during historical sync, but process all when at head
    fn indexing_mode(&self, event: &Event, config: &crate::EventProcessorConfig) -> crate::IndexingMode {
        // If metadata_updates_only_at_head is disabled, process all updates historically
        if !config.metadata_updates_only_at_head {
            return crate::IndexingMode::Historical;
        }

        // When at head, process all metadata updates as they come
        // This is determined at runtime in the task manager based on ParallelizedEvent.at_head
        // For now, we use Latest mode and the processor will check at_head at runtime
        let mut hasher = DefaultHasher::new();
        event.from_address.hash(&mut hasher);
        event.keys[0].hash(&mut hasher);
        // Include from_token_id and to_token_id in the hash to deduplicate per range
        if event.keys.len() >= 5 {
            event.keys[1].hash(&mut hasher); // from_token_id.low
            event.keys[2].hash(&mut hasher); // from_token_id.high
            event.keys[3].hash(&mut hasher); // to_token_id.low
            event.keys[4].hash(&mut hasher); // to_token_id.high
        }
        crate::IndexingMode::Latest(hasher.finish())
    }

    // Maybe dont need to depend on all of token ids.
    // fn task_dependencies(&self, event: &Event) -> Vec<TaskId> {
    //     let mut dependencies = Vec::new();
    //     let from_token_id = U256Cainome::cairo_deserialize(&event.keys, 1).unwrap();
    //     let mut from_token_id = U256::from_words(from_token_id.low, from_token_id.high);

    //     let to_token_id = U256Cainome::cairo_deserialize(&event.keys, 3).unwrap();
    //     let to_token_id = U256::from_words(to_token_id.low, to_token_id.high);

    //     while from_token_id <= to_token_id {
    //         let mut hasher = DefaultHasher::new();
    //         event.from_address.hash(&mut hasher);
    //         from_token_id.hash(&mut hasher);
    //         dependencies.push(hasher.finish());
    //         from_token_id += U256::from(1u8);
    //     }

    //     dependencies
    // }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        let token_address = ctx.event.from_address;
        let from_token_id = U256Cainome::cairo_deserialize(&ctx.event.keys, 1)?;
        let from_token_id = U256::from_words(from_token_id.low, from_token_id.high);

        let to_token_id = U256Cainome::cairo_deserialize(&ctx.event.keys, 3)?;
        let to_token_id = U256::from_words(to_token_id.low, to_token_id.high);

        // If metadata_updates_only_at_head is enabled and we're not at head, skip processing
        // This defers metadata fetching until we're truly at chain head for fresh data
        if ctx.config.metadata_updates_only_at_head && !ctx.at_head {
            debug!(
                target: LOG_TARGET,
                token_address = ?token_address,
                from_token_id = ?from_token_id,
                to_token_id = ?to_token_id,
                block_number = ctx.block_number,
                "Deferring batch metadata update - will process when at head"
            );
            return Ok(());
        }

        debug!(
            target: LOG_TARGET,
            token_address = ?token_address,
            from_token_id = ?from_token_id,
            to_token_id = ?to_token_id,
            block_number = ctx.block_number,
            at_head = ctx.at_head,
            "Processing batch NFT metadata update"
        );

        let mut tasks = Vec::new();
        let mut token_id = from_token_id;

        while token_id <= to_token_id {
            let storage = ctx.storage.clone();
            let nft_metadata_semaphore = ctx.nft_metadata_semaphore.clone();
            let provider = ctx.provider.clone();
            let token_address_clone = token_address;
            let current_token_id = token_id;

            tasks.push(tokio::spawn(async move {
                let _permit = nft_metadata_semaphore
                    .acquire()
                    .await
                    .map_err(TokenMetadataError::AcquireError)?;

                let metadata =
                    fetch_token_metadata(token_address_clone, current_token_id, &provider).await?;
                let id = TokenId::Nft(token_address_clone, current_token_id);
                storage.update_token_metadata(id, metadata).await?;
                Result::<_, Error>::Ok(())
            }));

            token_id += U256::from(1u8);
        }

        for result in try_join_all(tasks).await?.into_iter() {
            result?;
        }

        debug!(
            target: LOG_TARGET,
            token_address = ?token_address,
            from_token_id = ?from_token_id,
            to_token_id = ?to_token_id,
            "NFT metadata updated for token range"
        );

        Ok(())
    }
}
