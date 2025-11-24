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
        if !config.should_process_metadata_updates(&event.from_address) {
            return false;
        }

        // If metadata_updates_only_at_head is enabled, defer processing until we're at head
        // This check will be performed again at process time with the actual is_at_head value
        true
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

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        // If metadata_updates_only_at_head is enabled and we're not at head, skip processing
        if ctx.config.metadata_updates_only_at_head && !ctx.is_at_head {
            debug!(
                target: LOG_TARGET,
                token_address = ?ctx.event.from_address,
                "Skipping metadata update - not at head yet"
            );
            return Ok(());
        }

        let token_address = ctx.event.from_address;
        let token_id = U256Cainome::cairo_deserialize(&ctx.event.keys, 1)?;
        let token_id = U256::from_words(token_id.low, token_id.high);

        let id = TokenId::Nft(token_address, token_id);
        if !ctx.cache.is_token_registered(&id).await {
            return Ok(());
        }

        // If async mode is enabled, spawn a background task
        if ctx.config.async_metadata_updates {
            let storage = ctx.storage.clone();
            let provider = ctx.provider.clone();
            let semaphore = ctx.nft_metadata_semaphore.clone();

            tokio::spawn(async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(e) => {
                        debug!(
                            target: LOG_TARGET,
                            token_address = ?token_address,
                            token_id = ?token_id,
                            error = ?e,
                            "Failed to acquire semaphore for async metadata update"
                        );
                        return;
                    }
                };

                match fetch_token_metadata(token_address, token_id, &provider).await {
                    Ok(metadata) => {
                        if let Err(e) =
                            storage.update_token_metadata(id, metadata).await
                        {
                            debug!(
                                target: LOG_TARGET,
                                token_address = ?token_address,
                                token_id = ?token_id,
                                error = ?e,
                                "Failed to update token metadata in async mode"
                            );
                        } else {
                            debug!(
                                target: LOG_TARGET,
                                token_address = ?token_address,
                                token_id = ?token_id,
                                "NFT metadata updated for single token (async)"
                            );
                        }
                    }
                    Err(e) => {
                        debug!(
                            target: LOG_TARGET,
                            token_address = ?token_address,
                            token_id = ?token_id,
                            error = ?e,
                            "Failed to fetch token metadata in async mode"
                        );
                    }
                }
            });

            return Ok(());
        }

        // Blocking mode (original behavior)
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
