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
