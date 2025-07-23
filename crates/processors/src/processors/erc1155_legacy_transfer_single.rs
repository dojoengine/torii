use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use cainome::cairo_serde::{CairoSerde, U256 as U256Cainome};
use starknet::core::types::{Event, U256};
use starknet::providers::Provider;
use tracing::debug;

use crate::erc::{felt_and_u256_to_sql_string, try_register_nft_token_metadata};
use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc1155_legacy_transfer_single";

#[derive(Default, Debug)]
pub struct Erc1155LegacyTransferSingleProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc1155LegacyTransferSingleProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "TransferSingle".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // Legacy format:
        // key: [hash(TransferSingle)]
        // data: [operator, from, to, token_id.low, token_id.high, value.low, value.high]
        if event.keys.len() == 1 && event.data.len() == 7 {
            return true;
        }

        false
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        // Hash the contract address
        event.from_address.hash(&mut hasher);

        // For ERC1155, we can safely parallelize by token ID since each token is unique
        // and can only be owned by one address at a time for transfer operations
        let token_id = U256Cainome::cairo_deserialize(&event.data, 3).unwrap();
        let token_id = U256::from_words(token_id.low, token_id.high);
        token_id.hash(&mut hasher);

        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        let token_address = ctx.event.from_address;
        let _operator = ctx.event.data[0];
        let from = ctx.event.data[1];
        let to = ctx.event.data[2];

        let token_id = U256Cainome::cairo_deserialize(&ctx.event.data, 3)?;
        let token_id = U256::from_words(token_id.low, token_id.high);

        let value = U256Cainome::cairo_deserialize(&ctx.event.data, 5)?;
        let value = U256::from_words(value.low, value.high);

        let id = felt_and_u256_to_sql_string(&token_address, &token_id);
        try_register_nft_token_metadata(
            &id,
            token_address,
            token_id,
            ctx.world.provider(),
            ctx.cache.clone(),
            ctx.storage.clone(),
            ctx.nft_metadata_semaphore.clone(),
        )
        .await?;

        ctx.cache.update_balance_diff(&id, from, to, value).await;

        ctx.storage
            .store_erc_transfer_event(
                token_address,
                from,
                to,
                value,
                Some(token_id),
                ctx.block_timestamp,
                &ctx.event_id,
            )
            .await?;

        debug!(target: LOG_TARGET, from = ?from, to = ?to, token_id = ?token_id, value = ?value, "ERC1155 Legacy TransferSingle.");

        Ok(())
    }
}
