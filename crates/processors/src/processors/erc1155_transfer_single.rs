use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use cainome::cairo_serde::{CairoSerde, U256 as U256Cainome};
use starknet::core::types::{Event, U256};
use starknet::providers::Provider;
use tracing::debug;

use crate::erc::{felt_and_u256_to_sql_string, try_register_nft_token_metadata, try_register_nft_contract};
use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc1155_transfer_single";

#[derive(Default, Debug)]
pub struct Erc1155TransferSingleProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc1155TransferSingleProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "TransferSingle".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // key: [hash(TransferSingle), operator, from, to]
        // data: [id.low, id.high, value.low, value.high]
        event.keys.len() == 4 && event.data.len() == 4
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        event.from_address.hash(&mut hasher);
        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        let token_address = ctx.event.from_address;
        let from = ctx.event.keys[2];
        let to = ctx.event.keys[3];

        let token_id = U256Cainome::cairo_deserialize(&ctx.event.data, 0)?;
        let token_id = U256::from_words(token_id.low, token_id.high);

        let amount = U256Cainome::cairo_deserialize(&ctx.event.data, 2)?;
        let amount = U256::from_words(amount.low, amount.high);

        let id = felt_and_u256_to_sql_string(&token_address, &token_id);
        
        // Register the contract first (like ERC20 does)
        try_register_nft_contract(
            token_address,
            &ctx.provider,
            ctx.storage.clone(),
            ctx.cache.clone(),
        )
        .await?;

        // Then register the specific NFT token
        try_register_nft_token_metadata(
            &id,
            token_address,
            token_id,
            &ctx.provider,
            ctx.cache.clone(),
            ctx.storage.clone(),
            ctx.nft_metadata_semaphore.clone(),
        )
        .await?;

        ctx.cache.update_balance_diff(&id, from, to, amount).await;

        ctx.storage
            .store_erc_transfer_event(
                token_address,
                from,
                to,
                amount,
                Some(token_id),
                ctx.block_timestamp,
                &ctx.event_id,
            )
            .await?;

        debug!(target: LOG_TARGET, from = ?from, to = ?to, token_id = ?token_id, amount = ?amount, "ERC1155 TransferSingle");

        Ok(())
    }
}
