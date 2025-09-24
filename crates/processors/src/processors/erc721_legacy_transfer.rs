use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use cainome::cairo_serde::{CairoSerde, U256 as U256Cainome};
use starknet::core::types::{Event, U256};
use starknet::providers::Provider;
use tracing::debug;

use crate::erc::{try_register_nft_token_metadata, try_register_token_contract};
use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};
use torii_proto::TokenId;

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc721_legacy_transfer";

#[derive(Default, Debug)]
pub struct Erc721LegacyTransferProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc721LegacyTransferProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "Transfer".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // ref: https://github.com/OpenZeppelin/cairo-contracts/blob/1f9359219a92cdb1576f953db71ee993b8ef5f70/src/openzeppelin/token/erc721/library.cairo#L27-L29
        // key: [hash(Transfer)]
        // data: [from, to, token_id.0, token_id.1]
        if event.keys.len() == 1 && event.data.len() == 4 {
            return true;
        }

        false
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        // Hash the contract address
        event.from_address.hash(&mut hasher);

        // For ERC721, we can safely parallelize by token ID since each token is unique
        // and can only be owned by one address at a time. This means:
        // 1. Transfers of different tokens can happen in parallel
        // 2. Multiple transfers of the same token must be sequential
        let token_id = U256Cainome::cairo_deserialize(&event.data, 2).unwrap();
        let token_id = U256::from_words(token_id.low, token_id.high);
        token_id.hash(&mut hasher);

        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        let token_address = ctx.event.from_address;
        let from = ctx.event.data[0];
        let to = ctx.event.data[1];

        let token_id = U256Cainome::cairo_deserialize(&ctx.event.data, 2)?;
        let token_id = U256::from_words(token_id.low, token_id.high);

        let id = TokenId::Nft(token_address, token_id);

        // Register the contract first
        try_register_token_contract(
            token_address,
            &ctx.provider,
            ctx.storage.clone(),
            ctx.cache.clone(),
            false,
        )
        .await?;

        // Then register the specific NFT token
        try_register_nft_token_metadata(
            id.clone(),
            token_address,
            token_id,
            &ctx.provider,
            ctx.cache.clone(),
            ctx.storage.clone(),
            ctx.nft_metadata_semaphore.clone(),
        )
        .await?;

        ctx.cache
            .update_balance_diff(id.clone(), from, to, U256::from(1u8))
            .await;

        ctx.storage
            .store_token_transfer(
                id,
                from,
                to,
                U256::from(1u8),
                ctx.block_timestamp,
                &ctx.event_id,
            )
            .await?;

        debug!(target: LOG_TARGET, from = ?from, to = ?to, token_id = ?token_id, "ERC721 Transfer.");

        Ok(())
    }
}
