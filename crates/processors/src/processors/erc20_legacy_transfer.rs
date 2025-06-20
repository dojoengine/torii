use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use cainome::cairo_serde::{CairoSerde, U256 as U256Cainome};
use starknet::core::types::{Event, U256};
use starknet::providers::Provider;
use tracing::debug;

use crate::erc::{try_register_erc20_token, update_erc_balance_diff};
use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc20_legacy_transfer";

#[derive(Default, Debug)]
pub struct Erc20LegacyTransferProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc20LegacyTransferProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "Transfer".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // ref: https://github.com/OpenZeppelin/cairo-contracts/blob/1f9359219a92cdb1576f953db71ee993b8ef5f70/src/openzeppelin/token/erc20/library.cairo#L19-L21
        // key: [hash(Transfer)]
        // data: [from, to, value.0, value.1]
        if event.keys.len() == 1 && event.data.len() == 4 {
            return true;
        }

        false
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        // Hash the contract address
        event.from_address.hash(&mut hasher);

        // Take the max of from/to addresses to get a canonical representation
        // This ensures transfers between the same pair of addresses are grouped together
        // regardless of direction (A->B or B->A)
        let canonical_pair = std::cmp::max(event.data[0], event.data[1]);
        canonical_pair.hash(&mut hasher);

        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        let token_address = ctx.event.from_address;
        let from = ctx.event.data[0];
        let to = ctx.event.data[1];

        let value = U256Cainome::cairo_deserialize(&ctx.event.data, 2)?;
        let value = U256::from_words(value.low, value.high);

        // optimistically add the token_id to cache
        // this cache is used while applying the cache diff
        // so we need to make sure that all RegisterErc*Token queries
        // are applied before the cache diff is applied
        try_register_erc20_token(
            token_address,
            ctx.world.provider(),
            ctx.storage.clone(),
            ctx.cache.clone(),
        )
        .await?;

        // Update the balances diffs on the cache
        update_erc_balance_diff(ctx.cache.clone(), token_address, from, to, value)?;

        ctx.storage
            .store_erc_transfer_event(
                token_address,
                from,
                to,
                value,
                None,
                ctx.block_timestamp,
                &ctx.event_id,
            )
            .await?;
        debug!(target: LOG_TARGET,from = ?from, to = ?to, value = ?value, "Legacy ERC20 Transfer.");

        Ok(())
    }
}
