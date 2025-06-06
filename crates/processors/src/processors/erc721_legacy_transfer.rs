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
use crate::{EventProcessor, EventProcessorConfig};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc721_legacy_transfer";

#[derive(Default, Debug)]
pub struct Erc721LegacyTransferProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc721LegacyTransferProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
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

    async fn process(
        &self,
        world: Arc<WorldContractReader<P>>,
        db: &mut Sql,
        _block_number: u64,
        block_timestamp: u64,
        event_id: &str,
        event: &Event,
        _config: &EventProcessorConfig,
    ) -> Result<(), Error> {
        let token_address = event.from_address;
        let from = event.data[0];
        let to = event.data[1];

        let token_id = U256Cainome::cairo_deserialize(&event.data, 2)?;
        let token_id = U256::from_words(token_id.low, token_id.high);

        db.handle_nft_transfer(
            world.provider(),
            token_address,
            from,
            to,
            token_id,
            U256::from(1u8),
            block_timestamp,
            event_id,
        )
        .await?;
        debug!(target: LOG_TARGET, from = ?from, to = ?to, token_id = ?token_id, "ERC721 Transfer.");

        Ok(())
    }
}
