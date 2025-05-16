use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use anyhow::Error;
use async_trait::async_trait;
use cainome::cairo_serde::{CairoSerde, U256 as U256Cainome};
use dojo_world::contracts::world::WorldContractReader;
use starknet::core::types::{Event, U256};
use starknet::providers::Provider;
use torii_sqlite::Sql;
use tracing::debug;

use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc1155_transfer_single";

#[derive(Default, Debug)]
pub struct Erc1155TransferSingleProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc1155TransferSingleProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
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
        let from = event.keys[2];
        let to = event.keys[3];

        let token_id = U256Cainome::cairo_deserialize(&event.data, 0)?;
        let token_id = U256::from_words(token_id.low, token_id.high);

        let amount = U256Cainome::cairo_deserialize(&event.data, 2)?;
        let amount = U256::from_words(amount.low, amount.high);

        db.handle_nft_transfer(
            world.provider(),
            token_address,
            from,
            to,
            token_id,
            amount,
            block_timestamp,
            event_id,
        )
        .await?;
        debug!(target: LOG_TARGET, from = ?from, to = ?to, token_id = ?token_id, amount = ?amount, "ERC1155 TransferSingle");

        Ok(())
    }
}
