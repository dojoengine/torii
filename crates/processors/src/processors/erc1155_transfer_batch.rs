use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use cainome::cairo_serde::{CairoSerde, U256 as U256Cainome};
use dojo_world::contracts::world::WorldContractReader;
use futures_util::future::try_join_all;
use starknet::core::types::{Event, U256};
use starknet::providers::Provider;
use torii_sqlite::Sql;
use tracing::debug;

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc1155_transfer_batch";

#[derive(Default, Debug)]
pub struct Erc1155TransferBatchProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc1155TransferBatchProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static + 'static,
{
    fn event_key(&self) -> String {
        "TransferBatch".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // key: [hash(TransferBatch), operator, from, to]
        // data: [ids_len, ids[0].low, ids[0].high, ..., values_len, values[0].low, values[0].high,
        // ...]
        event.keys.len() == 4 && !event.data.is_empty()
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

        // ERC1155 TransferBatch event data format:
        // - ids_len: felt (first element)
        // - ids: U256[] (each element stored as 2 felts: [low, high])
        // - values_len: felt
        // - values: U256[] (each element stored as 2 felts: [low, high])
        // Spec reference: https://eips.ethereum.org/EIPS/eip-1155#transferbatch
        let ids_len = event.data[0].try_into().unwrap_or(0u64) as usize;
        let mut current_idx = 1;

        // First pass: read all token IDs
        let mut token_ids = Vec::with_capacity(ids_len);
        for _ in 0..ids_len {
            if current_idx + 1 >= event.data.len() {
                break;
            }
            let token_id = U256Cainome::cairo_deserialize(&event.data, current_idx)?;
            token_ids.push(U256::from_words(token_id.low, token_id.high));
            current_idx += 2;
        }

        // Move index to values array
        let values_len = event.data[current_idx].try_into().unwrap_or(0u64) as usize;
        current_idx += 1;

        let mut tasks = Vec::new();

        // Second pass: read and process amounts
        for (idx, token_id) in token_ids.iter().enumerate() {
            if idx >= values_len || current_idx + (idx * 2) + 1 >= event.data.len() {
                break;
            }

            let mut db = db.clone();
            let world = world.clone();
            let token_address_clone = token_address;
            let from_clone = from;
            let to_clone = to;
            let token_id_clone = *token_id;
            let event_id_clone = event_id.to_string();
            let data_clone = event.data.clone();
            let current_idx_clone = current_idx + (idx * 2);

            tasks.push(tokio::spawn(async move {
                let amount = U256Cainome::cairo_deserialize(&data_clone, current_idx_clone)?;
                let amount = U256::from_words(amount.low, amount.high);

                db.handle_nft_transfer(
                    world.provider(),
                    token_address_clone,
                    from_clone,
                    to_clone,
                    token_id_clone,
                    amount,
                    block_timestamp,
                    &event_id_clone,
                )
                .await?;

                debug!(
                    target: LOG_TARGET,
                    from = ?from_clone,
                    to = ?to_clone,
                    token_id = ?token_id_clone,
                    amount = ?amount,
                    "ERC1155 TransferBatch"
                );
                Result::<_, Error>::Ok(())
            }));
        }

        for result in try_join_all(tasks).await?.into_iter() {
            result?;
        }

        Ok(())
    }
}
