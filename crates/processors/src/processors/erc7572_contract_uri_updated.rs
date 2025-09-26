use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use starknet::core::types::Event;
use starknet::providers::Provider;
use tracing::debug;

use crate::erc::update_contract_metadata;
use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig, EventProcessorContext, IndexingMode};
use torii_proto::TokenId;

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::erc7572_contract_uri_updated";

#[derive(Default, Debug)]
pub struct Erc7572ContractUriUpdatedProcessor;

#[async_trait]
impl<P> EventProcessor<P> for Erc7572ContractUriUpdatedProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "ContractURIUpdated".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        // ERC-7572 ContractURIUpdated event format:
        // key: [hash(ContractURIUpdated)]
        // data: [] (no data, just the event emission)
        event.keys.len() == 1 && event.data.is_empty()
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        // Hash the contract address since we're updating contract-level metadata
        event.from_address.hash(&mut hasher);
        hasher.finish()
    }

    // We can dedup contract uri updates. To only keep the latest one.
    fn indexing_mode(&self, event: &Event, _config: &EventProcessorConfig) -> IndexingMode {
        let mut hasher = DefaultHasher::new();
        event.keys[0].hash(&mut hasher);
        IndexingMode::Latest(hasher.finish())
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        let contract_address = ctx.event.from_address;
        let token_id = TokenId::Contract(contract_address);
        if !ctx.cache.is_token_registered(&token_id).await {
            return Ok(());
        }

        // Update the contract metadata for this contract
        update_contract_metadata(contract_address, &ctx.provider, ctx.storage.clone()).await?;

        debug!(
            target: LOG_TARGET,
            contract_address = ?contract_address,
            "Contract URI updated for ERC-7572 contract."
        );

        Ok(())
    }
}
