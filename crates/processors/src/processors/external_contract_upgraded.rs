use std::hash::{DefaultHasher, Hash, Hasher};

use dojo_world::contracts::world::WorldEvent;
use starknet::providers::Provider;

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::external_contract_upgraded";

#[derive(Default, Debug)]
pub struct ExternalContractUpgradedProcessor;

#[async_trait::async_trait]
impl<P> EventProcessor<P> for ExternalContractUpgradedProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "ExternalContractUpgraded".to_owned()
    }

    fn validate(&self, _event: &starknet::core::types::Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &starknet::core::types::Event) -> TaskId {
        let selector = match WorldEvent::try_from(event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <ExternalContractUpgradedProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::ExternalContractUpgraded(e) => e.contract_selector,
            _ => {
                unreachable!()
            }
        };

        let mut hasher = DefaultHasher::new();
        selector.hash(&mut hasher);
        hasher.finish()
    }

    async fn process(
        &self,
        _world: std::sync::Arc<dojo_world::contracts::WorldContractReader<P>>,
        _db: &mut torii_sqlite::Sql,
        _block_number: u64,
        _block_timestamp: u64,
        _event_id: &str,
        event: &starknet::core::types::Event,
        config: &EventProcessorConfig,
    ) -> anyhow::Result<(), Error> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <ExternalContractUpgradedProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::ExternalContractUpgraded(e) => e,
            _ => {
                unreachable!()
            }
        };

        // Safe to unwrap, since it's coming from the chain.
        let namespace = event.namespace.to_string().unwrap();
        let address = event.contract_address.0.to_hex_string();

        // If the namespace is not in the list of namespaces to index, silently ignore it.
        // If our config is empty, we index all namespaces.
        if !config.should_index(&namespace) {
            return Ok(());
        }

        // upgrade metadata on tokens having collection address {address}

        todo!()
    }
}
