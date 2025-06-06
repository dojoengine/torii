use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use dojo_types::naming::compute_selector_from_names;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::contracts::model::{ModelRPCReader, ModelReader};
use dojo_world::contracts::world::WorldContractReader;
use starknet::core::types::{BlockId, Event};
use starknet::providers::Provider;
use torii_sqlite::Sql;
use tracing::{debug, info};

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::register_model";

#[derive(Default, Debug)]
pub struct RegisterModelProcessor;

#[async_trait]
impl<P> EventProcessor<P> for RegisterModelProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "ModelRegistered".to_string()
    }

    // We might not need this anymore, since we don't have fallback and all world events must
    // be handled.
    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let selector = match WorldEvent::try_from(event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <RegisterModelProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::ModelRegistered(e) => compute_selector_from_names(
                &e.namespace.to_string().unwrap(),
                &e.name.to_string().unwrap(),
            ),
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
        world: Arc<WorldContractReader<P>>,
        db: &mut Sql,
        block_number: u64,
        block_timestamp: u64,
        _event_id: &str,
        event: &Event,
        config: &EventProcessorConfig,
    ) -> Result<(), Error> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <RegisterModelProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::ModelRegistered(e) => e,
            _ => {
                unreachable!()
            }
        };

        // Safe to unwrap, since it's coming from the chain.
        let namespace = event.namespace.to_string().unwrap();
        let name = event.name.to_string().unwrap();

        // If the namespace is not in the list of namespaces to index, silently ignore it.
        // If our config is empty, we index all namespaces.
        if !config.should_index(&namespace) {
            return Ok(());
        }

        let mut model = ModelRPCReader::new(
            &namespace,
            &name,
            event.address.0,
            event.class_hash.0,
            &world,
        )
        .await;
        if config.strict_model_reader {
            model.set_block(BlockId::Number(block_number)).await;
        }
        let schema = model.schema().await?;
        let layout = model.layout().await?;

        let unpacked_size: u32 = model.unpacked_size().await?;
        let packed_size: u32 = model.packed_size().await?;

        info!(
            target: LOG_TARGET,
            namespace = %namespace,
            name = %name,
            "Registered model."
        );

        debug!(
            target: LOG_TARGET,
            name,
            schema = ?schema,
            layout = ?layout,
            class_hash = ?event.class_hash,
            contract_address = ?event.address,
            packed_size = %packed_size,
            unpacked_size = %unpacked_size,
            "Registered model content."
        );

        db.register_model(
            &namespace,
            &schema,
            layout,
            event.class_hash.into(),
            event.address.into(),
            packed_size,
            unpacked_size,
            block_timestamp,
            None,
            None,
        )
        .await?;

        Ok(())
    }
}
