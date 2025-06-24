use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use cainome::cairo_serde::CairoSerde;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use starknet::core::types::{Event, Felt};
use starknet::providers::Provider;
use starknet_crypto::poseidon_hash_many;
use tracing::info;

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig, EventProcessorContext, IndexingMode};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::event_message";

#[derive(Default, Debug)]
pub struct EventMessageProcessor;

#[async_trait]
impl<P> EventProcessor<P> for EventMessageProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "EventEmitted".to_string()
    }

    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        let keys = Vec::<Felt>::cairo_deserialize(&event.data, 0).unwrap_or_else(|e| {
            panic!("Expected EventEmitted keys to be well formed: {:?}", e);
        });
        // selector
        event.keys[1].hash(&mut hasher);
        // entity id
        let entity_id = poseidon_hash_many(&keys);
        entity_id.hash(&mut hasher);
        hasher.finish()
    }

    fn task_dependencies(&self, event: &Event) -> Vec<TaskId> {
        let mut hasher = DefaultHasher::new();
        // selector
        event.keys[1].hash(&mut hasher);
        vec![hasher.finish()]
    }

    fn indexing_mode(&self, event: &Event, config: &EventProcessorConfig) -> IndexingMode {
        let model_id = event.keys[1];
        let is_historical = config.is_historical(&model_id);
        if is_historical {
            IndexingMode::Historical
        } else {
            let mut hasher = DefaultHasher::new();
            event.keys[0].hash(&mut hasher);
            IndexingMode::Latest(hasher.finish())
        }
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <EventMessageProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::EventEmitted(e) => e,
            _ => {
                unreachable!()
            }
        };

        // silently ignore if the model is not found
        let model = match ctx.cache.model(event.selector).await {
            Ok(model) => model,
            Err(_) => return Ok(()),
        };

        info!(
            target: LOG_TARGET,
            namespace = %model.namespace,
            name = %model.name,
            system = %format!("{:#x}", Felt::from(event.system_address)),
            "Store event message."
        );

        let mut keys_and_unpacked = [event.keys, event.values].concat();

        let mut entity = model.schema.clone();
        entity.deserialize(&mut keys_and_unpacked)?;

        ctx.storage
            .set_event_message(entity, &ctx.event_id, ctx.block_timestamp)
            .await?;
        Ok(())
    }
}
