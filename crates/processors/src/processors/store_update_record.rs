use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use dojo_types::schema::Ty;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use starknet::core::types::Event;
use starknet::providers::Provider;
use tracing::{debug, info};

use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig, EventProcessorContext};
use crate::{IndexingMode, Result};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::store_update_record";

#[derive(Default, Debug)]
pub struct StoreUpdateRecordProcessor;

#[async_trait]
impl<P> EventProcessor<P> for StoreUpdateRecordProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "StoreUpdateRecord".to_string()
    }

    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        // model selector
        event.keys[1].hash(&mut hasher);
        // entity id
        event.keys[2].hash(&mut hasher);
        hasher.finish()
    }

    fn task_dependencies(&self, event: &Event) -> Vec<TaskId> {
        let mut hasher = DefaultHasher::new();
        event.keys[1].hash(&mut hasher); // Use the model selector to create a unique ID
        vec![hasher.finish()] // Return the dependency on the register_model task
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

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<()> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <StoreUpdateRecordProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::StoreUpdateRecord(e) => e,
            _ => {
                unreachable!()
            }
        };

        let model_selector = event.selector;
        let entity_id = event.entity_id;

        // If the model does not exist, silently ignore it.
        // This can happen if only specific namespaces are indexed.
        let model = match ctx.cache.model_cache.model(&event.selector).await {
            Ok(m) => m,
            Err(e) if e.to_string().contains("no rows") && !ctx.config.namespaces.is_empty() => {
                debug!(
                    target: LOG_TARGET,
                    selector = %event.selector,
                    "Model does not exist, skipping."
                );
                return Ok(());
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        info!(
            target: LOG_TARGET,
            namespace = %model.namespace,
            name = %model.name,
            entity_id = format!("{:#x}", entity_id),
            "Store update record.",
        );

        let mut entity = model.schema;
        match entity {
            Ty::Struct(ref mut struct_) => {
                // we do not need the keys. the entity Ty has the keys in its schema
                // so we should get rid of them to avoid trying to deserialize them
                struct_.children.retain(|field| !field.key);
            }
            _ => unreachable!(),
        }

        let mut values = event.values.to_vec();
        entity.deserialize(&mut values)?;

        ctx.storage
            .set_entity(
                entity,
                &ctx.event_id,
                ctx.block_timestamp,
                entity_id,
                model_selector,
                None,
            )
            .await?;
        Ok(())
    }
}
