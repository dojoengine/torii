use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::contracts::model::{ModelRPCReader, ModelReader};
use starknet::core::types::{BlockId, Event};
use starknet::providers::Provider;
use torii_storage::types::Model;
use tracing::{debug, info};

use crate::task_manager::TaskId;
use crate::EventProcessor;
use crate::{EventProcessorContext, Result};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::upgrade_model";

#[derive(Default, Debug)]
pub struct UpgradeModelProcessor;

#[async_trait]
impl<P> EventProcessor<P> for UpgradeModelProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "ModelUpgraded".to_string()
    }

    // We might not need this anymore, since we don't have fallback and all world events must
    // be handled.
    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        event.keys[1].hash(&mut hasher); // Use the model selector to create a unique ID
        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<()> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <UpgradeModelProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::ModelUpgraded(e) => e,
            _ => {
                unreachable!()
            }
        };

        // If the model does not exist, silently ignore it.
        // This can happen if only specific namespaces are indexed.
        let model = match ctx.cache.model(&event.selector).await {
            Ok(m) => m,
            Err(e) if e.to_string().contains("no rows") => {
                debug!(
                    target: LOG_TARGET,
                    selector = %event.selector,
                    "Model does not exist, skipping."
                );
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let name = model.name;
        let namespace = model.namespace;
        let prev_schema = model.schema;

        let mut model = ModelRPCReader::new(
            &namespace,
            &name,
            event.address.0,
            event.class_hash.0,
            &ctx.world,
        )
        .await;
        if ctx.config.strict_model_reader {
            model.set_block(BlockId::Number(ctx.block_number)).await;
        }
        let new_schema = model.schema().await?;
        let schema_diff = new_schema.diff(&prev_schema);
        // // No changes to the schema. This can happen if torii is re-run with a fresh database.
        // // As the register model fetches the latest schema from the chain.
        if schema_diff.is_none() {
            return Ok(());
        }

        let schema_diff = schema_diff.unwrap();
        let layout = model.layout().await?;

        let unpacked_size: u32 = model.unpacked_size().await?;
        let packed_size: u32 = model.packed_size().await?;

        info!(
            target: LOG_TARGET,
            namespace = %namespace,
            name = %name,
            "Upgraded model."
        );

        debug!(
            target: LOG_TARGET,
            name = %name,
            diff = ?schema_diff,
            layout = ?layout,
            class_hash = ?event.class_hash,
            contract_address = ?event.address,
            packed_size = %packed_size,
            unpacked_size = %unpacked_size,
            "Upgraded model content."
        );

        ctx.storage
            .register_model(
                &namespace,
                &new_schema,
                &layout,
                event.class_hash.into(),
                event.address.into(),
                packed_size,
                unpacked_size,
                ctx.block_timestamp,
                Some(&schema_diff),
                // This will be Some if we have an "upgrade" diff. Which means
                // if some columns have been modified.
                prev_schema.diff(&new_schema).as_ref(),
            )
            .await?;

        ctx.cache.register_model(event.selector, Model {
            selector: event.selector,
            namespace,
            name,
            class_hash: event.class_hash.into(),
            contract_address: event.address.into(),
            packed_size,
            unpacked_size,
            layout,
            schema: new_schema,
        }).await;

        Ok(())
    }
}
