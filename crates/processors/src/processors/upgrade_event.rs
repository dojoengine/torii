use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use dojo_types::schema::Ty;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::contracts::model::{ModelRPCReader, ModelReader};
use dojo_world::contracts::WorldContractReader;
use starknet::core::types::{BlockId, Event};
use starknet::providers::Provider;
use torii_proto::Model;
use tracing::{debug, info};

use crate::task_manager::TaskId;
use crate::EventProcessor;
use crate::{EventProcessorContext, Result};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::upgrade_event";

#[derive(Default, Debug)]
pub struct UpgradeEventProcessor;

#[async_trait]
impl<P> EventProcessor<P> for UpgradeEventProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "EventUpgraded".to_string()
    }

    // We might not need this anymore, since we don't have fallback and all world events must
    // be handled.
    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        event.keys[1].hash(&mut hasher); // Use the event selector to create a unique ID
        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<()> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <UpgradeEventProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::EventUpgraded(e) => e,
            _ => {
                unreachable!()
            }
        };

        // Called model here by language, but it's an event. Torii rework will make clear
        // distinction.

        // If the model does not exist, silently ignore it.
        // This can happen if only specific namespaces are indexed.
        let model = match ctx
            .storage
            .model(ctx.contract_address, event.selector)
            .await
        {
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

        let world = WorldContractReader::new(ctx.event.from_address, &ctx.provider);
        let mut model = ModelRPCReader::new(
            &namespace,
            &name,
            event.address.0,
            event.class_hash.0,
            &world,
        )
        .await;
        if ctx.config.strict_model_reader {
            model.set_block(BlockId::Number(ctx.block_number)).await;
        }
        let mut new_schema = model.schema().await?;
        match &mut new_schema {
            Ty::Struct(struct_ty) => {
                struct_ty.name = format!("{}-{}", namespace, struct_ty.name);
            }
            _ => unreachable!(),
        }
        let schema_diff = new_schema.diff(&prev_schema);
        // No changes to the schema. This can happen if torii is re-run with a fresh database.
        // As the register model fetches the latest schema from the chain.
        if schema_diff.is_none() {
            return Ok(());
        }

        let schema_diff = schema_diff.unwrap();
        let layout = model.layout().await?;

        // Events are never stored onchain, hence no packing or unpacking.
        let unpacked_size: u32 = 0;
        let packed_size: u32 = 0;

        info!(
            target: LOG_TARGET,
            namespace = %namespace,
            name = %name,
            "Upgraded event."
        );

        debug!(
            target: LOG_TARGET,
            name,
            diff = ?schema_diff,
            layout = ?layout,
            class_hash = ?event.class_hash,
            contract_address = ?event.address,
            packed_size = %packed_size,
            unpacked_size = %unpacked_size,
            "Upgraded event content."
        );

        ctx.storage
            .register_model(
                ctx.contract_address,
                event.selector,
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
                // Event models, still use Serde for serialization. So we need to use the legacy store.
                true,
            )
            .await?;

        ctx.cache
            .register_model(
                ctx.contract_address,
                event.selector,
                Model {
                    world_address: ctx.contract_address,
                    selector: event.selector,
                    namespace,
                    name,
                    class_hash: event.class_hash.into(),
                    contract_address: event.address.into(),
                    packed_size,
                    unpacked_size,
                    layout,
                    schema: new_schema,
                    // Event models, still use Serde for serialization. So we need to use the legacy store.
                    use_legacy_store: true,
                },
            )
            .await;

        Ok(())
    }
}
