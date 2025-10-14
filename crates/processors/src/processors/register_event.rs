use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use dojo_types::naming::compute_selector_from_names;
use dojo_types::schema::Ty;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::contracts::model::{ModelRPCReader, ModelReader};
use dojo_world::contracts::WorldContractReader;
use starknet::core::types::{BlockId, Event};
use starknet::providers::Provider;
use torii_proto::Model;
use tracing::{debug, info};

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::register_event";

#[derive(Default, Debug)]
pub struct RegisterEventProcessor;

#[async_trait]
impl<P> EventProcessor<P> for RegisterEventProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "EventRegistered".to_string()
    }

    // We might not need this anymore, since we don't have fallback and all world events must
    // be handled.
    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let selector = match WorldEvent::try_from(event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <RegisterEventProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::EventRegistered(e) => compute_selector_from_names(
                &e.namespace.to_string().unwrap(),
                &e.name.to_string().unwrap(),
            ),
            _ => {
                unreachable!()
            }
        };

        let mut hasher = DefaultHasher::new();
        event.from_address.hash(&mut hasher);
        selector.hash(&mut hasher);
        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <RegisterEventProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::EventRegistered(e) => e,
            _ => {
                unreachable!()
            }
        };

        // Safe to unwrap, since it's coming from the chain.
        let namespace = event.namespace.to_string().unwrap();
        let name = event.name.to_string().unwrap();
        let selector = compute_selector_from_names(&namespace, &name);

        // If the namespace is not in the list of namespaces to index, silently ignore it.
        // If our config is empty, we index all namespaces.
        if !ctx.config.should_index(&namespace, &name) {
            return Ok(());
        }

        // Called model here by language, but it's an event. Torii rework will make clear
        // distinction.
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
        let mut schema = model.schema().await?;
        match &mut schema {
            Ty::Struct(struct_ty) => {
                struct_ty.name = format!("{}-{}", namespace, struct_ty.name);
            }
            _ => unreachable!(),
        }
        let layout = model.layout().await?;
        // Events are never stored onchain, hence no packing or unpacking.
        let unpacked_size: u32 = 0;
        let packed_size: u32 = 0;

        info!(
            target: LOG_TARGET,
            namespace = %namespace,
            name = %name,
            "Registered event."
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
            "Registered event content."
        );

        // Event models, still use Serde for serialization. So we need to use the legacy store.
        ctx.storage
            .register_model(
                ctx.contract_address,
                selector,
                &schema,
                &layout,
                event.class_hash.into(),
                event.address.into(),
                packed_size,
                unpacked_size,
                ctx.block_timestamp,
                None,
                None,
                true,
            )
            .await?;

        ctx.cache
            .register_model(
                ctx.contract_address,
                selector,
                Model {
                    world_address: ctx.contract_address,
                    selector,
                    namespace,
                    name,
                    class_hash: event.class_hash.into(),
                    contract_address: event.address.into(),
                    packed_size,
                    unpacked_size,
                    layout,
                    schema,
                    // Event models, still use Serde for serialization. So we need to use the legacy store.
                    use_legacy_store: true,
                },
            )
            .await;

        Ok(())
    }
}
