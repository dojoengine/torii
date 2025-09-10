use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use cainome::cairo_serde::Error as CainomeError;
use dojo_types::naming::compute_selector_from_names;
use dojo_types::schema::Ty;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::contracts::model::{ModelError, ModelRPCReader, ModelReader};
use dojo_world::contracts::WorldContractReader;
use starknet::core::types::{BlockId, Event, StarknetError};
use starknet::providers::{Provider, ProviderError};
use torii_proto::Model;
use tracing::{debug, info};

use crate::error::Error;
use crate::metrics::ProcessorMetrics;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::register_model";

#[derive(Default, Debug)]
pub struct RegisterModelProcessor;

#[async_trait]
impl<P> EventProcessor<P> for RegisterModelProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
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

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        let _timer = ProcessorMetrics::start_timer("register_model");
        
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
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
        let selector = compute_selector_from_names(&namespace, &name);

        // If the namespace is not in the list of namespaces to index, silently ignore it.
        // If our config is empty, we index all namespaces.
        if !ctx.config.should_index(&namespace, &name) {
            ProcessorMetrics::increment_success("register_model");
            return Ok(());
        }

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

        let use_legacy_store = match model.use_legacy_storage().await {
            Ok(use_legacy_store) => use_legacy_store,
            Err(ModelError::Cainome(CainomeError::Provider(ProviderError::StarknetError(
                StarknetError::EntrypointNotFound,
            )))) => {
                debug!(target: LOG_TARGET, namespace = %namespace, name = %name, "Entrypoint not found, using legacy store.");
                true
            }
            Err(e) => {
                return Err(e.into());
            }
        };
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

        ctx.storage
            .register_model(
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
                use_legacy_store,
            )
            .await?;

        ctx.cache
            .register_model(
                selector,
                Model {
                    selector,
                    namespace,
                    name,
                    class_hash: event.class_hash.into(),
                    contract_address: event.address.into(),
                    packed_size,
                    unpacked_size,
                    layout,
                    schema,
                    use_legacy_store,
                },
            )
            .await;

        ProcessorMetrics::increment_success("register_model");
        ProcessorMetrics::record_operation("model_registered", 1);

        Ok(())
    }
}
