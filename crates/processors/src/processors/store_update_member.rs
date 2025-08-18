use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use dojo_types::schema::{Struct, Ty};
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use starknet::core::types::Event;
use starknet::core::utils::get_selector_from_name;
use starknet::providers::Provider;
use tracing::{debug, info};

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig, EventProcessorContext};
use crate::{IndexingMode, Result};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::store_update_member";

#[derive(Default, Debug)]
pub struct StoreUpdateMemberProcessor;

#[async_trait]
impl<P> EventProcessor<P> for StoreUpdateMemberProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "StoreUpdateMember".to_string()
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
            event.keys[0].hash(&mut hasher); // event id
                                             // we need the memebr selector to make sure we dont miss some other member updates
            event.keys[3].hash(&mut hasher); // member selector.
            IndexingMode::Latest(hasher.finish())
        }
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<()> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <StoreUpdateMemberProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::StoreUpdateMember(e) => e,
            _ => {
                unreachable!()
            }
        };

        let model_selector = event.selector;
        let entity_id = event.entity_id;
        let member_selector = event.member_selector;

        // If the model does not exist, silently ignore it.
        // This can happen if only specific namespaces are indexed.
        let model = match ctx.cache.model(model_selector).await {
            Ok(m) => m,
            Err(e) if e.to_string().contains("no rows") && !ctx.config.namespaces.is_empty() => {
                debug!(
                    target: LOG_TARGET,
                    selector = %model_selector,
                    "Model does not exist, skipping."
                );
                return Ok(());
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        let schema = model.schema;

        let mut member = schema
            .as_struct()
            .expect("model schema must be a struct")
            .children
            .iter()
            .find(|c| {
                get_selector_from_name(&c.name).expect("invalid selector for member name")
                    == member_selector
            })
            .ok_or(Error::ModelMemberNotFound(format!(
                "{:#x}",
                member_selector
            )))?
            .clone();

        info!(
            target: LOG_TARGET,
            name = %model.name,
            entity_id = format!("{:#x}", entity_id),
            member = %member.name,
            "Store update member.",
        );

        let mut values = event.values.to_vec();
        member.ty.deserialize(&mut values, model.use_legacy_store)?;

        let wrapped_ty = Ty::Struct(Struct {
            name: schema.name(),
            children: vec![member],
        });

        ctx.storage
            .set_entity(
                wrapped_ty,
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
