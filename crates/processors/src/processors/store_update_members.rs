use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use dojo_types::schema::Ty;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::contracts::world::WorldContractReader;
use starknet::core::types::Event;
use starknet::providers::Provider;
use torii_sqlite::Sql;
use tracing::{debug, info};

use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig};
use crate::{IndexingMode, Result};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::store_update_members";

#[derive(Default, Debug)]
pub struct StoreUpdateMembersProcessor;

#[async_trait]
impl<P> EventProcessor<P> for StoreUpdateMembersProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "StoreUpdateMembers".to_string()
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
            let n_members: u32 = event.data[0].into();
            let mut members = event.data[1..(1 + n_members as usize)].to_vec();
            members.sort();
            members.hash(&mut hasher);
            IndexingMode::Latest(hasher.finish())
        }
    }

    async fn process(
        &self,
        _world: Arc<WorldContractReader<P>>,
        db: &mut Sql,
        _block_number: u64,
        block_timestamp: u64,
        event_id: &str,
        event: &Event,
        config: &EventProcessorConfig,
    ) -> Result<()> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <StoreUpdateSchemaProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::StoreUpdateMembers(e) => e,
            _ => {
                unreachable!()
            }
        };

        let model_selector = event.selector;
        let entity_id = event.entity_id;
        let member_selectors = event.member_selectors;

        // If the model does not exist, silently ignore it.
        // This can happen if only specific namespaces are indexed.
        let model = match db.model(event.selector).await {
            Ok(m) => m,
            Err(e) if e.to_string().contains("no rows") && !config.namespaces.is_empty() => {
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

        let schema = model.schema;
        let mut values = event.values.to_vec();

        let mut members = vec![];
        for member_selector in member_selectors {
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
            member.ty.deserialize(&mut values)?;
            members.append(member);
        }

        info!(
            target: LOG_TARGET,
            namespace = %model.namespace,
            name = %model.name,
            entity_id = format!("{:#x}", entity_id),
            "Store update members.",
        );

        let wrapped_ty = Ty::Struct(Struct {
            name: schema.name(),
            children: members,
        });

        db.set_entity(
            wrapped_ty,
            event_id,
            block_timestamp,
            entity_id,
            model_selector,
            None,
        )
        .await?;
        Ok(())
    }
}
