use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::contracts::world::WorldContractReader;
use starknet::core::types::Event;
use starknet::providers::Provider;
use torii_sqlite::utils::felts_to_sql_string;
use torii_sqlite::Sql;
use tracing::{debug, info};

use crate::error::Error;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorConfig, IndexingMode};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::store_set_record";

#[derive(Default, Debug)]
pub struct StoreSetRecordProcessor;

#[async_trait]
impl<P> EventProcessor<P> for StoreSetRecordProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "StoreSetRecord".to_string()
    }

    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        event.keys[1].hash(&mut hasher);
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
            IndexingMode::Latest
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
    ) -> Result<(), Error> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <StoreSetRecordProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::StoreSetRecord(e) => e,
            _ => {
                unreachable!()
            }
        };

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

        info!(
            target: LOG_TARGET,
            namespace = %model.namespace,
            name = %model.name,
            entity_id = format!("{:#x}", event.entity_id),
            "Store set record.",
        );

        let keys_str = felts_to_sql_string(&event.keys);

        let mut keys_and_unpacked = [event.keys, event.values].concat();

        let mut entity = model.schema;
        entity.deserialize(&mut keys_and_unpacked)?;

        db.set_entity(
            entity,
            event_id,
            block_timestamp,
            event.entity_id,
            event.selector,
            Some(&keys_str),
        )
        .await?;
        Ok(())
    }
}
