use std::hash::{DefaultHasher, Hash, Hasher};

use anyhow::{Error, Result};
use async_trait::async_trait;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::contracts::model::{ModelRPCReader, ModelReader};
use dojo_world::contracts::world::WorldContractReader;
use starknet::core::types::{BlockId, Event};
use starknet::providers::Provider;
use torii_sqlite::types::ContractType;
use torii_sqlite::Sql;
use tracing::{debug, info};

use crate::{EventProcessor, EventProcessorConfig};
use crate::TaskId;

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::upgrade_event";

#[derive(Default, Debug)]
pub struct UpgradeEventProcessor;

#[async_trait]
impl<P> EventProcessor<P> for UpgradeEventProcessor
where
    P: Provider + Send + Sync + std::fmt::Debug,
{
    fn contract_type() -> ContractType {
        ContractType::World
    }

    fn event_key() -> String {
        "EventUpgraded".to_string()
    }

    // We might not need this anymore, since we don't have fallback and all world events must
    // be handled.
    fn validate(event: &Event) -> bool {
        true
    }

    fn task_dependencies(event: &Event) -> Vec<TaskId> {
        vec![]
    }

    fn task_identifier(event: &Event) -> TaskId {
        let mut hasher = DefaultHasher::new();
        // model id
        event.keys[1].hash(&mut hasher);
        hasher.finish()
    }

    async fn process(
        world: &WorldContractReader<P>,
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
                <UpgradeEventProcessor as EventProcessor<P>>::event_key()
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
        let model = match db.model(event.selector).await {
            Ok(m) => m,
            Err(e) if e.to_string().contains("no rows") => {
                debug!(
                    target: LOG_TARGET,
                    selector = %event.selector,
                    "Model does not exist, skipping."
                );
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let name = model.name;
        let namespace = model.namespace;
        let prev_schema = model.schema;

        let mut model =
            ModelRPCReader::new(&namespace, &name, event.address.0, event.class_hash.0, world)
                .await;
        if config.strict_model_reader {
            model.set_block(BlockId::Number(block_number)).await;
        }
        let new_schema = model.schema().await?;
        let schema_diff = prev_schema.diff(&new_schema);
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

        db.register_model(
            &namespace,
            &new_schema,
            layout,
            event.class_hash.into(),
            event.address.into(),
            packed_size,
            unpacked_size,
            block_timestamp,
            Some(&schema_diff),
        )
        .await?;

        Ok(())
    }
}
