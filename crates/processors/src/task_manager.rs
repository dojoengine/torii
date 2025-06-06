use std::sync::Arc;

use dojo_world::contracts::WorldContractReader;
use hashlink::LinkedHashMap;
use starknet::core::types::Event;
use starknet::providers::Provider;
use starknet_crypto::Felt;
use torii_sqlite::types::ContractType;
use torii_sqlite::Sql;
use torii_task_network::TaskNetwork;
use tracing::{debug, error};

use crate::error::Error;
use crate::processors::Processors;
use crate::{EventProcessorConfig, IndexingMode};

const LOG_TARGET: &str = "torii::indexer::task_manager";

pub type TaskId = u64;
pub type TaskPriority = usize;

#[derive(Debug, Clone)]
pub struct ParallelizedEvent {
    pub indexing_mode: IndexingMode,
    pub contract_type: ContractType,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub event_id: String,
    pub event: Event,
}

#[derive(Debug, Clone, Default)]
struct TaskData {
    events: Vec<ParallelizedEvent>,
    latest_only_events: LinkedHashMap<Felt, ParallelizedEvent>,
}

#[allow(missing_debug_implementations)]
pub struct TaskManager<P: Provider + Send + Sync + std::fmt::Debug + 'static> {
    db: Sql,
    world: Arc<WorldContractReader<P>>,
    task_network: TaskNetwork<TaskId, TaskData>,
    processors: Arc<Processors<P>>,
    event_processor_config: EventProcessorConfig,
}

impl<P: Provider + Send + Sync + std::fmt::Debug + 'static> TaskManager<P> {
    pub fn new(
        db: Sql,
        world: Arc<WorldContractReader<P>>,
        processors: Arc<Processors<P>>,
        max_concurrent_tasks: usize,
        event_processor_config: EventProcessorConfig,
    ) -> Self {
        Self {
            db,
            world,
            task_network: TaskNetwork::new(max_concurrent_tasks),
            processors,
            event_processor_config,
        }
    }

    pub fn add_parallelized_event(
        &mut self,
        task_identifier: TaskId,
        parallelized_event: ParallelizedEvent,
    ) {
        self.add_parallelized_event_with_dependencies(task_identifier, vec![], parallelized_event);
    }

    pub fn add_parallelized_event_with_dependencies(
        &mut self,
        task_identifier: TaskId,
        dependencies: Vec<TaskId>,
        parallelized_event: ParallelizedEvent,
    ) {
        if let Some(task_data) = self.task_network.get_mut(&task_identifier) {
            if parallelized_event.indexing_mode == IndexingMode::Latest {
                task_data
                    .latest_only_events
                    .insert(parallelized_event.event.keys[0], parallelized_event);
            } else {
                task_data.events.push(parallelized_event);
            }
        } else {
            let task_data = if parallelized_event.indexing_mode == IndexingMode::Latest {
                TaskData {
                    latest_only_events: LinkedHashMap::from_iter(vec![(
                        parallelized_event.event.keys[0],
                        parallelized_event.clone(),
                    )]),
                    ..Default::default()
                }
            } else {
                TaskData {
                    events: vec![parallelized_event.clone()],
                    ..Default::default()
                }
            };

            if let Err(e) = self.task_network.add_task_with_dependencies(
                task_identifier,
                task_data,
                dependencies.clone(),
            ) {
                error!(
                    target: LOG_TARGET,
                    error = ?e,
                    task_id = %task_identifier,
                    dependencies = ?dependencies,
                    parallelized_event = ?parallelized_event,
                    "Failed to add task with dependencies to network."
                );
            }
        }
    }

    pub async fn process_tasks(&mut self) -> Result<(), Error> {
        if self.task_network.is_empty() {
            return Ok(());
        }

        let db = self.db.clone();
        let world = self.world.clone();
        let processors = self.processors.clone();
        let event_processor_config = self.event_processor_config.clone();

        self.task_network
            .process_tasks(move |task_id, task_data| {
                let db = db.clone();
                let world = world.clone();
                let processors = processors.clone();
                let event_processor_config = event_processor_config.clone();

                async move {
                    let mut local_db = db.clone();

                    // Process all events for this task sequentially
                    for ParallelizedEvent {
                        contract_type,
                        event,
                        block_number,
                        block_timestamp,
                        event_id,
                        ..
                    } in task_data
                        .events
                        .iter()
                        .chain(task_data.latest_only_events.values())
                    {
                        let contract_processors = processors.get_event_processors(*contract_type);
                        if let Some(processors) = contract_processors.get(&event.keys[0]) {
                            let processor = processors
                                .iter()
                                .find(|p| p.validate(event))
                                .expect("Must find at least one processor for the event");

                            debug!(
                                target: LOG_TARGET,
                                event_name = processor.event_key(),
                                event_id = %event_id,
                                block_number = %block_number,
                                task_id = %task_id,
                                "Processing parallelized event."
                            );

                            if let Err(e) = processor
                                .process(
                                    world.clone(),
                                    &mut local_db,
                                    *block_number,
                                    *block_timestamp,
                                    event_id,
                                    event,
                                    &event_processor_config,
                                )
                                .await
                            {
                                error!(
                                    target: LOG_TARGET,
                                    event_name = processor.event_key(),
                                    error = ?e,
                                    task_id = %task_id,
                                    "Processing parallelized event."
                                );
                                return Err(e);
                            }
                        }
                    }

                    Ok::<_, Error>(())
                }
            })
            .await
            .map_err(Error::TaskNetworkError)?;

        Ok(())
    }

    pub fn clear_tasks(&mut self) {
        self.task_network.clear();
    }
}
