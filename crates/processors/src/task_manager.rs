use std::sync::Arc;

use hashlink::LinkedHashMap;
use starknet::core::types::Event;
use starknet::providers::Provider;
use starknet_crypto::Felt;
use tokio::sync::Semaphore;
use torii_cache::Cache;
use torii_proto::ContractType;
use torii_storage::Storage;
use torii_task_network::TaskNetwork;
use tracing::{debug, error};

use crate::error::Error;
use crate::processors::Processors;
use crate::{EventKey, EventProcessorConfig, EventProcessorContext, IndexingMode};
use metrics::{counter, histogram};

const LOG_TARGET: &str = "torii::indexer::task_manager";

pub type TaskId = u64;
pub type TaskPriority = usize;

#[derive(Debug, Clone)]
pub struct ParallelizedEvent {
    pub indexing_mode: IndexingMode,
    pub contract_type: ContractType,
    pub contract_address: Felt,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub event_id: String,
    pub event: Event,
}

#[derive(Debug, Clone, Default)]
struct TaskData {
    events: Vec<ParallelizedEvent>,
    latest_only_events: LinkedHashMap<EventKey, ParallelizedEvent>,
}

#[allow(missing_debug_implementations)]
pub struct TaskManager<P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static> {
    storage: Arc<dyn Storage>,
    cache: Arc<dyn Cache>,
    provider: P,
    task_network: TaskNetwork<TaskId, TaskData>,
    processors: Arc<Processors<P>>,
    event_processor_config: EventProcessorConfig,
    nft_metadata_semaphore: Arc<Semaphore>,
}

impl<P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static> TaskManager<P> {
    pub fn new(
        storage: Arc<dyn Storage>,
        cache: Arc<dyn Cache>,
        provider: P,
        processors: Arc<Processors<P>>,
        max_concurrent_tasks: usize,
        event_processor_config: EventProcessorConfig,
    ) -> Self {
        Self {
            storage,
            cache,
            provider,
            task_network: TaskNetwork::new(max_concurrent_tasks),
            processors,
            nft_metadata_semaphore: Arc::new(Semaphore::new(
                event_processor_config.max_metadata_tasks,
            )),
            event_processor_config,
        }
    }

    pub fn pending_tasks_count(&self) -> usize {
        self.task_network.len()
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
            match parallelized_event.indexing_mode {
                IndexingMode::Latest(event_key) => {
                    task_data
                        .latest_only_events
                        .insert(event_key, parallelized_event);
                }
                IndexingMode::Historical => {
                    task_data.events.push(parallelized_event);
                }
            }
        } else {
            let task_data = match parallelized_event.indexing_mode {
                IndexingMode::Latest(event_key) => TaskData {
                    latest_only_events: LinkedHashMap::from_iter(vec![(
                        event_key,
                        parallelized_event.clone(),
                    )]),
                    ..Default::default()
                },
                IndexingMode::Historical => TaskData {
                    events: vec![parallelized_event.clone()],
                    ..Default::default()
                },
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

        let storage = self.storage.clone();
        let processors = self.processors.clone();
        let provider = self.provider.clone();
        let event_processor_config = self.event_processor_config.clone();
        let cache = self.cache.clone();
        let nft_metadata_semaphore = self.nft_metadata_semaphore.clone();

        self.task_network
            .process_tasks(move |task_id, task_data| {
                let storage = storage.clone();
                let processors = processors.clone();
                let provider = provider.clone();
                let event_processor_config = event_processor_config.clone();
                let cache = cache.clone();
                let nft_metadata_semaphore = nft_metadata_semaphore.clone();

                async move {
                    // Process all events for this task sequentially
                    for ParallelizedEvent {
                        contract_type,
                        contract_address,
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

                            let ctx = EventProcessorContext {
                                contract_address: *contract_address,
                                storage: storage.clone(),
                                cache: cache.clone(),
                                provider: provider.clone(),
                                block_number: *block_number,
                                block_timestamp: *block_timestamp,
                                event_id: event_id.clone(),
                                event: event.clone(),
                                config: event_processor_config.clone(),
                                nft_metadata_semaphore: nft_metadata_semaphore.clone(),
                            };

                            // Record processor timing and success/error metrics
                            let start_time = std::time::Instant::now();
                            let processor_name = processor.event_key();

                            let result = processor.process(&ctx).await;

                            let duration = start_time.elapsed();
                            histogram!(
                                "torii_processor_duration_seconds",
                                "processor" => processor_name.clone()
                            )
                            .record(duration.as_secs_f64());

                            match &result {
                                Ok(_) => {
                                    counter!(
                                        "torii_processor_events_processed_total",
                                        "processor" => processor_name.clone(),
                                        "status" => "success"
                                    )
                                    .increment(1);
                                }
                                Err(_) => {
                                    counter!(
                                        "torii_processor_events_processed_total",
                                        "processor" => processor_name.clone(),
                                        "status" => "error"
                                    )
                                    .increment(1);
                                }
                            }

                            if let Err(e) = result {
                                error!(
                                    target: LOG_TARGET,
                                    event_name = processor_name,
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
