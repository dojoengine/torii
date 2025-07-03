use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use dojo_world::contracts::world::WorldContractReader;
use lazy_static::lazy_static;
use starknet::core::types::{Event, TransactionContent};
use starknet::macros::selector;
use starknet::providers::Provider;
use starknet_crypto::Felt;
use tokio::sync::broadcast::Sender;
use tokio::time::{sleep, Instant};
use torii_processors::{EventProcessorConfig, Processors};
use torii_sqlite::cache::ContractClassCache;
use torii_sqlite::controllers::ControllersSync;
use torii_sqlite::types::{Contract, ContractType};
use torii_sqlite::utils::format_event_id;
use torii_sqlite::Sql;
use tracing::{debug, error, info, trace};

use crate::constants::LOG_TARGET;
use crate::error::{Error, ProcessError};
use crate::IndexingFlags;
use torii_indexer_fetcher::{
    FetchPendingResult, FetchRangeResult, FetchResult, Fetcher, FetcherConfig,
};
use torii_processors::task_manager::{ParallelizedEvent, TaskManager};

lazy_static! {
    static ref DOJO_RELATED_EVENTS: HashSet<Felt> = {
        HashSet::from([
            selector!("StoreSetRecord"),
            selector!("StoreUpdateRecord"),
            selector!("StoreDelRecord"),
            selector!("StoreUpdateMember"),
            selector!("EventEmitted"),
        ])
    };
}

#[derive(Debug)]
pub struct EngineConfig {
    pub polling_interval: Duration,
    pub fetcher_config: FetcherConfig,
    pub max_concurrent_tasks: usize,
    pub flags: IndexingFlags,
    pub event_processor_config: EventProcessorConfig,
    pub world_block: u64,
}

#[allow(missing_debug_implementations)]
pub struct Engine<P: Provider + Send + Sync + std::fmt::Debug + 'static> {
    world: Arc<WorldContractReader<P>>,
    db: Sql,
    provider: Arc<P>,
    processors: Arc<Processors<P>>,
    config: EngineConfig,
    shutdown_tx: Sender<()>,
    task_manager: TaskManager<P>,
    contracts: Arc<HashMap<Felt, ContractType>>,
    contract_class_cache: Arc<ContractClassCache<P>>,
    controllers: Option<Arc<ControllersSync>>,
    fetcher: Fetcher<P>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            polling_interval: Duration::from_millis(500),
            max_concurrent_tasks: 100,
            flags: IndexingFlags::empty(),
            event_processor_config: EventProcessorConfig::default(),
            world_block: 0,
            fetcher_config: FetcherConfig::default(),
        }
    }
}

struct UnprocessedEvent {
    keys: Vec<String>,
    data: Vec<String>,
}

impl<P: Provider + Send + Sync + std::fmt::Debug + 'static> Engine<P> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        world: WorldContractReader<P>,
        db: Sql,
        provider: P,
        processors: Processors<P>,
        config: EngineConfig,
        shutdown_tx: Sender<()>,
        contracts: &[Contract],
    ) -> Self {
        Self::new_with_controllers(
            world,
            db,
            provider,
            processors,
            config,
            shutdown_tx,
            contracts,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_controllers(
        world: WorldContractReader<P>,
        db: Sql,
        provider: P,
        processors: Processors<P>,
        config: EngineConfig,
        shutdown_tx: Sender<()>,
        contracts: &[Contract],
        controllers: Option<Arc<ControllersSync>>,
    ) -> Self {
        let contracts = Arc::new(
            contracts
                .iter()
                .map(|contract| (contract.address, contract.r#type))
                .collect(),
        );
        let world = Arc::new(world);
        let processors = Arc::new(processors);
        let max_concurrent_tasks = config.max_concurrent_tasks;
        let event_processor_config = config.event_processor_config.clone();
        let fetcher_config = config.fetcher_config.clone();
        let provider = Arc::new(provider);

        Self {
            world: world.clone(),
            db: db.clone(),
            provider: provider.clone(),
            processors: processors.clone(),
            config,
            shutdown_tx,
            contracts,
            task_manager: TaskManager::new(
                db,
                world,
                processors,
                max_concurrent_tasks,
                event_processor_config,
            ),
            contract_class_cache: Arc::new(ContractClassCache::new(provider.clone())),
            controllers,
            fetcher: Fetcher::new(provider.clone(), fetcher_config),
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut fetching_backoff_delay = Duration::from_secs(1);
        let mut processing_backoff_delay = Duration::from_secs(1);
        let max_backoff_delay = Duration::from_secs(60);

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let mut fetching_erroring_out = false;
        let mut processing_erroring_out = false;
        // The last fetch result & cursors, in case the processing fails, but not fetching.
        // Thus we can retry the processing with the same data instead of fetching again.
        let mut cached_data: Option<Arc<FetchResult>> = None;

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    break Ok(());
                }
                res = async {
                    if let Some(last_fetch_result) = cached_data.as_ref() {
                        Result::<_, Error>::Ok(last_fetch_result.clone())
                    } else {
                        let cursors = self.db.cursors().await?;
                        let fetch_result = self.fetcher.fetch(&cursors).await?;
                        Ok(Arc::new(fetch_result))
                    }
                } => {
                    match res {
                        Ok(fetch_result) => {
                            if fetching_erroring_out && cached_data.is_none() {
                                fetching_erroring_out = false;
                                fetching_backoff_delay = Duration::from_secs(1);
                                info!(target: LOG_TARGET, "Fetching reestablished.");
                            }

                            // Cache the fetch result for retry
                            cached_data = Some(fetch_result.clone());

                            let instant = Instant::now();
                            match self.process(&fetch_result).await {
                                Ok(_) => {
                                    // Only reset backoff delay after successful processing
                                    if processing_erroring_out {
                                        processing_erroring_out = false;
                                        processing_backoff_delay = Duration::from_secs(1);
                                        info!(target: LOG_TARGET, "Processing reestablished.");
                                    }
                                    // Reset the cached data
                                    cached_data = None;
                                    // Sync controllers
                                    if let Some(controllers) = &self.controllers {
                                        let instant = Instant::now();
                                        debug!(target: LOG_TARGET, "Syncing controllers.");
                                        let num_controllers = controllers.sync().await.map_err(Error::ControllerSync)?;
                                        debug!(target: LOG_TARGET, duration = ?instant.elapsed(), num_controllers = num_controllers, "Synced controllers.");
                                        if num_controllers > 0 {
                                            info!(target: LOG_TARGET, num_controllers = num_controllers, "Synced controllers.");
                                        }
                                    }
                                    self.db.execute().await?;
                                },
                                Err(e) => {
                                    error!(target: LOG_TARGET, error = ?e, "Processing fetched data.");
                                    processing_erroring_out = true;
                                    self.db.rollback().await?;
                                    self.task_manager.clear_tasks();
                                    sleep(processing_backoff_delay).await;
                                    if processing_backoff_delay < max_backoff_delay {
                                        processing_backoff_delay *= 2;
                                    }
                                }
                            }

                            debug!(target: LOG_TARGET, duration = ?instant.elapsed(), "Processed fetched data.");
                        }
                        Err(e) => {
                            fetching_erroring_out = true;
                            cached_data = None;
                            error!(target: LOG_TARGET, error = ?e, "Fetching data.");
                            sleep(fetching_backoff_delay).await;
                            if fetching_backoff_delay < max_backoff_delay {
                                fetching_backoff_delay *= 2;
                            }
                        }
                    };
                    sleep(self.config.polling_interval).await;
                }
            }
        }
    }

    pub async fn process(&mut self, fetch_result: &FetchResult) -> Result<(), ProcessError> {
        let FetchResult { range, pending } = fetch_result;

        self.process_range(range).await?;
        if let Some(pending) = pending {
            self.process_pending(pending).await?;
        }

        Ok(())
    }

    pub async fn process_range(&mut self, range: &FetchRangeResult) -> Result<(), ProcessError> {
        let mut processed_blocks = HashSet::new();

        // Process all transactions in the chunk
        for (block_number, block) in &range.blocks {
            for (transaction_hash, tx) in &block.transactions {
                if tx.events.is_empty() {
                    continue;
                }

                trace!(target: LOG_TARGET, "Processing transaction hash: {:#x}", transaction_hash);

                self.process_transaction_with_events(
                    *transaction_hash,
                    tx.events.as_slice(),
                    *block_number,
                    block.timestamp,
                    &tx.transaction,
                )
                .await?;
            }

            // Process block
            if !processed_blocks.contains(&block_number) {
                self.process_block(*block_number, block.timestamp).await?;
                processed_blocks.insert(block_number);
            }
        }

        // Process parallelized events
        debug!(target: LOG_TARGET, "Processing parallelized events.");
        let instant = Instant::now();
        self.task_manager
            .process_tasks()
            .await
            .map_err(ProcessError::Processors)?;

        debug!(target: LOG_TARGET, duration = ?instant.elapsed(), "Processed parallelized events.");

        // Apply ERC balances cache diff
        debug!(target: LOG_TARGET, "Applying ERC balances cache diff.");
        let instant = Instant::now();
        self.db.apply_cache_diff(range.cursors.clone()).await?;
        debug!(target: LOG_TARGET, duration = ?instant.elapsed(), "Applied ERC balances cache diff.");

        // Update cursors
        // The update cursors query should absolutely succeed, otherwise we will rollback.
        debug!(target: LOG_TARGET, cursors = ?range.cursors, "Updating cursors.");
        self.db
            .update_cursors(range.cursors.clone(), range.cursor_transactions.clone())
            .await?;

        Ok(())
    }

    pub async fn process_pending(&mut self, data: &FetchPendingResult) -> Result<(), ProcessError> {
        for (tx_hash, tx) in &data.transactions {
            if tx.events.is_empty() {
                continue;
            }

            if let Err(e) = self
                .process_transaction_with_events(
                    *tx_hash,
                    tx.events.as_slice(),
                    data.block_number,
                    data.timestamp,
                    &tx.transaction,
                )
                .await
            {
                error!(target: LOG_TARGET, error = %e, transaction_hash = %format!("{:#x}", tx_hash), "Processing pending transaction.");
                return Err(e);
            }

            debug!(target: LOG_TARGET, transaction_hash = %format!("{:#x}", tx_hash), "Processed pending transaction.");
        }

        // Process parallelized events
        self.task_manager.process_tasks().await?;

        // The update cursors query should absolutely succeed, otherwise we will rollback.
        self.db
            .update_cursors(data.cursors.clone(), data.cursor_transactions.clone())
            .await?;

        Ok(())
    }

    async fn process_transaction_with_events(
        &mut self,
        transaction_hash: Felt,
        events: &[Event],
        block_number: u64,
        block_timestamp: u64,
        transaction: &Option<TransactionContent>,
    ) -> Result<(), ProcessError> {
        let mut unique_contracts = HashSet::new();
        let mut unique_models = HashSet::new();
        // Contract -> Cursor
        for (event_idx, event) in events.iter().enumerate() {
            // NOTE: erc* processors expect the event_id to be in this format to get
            // transaction_hash:
            let event_id = format_event_id(
                block_number,
                &transaction_hash,
                &event.from_address,
                event_idx as u64,
            );

            let Some(&contract_type) = self.contracts.get(&event.from_address) else {
                continue;
            };

            unique_contracts.insert(event.from_address);
            let event_key = event.keys[0];
            if contract_type == ContractType::WORLD && DOJO_RELATED_EVENTS.contains(&event_key) {
                unique_models.insert(event.keys[1]);
            }

            self.process_event(
                block_number,
                block_timestamp,
                &event_id,
                event,
                transaction_hash,
                contract_type,
            )
            .await?;
        }

        if let Some(transaction) = transaction {
            Self::process_transaction(
                self,
                block_number,
                block_timestamp,
                transaction_hash,
                &unique_contracts,
                transaction,
                &unique_models,
            )
            .await?;
        }

        Ok(())
    }

    async fn process_block(
        &mut self,
        block_number: u64,
        block_timestamp: u64,
    ) -> Result<(), ProcessError> {
        for processor in &self.processors.block {
            processor
                .process(
                    &mut self.db,
                    self.provider.as_ref(),
                    block_number,
                    block_timestamp,
                )
                .await?
        }

        trace!(target: LOG_TARGET, block_number = %block_number, "Processed block.");
        Ok(())
    }

    async fn process_transaction(
        &mut self,
        block_number: u64,
        block_timestamp: u64,
        transaction_hash: Felt,
        contract_addresses: &HashSet<Felt>,
        transaction: &TransactionContent,
        unique_models: &HashSet<Felt>,
    ) -> Result<(), ProcessError> {
        for processor in &self.processors.transaction {
            processor
                .process(
                    &mut self.db,
                    self.provider.as_ref(),
                    block_number,
                    block_timestamp,
                    transaction_hash,
                    contract_addresses,
                    transaction,
                    self.contract_class_cache.as_ref(),
                    unique_models,
                )
                .await?
        }

        Ok(())
    }

    async fn process_event(
        &mut self,
        block_number: u64,
        block_timestamp: u64,
        event_id: &str,
        event: &Event,
        transaction_hash: Felt,
        contract_type: ContractType,
    ) -> Result<(), ProcessError> {
        if self.config.flags.contains(IndexingFlags::RAW_EVENTS) {
            self.db
                .store_event(event_id, event, transaction_hash, block_timestamp)
                .map_err(ProcessError::Sqlite)?;
        }

        let event_key = event.keys[0];

        let processors = self.processors.get_event_processors(contract_type);
        let Some(processors) = processors.get(&event_key) else {
            // if we dont have a processor for this event, we try the catch all processor
            if self.processors.catch_all_event.validate(event) {
                if let Err(e) = self
                    .processors
                    .catch_all_event
                    .process(
                        self.world.clone(),
                        &mut self.db,
                        block_number,
                        block_timestamp,
                        event_id,
                        event,
                        &self.config.event_processor_config,
                    )
                    .await
                {
                    error!(target: LOG_TARGET, error = ?e, "Processing catch all event processor.");
                    return Err(e.into());
                }
            } else {
                let unprocessed_event = UnprocessedEvent {
                    keys: event.keys.iter().map(|k| format!("{:#x}", k)).collect(),
                    data: event.data.iter().map(|d| format!("{:#x}", d)).collect(),
                };

                trace!(
                    target: LOG_TARGET,
                    keys = ?unprocessed_event.keys,
                    data = ?unprocessed_event.data,
                    "Unprocessed event.",
                );
            }

            return Ok(());
        };

        let processor = processors
            .iter()
            .find(|p| p.validate(event))
            .expect("Must find atleast one processor for the event");

        let task_identifier = processor.task_identifier(event);
        let dependencies = processor.task_dependencies(event);
        let indexing_mode = processor.indexing_mode(event, &self.config.event_processor_config);

        self.task_manager.add_parallelized_event_with_dependencies(
            task_identifier,
            dependencies,
            ParallelizedEvent {
                indexing_mode,
                contract_type,
                event_id: event_id.to_string(),
                event: event.clone(),
                block_number,
                block_timestamp,
            },
        );

        Ok(())
    }
}
