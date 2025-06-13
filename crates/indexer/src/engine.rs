use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use bitflags::bitflags;
use dojo_utils::provider as provider_utils;
use dojo_world::contracts::world::WorldContractReader;
use futures_util::future::try_join_all;
use hashlink::lru_cache::Entry;
use hashlink::LinkedHashMap;
use starknet::core::types::requests::{
    GetBlockWithTxHashesRequest, GetEventsRequest, GetTransactionByHashRequest,
};
use starknet::core::types::{
    BlockHashAndNumber, BlockId, BlockTag, EmittedEvent, Event, EventFilter, EventFilterWithPage,
    MaybePendingBlockWithTxHashes, ResultPageRequest, Transaction,
};
use starknet::macros::selector;
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_crypto::Felt;
use tokio::sync::broadcast::Sender;
use tokio::time::{sleep, Instant};
use torii_processors::{EventProcessorConfig, Processors};
use torii_sqlite::cache::ContractClassCache;
use torii_sqlite::controllers::ControllersSync;
use torii_sqlite::types::{Contract, ContractType};
use torii_sqlite::utils::{format_event_id, parse_event_id};
use torii_sqlite::{Cursor, Sql};
use tracing::{debug, error, info, trace, warn};

use crate::constants::LOG_TARGET;
use crate::error::{Error, FetchError, ProcessError};
use torii_processors::task_manager::{ParallelizedEvent, TaskManager};

bitflags! {
    #[derive(Debug, Clone)]
    pub struct IndexingFlags: u32 {
        const TRANSACTIONS = 0b00000001;
        const RAW_EVENTS = 0b00000010;
        const PENDING_BLOCKS = 0b00000100;
    }
}

#[derive(Debug)]
pub struct EngineConfig {
    pub polling_interval: Duration,
    pub batch_chunk_size: usize,
    pub blocks_chunk_size: u64,
    pub events_chunk_size: u64,
    pub max_concurrent_tasks: usize,
    pub flags: IndexingFlags,
    pub event_processor_config: EventProcessorConfig,
    pub world_block: u64,
}

#[derive(Debug, Clone)]
pub struct FetchRangeBlock {
    // For pending blocks, this is None.
    // We check the parent hash of the pending block to the latest block
    // to see if we need to re fetch the pending block.
    pub block_hash: Option<Felt>,
    pub timestamp: u64,
    pub transactions: LinkedHashMap<Felt, FetchRangeTransaction>,
}

#[derive(Debug, Clone)]
pub struct FetchRangeTransaction {
    // this is Some if the transactions indexing flag
    // is enabled
    pub transaction: Option<Transaction>,
    pub events: Vec<EmittedEvent>,
}

#[derive(Debug, Clone)]
pub struct FetchRangeResult {
    // block_number -> block and transactions
    pub blocks: BTreeMap<u64, FetchRangeBlock>,
    // contract_address -> transaction count
    pub num_transactions: HashMap<Felt, u64>,
    // new updated cursors
    pub cursors: HashMap<Felt, Cursor>,
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
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            polling_interval: Duration::from_millis(500),
            batch_chunk_size: 1024,
            blocks_chunk_size: 10240,
            events_chunk_size: 1024,
            max_concurrent_tasks: 100,
            flags: IndexingFlags::empty(),
            event_processor_config: EventProcessorConfig::default(),
            world_block: 0,
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
        Self::new_with_controllers(world, db, provider, processors, config, shutdown_tx, contracts, None)
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
            contract_class_cache: Arc::new(ContractClassCache::new(provider)),
            controllers,
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        if let Err(e) = provider_utils::health_check_provider(self.provider.clone()).await {
            error!(target: LOG_TARGET,"Provider health check failed during engine start");
            return Err(Error::AnyhowError(e));
        }

        let mut fetching_backoff_delay = Duration::from_secs(1);
        let mut processing_backoff_delay = Duration::from_secs(1);
        let max_backoff_delay = Duration::from_secs(60);

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let mut fetching_erroring_out = false;
        let mut processing_erroring_out = false;
        // The last fetch result & cursors, in case the processing fails, but not fetching.
        // Thus we can retry the processing with the same data instead of fetching again.
        let mut cached_data: Option<Arc<FetchRangeResult>> = None;

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    break Ok(());
                }
                res = async {
                    if let Some(last_fetch_result) = cached_data.as_ref() {
                        Result::<_, Error>::Ok(last_fetch_result.clone())
                    } else {
                        let mut cursors = self.db.cursors().await?;
                        let fetch_result = self.fetch(&mut cursors).await?;
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
                                        controllers.sync().await.map_err(Error::ControllerSync)?;
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

    pub async fn fetch(
        &mut self,
        cursors: &mut HashMap<Felt, Cursor>,
    ) -> Result<FetchRangeResult, FetchError> {
        let latest_block = self.provider.block_hash_and_number().await?;

        let instant = Instant::now();
        // Fetch all events from 'from' to our blocks chunk size
        let range = self.fetch_range(cursors, latest_block).await?;
        debug!(target: LOG_TARGET, duration = ?instant.elapsed(), cursors = ?cursors, "Fetched data for range.");

        Ok(range)
    }

    pub async fn fetch_range(
        &self,
        cursors: &HashMap<Felt, Cursor>,
        latest_block: BlockHashAndNumber,
    ) -> Result<FetchRangeResult, FetchError> {
        let mut events = vec![];
        let mut cursors = cursors.clone();
        let mut blocks = BTreeMap::new();
        let mut block_numbers = BTreeSet::new();
        let mut num_transactions = HashMap::new();

        // Step 1: Create initial batch requests for events from all contracts
        let mut event_requests = Vec::new();
        for (contract_address, cursor) in cursors.iter() {
            let from = cursor
                .head
                .map_or(self.config.world_block, |h| if h == 0 { h } else { h + 1 });
            let to = from + self.config.blocks_chunk_size;

            let events_filter = EventFilter {
                from_block: Some(BlockId::Number(from)),
                to_block: Some(BlockId::Tag(
                    if self.config.flags.contains(IndexingFlags::PENDING_BLOCKS)
                        && from > latest_block.block_number
                    {
                        BlockTag::Pending
                    } else {
                        BlockTag::Latest
                    },
                )),
                address: Some(*contract_address),
                keys: None,
            };

            event_requests.push((
                *contract_address,
                to,
                ProviderRequestData::GetEvents(GetEventsRequest {
                    filter: EventFilterWithPage {
                        event_filter: events_filter,
                        result_page_request: ResultPageRequest {
                            continuation_token: None,
                            chunk_size: self.config.events_chunk_size,
                        },
                    },
                }),
            ));
        }

        // Step 2: Fetch all events recursively
        events.extend(
            self.fetch_events(event_requests, &mut cursors, latest_block.block_number)
                .await?,
        );

        // Step 3: Collect unique block numbers from events and cursors
        for event in &events {
            let block_number = match event.block_number {
                Some(block_number) => block_number,
                None => latest_block.block_number + 1, // Pending block
            };
            block_numbers.insert(block_number);
        }
        for (_, cursor) in cursors.iter() {
            if let Some(head) = cursor.head {
                block_numbers.insert(head);
            }
        }

        // Step 4: Fetch block data (timestamps and transaction hashes)
        let mut block_requests = Vec::new();
        for block_number in &block_numbers {
            block_requests.push(ProviderRequestData::GetBlockWithTxHashes(
                GetBlockWithTxHashesRequest {
                    block_id: if *block_number > latest_block.block_number {
                        BlockId::Tag(BlockTag::Pending)
                    } else {
                        BlockId::Number(*block_number)
                    },
                },
            ));
        }

        // Step 5: Execute block requests in batch and initialize blocks with transaction order
        if !block_requests.is_empty() {
            let block_results = self.chunked_batch_requests(&block_requests).await?;
            for (block_number, result) in block_numbers.iter().zip(block_results) {
                match result {
                    ProviderResponseData::GetBlockWithTxHashes(block) => {
                        let (timestamp, tx_hashes, block_hash) = match block {
                            MaybePendingBlockWithTxHashes::Block(block) => {
                                (block.timestamp, block.transactions, Some(block.block_hash))
                            }
                            MaybePendingBlockWithTxHashes::PendingBlock(block) => {
                                if block.parent_hash != latest_block.block_hash {
                                    // if the parent hash is not the same as the previous block,
                                    // we need to re fetch the pending block with a specific block number
                                    let block = self
                                        .provider
                                        .get_block_with_tx_hashes(BlockId::Number(*block_number))
                                        .await?;
                                    match block {
                                        // we assume that our pending block has now been validated.
                                        MaybePendingBlockWithTxHashes::Block(block) => (
                                            block.timestamp,
                                            block.transactions,
                                            Some(block.block_hash),
                                        ),
                                        _ => unreachable!(),
                                    }
                                } else {
                                    (block.timestamp, block.transactions, None)
                                }
                            }
                        };
                        // Initialize block with transactions in the order provided by the block
                        let mut transactions = LinkedHashMap::new();
                        for tx_hash in tx_hashes {
                            transactions.insert(
                                tx_hash,
                                FetchRangeTransaction {
                                    transaction: None,
                                    events: vec![],
                                },
                            );
                        }
                        blocks.insert(
                            *block_number,
                            FetchRangeBlock {
                                block_hash,
                                timestamp,
                                transactions,
                            },
                        );
                    }
                    _ => unreachable!(),
                }
            }
        }

        // Step 6: Assign events to their respective blocks and transactions
        for event in events {
            let block_number = match event.block_number {
                Some(block_number) => block_number,
                None => latest_block.block_number + 1, // Pending block
            };

            let block = blocks.get_mut(&block_number).expect("Block not found");
            match block.transactions.entry(event.transaction_hash) {
                Entry::Occupied(mut tx) => {
                    // Increment transaction count for the contract
                    let entry = num_transactions.entry(event.from_address).or_insert(0);
                    *entry += 1;
                    // Add event to the transaction
                    tx.get_mut().events.push(event);
                }
                Entry::Vacant(tx) => {
                    let address = event.from_address;
                    tx.insert(FetchRangeTransaction {
                        transaction: None,
                        events: vec![event],
                    });

                    // Increment transaction count for the contract
                    let entry = num_transactions.entry(address).or_insert(0);
                    *entry += 1;
                }
            }
        }

        // Step 7: Fetch transaction details if enabled
        if self.config.flags.contains(IndexingFlags::TRANSACTIONS) && !blocks.is_empty() {
            let mut transaction_requests = Vec::new();
            let mut block_numbers_for_tx = Vec::new();
            for (block_number, block) in &blocks {
                for (transaction_hash, tx) in &block.transactions {
                    if tx.events.is_empty() {
                        continue;
                    }

                    transaction_requests.push(ProviderRequestData::GetTransactionByHash(
                        GetTransactionByHashRequest {
                            transaction_hash: *transaction_hash,
                        },
                    ));
                    block_numbers_for_tx.push(*block_number);
                }
            }

            let transaction_results = self.chunked_batch_requests(&transaction_requests).await?;
            for (block_number, result) in block_numbers_for_tx.into_iter().zip(transaction_results)
            {
                match result {
                    ProviderResponseData::GetTransactionByHash(transaction) => {
                        if let Some(block) = blocks.get_mut(&block_number) {
                            if let Some(tx) =
                                block.transactions.get_mut(transaction.transaction_hash())
                            {
                                tx.transaction = Some(transaction);
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        // Step 8: Update cursor timestamps
        for (_, cursor) in cursors.iter_mut() {
            if let Some(head) = cursor.head {
                if let Some(block) = blocks.get(&head) {
                    cursor.last_block_timestamp = Some(block.timestamp);
                }
            }
        }

        trace!(target: LOG_TARGET, "Blocks: {}", blocks.len());

        Ok(FetchRangeResult {
            blocks,
            num_transactions,
            cursors,
        })
    }

    async fn fetch_events(
        &self,
        initial_requests: Vec<(Felt, u64, ProviderRequestData)>,
        cursors: &mut HashMap<Felt, Cursor>,
        latest_block_number: u64,
    ) -> Result<Vec<EmittedEvent>, FetchError> {
        let mut all_events = Vec::new();
        let mut current_requests = initial_requests;
        let mut old_cursors = cursors.clone();

        while !current_requests.is_empty() {
            let mut next_requests = Vec::new();
            let mut events = Vec::new();

            // Extract just the requests without the contract addresses
            let batch_requests: Vec<ProviderRequestData> = current_requests
                .iter()
                .map(|(_, _, req)| req.clone())
                .collect();

            debug!(target: LOG_TARGET, "Retrieving events for contracts.");
            let instant = Instant::now();
            let batch_results = self.chunked_batch_requests(&batch_requests).await?;
            debug!(target: LOG_TARGET, duration = ?instant.elapsed(), "Retrieved events for contracts.");

            // Process results and prepare next batch of requests if needed
            for ((contract_address, to, original_request), result) in
                current_requests.into_iter().zip(batch_results)
            {
                let contract_type = self.contracts.get(&contract_address).unwrap();
                debug!(target: LOG_TARGET, address = format!("{:#x}", contract_address), r#type = ?contract_type, "Pre-processing events for contract.");

                let old_cursor = old_cursors.get_mut(&contract_address).unwrap();
                let new_cursor = cursors.get_mut(&contract_address).unwrap();
                let mut previous_contract_tx = None;
                let mut event_idx = 0;
                let mut last_pending_block_event_id_tmp =
                    old_cursor.last_pending_block_event_id.clone();
                let mut last_validated_block_number = None;
                let mut last_block_number = None;
                let mut is_pending = false;

                match result {
                    ProviderResponseData::GetEvents(events_page) => {
                        // Process events for this page, only including events up to our target
                        // block
                        for event in events_page.events.clone() {
                            let block_number = match event.block_number {
                                Some(block_number) => {
                                    last_validated_block_number = Some(block_number);
                                    block_number
                                }
                                // If we don't have a block number, this must be a pending block event
                                None => latest_block_number + 1,
                            };

                            last_block_number = Some(block_number);
                            is_pending = event.block_number.is_none();

                            if previous_contract_tx != Some(event.transaction_hash) {
                                event_idx = 0;
                                previous_contract_tx = Some(event.transaction_hash);
                            }
                            let event_id =
                                format_event_id(block_number, &event.transaction_hash, event_idx);
                            event_idx += 1;

                            // Then we skip all transactions until we reach the last pending
                            // processed transaction (if any)
                            if let Some(last_pending_block_event_id) =
                                last_pending_block_event_id_tmp.clone()
                            {
                                let (cursor_block_number, _, _) =
                                    parse_event_id(&last_pending_block_event_id);
                                if event_id != last_pending_block_event_id
                                    && cursor_block_number == block_number
                                {
                                    continue;
                                }
                                last_pending_block_event_id_tmp = None;
                            }

                            // Skip the latest pending block transaction events
                            // * as we might have multiple events for the same transaction
                            if let Some(last_contract_tx) =
                                old_cursor.last_pending_block_event_id.take()
                            {
                                let (cursor_block_number, _, _) = parse_event_id(&last_contract_tx);
                                if event_id == last_contract_tx
                                    && cursor_block_number == block_number
                                {
                                    continue;
                                }
                                new_cursor.last_pending_block_event_id = None;
                            }

                            if is_pending {
                                new_cursor.last_pending_block_event_id = Some(event_id.clone());
                            }

                            events.push(event);
                        }

                        // Add continuation request to next_requests instead of recursing
                        // We only fetch next events if;
                        // - we have a continuation token
                        // - we have a last block number (which means we have processed atleast one event)
                        // - the last block number is less than the to block
                        if events_page.continuation_token.is_some()
                            && (last_block_number.is_none() || last_block_number.unwrap() < to)
                        {
                            debug!(target: LOG_TARGET, address = format!("{:#x}", contract_address), r#type = ?contract_type, "Adding continuation request for contract.");
                            if let ProviderRequestData::GetEvents(mut next_request) =
                                original_request
                            {
                                next_request.filter.result_page_request.continuation_token =
                                    events_page.continuation_token;
                                next_requests.push((
                                    contract_address,
                                    to,
                                    ProviderRequestData::GetEvents(next_request),
                                ));
                            }
                        } else {
                            let new_head = to.max(last_block_number.unwrap_or(0));
                            let new_head = new_head
                                .min(last_validated_block_number.unwrap_or(latest_block_number));
                            // We only reset the last pending block contract tx if we are not
                            // processing pending events anymore. It can happen that during a short lapse,
                            // we can have some pending events while the latest block number has been incremented.
                            // So we want to make sure we don't reset the last pending block contract tx
                            if new_cursor.head != Some(new_head) && !is_pending {
                                new_cursor.last_pending_block_event_id = None;
                            }
                            new_cursor.head = Some(new_head);

                            debug!(target: LOG_TARGET, address = format!("{:#x}", contract_address), r#type = ?contract_type, head = new_cursor.head, last_pending_block_event_id = new_cursor.last_pending_block_event_id, "Fetched and pre-processed events for contract.");
                        }
                    }
                    _ => unreachable!(),
                }
            }

            all_events.extend(events);
            current_requests = next_requests;
        }

        Ok(all_events)
    }

    pub async fn process(&mut self, range: &FetchRangeResult) -> Result<(), ProcessError> {
        let mut processed_blocks = HashSet::new();

        // Process all transactions in the chunk
        for (block_number, block) in &range.blocks {
            for (transaction_hash, tx) in &block.transactions {
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
        debug!(target: LOG_TARGET, cursors = ?range.cursors, "Updating cursors.");
        self.db
            .update_cursors(range.cursors.clone(), range.num_transactions.clone())?;

        Ok(())
    }

    async fn process_transaction_with_events(
        &mut self,
        transaction_hash: Felt,
        events: &[EmittedEvent],
        block_number: u64,
        block_timestamp: u64,
        transaction: &Option<Transaction>,
    ) -> Result<(), ProcessError> {
        let mut unique_contracts = HashSet::new();
        let mut unique_models = HashSet::new();
        // Contract -> Cursor
        for (event_idx, event) in events.iter().enumerate() {
            // NOTE: erc* processors expect the event_id to be in this format to get
            // transaction_hash:
            let event_id = format_event_id(block_number, &transaction_hash, event_idx as u64);

            let event = Event {
                from_address: event.from_address,
                keys: event.keys.clone(),
                data: event.data.clone(),
            };

            let Some(&contract_type) = self.contracts.get(&event.from_address) else {
                continue;
            };

            unique_contracts.insert(event.from_address);
            let event_key = event.keys[0];
            if contract_type == ContractType::WORLD
                && (event_key == selector!("StoreSetRecord")
                    || event_key == selector!("StoreUpdateRecord")
                    || event_key == selector!("StoreDelRecord")
                    || event_key == selector!("StoreUpdateMember")
                    || event_key == selector!("EventEmitted"))
            {
                unique_models.insert(event.keys[1]);
            }

            self.process_event(
                block_number,
                block_timestamp,
                &event_id,
                &event,
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
        transaction: &Transaction,
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

    async fn chunked_batch_requests(
        &self,
        requests: &[ProviderRequestData],
    ) -> Result<Vec<ProviderResponseData>, FetchError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        const MAX_RETRIES: u32 = 3;
        const INITIAL_BACKOFF: Duration = Duration::from_secs(2);

        let mut futures = Vec::new();
        for chunk in requests.chunks(self.config.batch_chunk_size) {
            futures.push(async move {
                let mut attempt = 0;
                loop {
                    match self.provider.batch_requests(chunk).await {
                        Ok(results) => return Ok::<Vec<ProviderResponseData>, FetchError>(results),
                        Err(e) => {
                            if attempt < MAX_RETRIES {
                                let backoff = INITIAL_BACKOFF * 2u32.pow(attempt);
                                warn!(
                                    target: LOG_TARGET,
                                    attempt = attempt + 1,
                                    backoff_secs = backoff.as_secs(),
                                    error = ?e,
                                    chunk_size = chunk.len(),
                                    batch_chunk_size = self.config.batch_chunk_size,
                                    "Retrying failed batch request for chunk."
                                );
                                sleep(backoff).await;
                                attempt += 1;
                            } else {
                                error!(
                                    target: LOG_TARGET,
                                    error = ?e,
                                    chunk_size = chunk.len(),
                                    batch_chunk_size = self.config.batch_chunk_size,
                                    "Chunk batch request failed after all retries. This could be due to the provider being overloaded. You can try reducing the batch chunk size."
                                );
                                return Err(FetchError::BatchRequest(Box::new(e.into())));
                            }
                        }
                    }
                }
            });
        }

        let results_of_chunks = try_join_all(futures).await?;
        let flattened_results = results_of_chunks.into_iter().flatten().collect();
        Ok(flattened_results)
    }
}

// event_id format: block_number:transaction_hash:event_idx
pub fn get_transaction_hash_from_event_id(event_id: &str) -> String {
    event_id.split(':').nth(1).unwrap().to_string()
}
