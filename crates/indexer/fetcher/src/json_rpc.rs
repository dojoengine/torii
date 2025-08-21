use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::time::Duration;

use futures_util::future::try_join_all;
use indexmap::IndexMap;
use metrics::{counter, histogram};
use starknet::core::types::requests::{
    GetBlockWithTxHashesRequest, GetEventsRequest, GetTransactionByHashRequest,
};
use starknet::core::types::{
    BlockId, BlockTag, EmittedEvent, Event, EventFilter, EventFilterWithPage,
    MaybePreConfirmedBlockWithReceipts, MaybePreConfirmedBlockWithTxHashes, ResultPageRequest,
    TransactionExecutionStatus,
};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_crypto::Felt;
use tokio::time::{sleep, Instant};
use torii_storage::proto::ContractCursor;
use tracing::{debug, error, trace, warn};

use crate::error::Error;
use crate::{
    Cursors, FetchPreconfirmedBlockResult, FetchRangeBlock, FetchRangeResult, FetchResult,
    FetchTransaction, FetcherConfig, FetchingFlags,
};

pub(crate) const LOG_TARGET: &str = "torii::indexer::fetcher";

#[derive(Debug)]
pub struct Fetcher<P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static> {
    pub provider: P,
    pub config: FetcherConfig,
}

impl<P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static> Fetcher<P> {
    pub fn new(provider: P, config: FetcherConfig) -> Self {
        Self { config, provider }
    }

    pub async fn fetch(
        &self,
        cursors: &HashMap<Felt, ContractCursor>,
    ) -> Result<FetchResult, Error> {
        let fetch_start = Instant::now();

        let latest_block_number = self.provider.block_hash_and_number().await?.block_number;

        let range_start = Instant::now();
        // Fetch all events from 'from' to our blocks chunk size
        let (range, cursors) = self.fetch_range(cursors, latest_block_number).await?;
        histogram!("torii_fetcher_range_duration_seconds")
            .record(range_start.elapsed().as_secs_f64());
        debug!(target: LOG_TARGET, duration = ?range_start.elapsed(), cursors = ?cursors, "Fetched data for range.");

        let (preconfirmed_block, cursors) =
            if self.config.flags.contains(FetchingFlags::PENDING_BLOCKS)
                && cursors
                    .cursors
                    .values()
                    .any(|c| c.head == Some(latest_block_number))
            {
                let pending_start = Instant::now();
                let pending_result = self
                    .fetch_preconfirmed_block(latest_block_number, &cursors)
                    .await?;
                histogram!("torii_fetcher_pending_duration_seconds")
                    .record(pending_start.elapsed().as_secs_f64());

                pending_result
            } else {
                (None, cursors)
            };

        histogram!("torii_fetcher_total_duration_seconds")
            .record(fetch_start.elapsed().as_secs_f64());
        counter!("torii_fetcher_fetch_total", "status" => "success").increment(1);

        Ok(FetchResult {
            range,
            preconfirmed_block,
            cursors,
        })
    }

    pub async fn fetch_range(
        &self,
        cursors: &HashMap<Felt, ContractCursor>,
        latest_block_number: u64,
    ) -> Result<(FetchRangeResult, Cursors), Error> {
        let mut events = vec![];
        let mut cursors = cursors.clone();
        let mut blocks = BTreeMap::new();
        let mut block_numbers = BTreeSet::new();
        let mut cursor_transactions = HashMap::new();

        // Step 1: Create initial batch requests for events from all contracts
        let mut event_requests = Vec::new();
        for (contract_address, cursor) in cursors.iter() {
            if cursor.head == Some(latest_block_number) {
                continue;
            }

            let from = cursor
                .head
                .map_or(self.config.world_block, |h| if h == 0 { h } else { h + 1 });
            let to = (from + self.config.blocks_chunk_size).min(latest_block_number);

            let events_filter = EventFilter {
                from_block: Some(BlockId::Number(from)),
                to_block: Some(BlockId::Tag(BlockTag::Latest)),
                address: Some(*contract_address),
                keys: None,
            };

            event_requests.push((
                *contract_address,
                from,
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
        let events_start = Instant::now();
        let fetched_events = self
            .fetch_events(event_requests, &mut cursors, latest_block_number)
            .await?;
        histogram!("torii_fetcher_events_duration_seconds")
            .record(events_start.elapsed().as_secs_f64());
        counter!("torii_fetcher_events_fetched_total").increment(fetched_events.len() as u64);
        events.extend(fetched_events);

        // Step 3: Collect unique block numbers from events and cursors
        for event in &events {
            block_numbers.insert(event.block_number.unwrap());
        }
        for (_, cursor) in cursors.iter() {
            if let Some(head) = cursor.head {
                block_numbers.insert(head);
            }
        }

        // Step 4: Fetch block data (timestamps and transaction hashes)
        let mut block_requests = Vec::new();
        counter!("torii_fetcher_blocks_to_fetch_total").increment(block_numbers.len() as u64);
        for block_number in &block_numbers {
            block_requests.push(ProviderRequestData::GetBlockWithTxHashes(
                GetBlockWithTxHashesRequest {
                    block_id: BlockId::Number(*block_number),
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
                            MaybePreConfirmedBlockWithTxHashes::Block(block) => {
                                (block.timestamp, block.transactions, Some(block.block_hash))
                            }
                            _ => unreachable!(),
                        };
                        // Initialize block with transactions in the order provided by the block
                        let transactions = IndexMap::from_iter(tx_hashes.iter().map(|tx_hash| {
                            (
                                *tx_hash,
                                FetchTransaction {
                                    transaction: None,
                                    events: vec![],
                                },
                            )
                        }));

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
            let block_number = event.block_number.unwrap();

            let block = blocks.get_mut(&block_number).expect("Block not found");

            // Push the event to the transaction
            block
                .transactions
                .get_mut(&event.transaction_hash)
                .expect("Transaction should exist.")
                .events
                .push(Event {
                    from_address: event.from_address,
                    keys: event.keys.clone(),
                    data: event.data.clone(),
                });

            // Add transaction to cursor transactions
            cursor_transactions
                .entry(event.from_address)
                .or_insert(HashSet::new())
                .insert(event.transaction_hash);
        }

        // Step 7: Filter out transactions that don't have any events (not relevant to indexed contracts)
        for (_, block) in blocks.iter_mut() {
            block.transactions.retain(|_, tx| !tx.events.is_empty());
        }

        // Step 7: Fetch transaction details if enabled
        if self.config.flags.contains(FetchingFlags::TRANSACTIONS) && !blocks.is_empty() {
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
                                tx.transaction = Some(transaction.into());
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

        Ok((
            FetchRangeResult { blocks },
            Cursors {
                cursor_transactions,
                cursors,
            },
        ))
    }

    async fn fetch_preconfirmed_block(
        &self,
        latest_block_number: u64,
        cursors: &Cursors,
    ) -> Result<(Option<FetchPreconfirmedBlockResult>, Cursors), Error> {
        debug!(
            target: LOG_TARGET,
            latest_block = latest_block_number,
            "Fetching preconfirmed block"
        );

        let preconf_block = if let MaybePreConfirmedBlockWithReceipts::PreConfirmedBlock(preconf) =
            self.provider
                .get_block_with_receipts(BlockId::Tag(BlockTag::PreConfirmed))
                .await?
        {
            debug!(
                target: LOG_TARGET,
                preconf_block_number = preconf.block_number,
                latest_block = latest_block_number,
                expected_preconf = latest_block_number.saturating_add(1),
                "Retrieved preconfirmed block"
            );

            // if the preconfirmed block number is not incremented by one of the latest block number that we fetched, then it means
            // a new block got mined just after we fetched the latest block information
            if latest_block_number.saturating_add(1) != preconf.block_number {
                debug!(
                    target: LOG_TARGET,
                    preconf_block_number = preconf.block_number,
                    expected_block_number = latest_block_number.saturating_add(1),
                    "Skipping preconfirmed block - block number mismatch (new block mined)"
                );
                return Ok((None, cursors.clone()));
            }

            preconf
        } else {
            debug!(
                target: LOG_TARGET,
                "No preconfirmed block available"
            );
            // TODO: change this to unreachable once katana is updated to return PendingBlockWithTxs
            // when BlockTag is Pending unreachable!("We requested pending block, so it
            // must be pending");
            return Ok((None, cursors.clone()));
        };

        // Skip transactions that have been processed already
        // Our cursor is the last processed transaction

        let mut new_cursors = cursors.clone();

        let block_number = preconf_block.block_number;
        let timestamp = preconf_block.timestamp;

        debug!(
            target: LOG_TARGET,
            block_number = block_number,
            timestamp = timestamp,
            total_transactions = preconf_block.transactions.len(),
            "Processing preconfirmed block transactions"
        );

        let mut transactions: IndexMap<Felt, FetchTransaction> = IndexMap::new();
        for (contract_address, cursor) in &mut new_cursors.cursors {
            if cursor.head != Some(latest_block_number) {
                debug!(
                    target: LOG_TARGET,
                    contract = format!("{:#x}", contract_address),
                    cursor_head = cursor.head,
                    latest_block = latest_block_number,
                    "Skipping contract - not up to date with latest block"
                );
                continue;
            }

            debug!(
                target: LOG_TARGET,
                contract = format!("{:#x}", contract_address),
                last_pending_tx = cursor.last_pending_block_tx.map(|tx| format!("{:#x}", tx)),
                "Processing preconfirmed block for contract"
            );

            let mut last_pending_block_tx_tmp = cursor.last_pending_block_tx;
            let mut contract_events_count = 0;
            let mut contract_transactions_processed = 0;

            for t in &preconf_block.transactions {
                let tx_hash = t.receipt.transaction_hash();

                // Skip all transactions until we reach the last processed transaction
                if let Some(tx) = last_pending_block_tx_tmp {
                    if tx_hash != &tx {
                        continue;
                    }
                    last_pending_block_tx_tmp = None;
                }

                // Skip the last processed transaction itself (since it was already processed)
                if let Some(last_tx) = cursor.last_pending_block_tx {
                    if tx_hash == &last_tx {
                        continue;
                    }
                }

                if t.receipt.execution_result().status() == TransactionExecutionStatus::Reverted {
                    trace!(
                        target: LOG_TARGET,
                        contract = format!("{:#x}", contract_address),
                        tx_hash = format!("{:#x}", tx_hash),
                        "Skipping reverted transaction"
                    );
                    continue;
                }

                let events = t
                    .receipt
                    .events()
                    .iter()
                    .filter(|e| e.from_address == *contract_address)
                    .cloned()
                    .collect::<Vec<_>>();

                if events.is_empty() {
                    continue;
                }

                contract_events_count += events.len();
                contract_transactions_processed += 1;

                trace!(
                    target: LOG_TARGET,
                    contract = format!("{:#x}", contract_address),
                    tx_hash = format!("{:#x}", tx_hash),
                    events_count = events.len(),
                    "Processing transaction with events"
                );

                new_cursors
                    .cursor_transactions
                    .entry(*contract_address)
                    .or_default()
                    .insert(*tx_hash);

                transactions.insert(
                    *tx_hash,
                    FetchTransaction {
                        transaction: Some(t.transaction.clone()),
                        events,
                    },
                );
                cursor.last_pending_block_tx = Some(*tx_hash);
                cursor.last_block_timestamp = Some(timestamp);
            }

            debug!(
                target: LOG_TARGET,
                contract = format!("{:#x}", contract_address),
                events_count = contract_events_count,
                transactions_processed = contract_transactions_processed,
                "Completed processing preconfirmed block for contract"
            );
        }

        Ok((
            Some(FetchPreconfirmedBlockResult {
                timestamp,
                transactions,
                block_number,
            }),
            new_cursors,
        ))
    }

    async fn fetch_events(
        &self,
        initial_requests: Vec<(Felt, u64, u64, ProviderRequestData)>,
        cursors: &mut HashMap<Felt, ContractCursor>,
        latest_block_number: u64,
    ) -> Result<Vec<EmittedEvent>, Error> {
        let mut all_events = Vec::new();
        let mut current_requests = initial_requests;
        let mut old_cursors = cursors.clone();

        while !current_requests.is_empty() {
            let mut next_requests = Vec::new();
            let mut events = Vec::new();

            // Log details about each request in the batch
            for (contract_address, from, to, _) in &current_requests {
                debug!(
                    target: LOG_TARGET,
                    contract = format!("{:#x}", contract_address),
                    from_block = from,
                    to_block = to,
                    "Preparing to fetch events for contract"
                );
            }

            // Extract just the requests without the contract addresses
            let batch_requests: Vec<ProviderRequestData> = current_requests
                .iter()
                .map(|(_, _, _, req)| req.clone())
                .collect();

            debug!(
                target: LOG_TARGET,
                batch_size = batch_requests.len(),
                "Retrieving events for {} contracts",
                batch_requests.len()
            );
            let instant = Instant::now();
            histogram!("torii_fetcher_batch_size").record(batch_requests.len() as f64);
            let batch_results = self.chunked_batch_requests(&batch_requests).await?;
            histogram!("torii_fetcher_rpc_batch_duration_seconds")
                .record(instant.elapsed().as_secs_f64());
            counter!("torii_fetcher_rpc_requests_total").increment(batch_requests.len() as u64);
            debug!(
                target: LOG_TARGET,
                duration = ?instant.elapsed(),
                batch_size = batch_requests.len(),
                "Retrieved events for {} contracts",
                batch_requests.len()
            );

            // Process results and prepare next batch of requests if needed
            for ((contract_address, mut from, mut to, original_request), result) in
                current_requests.into_iter().zip(batch_results)
            {
                debug!(
                    target: LOG_TARGET,
                    contract = format!("{:#x}", contract_address),
                    from_block = from,
                    to_block = to,
                    "Processing events for contract"
                );

                let old_cursor = old_cursors.get_mut(&contract_address).unwrap();
                let new_cursor = cursors.get_mut(&contract_address).unwrap();
                let mut last_pending_block_tx_tmp = old_cursor.last_pending_block_tx;
                let mut done = false;
                let mut contract_events_count = 0;

                match result {
                    ProviderResponseData::GetEvents(events_page) => {
                        debug!(
                            target: LOG_TARGET,
                            contract = format!("{:#x}", contract_address),
                            raw_events_count = events_page.events.len(),
                            has_continuation = events_page.continuation_token.is_some(),
                            "Received events page for contract"
                        );

                        // Process events for this page, only including events up to our target
                        // block
                        for event in events_page.events.clone() {
                            if from == 0 {
                                from = event.block_number.unwrap();
                                to =
                                    (from + self.config.blocks_chunk_size).min(latest_block_number);
                            }

                            if event.block_number.unwrap() > to {
                                done = true;
                                break;
                            }

                            // Then we skip all transactions until we reach the last pending
                            // processed transaction (if any)
                            if let Some(last_pending_block_tx) = last_pending_block_tx_tmp {
                                if event.transaction_hash != last_pending_block_tx {
                                    continue;
                                }
                                last_pending_block_tx_tmp = None;
                            }

                            // Skip the latest pending block transaction events
                            // * as we might have multiple events for the same transaction
                            if let Some(last_pending_block_tx) = old_cursor.last_pending_block_tx {
                                if event.transaction_hash == last_pending_block_tx {
                                    continue;
                                }
                            }

                            events.push(event);
                            contract_events_count += 1;
                        }

                        debug!(
                            target: LOG_TARGET,
                            contract = format!("{:#x}", contract_address),
                            processed_events = contract_events_count,
                            from_block = from,
                            to_block = to,
                            "Processed {} events for contract from block {} to {}",
                            contract_events_count, from, to
                        );

                        if let Some(event) = events.last() {
                            if new_cursor.head != Some(to) {
                                new_cursor.last_pending_block_tx = None;
                            }
                            let new_head = event.block_number.unwrap();
                            new_cursor.head = Some(new_head);
                            debug!(
                                target: LOG_TARGET,
                                contract = format!("{:#x}", contract_address),
                                new_head = new_head,
                                events_processed = contract_events_count,
                                "Updated cursor head to last processed event block"
                            );
                        }

                        // Add continuation request to next_requests instead of recursing
                        if events_page.continuation_token.is_some() && !done {
                            debug!(
                                target: LOG_TARGET,
                                contract = format!("{:#x}", contract_address),
                                from_block = from,
                                to_block = to,
                                "Adding continuation request for contract (more events available)"
                            );
                            if let ProviderRequestData::GetEvents(mut next_request) =
                                original_request
                            {
                                next_request.filter.result_page_request.continuation_token =
                                    events_page.continuation_token;
                                next_requests.push((
                                    contract_address,
                                    from,
                                    to,
                                    ProviderRequestData::GetEvents(next_request),
                                ));
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }

            debug!(
                target: LOG_TARGET,
                events_in_batch = events.len(),
                total_events = all_events.len() + events.len(),
                next_requests = next_requests.len(),
                "Batch processing complete: {} events in this batch, {} total events, {} continuation requests",
                events.len(),
                all_events.len() + events.len(),
                next_requests.len()
            );

            all_events.extend(events);
            current_requests = next_requests;
        }

        Ok(all_events)
    }

    async fn chunked_batch_requests(
        &self,
        requests: &[ProviderRequestData],
    ) -> Result<Vec<ProviderResponseData>, Error> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        const MAX_RETRIES: u32 = 5;
        const INITIAL_BACKOFF: Duration = Duration::from_millis(50);

        let mut futures = Vec::new();
        for chunk in requests.chunks(self.config.batch_chunk_size) {
            futures.push(async move {
                let mut attempt = 0;
                loop {
                    let rpc_start = Instant::now();
                    match self.provider.batch_requests(chunk).await {
                        Ok(results) => {
                            histogram!("torii_fetcher_rpc_duration_seconds")
                                .record(rpc_start.elapsed().as_secs_f64());
                            counter!("torii_fetcher_rpc_requests_total", "status" => "success")
                                .increment(chunk.len() as u64);
                            return Ok::<Vec<ProviderResponseData>, Error>(results);
                        }
                        Err(e) => {
                            counter!("torii_fetcher_rpc_requests_total", "status" => "error")
                                .increment(chunk.len() as u64);
                            counter!("torii_fetcher_errors_total", "operation" => "batch_request")
                                .increment(1);
                            if attempt < MAX_RETRIES {
                                let backoff = INITIAL_BACKOFF * 2u32.pow(attempt);
                                warn!(
                                    target: LOG_TARGET,
                                    attempt = attempt + 1,
                                    backoff_secs = backoff.as_secs(),
                                    error = ?e,
                                    chunk_size = chunk.len(),
                                    batch_chunk_size = self.config.batch_chunk_size,
                                    first_request = ?chunk.first(),
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
                                    first_request = ?chunk.first(),
                                    "Chunk batch request failed after all retries. This could be due to the provider being overloaded. You can try reducing the batch chunk size."
                                );
                                return Err(Error::BatchRequest(Box::new(e.into())));
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
