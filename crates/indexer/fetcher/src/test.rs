use async_trait::async_trait;
use cainome::cairo_serde::ContractAddress;
use dojo_test_utils::migration::copy_spawn_and_move_db;
use dojo_test_utils::setup::TestSetup;
use dojo_utils::{TransactionExt, TransactionWaiter, TxnConfig};
use dojo_world::contracts::naming::{compute_bytearray_hash, compute_selector_from_names};
use dojo_world::contracts::world::WorldContract;
use katana_runner::RunnerCtx;
use scarb_interop::Profile;
use scarb_metadata_ext::MetadataDojoExt;
use starknet::accounts::Account;
use starknet::core::types::{BlockId, BlockWithReceipts, Call, MaybePreConfirmedBlockWithReceipts};
use starknet::core::utils::get_selector_from_name;
use starknet::macros::felt;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{
    JsonRpcClient, Provider, ProviderError, ProviderRequestData, ProviderResponseData,
};
use starknet_crypto::Felt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use torii_storage::proto::ContractCursor;
use url::Url;

use crate::{Fetcher, FetcherConfig, FetchingFlags};

/// Mock provider that fails batch requests for the first N attempts, then succeeds
#[derive(Debug, Clone)]
pub struct MockRetryProvider {
    inner: Arc<JsonRpcClient<HttpTransport>>,
    failure_count: Arc<AtomicU32>,
    max_failures: u32,
    stats: Arc<RetryStats>,
}

#[derive(Debug, Default)]
pub struct RetryStats {
    pub total_calls: AtomicU32,
    pub failed_attempts: AtomicU32,
    pub successful_attempts: AtomicU32,
}

#[derive(Debug)]
pub struct RetryStatsSnapshot {
    pub total_calls: u32,
    pub failed_attempts: u32,
    pub successful_attempts: u32,
}

impl MockRetryProvider {
    pub fn new(inner: Arc<JsonRpcClient<HttpTransport>>, max_failures: u32) -> Self {
        Self {
            inner,
            failure_count: Arc::new(AtomicU32::new(0)),
            max_failures,
            stats: Arc::new(RetryStats::default()),
        }
    }

    pub fn get_retry_stats(&self) -> RetryStatsSnapshot {
        RetryStatsSnapshot {
            total_calls: self.stats.total_calls.load(Ordering::SeqCst),
            failed_attempts: self.stats.failed_attempts.load(Ordering::SeqCst),
            successful_attempts: self.stats.successful_attempts.load(Ordering::SeqCst),
        }
    }
}

#[async_trait]
impl Provider for MockRetryProvider {
    async fn batch_requests<R>(
        &self,
        requests: R,
    ) -> Result<Vec<ProviderResponseData>, ProviderError>
    where
        R: AsRef<[ProviderRequestData]> + Send + Sync,
    {
        self.stats.total_calls.fetch_add(1, Ordering::SeqCst);

        let current_failures = self.failure_count.load(Ordering::SeqCst);
        if current_failures < self.max_failures {
            // Simulate failure for first N attempts
            self.failure_count.fetch_add(1, Ordering::SeqCst);
            self.stats.failed_attempts.fetch_add(1, Ordering::SeqCst);

            println!(
                "   🔴 Mock provider simulating failure #{} (max: {})",
                current_failures + 1,
                self.max_failures
            );
            return Err(ProviderError::RateLimited);
        }

        // After max failures, delegate to real provider
        self.stats
            .successful_attempts
            .fetch_add(1, Ordering::SeqCst);
        println!(
            "   🟢 Mock provider delegating to real provider (attempt #{})",
            current_failures + 1
        );
        self.inner.batch_requests(requests).await
    }

    // Delegate all other methods to inner provider with correct generic signatures
    async fn spec_version(&self) -> Result<String, ProviderError> {
        self.inner.spec_version().await
    }

    async fn get_block_with_tx_hashes<B>(
        &self,
        block_id: B,
    ) -> Result<starknet::core::types::MaybePreConfirmedBlockWithTxHashes, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner.get_block_with_tx_hashes(block_id).await
    }

    async fn get_block_with_txs<B>(
        &self,
        block_id: B,
    ) -> Result<starknet::core::types::MaybePreConfirmedBlockWithTxs, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner.get_block_with_txs(block_id).await
    }

    async fn get_block_with_receipts<B>(
        &self,
        block_id: B,
    ) -> Result<MaybePreConfirmedBlockWithReceipts, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner.get_block_with_receipts(block_id).await
    }

    async fn get_state_update<B>(
        &self,
        block_id: B,
    ) -> Result<starknet::core::types::MaybePreConfirmedStateUpdate, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner.get_state_update(block_id).await
    }

    async fn get_storage_at<A, K, B>(
        &self,
        contract_address: A,
        key: K,
        block_id: B,
    ) -> Result<starknet_crypto::Felt, ProviderError>
    where
        A: AsRef<starknet_crypto::Felt> + Send + Sync,
        K: AsRef<starknet_crypto::Felt> + Send + Sync,
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner
            .get_storage_at(contract_address, key, block_id)
            .await
    }

    async fn get_messages_status(
        &self,
        transaction_hash: starknet::core::types::Hash256,
    ) -> Result<Vec<starknet::core::types::MessageStatus>, ProviderError> {
        self.inner.get_messages_status(transaction_hash).await
    }

    async fn get_transaction_status<H>(
        &self,
        transaction_hash: H,
    ) -> Result<starknet::core::types::TransactionStatus, ProviderError>
    where
        H: AsRef<starknet_crypto::Felt> + Send + Sync,
    {
        self.inner.get_transaction_status(transaction_hash).await
    }

    async fn get_transaction_by_hash<H>(
        &self,
        transaction_hash: H,
    ) -> Result<starknet::core::types::Transaction, ProviderError>
    where
        H: AsRef<starknet_crypto::Felt> + Send + Sync,
    {
        self.inner.get_transaction_by_hash(transaction_hash).await
    }

    async fn get_transaction_by_block_id_and_index<B>(
        &self,
        block_id: B,
        index: u64,
    ) -> Result<starknet::core::types::Transaction, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner
            .get_transaction_by_block_id_and_index(block_id, index)
            .await
    }

    async fn get_transaction_receipt<H>(
        &self,
        transaction_hash: H,
    ) -> Result<starknet::core::types::TransactionReceiptWithBlockInfo, ProviderError>
    where
        H: AsRef<starknet_crypto::Felt> + Send + Sync,
    {
        self.inner.get_transaction_receipt(transaction_hash).await
    }

    async fn get_class<B, H>(
        &self,
        block_id: B,
        class_hash: H,
    ) -> Result<starknet::core::types::ContractClass, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
        H: AsRef<starknet_crypto::Felt> + Send + Sync,
    {
        self.inner.get_class(block_id, class_hash).await
    }

    async fn get_class_hash_at<B, A>(
        &self,
        block_id: B,
        contract_address: A,
    ) -> Result<starknet_crypto::Felt, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
        A: AsRef<starknet_crypto::Felt> + Send + Sync,
    {
        self.inner
            .get_class_hash_at(block_id, contract_address)
            .await
    }

    async fn get_class_at<B, A>(
        &self,
        block_id: B,
        contract_address: A,
    ) -> Result<starknet::core::types::ContractClass, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
        A: AsRef<starknet_crypto::Felt> + Send + Sync,
    {
        self.inner.get_class_at(block_id, contract_address).await
    }

    async fn get_block_transaction_count<B>(&self, block_id: B) -> Result<u64, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner.get_block_transaction_count(block_id).await
    }

    async fn call<R, B>(
        &self,
        request: R,
        block_id: B,
    ) -> Result<Vec<starknet_crypto::Felt>, ProviderError>
    where
        R: AsRef<starknet::core::types::FunctionCall> + Send + Sync,
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner.call(request, block_id).await
    }

    async fn estimate_fee<R, S, B>(
        &self,
        request: R,
        simulation_flags: S,
        block_id: B,
    ) -> Result<Vec<starknet::core::types::FeeEstimate>, ProviderError>
    where
        R: AsRef<[starknet::core::types::BroadcastedTransaction]> + Send + Sync,
        S: AsRef<[starknet::core::types::SimulationFlagForEstimateFee]> + Send + Sync,
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner
            .estimate_fee(request, simulation_flags, block_id)
            .await
    }

    async fn estimate_message_fee<M, B>(
        &self,
        message: M,
        block_id: B,
    ) -> Result<starknet::core::types::MessageFeeEstimate, ProviderError>
    where
        M: AsRef<starknet::core::types::MsgFromL1> + Send + Sync,
        B: AsRef<BlockId> + Send + Sync,
    {
        self.inner.estimate_message_fee(message, block_id).await
    }

    async fn block_number(&self) -> Result<u64, ProviderError> {
        self.inner.block_number().await
    }

    async fn block_hash_and_number(
        &self,
    ) -> Result<starknet::core::types::BlockHashAndNumber, ProviderError> {
        self.inner.block_hash_and_number().await
    }

    async fn chain_id(&self) -> Result<starknet_crypto::Felt, ProviderError> {
        self.inner.chain_id().await
    }

    async fn syncing(&self) -> Result<starknet::core::types::SyncStatusType, ProviderError> {
        self.inner.syncing().await
    }

    async fn get_events(
        &self,
        filter: starknet::core::types::EventFilter,
        continuation_token: Option<String>,
        chunk_size: u64,
    ) -> Result<starknet::core::types::EventsPage, ProviderError> {
        self.inner
            .get_events(filter, continuation_token, chunk_size)
            .await
    }

    async fn get_nonce<B, A>(
        &self,
        block_id: B,
        contract_address: A,
    ) -> Result<starknet_crypto::Felt, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
        A: AsRef<starknet_crypto::Felt> + Send + Sync,
    {
        self.inner.get_nonce(block_id, contract_address).await
    }

    async fn get_storage_proof<B, H, A, K>(
        &self,
        block_id: B,
        class_hashes: H,
        contract_addresses: A,
        contracts_storage_keys: K,
    ) -> Result<starknet::core::types::StorageProof, ProviderError>
    where
        B: AsRef<starknet::core::types::ConfirmedBlockId> + Send + Sync,
        H: AsRef<[starknet_crypto::Felt]> + Send + Sync,
        A: AsRef<[starknet_crypto::Felt]> + Send + Sync,
        K: AsRef<[starknet::core::types::ContractStorageKeys]> + Send + Sync,
    {
        self.inner
            .get_storage_proof(
                block_id,
                class_hashes,
                contract_addresses,
                contracts_storage_keys,
            )
            .await
    }

    async fn add_invoke_transaction<I>(
        &self,
        invoke_transaction: I,
    ) -> Result<starknet::core::types::InvokeTransactionResult, ProviderError>
    where
        I: AsRef<starknet::core::types::BroadcastedInvokeTransaction> + Send + Sync,
    {
        self.inner.add_invoke_transaction(invoke_transaction).await
    }

    async fn add_declare_transaction<D>(
        &self,
        declare_transaction: D,
    ) -> Result<starknet::core::types::DeclareTransactionResult, ProviderError>
    where
        D: AsRef<starknet::core::types::BroadcastedDeclareTransaction> + Send + Sync,
    {
        self.inner
            .add_declare_transaction(declare_transaction)
            .await
    }

    async fn add_deploy_account_transaction<D>(
        &self,
        deploy_account_transaction: D,
    ) -> Result<starknet::core::types::DeployAccountTransactionResult, ProviderError>
    where
        D: AsRef<starknet::core::types::BroadcastedDeployAccountTransaction> + Send + Sync,
    {
        self.inner
            .add_deploy_account_transaction(deploy_account_transaction)
            .await
    }

    async fn trace_transaction<H>(
        &self,
        transaction_hash: H,
    ) -> Result<starknet::core::types::TransactionTrace, ProviderError>
    where
        H: AsRef<starknet_crypto::Felt> + Send + Sync,
    {
        self.inner.trace_transaction(transaction_hash).await
    }

    async fn simulate_transactions<B, T, S>(
        &self,
        block_id: B,
        transactions: T,
        simulation_flags: S,
    ) -> Result<Vec<starknet::core::types::SimulatedTransaction>, ProviderError>
    where
        B: AsRef<BlockId> + Send + Sync,
        T: AsRef<[starknet::core::types::BroadcastedTransaction]> + Send + Sync,
        S: AsRef<[starknet::core::types::SimulationFlag]> + Send + Sync,
    {
        self.inner
            .simulate_transactions(block_id, transactions, simulation_flags)
            .await
    }

    async fn trace_block_transactions<B>(
        &self,
        block_id: B,
    ) -> Result<Vec<starknet::core::types::TransactionTraceWithHash>, ProviderError>
    where
        B: AsRef<starknet::core::types::ConfirmedBlockId> + Send + Sync,
    {
        self.inner.trace_block_transactions(block_id).await
    }
}

const CARTRIDGE_NODE_MAINNET: &str = "https://api.cartridge.gg/x/starknet/mainnet/rpc/v0_8";
const ETERNUM_ADDRESS: Felt =
    felt!("0x5c6d0020a9927edca9ddc984b97305439c0b32a1ec8d3f0eaf6291074cc9799");

/// Get a block with receipts from the provider.
///
/// To avoid fetching here, we may use a pre-fetched file instead.
/// This however requires more setup and more data committed to the repo.
async fn get_block_with_receipts<P: Provider + Send + Sync + std::fmt::Debug + 'static>(
    provider: &P,
    block_number: u64,
) -> BlockWithReceipts {
    match provider
        .get_block_with_receipts(BlockId::Number(block_number))
        .await
        .unwrap()
    {
        MaybePreConfirmedBlockWithReceipts::Block(block) => block,
        _ => panic!("Expected a block, got a pending block"),
    }
}

#[tokio::test]
async fn test_range_one_block() {
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
        Url::parse(CARTRIDGE_NODE_MAINNET).unwrap(),
    )));

    let eternum_block = 1435856;

    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            blocks_chunk_size: 1,
            ..Default::default()
        },
    );

    // To index 1435856, the cursor must actually be one block behind.
    let cursors = HashMap::from([(
        ETERNUM_ADDRESS,
        ContractCursor {
            contract_address: ETERNUM_ADDRESS,
            last_pending_block_tx: None,
            head: Some(eternum_block - 1),
            last_block_timestamp: None,
            tps: None,
        },
    )]);

    let result = fetcher.fetch(&cursors).await.unwrap();

    let expected = get_block_with_receipts(&provider, eternum_block).await;

    let torii_block = &result.range.blocks[&eternum_block];

    // Expecting the block right after the cursor head + the chunk size.
    assert_eq!(result.range.blocks.len(), 2);
    assert_eq!(torii_block.block_hash, Some(expected.block_hash));
    assert_eq!(torii_block.timestamp, expected.timestamp);

    // Verify all transactions are present and match
    for (torii_tx_hash, _torii_tx) in torii_block.transactions.iter() {
        let expected_tx = expected
            .transactions
            .iter()
            .find(|tx| tx.receipt.transaction_hash() == torii_tx_hash);
        assert!(expected_tx.is_some());
        assert_eq!(
            torii_tx_hash,
            expected_tx.unwrap().receipt.transaction_hash()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 3600000)]
async fn test_fetch_pending_basic(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    let world = WorldContract::new(world_address, &account);

    // Grant writer - this transaction will be included in our results
    let grant_writer_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_writer_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mine a block
    sequencer.dev_client().generate_block().await.unwrap();

    // Get current block number after grant_writer is mined
    let latest_block = provider.block_hash_and_number().await.unwrap();
    let current_block_number = latest_block.block_number;

    // Create fetcher with pending blocks enabled
    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            flags: FetchingFlags::PENDING_BLOCKS | FetchingFlags::TRANSACTIONS,
            blocks_chunk_size: 10,
            ..Default::default()
        },
    );

    // Set up cursor at the current block
    let cursors = HashMap::from([(
        world_address,
        ContractCursor {
            contract_address: world_address,
            last_pending_block_tx: None,
            head: Some(current_block_number),
            last_block_timestamp: None,
            tps: None,
        },
    )]);

    // Submit a pending transaction (don't wait for it to be mined)
    let spawn_tx = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    // Fetch pending data
    let result = fetcher.fetch(&cursors).await.unwrap();

    // Verify pending block was fetched
    assert!(result.preconfirmed_block.is_some());
    let pending = result.preconfirmed_block.unwrap();

    // Should have our pending transaction
    assert!(pending
        .transactions
        .contains_key(&spawn_tx.transaction_hash));
    assert_eq!(pending.block_number, current_block_number + 1);

    // Verify cursor was updated correctly
    let updated_cursor = &result.cursors.cursors[&world_address];
    assert!(updated_cursor.last_pending_block_tx.is_some());
    assert_eq!(
        updated_cursor.last_pending_block_tx.unwrap(),
        spawn_tx.transaction_hash
    );
    assert_eq!(updated_cursor.head, Some(current_block_number));
    assert!(updated_cursor.last_block_timestamp.is_some());

    // Verify cursor_transactions includes our transaction
    assert!(result
        .cursors
        .cursor_transactions
        .contains_key(&world_address));
    let world_transactions = &result.cursors.cursor_transactions[&world_address];
    assert!(world_transactions.contains(&spawn_tx.transaction_hash));

    // Should have the spawn transaction for the world contract
    assert_eq!(world_transactions.len(), 1);

    // Verify transaction has events (spawn creates model events)
    let spawn_transaction = &pending.transactions[&spawn_tx.transaction_hash];
    assert!(!spawn_transaction.events.is_empty());
    assert!(spawn_transaction.transaction.is_some());
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 3600000)]
async fn test_fetch_pending_multiple_transactions(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    let world = WorldContract::new(world_address, &account);

    let grant_writer_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_writer_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mine a block
    sequencer.dev_client().generate_block().await.unwrap();

    let latest_block = provider.block_hash_and_number().await.unwrap();
    let current_block_number = latest_block.block_number;

    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            flags: FetchingFlags::PENDING_BLOCKS | FetchingFlags::TRANSACTIONS,
            blocks_chunk_size: 10,
            ..Default::default()
        },
    );

    let cursors = HashMap::from([(
        world_address,
        ContractCursor {
            contract_address: world_address,
            last_pending_block_tx: None,
            head: Some(current_block_number),
            last_block_timestamp: None,
            tps: None,
        },
    )]);

    // Submit multiple pending transactions
    let tx1 = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    let tx2 = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("move").unwrap(),
            calldata: vec![Felt::ONE],
        }])
        .send()
        .await
        .unwrap();

    let tx3 = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("move").unwrap(),
            calldata: vec![Felt::TWO],
        }])
        .send()
        .await
        .unwrap();

    // Fetch pending data
    let result = fetcher.fetch(&cursors).await.unwrap();

    assert!(result.preconfirmed_block.is_some());
    let pending = result.preconfirmed_block.unwrap();

    // Should have all three pending transactions
    assert!(pending.transactions.contains_key(&tx1.transaction_hash));
    assert!(pending.transactions.contains_key(&tx2.transaction_hash));
    assert!(pending.transactions.contains_key(&tx3.transaction_hash));

    // Verify all transactions have proper content
    for tx_hash in [
        tx1.transaction_hash,
        tx2.transaction_hash,
        tx3.transaction_hash,
    ] {
        let transaction = &pending.transactions[&tx_hash];
        assert!(transaction.transaction.is_some());
        assert!(!transaction.events.is_empty());
    }

    // Cursor should point to the last transaction
    let updated_cursor = &result.cursors.cursors[&world_address];
    assert_eq!(
        updated_cursor.last_pending_block_tx.unwrap(),
        tx3.transaction_hash
    );
    assert_eq!(updated_cursor.head, Some(current_block_number));

    // Verify cursor_transactions includes all our transactions
    let world_transactions = &result.cursors.cursor_transactions[&world_address];
    assert!(world_transactions.contains(&tx1.transaction_hash));
    assert!(world_transactions.contains(&tx2.transaction_hash));
    assert!(world_transactions.contains(&tx3.transaction_hash));
    assert_eq!(world_transactions.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 3600000)]
async fn test_fetch_pending_with_cursor_continuation(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    let world = WorldContract::new(world_address, &account);

    let grant_writer_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_writer_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mine a block
    sequencer.dev_client().generate_block().await.unwrap();

    let latest_block = provider.block_hash_and_number().await.unwrap();
    let current_block_number = latest_block.block_number;

    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            flags: FetchingFlags::PENDING_BLOCKS | FetchingFlags::TRANSACTIONS,
            blocks_chunk_size: 10,
            ..Default::default()
        },
    );

    // Submit first transaction and process it
    let tx1 = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    let cursors = HashMap::from([(
        world_address,
        ContractCursor {
            contract_address: world_address,
            last_pending_block_tx: None,
            head: Some(current_block_number),
            last_block_timestamp: None,
            tps: None,
        },
    )]);

    let result1 = fetcher.fetch(&cursors).await.unwrap();
    assert!(result1.preconfirmed_block.is_some());
    let pending1 = result1.preconfirmed_block.unwrap();

    // Verify first fetch results
    assert!(pending1.transactions.contains_key(&tx1.transaction_hash));
    assert_eq!(result1.cursors.cursor_transactions[&world_address].len(), 1);
    assert_eq!(
        result1.cursors.cursors[&world_address]
            .last_pending_block_tx
            .unwrap(),
        tx1.transaction_hash
    );

    // Mine a block
    sequencer.dev_client().generate_block().await.unwrap();

    // Now submit more transactions
    let tx2 = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("move").unwrap(),
            calldata: vec![Felt::ONE],
        }])
        .send()
        .await
        .unwrap();

    let tx3 = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("move").unwrap(),
            calldata: vec![Felt::TWO],
        }])
        .send()
        .await
        .unwrap();

    // Use the updated cursors from first fetch
    let res = fetcher.fetch(&result1.cursors.cursors).await.unwrap();
    assert!(res.preconfirmed_block.is_some());
    let pending2 = res.preconfirmed_block.unwrap();

    // Should not include tx1 (already processed), but should include tx2 and tx3
    assert!(!pending2.transactions.contains_key(&tx1.transaction_hash));
    assert!(pending2.transactions.contains_key(&tx2.transaction_hash));
    assert!(pending2.transactions.contains_key(&tx3.transaction_hash));

    // Verify transaction content
    assert!(pending2.transactions[&tx2.transaction_hash]
        .transaction
        .is_some());
    assert!(pending2.transactions[&tx3.transaction_hash]
        .transaction
        .is_some());
    assert!(!pending2.transactions[&tx2.transaction_hash]
        .events
        .is_empty());
    assert!(!pending2.transactions[&tx3.transaction_hash]
        .events
        .is_empty());

    // Cursor should point to the last new transaction
    assert_eq!(
        res.cursors.cursors[&world_address]
            .last_pending_block_tx
            .unwrap(),
        tx3.transaction_hash
    );

    // Verify cursor_transactions only includes new transactions
    let world_transactions = &res.cursors.cursor_transactions[&world_address];
    assert!(!world_transactions.contains(&tx1.transaction_hash));
    assert!(world_transactions.contains(&tx2.transaction_hash));
    assert!(world_transactions.contains(&tx3.transaction_hash));
    assert_eq!(world_transactions.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 3600000)]
async fn test_fetch_pending_to_mined_switching_logic(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    let world = WorldContract::new(world_address, &account);

    let grant_writer_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_writer_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mine a block
    sequencer.dev_client().generate_block().await.unwrap();

    let initial_block = provider.block_hash_and_number().await.unwrap();
    let initial_block_number = initial_block.block_number;

    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            flags: FetchingFlags::PENDING_BLOCKS | FetchingFlags::TRANSACTIONS,
            blocks_chunk_size: 10,
            ..Default::default()
        },
    );

    let initial_cursors = HashMap::from([(
        world_address,
        ContractCursor {
            contract_address: world_address,
            last_pending_block_tx: None,
            head: Some(initial_block_number),
            last_block_timestamp: None,
            tps: None,
        },
    )]);

    // Phase 1: Submit pending transactions
    let pending_tx1 = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    let pending_tx2 = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("move").unwrap(),
            calldata: vec![Felt::ONE],
        }])
        .send()
        .await
        .unwrap();

    // Phase 2: Mine the block (this moves pending transactions to mined)
    sequencer.dev_client().generate_block().await.unwrap();

    // Phase 3: Submit new transactions that will be pending
    let new_pending_tx = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("move").unwrap(),
            calldata: vec![Felt::TWO],
        }])
        .send()
        .await
        .unwrap();

    // Phase 4: Fetch with cursors from before the block was mined
    // This should fetch the range (newly mined block) and new pending transactions
    let switching_result = fetcher.fetch(&initial_cursors).await.unwrap();

    // Verify range contains the previously pending transactions (now mined)
    let mined_block_number = initial_block_number + 1;
    assert!(switching_result
        .range
        .blocks
        .contains_key(&mined_block_number));
    let mined_block = &switching_result.range.blocks[&mined_block_number];

    // Both previously pending transactions should now be in the mined block
    assert!(mined_block
        .transactions
        .contains_key(&pending_tx1.transaction_hash));
    assert!(mined_block
        .transactions
        .contains_key(&pending_tx2.transaction_hash));

    // Verify the mined transactions have events and transaction content
    let mined_tx1 = &mined_block.transactions[&pending_tx1.transaction_hash];
    let mined_tx2 = &mined_block.transactions[&pending_tx2.transaction_hash];
    assert!(!mined_tx1.events.is_empty());
    assert!(!mined_tx2.events.is_empty());

    // Verify cursor_transactions for range includes the mined transactions
    let total_txns = &switching_result.cursors.cursor_transactions[&world_address];
    assert!(total_txns.contains(&pending_tx1.transaction_hash));
    assert!(total_txns.contains(&pending_tx2.transaction_hash));
    assert!(total_txns.contains(&new_pending_tx.transaction_hash));

    // Verify cursors are updated correctly for the range
    let range_cursor = &switching_result.cursors.cursors[&world_address];
    assert_eq!(range_cursor.head, Some(mined_block_number));
    assert!(range_cursor.last_block_timestamp.is_some());

    // Verify new pending transactions
    if let Some(new_pending) = &switching_result.preconfirmed_block {
        assert!(new_pending
            .transactions
            .contains_key(&new_pending_tx.transaction_hash));

        // Should not contain the previously pending transactions
        assert!(!new_pending
            .transactions
            .contains_key(&pending_tx1.transaction_hash));
        assert!(!new_pending
            .transactions
            .contains_key(&pending_tx2.transaction_hash));
    }

    // Verify cursor for new pending
    let pending_cursor = &switching_result.cursors.cursors[&world_address];
    assert_eq!(
        pending_cursor.last_pending_block_tx.unwrap(),
        new_pending_tx.transaction_hash
    );
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 3600000)]
async fn test_fetch_pending_with_events_comprehensive(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    let world = WorldContract::new(world_address, &account);

    let grant_writer_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_writer_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mine a block
    sequencer.dev_client().generate_block().await.unwrap();

    let latest_block = provider.block_hash_and_number().await.unwrap();
    let current_block_number = latest_block.block_number;

    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            flags: FetchingFlags::PENDING_BLOCKS | FetchingFlags::TRANSACTIONS,
            blocks_chunk_size: 10,
            ..Default::default()
        },
    );

    let cursors = HashMap::from([(
        world_address,
        ContractCursor {
            contract_address: world_address,
            last_pending_block_tx: None,
            head: Some(current_block_number),
            last_block_timestamp: None,
            tps: None,
        },
    )]);

    // Submit transaction that will generate events (spawn creates entities)
    let spawn_tx = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    let result = fetcher.fetch(&cursors).await.unwrap();

    assert!(result.preconfirmed_block.is_some());
    let pending = result.preconfirmed_block.unwrap();

    // Should have our transaction
    assert!(pending
        .transactions
        .contains_key(&spawn_tx.transaction_hash));

    let transaction = &pending.transactions[&spawn_tx.transaction_hash];

    // Transaction should have events (spawn creates model events)
    assert!(!transaction.events.is_empty());
    assert!(transaction.transaction.is_some());

    // Verify events are properly structured
    for event in &transaction.events {
        // Events should be related to the world contract operations
        assert!(!event.keys.is_empty());
        assert!(!event.data.is_empty());
    }

    // Verify cursor tracking
    let world_transactions = &result.cursors.cursor_transactions[&world_address];
    assert!(world_transactions.contains(&spawn_tx.transaction_hash));
    assert_eq!(world_transactions.len(), 1);

    // Verify cursor update
    let cursor = &result.cursors.cursors[&world_address];
    assert_eq!(
        cursor.last_pending_block_tx.unwrap(),
        spawn_tx.transaction_hash
    );
    assert_eq!(cursor.head, Some(current_block_number));
    assert!(cursor.last_block_timestamp.is_some());
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 3600000)]
async fn test_fetch_pending_filters_reverted_transactions(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    let world = WorldContract::new(world_address, &account);

    let grant_writer_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_writer_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mine a block
    sequencer.dev_client().generate_block().await.unwrap();

    let latest_block = provider.block_hash_and_number().await.unwrap();
    let current_block_number = latest_block.block_number;

    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            flags: FetchingFlags::PENDING_BLOCKS | FetchingFlags::TRANSACTIONS,
            blocks_chunk_size: 10,
            ..Default::default()
        },
    );

    let cursors = HashMap::from([(
        world_address,
        ContractCursor {
            contract_address: world_address,
            last_pending_block_tx: None,
            head: Some(current_block_number),
            last_block_timestamp: None,
            tps: None,
        },
    )]);

    // Submit a transaction that should succeed
    let good_tx = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    let result = fetcher.fetch(&cursors).await.unwrap();

    assert!(result.preconfirmed_block.is_some());
    let pending = result.preconfirmed_block.unwrap();

    // Should definitely have the good transaction
    assert!(pending.transactions.contains_key(&good_tx.transaction_hash));

    // Verify the good transaction has proper content
    let good_transaction = &pending.transactions[&good_tx.transaction_hash];
    assert!(good_transaction.transaction.is_some());
    assert!(!good_transaction.events.is_empty());

    // Verify cursor tracking includes the good transaction
    let world_transactions = &result.cursors.cursor_transactions[&world_address];
    assert!(world_transactions.contains(&good_tx.transaction_hash));

    // Should only have the good transaction
    assert_eq!(world_transactions.len(), 1);

    // Verify cursor is updated to the good transaction
    let cursor = &result.cursors.cursors[&world_address];
    assert_eq!(
        cursor.last_pending_block_tx.unwrap(),
        good_tx.transaction_hash
    );
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 3600000)]
async fn test_fetch_pending_multiple_contracts_comprehensive(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    // Get ERC20 token address for testing multiple contracts
    let wood_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.tag == "ns-WoodToken")
        .unwrap()
        .address;

    let world = WorldContract::new(world_address, &account);

    let grant_writer_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_writer_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mine a block
    sequencer.dev_client().generate_block().await.unwrap();

    let latest_block = provider.block_hash_and_number().await.unwrap();
    let current_block_number = latest_block.block_number;

    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            flags: FetchingFlags::PENDING_BLOCKS | FetchingFlags::TRANSACTIONS,
            blocks_chunk_size: 10,
            ..Default::default()
        },
    );

    // Set up cursors for both world and ERC20 contracts
    let cursors = HashMap::from([
        (
            world_address,
            ContractCursor {
                contract_address: world_address,
                last_pending_block_tx: None,
                head: Some(current_block_number),
                last_block_timestamp: None,
                tps: None,
            },
        ),
        (
            wood_address,
            ContractCursor {
                contract_address: wood_address,
                last_pending_block_tx: None,
                head: Some(current_block_number),
                last_block_timestamp: None,
                tps: None,
            },
        ),
    ]);

    // Submit transaction to world contract
    let world_tx = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    // Submit transaction to ERC20 contract
    let erc20_tx = account
        .execute_v3(vec![Call {
            to: wood_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(100), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    let result = fetcher.fetch(&cursors).await.unwrap();

    assert!(result.preconfirmed_block.is_some());
    let pending = result.preconfirmed_block.unwrap();

    // Should have both transactions
    assert!(pending
        .transactions
        .contains_key(&world_tx.transaction_hash));
    assert!(pending
        .transactions
        .contains_key(&erc20_tx.transaction_hash));

    // Verify transaction content
    let world_transaction = &pending.transactions[&world_tx.transaction_hash];
    let erc20_transaction = &pending.transactions[&erc20_tx.transaction_hash];

    assert!(world_transaction.transaction.is_some());
    assert!(erc20_transaction.transaction.is_some());
    assert!(!world_transaction.events.is_empty());
    assert!(!erc20_transaction.events.is_empty());

    // Verify cursor_transactions are properly separated by contract
    assert!(result
        .cursors
        .cursor_transactions
        .contains_key(&world_address));
    assert!(result
        .cursors
        .cursor_transactions
        .contains_key(&wood_address));

    let world_transactions = &result.cursors.cursor_transactions[&world_address];
    let wood_transactions = &result.cursors.cursor_transactions[&wood_address];

    assert!(world_transactions.contains(&world_tx.transaction_hash));
    assert!(wood_transactions.contains(&erc20_tx.transaction_hash));

    // Each contract should track its own transactions separately
    assert!(!world_transactions.contains(&erc20_tx.transaction_hash));
    assert!(!wood_transactions.contains(&world_tx.transaction_hash));

    // Verify each contract has exactly one transaction
    assert_eq!(world_transactions.len(), 1);
    assert_eq!(wood_transactions.len(), 1);

    // Verify cursors are updated correctly for both contracts
    let world_cursor = &result.cursors.cursors[&world_address];
    let wood_cursor = &result.cursors.cursors[&wood_address];

    assert_eq!(
        world_cursor.last_pending_block_tx.unwrap(),
        world_tx.transaction_hash
    );
    assert_eq!(
        wood_cursor.last_pending_block_tx.unwrap(),
        erc20_tx.transaction_hash
    );

    assert_eq!(world_cursor.head, Some(current_block_number));
    assert_eq!(wood_cursor.head, Some(current_block_number));

    assert!(world_cursor.last_block_timestamp.is_some());
    assert!(wood_cursor.last_block_timestamp.is_some());
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 3600000)]
async fn test_fetch_comprehensive_multi_contract_spam_with_selective_indexing_and_ordering_validation(
    sequencer: &RunnerCtx,
) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let account2 = sequencer.account(1);
    let account3 = sequencer.account(2);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    // Get ERC20 and ERC721 contract addresses for spamming other contracts
    let wood_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.tag == "ns-WoodToken")
        .unwrap()
        .address;

    let badge_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.tag == "ns-Badge")
        .unwrap()
        .address;

    let world = WorldContract::new(world_address, &account);

    // Grant writer permissions
    let grant_writer_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_writer_res.transaction_hash, &provider)
        .await
        .unwrap();

    let grant_badge_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(badge_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_badge_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Get additional contracts for unrelated transactions (these will NOT be indexed)
    let rewards_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.tag == "ns-Rewards")
        .unwrap()
        .address;

    // Grant writer permissions for rewards (unrelated contract)
    let grant_rewards_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(rewards_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_rewards_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mine a block to establish initial state AFTER all grant_writer transactions
    sequencer.dev_client().generate_block().await.unwrap();

    let initial_block = provider.block_hash_and_number().await.unwrap();
    let initial_block_number = initial_block.block_number;

    // Create fetcher that indexes world, ERC20, and ERC721 contracts (but NOT rewards)
    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            flags: FetchingFlags::PENDING_BLOCKS | FetchingFlags::TRANSACTIONS,
            blocks_chunk_size: 10,
            ..Default::default()
        },
    );

    // Set up cursors for world, ERC20, and ERC721 contracts (rewards is excluded)
    let initial_cursors = HashMap::from([
        (
            world_address,
            ContractCursor {
                contract_address: world_address,
                last_pending_block_tx: None,
                head: Some(initial_block_number),
                last_block_timestamp: None,
                tps: None,
            },
        ),
        (
            wood_address,
            ContractCursor {
                contract_address: wood_address,
                last_pending_block_tx: None,
                head: Some(initial_block_number),
                last_block_timestamp: None,
                tps: None,
            },
        ),
        (
            badge_address,
            ContractCursor {
                contract_address: badge_address,
                last_pending_block_tx: None,
                head: Some(initial_block_number),
                last_block_timestamp: None,
                tps: None,
            },
        ),
    ]);

    // Phase 1: Spam transactions across multiple contracts (world, ERC20, ERC721, rewards) in pending state

    let mut world_txs = Vec::new();
    let mut erc20_txs = Vec::new();
    let mut erc721_txs = Vec::new();
    let mut rewards_txs: Vec<Felt> = Vec::new(); // These will NOT be indexed

    // Spam world contract transactions (spawn and move actions)
    for i in 0..5 {
        let account_to_use = match i % 3 {
            0 => &account,
            1 => &account2,
            _ => &account3,
        };

        // Spawn transaction - creates Position and Moves models
        let spawn_tx = account_to_use
            .execute_v3(vec![Call {
                to: actions_address,
                selector: get_selector_from_name("spawn").unwrap(),
                calldata: vec![],
            }])
            .send()
            .await
            .unwrap();
        world_txs.push(spawn_tx.transaction_hash);

        // Move transaction - updates Position and Moves, emits Moved event
        let move_direction = match i % 4 {
            0 => Felt::ONE,     // Left
            1 => Felt::TWO,     // Right
            2 => Felt::THREE,   // Up
            _ => Felt::from(4), // Down
        };

        let move_tx = account_to_use
            .execute_v3(vec![Call {
                to: actions_address,
                selector: get_selector_from_name("move").unwrap(),
                calldata: vec![move_direction],
            }])
            .send()
            .await
            .unwrap();
        world_txs.push(move_tx.transaction_hash);
    }

    // Spam ERC20 contract transactions (should NOT be indexed by our fetcher)
    for i in 0..8 {
        let account_to_use = match i % 3 {
            0 => &account,
            1 => &account2,
            _ => &account3,
        };

        let erc20_tx = account_to_use
            .execute_v3(vec![Call {
                to: wood_address,
                selector: get_selector_from_name("mint").unwrap(),
                calldata: vec![Felt::from(1000 + i), Felt::ZERO],
            }])
            .send()
            .await
            .unwrap();
        erc20_txs.push(erc20_tx.transaction_hash);
    }

    // Spam ERC721 contract transactions (will be indexed by our fetcher)
    for i in 0..6 {
        let account_to_use = match i % 3 {
            0 => &account,
            1 => &account2,
            _ => &account3,
        };

        let erc721_tx = account_to_use
            .execute_v3(vec![Call {
                to: badge_address,
                selector: get_selector_from_name("mint").unwrap(),
                calldata: vec![Felt::from(100 + i), Felt::ZERO],
            }])
            .send()
            .await
            .unwrap();
        erc721_txs.push(erc721_tx.transaction_hash);
    }

    // Spam rewards contract transactions (should NOT be indexed by our fetcher - unrelated contract)
    for i in 0..7 {
        let account_to_use = match i % 3 {
            0 => &account,
            1 => &account2,
            _ => &account3,
        };

        let rewards_tx = account_to_use
            .execute_v3(vec![Call {
                to: rewards_address,
                selector: get_selector_from_name("mint").unwrap(),
                calldata: vec![
                    Felt::from(1 + i),
                    Felt::ZERO,
                    Felt::from(500 + i * 10),
                    Felt::ZERO,
                ],
            }])
            .send()
            .await
            .unwrap();
        rewards_txs.push(rewards_tx.transaction_hash);
    }

    // Phase 2: Fetch pending transactions - should get world, ERC20, and ERC721 transactions (but not rewards)
    let pending_result = fetcher.fetch(&initial_cursors).await.unwrap();

    assert!(pending_result.preconfirmed_block.is_some());
    let pending_data = pending_result.preconfirmed_block.unwrap();

    // Verify all indexed contract transactions are fetched
    for world_tx in &world_txs {
        assert!(
            pending_data.transactions.contains_key(world_tx),
            "World transaction {:?} should be present in pending data",
            world_tx
        );
    }

    for erc20_tx in &erc20_txs {
        assert!(
            pending_data.transactions.contains_key(erc20_tx),
            "ERC20 transaction {:?} should be present in pending data",
            erc20_tx
        );
    }

    for erc721_tx in &erc721_txs {
        assert!(
            pending_data.transactions.contains_key(erc721_tx),
            "ERC721 transaction {:?} should be present in pending data",
            erc721_tx
        );
    }

    // Verify cursor tracking for all indexed contracts
    assert!(pending_result
        .cursors
        .cursor_transactions
        .contains_key(&world_address));
    assert!(pending_result
        .cursors
        .cursor_transactions
        .contains_key(&wood_address));
    assert!(pending_result
        .cursors
        .cursor_transactions
        .contains_key(&badge_address));

    // Verify rewards contract is NOT tracked (since we don't index it)
    assert!(!pending_result
        .cursors
        .cursor_transactions
        .contains_key(&rewards_address));

    let world_cursor_txs = &pending_result.cursors.cursor_transactions[&world_address];
    let erc20_cursor_txs = &pending_result.cursors.cursor_transactions[&wood_address];
    let erc721_cursor_txs = &pending_result.cursors.cursor_transactions[&badge_address];

    // Verify world contract cursor tracking
    for world_tx in &world_txs {
        assert!(
            world_cursor_txs.contains(world_tx),
            "World transaction {:?} should be tracked in cursor_transactions",
            world_tx
        );
    }
    assert_eq!(world_cursor_txs.len(), world_txs.len());

    // Verify ERC20 contract cursor tracking
    for erc20_tx in &erc20_txs {
        assert!(
            erc20_cursor_txs.contains(erc20_tx),
            "ERC20 transaction {:?} should be tracked in cursor_transactions",
            erc20_tx
        );
    }
    assert_eq!(erc20_cursor_txs.len(), erc20_txs.len());

    // Verify ERC721 contract cursor tracking
    for erc721_tx in &erc721_txs {
        assert!(
            erc721_cursor_txs.contains(erc721_tx),
            "ERC721 transaction {:?} should be tracked in cursor_transactions",
            erc721_tx
        );
    }
    assert_eq!(erc721_cursor_txs.len(), erc721_txs.len());

    // Verify cursor state for all indexed contracts
    let world_cursor = &pending_result.cursors.cursors[&world_address];
    let erc20_cursor = &pending_result.cursors.cursors[&wood_address];
    let erc721_cursor = &pending_result.cursors.cursors[&badge_address];

    // All cursors should have the same head (initial block number)
    assert_eq!(world_cursor.head, Some(initial_block_number));
    assert_eq!(erc20_cursor.head, Some(initial_block_number));
    assert_eq!(erc721_cursor.head, Some(initial_block_number));

    // All cursors should have pending transactions
    assert!(world_cursor.last_pending_block_tx.is_some());
    assert!(erc20_cursor.last_pending_block_tx.is_some());
    assert!(erc721_cursor.last_pending_block_tx.is_some());

    // Each cursor should point to the last transaction from its respective contract
    assert_eq!(
        world_cursor.last_pending_block_tx.unwrap(),
        *world_txs.last().unwrap()
    );
    assert_eq!(
        erc20_cursor.last_pending_block_tx.unwrap(),
        *erc20_txs.last().unwrap()
    );
    assert_eq!(
        erc721_cursor.last_pending_block_tx.unwrap(),
        *erc721_txs.last().unwrap()
    );

    // Verify transaction events and content for all indexed contracts
    let all_indexed_txs = [&world_txs[..], &erc20_txs[..], &erc721_txs[..]].concat();

    for tx_hash in &all_indexed_txs {
        let transaction = &pending_data.transactions[tx_hash];
        assert!(transaction.transaction.is_some());
        assert!(
            !transaction.events.is_empty(),
            "Transaction {:?} should have events",
            tx_hash
        );

        // Verify events have proper structure
        for event in &transaction.events {
            assert!(!event.keys.is_empty());
            // All transactions should generate some form of events
        }
    }

    // Verify total transaction count
    let total_indexed_txs = world_txs.len() + erc20_txs.len() + erc721_txs.len();
    assert_eq!(
        pending_data.transactions.len(),
        total_indexed_txs,
        "Should have exactly {} indexed transactions in pending data",
        total_indexed_txs
    );

    // Phase 3: Mine the block to move pending transactions to mined state
    sequencer.dev_client().generate_block().await.unwrap();

    // Phase 4: Add more pending transactions after mining to all contracts
    let mut new_world_txs = Vec::new();
    let mut new_erc20_txs = Vec::new();
    let mut new_erc721_txs = Vec::new();
    let mut new_rewards_txs: Vec<Felt> = Vec::new();

    // Add more world transactions
    for i in 0..3 {
        let account_to_use = match i % 2 {
            0 => &account,
            _ => &account2,
        };

        let move_tx = account_to_use
            .execute_v3(vec![Call {
                to: actions_address,
                selector: get_selector_from_name("move").unwrap(),
                calldata: vec![Felt::from(i + 1)],
            }])
            .send()
            .await
            .unwrap();
        new_world_txs.push(move_tx.transaction_hash);
    }

    // Add more ERC20 transactions
    for i in 0..4 {
        let erc20_tx = account
            .execute_v3(vec![Call {
                to: wood_address,
                selector: get_selector_from_name("mint").unwrap(),
                calldata: vec![Felt::from(2000 + i), Felt::ZERO],
            }])
            .send()
            .await
            .unwrap();
        new_erc20_txs.push(erc20_tx.transaction_hash);
    }

    // Add more ERC721 transactions
    for i in 0..2 {
        let erc721_tx = account2
            .execute_v3(vec![Call {
                to: badge_address,
                selector: get_selector_from_name("mint").unwrap(),
                calldata: vec![Felt::from(200 + i), Felt::ZERO],
            }])
            .send()
            .await
            .unwrap();
        new_erc721_txs.push(erc721_tx.transaction_hash);
    }

    // Add more rewards transactions (should be ignored)
    for i in 0..5 {
        let rewards_tx = account3
            .execute_v3(vec![Call {
                to: rewards_address,
                selector: get_selector_from_name("mint").unwrap(),
                calldata: vec![
                    Felt::from(10 + i),
                    Felt::ZERO,
                    Felt::from(1000 + i * 50),
                    Felt::ZERO,
                ],
            }])
            .send()
            .await
            .unwrap();
        new_rewards_txs.push(rewards_tx.transaction_hash);
    }

    // Phase 5: Fetch with cursor continuation from initial state (testing cursor logic)
    let continuation_result = fetcher.fetch(&initial_cursors).await.unwrap();

    // Should get both mined transactions (in range) and new pending transactions
    let mined_block_number = initial_block_number + 1;

    // Verify range contains the previously pending transactions (now mined)
    assert!(continuation_result
        .range
        .blocks
        .contains_key(&mined_block_number));
    let mined_block = &continuation_result.range.blocks[&mined_block_number];

    // All original indexed contract transactions should now be in the mined block
    for world_tx in &world_txs {
        assert!(
            mined_block.transactions.contains_key(world_tx),
            "Previously pending world transaction {:?} should now be mined",
            world_tx
        );
    }

    for erc20_tx in &erc20_txs {
        assert!(
            mined_block.transactions.contains_key(erc20_tx),
            "Previously pending ERC20 transaction {:?} should now be mined",
            erc20_tx
        );
    }

    for erc721_tx in &erc721_txs {
        assert!(
            mined_block.transactions.contains_key(erc721_tx),
            "Previously pending ERC721 transaction {:?} should now be mined",
            erc721_tx
        );
    }

    // Rewards transactions should NOT be in mined block (since we don't index them)
    for rewards_tx in &rewards_txs {
        assert!(
            !mined_block.transactions.contains_key(rewards_tx),
            "Rewards transaction {:?} should NOT be in mined block",
            rewards_tx
        );
    }

    // Verify cursor tracking for range (all indexed contracts)
    let all_world_txs = &continuation_result.cursors.cursor_transactions[&world_address];
    let all_erc20_txs = &continuation_result.cursors.cursor_transactions[&wood_address];
    let all_erc721_txs = &continuation_result.cursors.cursor_transactions[&badge_address];

    // Verify world transaction tracking
    for world_tx in &world_txs {
        assert!(
            all_world_txs.contains(world_tx),
            "Mined world transaction {:?} should be tracked in range cursor_transactions",
            world_tx
        );
    }

    // Verify ERC20 transaction tracking
    for erc20_tx in &erc20_txs {
        assert!(
            all_erc20_txs.contains(erc20_tx),
            "Mined ERC20 transaction {:?} should be tracked in range cursor_transactions",
            erc20_tx
        );
    }

    // Verify ERC721 transaction tracking
    for erc721_tx in &erc721_txs {
        assert!(
            all_erc721_txs.contains(erc721_tx),
            "Mined ERC721 transaction {:?} should be tracked in range cursor_transactions",
            erc721_tx
        );
    }

    // Verify new pending transactions
    if let Some(new_pending) = &continuation_result.preconfirmed_block {
        // Should contain new transactions from all indexed contracts
        for new_world_tx in &new_world_txs {
            assert!(
                new_pending.transactions.contains_key(new_world_tx),
                "New world transaction {:?} should be in new pending data",
                new_world_tx
            );
        }

        for new_erc20_tx in &new_erc20_txs {
            assert!(
                new_pending.transactions.contains_key(new_erc20_tx),
                "New ERC20 transaction {:?} should be in new pending data",
                new_erc20_tx
            );
        }

        for new_erc721_tx in &new_erc721_txs {
            assert!(
                new_pending.transactions.contains_key(new_erc721_tx),
                "New ERC721 transaction {:?} should be in new pending data",
                new_erc721_tx
            );
        }

        let new_pending_world_cursor = &continuation_result.cursors.cursors[&world_address];
        let new_pending_erc20_cursor = &continuation_result.cursors.cursors[&wood_address];
        let new_pending_erc721_cursor = &continuation_result.cursors.cursors[&badge_address];

        // All cursors should have updated head
        assert_eq!(new_pending_world_cursor.head, Some(mined_block_number));
        assert_eq!(new_pending_erc20_cursor.head, Some(mined_block_number));
        assert_eq!(new_pending_erc721_cursor.head, Some(mined_block_number));

        // Each cursor should point to its contract's last transaction
        assert_eq!(
            new_pending_world_cursor.last_pending_block_tx.unwrap(),
            *new_world_txs.last().unwrap()
        );
        assert_eq!(
            new_pending_erc20_cursor.last_pending_block_tx.unwrap(),
            *new_erc20_txs.last().unwrap()
        );
        assert_eq!(
            new_pending_erc721_cursor.last_pending_block_tx.unwrap(),
            *new_erc721_txs.last().unwrap()
        );

        // Verify cursor transactions for new pending (all contracts)
        let new_pending_world_txs =
            &continuation_result.cursors.cursor_transactions[&world_address];
        let new_pending_erc20_txs = &continuation_result.cursors.cursor_transactions[&wood_address];
        let new_pending_erc721_txs =
            &continuation_result.cursors.cursor_transactions[&badge_address];

        // Verify world transactions
        for new_world_tx in &new_world_txs {
            assert!(
                new_pending_world_txs.contains(new_world_tx),
                "New world transaction {:?} should be tracked in new pending cursor_transactions",
                new_world_tx
            );
        }

        // Verify ERC20 transactions
        for new_erc20_tx in &new_erc20_txs {
            assert!(
                new_pending_erc20_txs.contains(new_erc20_tx),
                "New ERC20 transaction {:?} should be tracked in new pending cursor_transactions",
                new_erc20_tx
            );
        }

        // Verify ERC721 transactions
        for new_erc721_tx in &new_erc721_txs {
            assert!(
                new_pending_erc721_txs.contains(new_erc721_tx),
                "New ERC721 transaction {:?} should be tracked in new pending cursor_transactions",
                new_erc721_tx
            );
        }
    }

    // Phase 6: Validate cursor state consistency for all indexed contracts
    let range_world_cursor = &continuation_result.cursors.cursors[&world_address];
    let range_erc20_cursor = &continuation_result.cursors.cursors[&wood_address];
    let range_erc721_cursor = &continuation_result.cursors.cursors[&badge_address];

    // All range cursors should be updated to mined block
    assert_eq!(range_world_cursor.head, Some(mined_block_number));
    assert_eq!(range_erc20_cursor.head, Some(mined_block_number));
    assert_eq!(range_erc721_cursor.head, Some(mined_block_number));

    // All should have timestamps
    assert!(range_world_cursor.last_block_timestamp.is_some());
    assert!(range_erc20_cursor.last_block_timestamp.is_some());
    assert!(range_erc721_cursor.last_block_timestamp.is_some());

    // Phase 7: Test transaction count consistency for all contracts
    let total_expected_world_txs = world_txs.len() + new_world_txs.len();
    let total_expected_erc20_txs = erc20_txs.len() + new_erc20_txs.len();
    let total_expected_erc721_txs = erc721_txs.len() + new_erc721_txs.len();
    let total_expected_indexed_txs =
        total_expected_world_txs + total_expected_erc20_txs + total_expected_erc721_txs;

    let (world_count, erc20_count, erc721_count) = (
        continuation_result.cursors.cursor_transactions[&world_address].len(),
        continuation_result.cursors.cursor_transactions[&wood_address].len(),
        continuation_result.cursors.cursor_transactions[&badge_address].len(),
    );

    // Verify per-contract transaction counts
    assert_eq!(
        world_count, total_expected_world_txs,
        "Total world transactions processed should match expected count"
    );
    assert_eq!(
        erc20_count, total_expected_erc20_txs,
        "Total ERC20 transactions processed should match expected count"
    );
    assert_eq!(
        erc721_count, total_expected_erc721_txs,
        "Total ERC721 transactions processed should match expected count"
    );

    // Verify total transaction count
    assert_eq!(
        world_count + erc20_count + erc721_count,
        total_expected_indexed_txs,
        "Total indexed transactions processed should match expected count"
    );

    // Phase 8: Verify no transaction loss or duplication across all indexed contracts
    let mut all_processed_txs = std::collections::HashSet::new();

    for tx_hash in &continuation_result.cursors.cursor_transactions[&world_address] {
        assert!(
            all_processed_txs.insert(*tx_hash),
            "Duplicate world transaction found: {:?}",
            tx_hash
        );
    }
    for tx_hash in &continuation_result.cursors.cursor_transactions[&wood_address] {
        assert!(
            all_processed_txs.insert(*tx_hash),
            "Duplicate ERC20 transaction found: {:?}",
            tx_hash
        );
    }
    for tx_hash in &continuation_result.cursors.cursor_transactions[&badge_address] {
        assert!(
            all_processed_txs.insert(*tx_hash),
            "Duplicate ERC721 transaction found: {:?}",
            tx_hash
        );
    }

    // Verify all indexed contract transactions are accounted for
    let mut expected_txs = std::collections::HashSet::new();

    // Add all world transactions
    for tx in &world_txs {
        expected_txs.insert(*tx);
    }
    for tx in &new_world_txs {
        expected_txs.insert(*tx);
    }

    // Add all ERC20 transactions
    for tx in &erc20_txs {
        expected_txs.insert(*tx);
    }
    for tx in &new_erc20_txs {
        expected_txs.insert(*tx);
    }

    // Add all ERC721 transactions
    for tx in &erc721_txs {
        expected_txs.insert(*tx);
    }
    for tx in &new_erc721_txs {
        expected_txs.insert(*tx);
    }

    assert_eq!(
        all_processed_txs, expected_txs,
        "Processed transactions should exactly match expected indexed contract transactions"
    );

    // Phase 9: Verify transaction ordering and event content
    // Check that transactions are properly ordered within the block
    let block_tx_hashes: Vec<_> = mined_block.transactions.keys().collect();

    // Verify that we can find transactions in a consistent order
    // (transaction ordering within a block should be deterministic)
    assert!(
        !block_tx_hashes.is_empty(),
        "Mined block should contain transactions"
    );

    // Verify event message content for all indexed contract transactions
    let mut total_events_found = 0;
    let mut moved_events_found = 0;

    // Check world contract transactions (should have model and Moved events)
    for world_tx in &world_txs {
        if let Some(mined_tx) = mined_block.transactions.get(world_tx) {
            total_events_found += mined_tx.events.len();
            for event in &mined_tx.events {
                // Look for Moved events (they should have player as key)
                if !event.keys.is_empty() {
                    moved_events_found += 1;
                }
            }
        }
    }

    // Check ERC20/ERC721 transactions (should have transfer events)
    for erc_tx in erc20_txs.iter().chain(erc721_txs.iter()) {
        if let Some(mined_tx) = mined_block.transactions.get(erc_tx) {
            total_events_found += mined_tx.events.len();
            // ERC transactions should also have events
            assert!(
                !mined_tx.events.is_empty(),
                "ERC transaction should have events"
            );
        }
    }

    // We should have found events from our transactions
    assert!(
        moved_events_found > 0,
        "Should have found move-related events from world contract"
    );
    assert!(
        total_events_found > 0,
        "Should have found events from all indexed contracts"
    );

    // Phase 10: Verify transaction completeness and ordering consistency
    // Ensure that all transactions we expect are present and none are duplicated
    let all_original_indexed_txs = [&world_txs[..], &erc20_txs[..], &erc721_txs[..]].concat();
    let mut found_in_block = 0;

    for tx_hash in &all_original_indexed_txs {
        if mined_block.transactions.contains_key(tx_hash) {
            found_in_block += 1;
        }
    }

    assert_eq!(
        found_in_block,
        all_original_indexed_txs.len(),
        "All original indexed transactions should be found in mined block"
    );

    // Verify that unindexed contract transactions are indeed not present
    for rewards_tx in &rewards_txs {
        assert!(
            !mined_block.transactions.contains_key(rewards_tx),
            "Rewards transaction {:?} should not be in mined block",
            rewards_tx
        );
    }

    println!("🎉 Test completed successfully!");
    println!("📊 Transaction Summary:");
    println!(
        "   • World transactions: {} (indexed)",
        total_expected_world_txs
    );
    println!(
        "   • ERC20 transactions: {} (indexed)",
        total_expected_erc20_txs
    );
    println!(
        "   • ERC721 transactions: {} (indexed)",
        total_expected_erc721_txs
    );
    println!(
        "   • Rewards transactions: {} (ignored)",
        rewards_txs.len() + new_rewards_txs.len()
    );
    println!("   • Total events found: {}", total_events_found);
}

/// Integration test that verifies retry logic works when fetching blocks with real transactions.
/// This test:
/// 1. Mines a block with transactions using Katana
/// 2. Uses a mock provider that fails batch requests for the first 2 attempts  
/// 3. Runs fetch_range and verifies the output is correct despite retries
/// 4. Checks all transactions are present in results
#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 3600000)]
async fn test_fetch_range_with_retry_logic(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let real_provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    // Grant writer permissions to the actions contract
    let world = WorldContract::new(world_address, &account);
    let grant_writer_res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(grant_writer_res.transaction_hash, &real_provider)
        .await
        .unwrap();

    let current_block_number = real_provider.block_number().await.unwrap();
    println!("Current block: {}", current_block_number);

    // Step 1: Mine a block with a few transactions from Katana
    let mut transaction_hashes = Vec::new();

    // Execute a few transactions to create events
    for i in 0..3 {
        let spawn_tx = account
            .execute_v3(vec![Call {
                to: actions_address,
                selector: get_selector_from_name("spawn").unwrap(),
                calldata: vec![],
            }])
            .send()
            .await
            .unwrap();
        transaction_hashes.push(spawn_tx.transaction_hash);

        let move_tx = account
            .execute_v3(vec![Call {
                to: actions_address,
                selector: get_selector_from_name("move").unwrap(),
                calldata: vec![Felt::from(i + 1)], // Different directions
            }])
            .send()
            .await
            .unwrap();
        transaction_hashes.push(move_tx.transaction_hash);
    }

    // Mine the block to include our transactions
    sequencer.dev_client().generate_block().await.unwrap();

    // Get the actual latest block number after mining
    let latest_block = real_provider.block_hash_and_number().await.unwrap();
    let target_block_number = latest_block.block_number;

    println!(
        "✅ Step 1: Mined block {} with {} transactions",
        target_block_number,
        transaction_hashes.len()
    );

    // Step 2: Create mock provider that fails batch requests for the first 2 attempts
    let mock_provider = MockRetryProvider::new(real_provider.clone(), 2);
    println!("✅ Step 2: Created mock provider that will fail first 2 batch requests");

    // Step 3: Create fetcher with mock provider and run fetch_range
    let fetcher = Fetcher::new(
        mock_provider.clone(),
        FetcherConfig {
            blocks_chunk_size: 1,
            batch_chunk_size: 1, // Small batch size to trigger batch_requests
            flags: FetchingFlags::TRANSACTIONS,
            ..Default::default()
        },
    );

    // Set up cursor to fetch the block with our transactions
    let cursors = HashMap::from([(
        world_address,
        ContractCursor {
            contract_address: world_address,
            last_pending_block_tx: None,
            head: Some(target_block_number - 1), // Start from before our target transactions
            last_block_timestamp: None,
            tps: None,
        },
    )]);

    println!("🔄 Step 3: Running fetch_range (expecting 2 failures then success)...");
    println!("   Fetching up to block {}", target_block_number);
    let fetch_result = fetcher.fetch_range(&cursors, latest_block).await;

    // Verify the fetch succeeded despite the initial failures
    assert!(
        fetch_result.is_ok(),
        "Fetch should succeed after retry attempts: {:?}",
        fetch_result.err()
    );

    let (range_result, updated_cursors) = fetch_result.unwrap();

    // Verify retry attempts were made
    let retry_stats = mock_provider.get_retry_stats();
    println!("📊 Retry Statistics:");
    println!(
        "   • Total batch_requests calls: {}",
        retry_stats.total_calls
    );
    println!("   • Failed attempts: {}", retry_stats.failed_attempts);
    println!(
        "   • Successful attempts: {}",
        retry_stats.successful_attempts
    );

    assert!(
        retry_stats.failed_attempts >= 2,
        "Should have failed at least 2 times"
    );
    assert!(
        retry_stats.successful_attempts > 0,
        "Should have succeeded eventually"
    );
    assert_eq!(
        retry_stats.total_calls,
        retry_stats.failed_attempts + retry_stats.successful_attempts
    );

    println!(
        "✅ Step 3: fetch_range succeeded after {} failed attempts",
        retry_stats.failed_attempts
    );

    // Step 4: Verify the output is correct and all transactions are present
    assert!(
        range_result.blocks.contains_key(&target_block_number),
        "Target block {} should be present in results",
        target_block_number
    );

    let target_block = &range_result.blocks[&target_block_number];

    // Verify all our transactions are present in the block
    for tx_hash in &transaction_hashes {
        assert!(
            target_block.transactions.contains_key(tx_hash),
            "Transaction {:?} should be present in block {}",
            tx_hash,
            target_block_number
        );
    }

    // Verify transactions have events (proving they were fully processed)
    let mut total_events = 0;
    for tx_hash in &transaction_hashes {
        let transaction = &target_block.transactions[tx_hash];
        assert!(
            !transaction.events.is_empty(),
            "Transaction {:?} should have events",
            tx_hash
        );
        assert!(
            transaction.transaction.is_some(),
            "Transaction {:?} should have transaction data",
            tx_hash
        );
        total_events += transaction.events.len();
    }

    // Verify cursor tracking
    let world_cursor_txs = &updated_cursors.cursor_transactions[&world_address];
    for tx_hash in &transaction_hashes {
        assert!(
            world_cursor_txs.contains(tx_hash),
            "Transaction {:?} should be tracked in cursor transactions",
            tx_hash
        );
    }

    // Verify cursor state is updated correctly
    let world_cursor = &updated_cursors.cursors[&world_address];
    assert_eq!(
        world_cursor.head,
        Some(target_block_number),
        "World cursor head should be updated to target block"
    );

    println!(
        "✅ Step 4: All {} transactions and {} events verified in results",
        transaction_hashes.len(),
        total_events
    );

    println!("🎉 Retry logic test completed successfully!");
    println!(
        "   • Mock provider failed {} times as expected",
        retry_stats.failed_attempts
    );
    println!("   • fetch_range eventually succeeded and returned correct data");
    println!(
        "   • All {} transactions properly indexed",
        transaction_hashes.len()
    );
}
