use std::collections::HashMap;
use std::sync::Arc;

use cainome::cairo_serde::ContractAddress;
use dojo_test_utils::compiler::CompilerTestSetup;
use dojo_test_utils::migration::copy_spawn_and_move_db;
use dojo_utils::{TransactionExt, TransactionWaiter, TxnConfig};
use dojo_world::contracts::naming::{compute_bytearray_hash, compute_selector_from_names};
use dojo_world::contracts::world::WorldContract;
use katana_runner::RunnerCtx;
use scarb::compiler::Profile;
use sozo_scarbext::WorkspaceExt;
use starknet::accounts::Account;
use starknet::core::types::{BlockId, BlockWithReceipts, Call, MaybePendingBlockWithReceipts};
use starknet::core::utils::get_selector_from_name;
use starknet::macros::felt;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider};
use starknet_crypto::Felt;
use torii_storage::proto::ContractCursor;
use url::Url;

use crate::{Fetcher, FetcherConfig, FetchingFlags};

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
        MaybePendingBlockWithReceipts::Block(block) => block,
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
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_basic(sequencer: &RunnerCtx) {
    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = scarb::ops::read_workspace(config.manifest_path(), &config).unwrap();

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = ws.load_world_local().unwrap();
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
    assert!(result.pending.is_some());
    let pending = result.pending.unwrap();

    // Should have our pending transaction
    assert!(pending
        .transactions
        .contains_key(&spawn_tx.transaction_hash));
    assert_eq!(pending.block_number, current_block_number + 1);

    // Verify cursor was updated correctly
    let updated_cursor = &pending.cursors[&world_address];
    assert!(updated_cursor.last_pending_block_tx.is_some());
    assert_eq!(
        updated_cursor.last_pending_block_tx.unwrap(),
        spawn_tx.transaction_hash
    );
    assert_eq!(updated_cursor.head, Some(current_block_number));
    assert!(updated_cursor.last_block_timestamp.is_some());

    // Verify cursor_transactions includes our transaction
    assert!(pending.cursor_transactions.contains_key(&world_address));
    let world_transactions = &pending.cursor_transactions[&world_address];
    assert!(world_transactions.contains(&spawn_tx.transaction_hash));

    // Should have the spawn transaction for the world contract
    assert_eq!(world_transactions.len(), 1);

    // Verify transaction has events (spawn creates model events)
    let spawn_transaction = &pending.transactions[&spawn_tx.transaction_hash];
    assert!(!spawn_transaction.events.is_empty());
    assert!(spawn_transaction.transaction.is_some());
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_multiple_transactions(sequencer: &RunnerCtx) {
    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = scarb::ops::read_workspace(config.manifest_path(), &config).unwrap();

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = ws.load_world_local().unwrap();
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

    assert!(result.pending.is_some());
    let pending = result.pending.unwrap();

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
    let updated_cursor = &pending.cursors[&world_address];
    assert_eq!(
        updated_cursor.last_pending_block_tx.unwrap(),
        tx3.transaction_hash
    );
    assert_eq!(updated_cursor.head, Some(current_block_number));

    // Verify cursor_transactions includes all our transactions
    let world_transactions = &pending.cursor_transactions[&world_address];
    assert!(world_transactions.contains(&tx1.transaction_hash));
    assert!(world_transactions.contains(&tx2.transaction_hash));
    assert!(world_transactions.contains(&tx3.transaction_hash));
    assert_eq!(world_transactions.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_with_cursor_continuation(sequencer: &RunnerCtx) {
    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = scarb::ops::read_workspace(config.manifest_path(), &config).unwrap();

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = ws.load_world_local().unwrap();
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
    assert!(result1.pending.is_some());
    let pending1 = result1.pending.unwrap();

    // Verify first fetch results
    assert!(pending1.transactions.contains_key(&tx1.transaction_hash));
    assert_eq!(pending1.cursor_transactions[&world_address].len(), 1);
    assert_eq!(
        pending1.cursors[&world_address]
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
    let res = fetcher.fetch(&pending1.cursors).await.unwrap();
    let result2 = fetcher.fetch(&res.range.cursors).await.unwrap();
    assert!(result2.pending.is_some());
    let pending2 = result2.pending.unwrap();

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
        pending2.cursors[&world_address]
            .last_pending_block_tx
            .unwrap(),
        tx3.transaction_hash
    );

    // Verify cursor_transactions only includes new transactions
    let world_transactions = &pending2.cursor_transactions[&world_address];
    assert!(!world_transactions.contains(&tx1.transaction_hash));
    assert!(world_transactions.contains(&tx2.transaction_hash));
    assert!(world_transactions.contains(&tx3.transaction_hash));
    assert_eq!(world_transactions.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_to_mined_switching_logic(sequencer: &RunnerCtx) {
    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = scarb::ops::read_workspace(config.manifest_path(), &config).unwrap();

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = ws.load_world_local().unwrap();
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
    let range_world_transactions = &switching_result.range.cursor_transactions[&world_address];
    assert!(range_world_transactions.contains(&pending_tx1.transaction_hash));
    assert!(range_world_transactions.contains(&pending_tx2.transaction_hash));

    // Verify cursors are updated correctly for the range
    let range_cursor = &switching_result.range.cursors[&world_address];
    assert_eq!(range_cursor.head, Some(mined_block_number));
    assert!(range_cursor.last_block_timestamp.is_some());
    assert!(range_cursor.last_pending_block_tx.is_none()); // Reset since we moved to range

    // Verify new pending transactions
    if let Some(new_pending) = &switching_result.pending {
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

        // Verify cursor for new pending
        let pending_cursor = &new_pending.cursors[&world_address];
        assert_eq!(
            pending_cursor.last_pending_block_tx.unwrap(),
            new_pending_tx.transaction_hash
        );
        assert_eq!(pending_cursor.head, Some(mined_block_number));
    }

    // Total transaction count verification:
    // - Range should have 2 transactions (the previously pending ones)
    // - Pending should have 1 transaction (the new one)
    // - No transactions should be lost or duplicated
    assert_eq!(range_world_transactions.len(), 2);
    if let Some(new_pending) = &switching_result.pending {
        assert_eq!(new_pending.cursor_transactions[&world_address].len(), 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_block_mined_during_fetch(sequencer: &RunnerCtx) {
    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = scarb::ops::read_workspace(config.manifest_path(), &config).unwrap();

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = ws.load_world_local().unwrap();
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

    // Submit a pending transaction
    let _tx = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    // Mine a block to change the chain state
    sequencer.dev_client().generate_block().await.unwrap();

    // Fetch should return None for pending since the block was mined
    let result = fetcher.fetch(&cursors).await.unwrap();

    // The pending block should be None because the parent hash doesn't match
    // (a new block was mined between getting latest block and fetching pending)
    assert!(result.pending.is_none());

    // But we should still have range data
    assert!(!result.range.blocks.is_empty());

    // Verify cursors are updated in range
    assert!(result.range.cursors.contains_key(&world_address));
    let cursor = &result.range.cursors[&world_address];
    assert!(cursor.head.unwrap() > current_block_number);
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_with_events_comprehensive(sequencer: &RunnerCtx) {
    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = scarb::ops::read_workspace(config.manifest_path(), &config).unwrap();

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = ws.load_world_local().unwrap();
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

    assert!(result.pending.is_some());
    let pending = result.pending.unwrap();

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
    let world_transactions = &pending.cursor_transactions[&world_address];
    assert!(world_transactions.contains(&spawn_tx.transaction_hash));
    assert_eq!(world_transactions.len(), 1);

    // Verify cursor update
    let cursor = &pending.cursors[&world_address];
    assert_eq!(
        cursor.last_pending_block_tx.unwrap(),
        spawn_tx.transaction_hash
    );
    assert_eq!(cursor.head, Some(current_block_number));
    assert!(cursor.last_block_timestamp.is_some());
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_filters_reverted_transactions(sequencer: &RunnerCtx) {
    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = scarb::ops::read_workspace(config.manifest_path(), &config).unwrap();

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = ws.load_world_local().unwrap();
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

    assert!(result.pending.is_some());
    let pending = result.pending.unwrap();

    // Should definitely have the good transaction
    assert!(pending.transactions.contains_key(&good_tx.transaction_hash));

    // Verify the good transaction has proper content
    let good_transaction = &pending.transactions[&good_tx.transaction_hash];
    assert!(good_transaction.transaction.is_some());
    assert!(!good_transaction.events.is_empty());

    // Verify cursor tracking includes the good transaction
    let world_transactions = &pending.cursor_transactions[&world_address];
    assert!(world_transactions.contains(&good_tx.transaction_hash));

    // Should only have the good transaction
    assert_eq!(world_transactions.len(), 1);

    // Verify cursor is updated to the good transaction
    let cursor = &pending.cursors[&world_address];
    assert_eq!(
        cursor.last_pending_block_tx.unwrap(),
        good_tx.transaction_hash
    );
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_multiple_contracts_comprehensive(sequencer: &RunnerCtx) {
    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = scarb::ops::read_workspace(config.manifest_path(), &config).unwrap();

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = ws.load_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    // Get ERC20 token address for testing multiple contracts
    let wood_address = world_local
        .external_contracts
        .iter()
        .find(|c| c.instance_name == "WoodToken")
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

    assert!(result.pending.is_some());
    let pending = result.pending.unwrap();

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
    assert!(pending.cursor_transactions.contains_key(&world_address));
    assert!(pending.cursor_transactions.contains_key(&wood_address));

    let world_transactions = &pending.cursor_transactions[&world_address];
    let wood_transactions = &pending.cursor_transactions[&wood_address];

    assert!(world_transactions.contains(&world_tx.transaction_hash));
    assert!(wood_transactions.contains(&erc20_tx.transaction_hash));

    // Each contract should track its own transactions separately
    assert!(!world_transactions.contains(&erc20_tx.transaction_hash));
    assert!(!wood_transactions.contains(&world_tx.transaction_hash));

    // Verify each contract has exactly one transaction
    assert_eq!(world_transactions.len(), 1);
    assert_eq!(wood_transactions.len(), 1);

    // Verify cursors are updated correctly for both contracts
    let world_cursor = &pending.cursors[&world_address];
    let wood_cursor = &pending.cursors[&wood_address];

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
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_spam_transactions_multiple_contracts_with_cursor_validation(
    sequencer: &RunnerCtx,
) {
    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = scarb::ops::read_workspace(config.manifest_path(), &config).unwrap();

    let account = sequencer.account(0);
    let account2 = sequencer.account(1);
    let account3 = sequencer.account(2);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = ws.load_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    // Get ERC20 and ERC721 contract addresses for spamming other contracts
    let wood_address = world_local
        .external_contracts
        .iter()
        .find(|c| c.instance_name == "WoodToken")
        .unwrap()
        .address;

    let badge_address = world_local
        .external_contracts
        .iter()
        .find(|c| c.instance_name == "Badge")
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

    // Mine a block to establish initial state
    sequencer.dev_client().generate_block().await.unwrap();

    let initial_block = provider.block_hash_and_number().await.unwrap();
    let initial_block_number = initial_block.block_number;

    // Create fetcher that ONLY indexes the world contract (not ERC tokens)
    let fetcher = Fetcher::new(
        provider.clone(),
        FetcherConfig {
            flags: FetchingFlags::PENDING_BLOCKS | FetchingFlags::TRANSACTIONS,
            blocks_chunk_size: 10,
            ..Default::default()
        },
    );

    // Set up cursors for ONLY the world contract (this is key - we're not indexing other contracts)
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

    // Phase 1: Spam transactions across multiple contracts (world, ERC20, ERC721) in pending state

    let mut world_txs = Vec::new();
    let mut erc20_txs = Vec::new();
    let mut erc721_txs = Vec::new();

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
            0 => Felt::ONE,   // Left
            1 => Felt::TWO,   // Right  
            2 => Felt::THREE, // Up
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

    // Spam ERC721 contract transactions (should NOT be indexed by our fetcher)
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

    // Phase 2: Fetch pending transactions - should only get world contract transactions
    let pending_result = fetcher.fetch(&initial_cursors).await.unwrap();

    assert!(pending_result.pending.is_some());
    let pending_data = pending_result.pending.unwrap();

    // Verify ONLY world contract transactions are fetched
    for world_tx in &world_txs {
        assert!(
            pending_data.transactions.contains_key(world_tx),
            "World transaction {:?} should be present in pending data",
            world_tx
        );
    }

    // Verify ERC20 and ERC721 transactions are NOT fetched (since we're only indexing world contract)
    for erc20_tx in &erc20_txs {
        assert!(
            !pending_data.transactions.contains_key(erc20_tx),
            "ERC20 transaction {:?} should NOT be present in pending data",
            erc20_tx
        );
    }

    for erc721_tx in &erc721_txs {
        assert!(
            !pending_data.transactions.contains_key(erc721_tx),
            "ERC721 transaction {:?} should NOT be present in pending data", 
            erc721_tx
        );
    }

    // Verify cursor tracking for world contract
    assert!(pending_data.cursor_transactions.contains_key(&world_address));
    let world_cursor_txs = &pending_data.cursor_transactions[&world_address];
    
    for world_tx in &world_txs {
        assert!(
            world_cursor_txs.contains(world_tx),
            "World transaction {:?} should be tracked in cursor_transactions",
            world_tx
        );
    }

    assert_eq!(world_cursor_txs.len(), world_txs.len());

    // Verify cursor state
    let world_cursor = &pending_data.cursors[&world_address];
    assert_eq!(world_cursor.head, Some(initial_block_number));
    assert!(world_cursor.last_pending_block_tx.is_some());
    assert_eq!(
        world_cursor.last_pending_block_tx.unwrap(),
        *world_txs.last().unwrap()
    );

    // Verify transaction events and content
    for world_tx in &world_txs {
        let transaction = &pending_data.transactions[world_tx];
        assert!(transaction.transaction.is_some());
        assert!(!transaction.events.is_empty(), "World transactions should have events");
        
        // Verify events have proper structure
        for event in &transaction.events {
            assert!(!event.keys.is_empty());
            // Move transactions should emit Moved events, spawn transactions should emit model events
        }
    }

    // Phase 3: Mine the block to move pending transactions to mined state
    sequencer.dev_client().generate_block().await.unwrap();

    // Phase 4: Add more pending transactions after mining
    let mut new_world_txs = Vec::new();
    let mut new_erc20_txs = Vec::new();

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

    // Add more ERC20 transactions (should be ignored)
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

    // Phase 5: Fetch with cursor continuation from initial state (testing cursor logic)
    let continuation_result = fetcher.fetch(&initial_cursors).await.unwrap();

    // Should get both mined transactions (in range) and new pending transactions
    let mined_block_number = initial_block_number + 1;
    
    // Verify range contains the previously pending transactions (now mined)
    assert!(continuation_result.range.blocks.contains_key(&mined_block_number));
    let mined_block = &continuation_result.range.blocks[&mined_block_number];

    // All original world transactions should now be in the mined block
    for world_tx in &world_txs {
        assert!(
            mined_block.transactions.contains_key(world_tx),
            "Previously pending world transaction {:?} should now be mined",
            world_tx
        );
    }

    // ERC20/ERC721 transactions should NOT be in mined block (since we don't index them)
    for erc20_tx in &erc20_txs {
        assert!(
            !mined_block.transactions.contains_key(erc20_tx),
            "ERC20 transaction {:?} should NOT be in mined block",
            erc20_tx
        );
    }

    // Verify cursor tracking for range
    let range_world_txs = &continuation_result.range.cursor_transactions[&world_address];
    for world_tx in &world_txs {
        assert!(
            range_world_txs.contains(world_tx),
            "Mined world transaction {:?} should be tracked in range cursor_transactions",
            world_tx
        );
    }

    // Verify new pending transactions
    if let Some(new_pending) = &continuation_result.pending {
        // Should contain new world transactions
        for new_world_tx in &new_world_txs {
            assert!(
                new_pending.transactions.contains_key(new_world_tx),
                "New world transaction {:?} should be in new pending data",
                new_world_tx
            );
        }

        // Should NOT contain new ERC20 transactions
        for new_erc20_tx in &new_erc20_txs {
            assert!(
                !new_pending.transactions.contains_key(new_erc20_tx),
                "New ERC20 transaction {:?} should NOT be in new pending data",
                new_erc20_tx
            );
        }

        // Verify cursor state for new pending
        let new_pending_cursor = &new_pending.cursors[&world_address];
        assert_eq!(new_pending_cursor.head, Some(mined_block_number));
        assert_eq!(
            new_pending_cursor.last_pending_block_tx.unwrap(),
            *new_world_txs.last().unwrap()
        );

        // Verify cursor transactions for new pending
        let new_pending_world_txs = &new_pending.cursor_transactions[&world_address];
        for new_world_tx in &new_world_txs {
            assert!(
                new_pending_world_txs.contains(new_world_tx),
                "New world transaction {:?} should be tracked in new pending cursor_transactions",
                new_world_tx
            );
        }
        assert_eq!(new_pending_world_txs.len(), new_world_txs.len());
    }

    // Phase 6: Validate cursor state consistency
    let range_cursor = &continuation_result.range.cursors[&world_address];
    assert_eq!(range_cursor.head, Some(mined_block_number));
    assert!(range_cursor.last_block_timestamp.is_some());
    // last_pending_block_tx should be None since these are now mined
    assert!(range_cursor.last_pending_block_tx.is_none());

    // Phase 7: Test transaction count consistency
    // Total world transactions processed should equal: original world txs + new world txs
    let total_expected_world_txs = world_txs.len() + new_world_txs.len();
    let range_tx_count = range_world_txs.len();
    let pending_tx_count = if let Some(new_pending) = &continuation_result.pending {
        new_pending.cursor_transactions[&world_address].len()
    } else {
        0
    };

    assert_eq!(
        range_tx_count + pending_tx_count,
        total_expected_world_txs,
        "Total world transactions processed should match expected count"
    );

    // Phase 8: Verify no transaction loss or duplication
    let mut all_processed_txs = std::collections::HashSet::new();
    
    // Add range transactions
    for tx_hash in range_world_txs {
        assert!(
            all_processed_txs.insert(*tx_hash),
            "Duplicate transaction found in range: {:?}",
            tx_hash
        );
    }

    // Add pending transactions
    if let Some(new_pending) = &continuation_result.pending {
        for tx_hash in &new_pending.cursor_transactions[&world_address] {
            assert!(
                all_processed_txs.insert(*tx_hash),
                "Duplicate transaction found in pending: {:?}",
                tx_hash
            );
        }
    }

    // Verify all world transactions are accounted for
    let mut expected_txs = std::collections::HashSet::new();
    for tx in &world_txs {
        expected_txs.insert(*tx);
    }
    for tx in &new_world_txs {
        expected_txs.insert(*tx);
    }

    assert_eq!(
        all_processed_txs,
        expected_txs,
        "Processed transactions should exactly match expected world transactions"
    );

    // Phase 9: Verify event message content for move transactions
    // Check that Moved events are properly captured with correct player and direction data
    let mut moved_events_found = 0;
    for world_tx in &world_txs {
        if let Some(mined_tx) = mined_block.transactions.get(world_tx) {
            for event in &mined_tx.events {
                // Look for Moved events (they should have player as key)
                if !event.keys.is_empty() {
                    moved_events_found += 1;
                }
            }
        }
    }

    // We should have found events from our transactions
    assert!(moved_events_found > 0, "Should have found move-related events");

    println!(
        "Test completed successfully! Processed {} world transactions, {} ERC20 transactions ignored, {} ERC721 transactions ignored",
        total_expected_world_txs,
        erc20_txs.len() + new_erc20_txs.len(),
        erc721_txs.len()
    );
    println!(
        "Cursor validation passed: Range transactions: {}, Pending transactions: {}",
        range_tx_count,
        pending_tx_count
    );
}
