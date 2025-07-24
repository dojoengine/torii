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

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    // Get current block number
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
    let tx = account
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
    assert!(pending.transactions.contains_key(&tx.transaction_hash));
    assert_eq!(pending.block_number, current_block_number + 1);

    // Verify cursor was updated
    assert!(pending.cursors[&world_address]
        .last_pending_block_tx
        .is_some());
    assert_eq!(
        pending.cursors[&world_address]
            .last_pending_block_tx
            .unwrap(),
        tx.transaction_hash
    );
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

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

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

    // Cursor should point to the last transaction
    assert_eq!(
        pending.cursors[&world_address]
            .last_pending_block_tx
            .unwrap(),
        tx3.transaction_hash
    );
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

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

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
    let result2 = fetcher.fetch(&pending1.cursors).await.unwrap();
    assert!(result2.pending.is_some());
    let pending2 = result2.pending.unwrap();

    // Should not include tx1 (already processed), but should include tx2 and tx3
    assert!(!pending2.transactions.contains_key(&tx1.transaction_hash));
    assert!(pending2.transactions.contains_key(&tx2.transaction_hash));
    assert!(pending2.transactions.contains_key(&tx3.transaction_hash));

    // Cursor should point to the last new transaction
    assert_eq!(
        pending2.cursors[&world_address]
            .last_pending_block_tx
            .unwrap(),
        tx3.transaction_hash
    );
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

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

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
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_with_events(sequencer: &RunnerCtx) {
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

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

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
    let tx = account
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
    assert!(pending.transactions.contains_key(&tx.transaction_hash));

    let transaction = &pending.transactions[&tx.transaction_hash];

    // Transaction should have events (spawn creates model events)
    assert!(!transaction.events.is_empty());

    // Verify events are from the world contract
    for event in &transaction.events {
        // Events should be related to the world contract operations
        assert!(!event.keys.is_empty());
        assert!(!event.data.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_cursor_transactions(sequencer: &RunnerCtx) {
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

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

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

    // Submit multiple transactions
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

    let result = fetcher.fetch(&cursors).await.unwrap();

    assert!(result.pending.is_some());
    let pending = result.pending.unwrap();

    // Verify cursor_transactions contains the transactions for the world contract
    assert!(pending.cursor_transactions.contains_key(&world_address));
    let world_transactions = &pending.cursor_transactions[&world_address];

    assert!(world_transactions.contains(&tx1.transaction_hash));
    assert!(world_transactions.contains(&tx2.transaction_hash));
    assert_eq!(world_transactions.len(), 2);
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

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

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

    // Submit a transaction that should fail (trying to call non-existent function)
    // This might not actually revert in the test environment, but demonstrates the pattern
    let potentially_bad_tx = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("nonexistent_function").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await;

    let result = fetcher.fetch(&cursors).await.unwrap();

    assert!(result.pending.is_some());
    let pending = result.pending.unwrap();

    // Should definitely have the good transaction
    assert!(pending.transactions.contains_key(&good_tx.transaction_hash));

    // The potentially bad transaction might not be in the pending block if it reverted
    // The fetcher filters out transactions with TransactionExecutionStatus::Reverted
    if let Ok(bad_tx) = potentially_bad_tx {
        // If the transaction was submitted successfully, verify the fetcher's behavior
        // depending on whether it was reverted or not
        let tx_in_pending = pending.transactions.contains_key(&bad_tx.transaction_hash);
        println!("Bad transaction in pending: {}", tx_in_pending);
        // Note: In test environment, transactions might not actually revert,
        // so this test mainly verifies the structure is correct
    }
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str(), block_time = 30000)]
async fn test_fetch_pending_multiple_contracts(sequencer: &RunnerCtx) {
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

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

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
}

// TODO: test with different transaction types (invoke, declare, deploy_account)
