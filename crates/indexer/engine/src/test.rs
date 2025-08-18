use std::str::FromStr;
use std::sync::Arc;

use cainome::cairo_serde::{ByteArray, CairoSerde, ContractAddress};
use dojo_test_utils::migration::copy_spawn_and_move_db;
use dojo_test_utils::setup::TestSetup;
use dojo_utils::{TransactionExt, TransactionWaiter, TxnConfig};
use dojo_world::contracts::naming::{compute_bytearray_hash, compute_selector_from_names};
use dojo_world::contracts::world::WorldContract;
use katana_runner::RunnerCtx;
use num_traits::ToPrimitive;
use scarb_interop::Profile;
use scarb_metadata_ext::MetadataDojoExt;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use starknet::accounts::Account;
use starknet::core::types::{Call, Felt, U256};
use starknet::core::utils::get_selector_from_name;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider};
use starknet_crypto::poseidon_hash_many;
use tempfile::NamedTempFile;
use tokio::sync::broadcast;
use torii_cache::{Cache, InMemoryCache};
use torii_sqlite::executor::Executor;
use torii_sqlite::types::Token;
use torii_sqlite::utils::u256_to_sql_string;
use torii_sqlite::Sql;
use torii_storage::proto::{Contract, ContractType};
use torii_storage::Storage;

use crate::engine::{Engine, EngineConfig};
use torii_indexer_fetcher::{Fetcher, FetcherConfig};
use torii_processors::processors::Processors;

pub async fn bootstrap_engine<P>(
    db: Sql,
    cache: Arc<dyn Cache>,
    provider: P,
    contracts: &[Contract],
) -> Result<Engine<P>, Box<dyn std::error::Error>>
where
    P: Provider + Send + Sync + core::fmt::Debug + Clone + 'static,
{
    let (shutdown_tx, _) = broadcast::channel(1);
    let mut engine = Engine::new(
        Arc::new(db.clone()),
        Arc::clone(&cache),
        provider.clone(),
        Arc::new(Processors {
            ..Processors::default()
        }),
        EngineConfig::default(),
        shutdown_tx,
        contracts,
    );

    let cursors = contracts
        .iter()
        .map(|c| (c.address, Default::default()))
        .collect();

    let fetcher = Fetcher::new(Arc::new(provider), FetcherConfig::default());

    let data = fetcher.fetch(&cursors).await.unwrap();
    engine.process(&data).await.unwrap();

    db.apply_balances_diff(cache.balances_diff().await, cursors)
        .await
        .unwrap();
    db.execute().await.unwrap();

    Ok(engine)
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_load_from_remote(sequencer: &RunnerCtx) {
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

    // spawn
    let tx = &account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // move
    let tx = &account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("move").unwrap(),
            calldata: vec![Felt::ONE],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let contracts = vec![Contract {
        address: world_address,
        r#type: ContractType::WORLD,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    let _block_timestamp = 1710754478_u64;
    let models = sqlx::query("SELECT * FROM models")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(models.len(), 10);

    let (id, name, namespace, packed_size, unpacked_size): (String, String, String, u8, u8) =
        sqlx::query_as(
            "SELECT id, name, namespace, packed_size, unpacked_size FROM models WHERE name = \
             'Position'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(
        id,
        format!("{:#x}", compute_selector_from_names("ns", "Position"))
    );
    assert_eq!(name, "Position");
    assert_eq!(namespace, "ns");
    assert_eq!(packed_size, 1);
    assert_eq!(unpacked_size, 2);

    let (id, name, namespace, packed_size, unpacked_size): (String, String, String, u8, u8) =
        sqlx::query_as(
            "SELECT id, name, namespace, packed_size, unpacked_size FROM models WHERE name = \
             'Moves'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(
        id,
        format!("{:#x}", compute_selector_from_names("ns", "Moves"))
    );
    assert_eq!(name, "Moves");
    assert_eq!(namespace, "ns");
    assert_eq!(packed_size, 0);
    assert_eq!(unpacked_size, 2);

    let (id, name, namespace, packed_size, unpacked_size): (String, String, String, u8, u8) =
        sqlx::query_as(
            "SELECT id, name, namespace, packed_size, unpacked_size FROM models WHERE name = \
             'PlayerConfig'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(
        id,
        format!("{:#x}", compute_selector_from_names("ns", "PlayerConfig"))
    );
    assert_eq!(name, "PlayerConfig");
    assert_eq!(namespace, "ns");
    assert_eq!(packed_size, 0);
    assert_eq!(unpacked_size, 0);

    assert_eq!(count_table("entities", &pool).await, 2);
    assert_eq!(count_table("event_messages", &pool).await, 2);

    let (id, keys): (String, String) = sqlx::query_as(
        format!(
            "SELECT id, keys FROM entities WHERE id = '{:#x}'",
            poseidon_hash_many(&[account.address()])
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        id,
        format!("{:#x}", poseidon_hash_many(&[account.address()]))
    );
    assert_eq!(keys, format!("{:#x}/", account.address()));
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_load_from_remote_erc20(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let token_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.tag == "ns-WoodToken")
        .unwrap()
        .address;

    let mut balance = U256::from(0u64);

    // mint 123456789 wei tokens
    let tx = &account
        .execute_v3(vec![Call {
            to: token_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(123456789), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();
    balance += U256::from(123456789u32);

    // transfer 12345 tokens to some other address
    let tx = &account
        .execute_v3(vec![Call {
            to: token_address,
            selector: get_selector_from_name("transfer").unwrap(),
            calldata: vec![Felt::ONE, Felt::from(12345), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();
    balance -= U256::from(12345u32);

    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let contracts = vec![Contract {
        address: token_address,
        r#type: ContractType::ERC20,
    }];

    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    // first check if we indexed the token
    let token = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * from tokens where contract_address = '{:#x}'",
            token_address
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(token.name, "Wood");
    assert_eq!(token.symbol, "WOOD");
    assert_eq!(token.decimals, 18);

    // check the balance
    let remote_balance = sqlx::query_scalar::<_, String>(
        format!(
            "SELECT balance FROM token_balances WHERE account_address = '{:#x}' AND \
             contract_address = '{:#x}'",
            account.address(),
            token_address
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let remote_balance = crypto_bigint::U256::from_be_hex(remote_balance.trim_start_matches("0x"));
    assert_eq!(balance, remote_balance.into());
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_load_from_remote_erc721(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let world_address = world_local.deterministic_world_address().unwrap();

    let badge_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.tag == "ns-Badge")
        .unwrap()
        .address;

    let world = WorldContract::new(world_address, &account);

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(badge_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint multiple NFTs with different IDs
    for token_id in 1..=5 {
        let tx = &account
            .execute_v3(vec![Call {
                to: badge_address,
                selector: get_selector_from_name("mint").unwrap(),
                calldata: vec![Felt::from(token_id), Felt::ZERO],
            }])
            .send()
            .await
            .unwrap();

        TransactionWaiter::new(tx.transaction_hash, &provider)
            .await
            .unwrap();
    }

    // Transfer NFT ID 1 and 2 to another address
    for token_id in 1..=2 {
        let tx = &account
            .execute_v3(vec![Call {
                to: badge_address,
                selector: get_selector_from_name("transfer_from").unwrap(),
                calldata: vec![
                    account.address(),
                    Felt::ONE,
                    Felt::from(token_id),
                    Felt::ZERO,
                ],
            }])
            .send()
            .await
            .unwrap();

        TransactionWaiter::new(tx.transaction_hash, &provider)
            .await
            .unwrap();
    }

    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let contracts = vec![Contract {
        address: badge_address,
        r#type: ContractType::ERC721,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    // Check if we indexed all tokens
    let tokens = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * from tokens where contract_address = '{:#x}' AND token_id IS NOT NULL ORDER BY token_id",
            badge_address
        )
        .as_str(),
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(tokens.len(), 5, "Should have indexed 5 different tokens");

    for (i, token) in tokens.iter().enumerate() {
        assert_eq!(token.name, "Badge");
        assert_eq!(token.symbol, "BDG");
        assert_eq!(token.decimals, 0);
        let token_id = crypto_bigint::U256::from_be_hex(token.token_id.trim_start_matches("0x"));
        assert_eq!(
            U256::from(token_id),
            U256::from(i.to_u32().unwrap() + 1),
            "Token IDs should be sequential"
        );
    }

    // Check balances for transferred tokens
    for token_id in 1..=2 {
        let balance = sqlx::query_scalar::<_, String>(
            format!(
                "SELECT balance FROM token_balances WHERE account_address = '{:#x}' AND \
                 contract_address = '{:#x}' AND token_id = '{:#x}:{}'",
                Felt::ONE,
                badge_address,
                badge_address,
                u256_to_sql_string(&U256::from(token_id as u32))
            )
            .as_str(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let balance = crypto_bigint::U256::from_be_hex(balance.trim_start_matches("0x"));
        assert_eq!(
            U256::from(balance),
            U256::from(1u8),
            "Sender should have balance of 1 for transferred tokens"
        );
    }

    // Check balances for non-transferred tokens
    for token_id in 3..=5 {
        let balance = sqlx::query_scalar::<_, String>(
            format!(
                "SELECT balance FROM token_balances WHERE account_address = '{:#x}' AND \
                 contract_address = '{:#x}' AND token_id = '{:#x}:{}'",
                account.address(),
                badge_address,
                badge_address,
                u256_to_sql_string(&U256::from(token_id as u32))
            )
            .as_str(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let balance = crypto_bigint::U256::from_be_hex(balance.trim_start_matches("0x"));
        assert_eq!(
            U256::from(balance),
            U256::from(1u8),
            "Original owner should have balance of 1 for non-transferred tokens"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_load_from_remote_erc1155(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let other_account = sequencer.account(1);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let world_address = world_local.deterministic_world_address().unwrap();

    let rewards_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.tag == "ns-Rewards")
        .unwrap()
        .address;

    let world = WorldContract::new(world_address, &account);

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(rewards_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint different amounts for different token IDs
    let token_amounts: Vec<(u32, u32)> = vec![
        (1, 100),  // Token ID 1, amount 100
        (2, 500),  // Token ID 2, amount 500
        (3, 1000), // Token ID 3, amount 1000
    ];

    for (token_id, amount) in &token_amounts {
        let tx = &account
            .execute_v3(vec![Call {
                to: rewards_address,
                selector: get_selector_from_name("mint").unwrap(),
                calldata: vec![
                    Felt::from(*token_id),
                    Felt::ZERO,
                    Felt::from(*amount),
                    Felt::ZERO,
                ],
            }])
            .send()
            .await
            .unwrap();

        TransactionWaiter::new(tx.transaction_hash, &provider)
            .await
            .unwrap();
    }

    // Transfer half of each token amount to another address
    for (token_id, amount) in &token_amounts {
        let tx = &account
            .execute_v3(vec![Call {
                to: rewards_address,
                selector: get_selector_from_name("transfer_from").unwrap(),
                calldata: vec![
                    account.address(),
                    other_account.address(),
                    Felt::from(*token_id),
                    Felt::ZERO,
                    Felt::from(amount / 2),
                    Felt::ZERO,
                ],
            }])
            .send()
            .await
            .unwrap();

        TransactionWaiter::new(tx.transaction_hash, &provider)
            .await
            .unwrap();
    }

    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let contracts = vec![Contract {
        address: rewards_address,
        r#type: ContractType::ERC1155,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    // Check if we indexed all tokens
    let tokens = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * from tokens where contract_address = '{:#x}' AND token_id IS NOT NULL ORDER BY token_id",
            rewards_address
        )
        .as_str(),
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        tokens.len(),
        token_amounts.len(),
        "Should have indexed all token types"
    );

    for token in &tokens {
        assert_eq!(token.name, "");
        assert_eq!(token.symbol, "");
        assert_eq!(token.decimals, 0);
    }

    // Check balances for all tokens
    for (token_id, original_amount) in token_amounts {
        // Check recipient balance
        let recipient_balance = sqlx::query_scalar::<_, String>(
            format!(
                "SELECT balance FROM token_balances WHERE account_address = '{:#x}' AND \
                 contract_address = '{:#x}' AND token_id = '{:#x}:{}'",
                other_account.address(),
                rewards_address,
                rewards_address,
                u256_to_sql_string(&U256::from(token_id))
            )
            .as_str(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let recipient_balance =
            crypto_bigint::U256::from_be_hex(recipient_balance.trim_start_matches("0x"));
        assert_eq!(
            U256::from(recipient_balance),
            U256::from(original_amount / 2),
            "Recipient should have half of original amount for token {}",
            token_id
        );

        // Check sender remaining balance
        let sender_balance = sqlx::query_scalar::<_, String>(
            format!(
                "SELECT balance FROM token_balances WHERE account_address = '{:#x}' AND \
                 contract_address = '{:#x}' AND token_id = '{:#x}:{}'",
                account.address(),
                rewards_address,
                rewards_address,
                u256_to_sql_string(&U256::from(token_id))
            )
            .as_str(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let sender_balance =
            crypto_bigint::U256::from_be_hex(sender_balance.trim_start_matches("0x"));
        assert_eq!(
            U256::from(sender_balance),
            U256::from(original_amount / 2),
            "Sender should have half of original amount for token {}",
            token_id
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_load_from_remote_del(sequencer: &RunnerCtx) {
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

    // spawn
    let res = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    // Set player config.
    let res = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("set_player_config").unwrap(),
            // Empty ByteArray.
            calldata: vec![Felt::ZERO, Felt::ZERO, Felt::ZERO],
        }])
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    let res = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("reset_player_config").unwrap(),
            calldata: vec![],
        }])
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let contracts = vec![Contract {
        address: world_address,
        r#type: ContractType::WORLD,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    assert_eq!(count_table("ns-PlayerConfig", &pool).await, 0);
    assert_eq!(count_table("ns-Position", &pool).await, 0);
    assert_eq!(count_table("ns-Moves", &pool).await, 0);

    // our entity model relations should be deleted for our player entity
    let entity_model_count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM entity_model WHERE entity_id = '{:#x}'",
            poseidon_hash_many(&[account.address()])
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(entity_model_count, 0);
    // our player entity should be deleted
    let entity_count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM entities WHERE id = '{:#x}'",
            poseidon_hash_many(&[account.address()])
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(entity_count, 0);

    // TODO: check how we can have a test that is more chronological with Torii re-syncing
    // to ensure we can test intermediate states.
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_update_with_set_record(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let world_local = metadata.load_dojo_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();
    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

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

    // Send spawn transaction
    let spawn_res = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(spawn_res.transaction_hash, &provider)
        .await
        .unwrap();

    // Send move transaction
    let move_res = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("move").unwrap(),
            calldata: vec![Felt::ZERO],
        }])
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(move_res.transaction_hash, &provider)
        .await
        .unwrap();

    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let (shutdown_tx, _) = broadcast::channel(1);

    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let contracts = vec![Contract {
        address: world_address,
        r#type: ContractType::WORLD,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();
}

#[ignore = "This test is being flaky and need to find why. Sometimes it fails, sometimes it passes."]
#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_load_from_remote_update(sequencer: &RunnerCtx) {
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

    // spawn
    let res = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("spawn").unwrap(),
            calldata: vec![],
        }])
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    // Set player config.
    let res = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("set_player_config").unwrap(),
            // Empty ByteArray.
            calldata: vec![Felt::ZERO, Felt::ZERO, Felt::ZERO],
        }])
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    let name = ByteArray::from_string("mimi").unwrap();
    let res = account
        .execute_v3(vec![Call {
            to: actions_address,
            selector: get_selector_from_name("update_player_config_name").unwrap(),
            calldata: ByteArray::cairo_serialize(&name),
        }])
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let contracts = vec![Contract {
        address: world_address,
        r#type: ContractType::WORLD,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    let name: String = sqlx::query_scalar(
        format!(
            "SELECT name FROM [ns-PlayerConfig] WHERE internal_id = '{:#x}'",
            poseidon_hash_many(&[account.address()])
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(name, "mimi");
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_update_token_metadata_erc4906(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let world_address = world_local.deterministic_world_address().unwrap();

    let rewards_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.tag == "ns-Rewards")
        .unwrap()
        .address;

    let world = WorldContract::new(world_address, &account);

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(rewards_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    let tx = &account
        .execute_v3(vec![Call {
            to: rewards_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(1), Felt::ZERO, Felt::from(1), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    let owner_account = sequencer.account(3);
    let tx = &owner_account
        .execute_v3(vec![Call {
            to: rewards_address,
            selector: get_selector_from_name("update_token_metadata").unwrap(),
            calldata: vec![Felt::from(1), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    let block_number = provider.block_number().await.unwrap();

    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let contracts = vec![Contract {
        address: rewards_address,
        r#type: ContractType::ERC1155,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    let token = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * from tokens where contract_address = '{:#x}' AND token_id IS NOT NULL ORDER BY token_id",
            rewards_address
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(token.metadata.contains(&format!(
        "https://api.dicebear.com/9.x/lorelei-neutral/png?seed={}",
        block_number + 1
    )));
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_erc7572_contract_uri_updated(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let world_address = world_local.deterministic_world_address().unwrap();

    let rewards_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.contract_name == "ERC1155Token")
        .unwrap()
        .address;

    let world = WorldContract::new(world_address, &account);

    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(rewards_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint a token first to ensure contract is registered
    let tx = &account
        .execute_v3(vec![Call {
            to: rewards_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(1), Felt::ZERO, Felt::from(100), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Update contract URI (ERC-7572)
    let owner_account = sequencer.account(3);
    let tx = &owner_account
        .execute_v3(vec![Call {
            to: rewards_address,
            selector: get_selector_from_name("update_contract_uri").unwrap(),
            calldata: vec![],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    let block_number = provider.block_number().await.unwrap();

    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let contracts = vec![Contract {
        address: rewards_address,
        r#type: ContractType::ERC1155,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    // Check only the contract-level metadata (token_id = 0)
    let contract_token = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * FROM tokens WHERE contract_address = '{:#x}' AND token_id IS NULL",
            rewards_address,
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Verify the contract metadata contains the new seed from the ContractURIUpdated transaction block
    assert!(contract_token.metadata.contains(&format!(
        "https://api.dicebear.com/9.x/shapes/png?seed={}",
        block_number + 1
    )));
}

/// Count the number of rows in a table.
///
/// # Arguments
/// * `table_name` - The name of the table to count the rows of.
/// * `pool` - The database pool.
///
/// # Returns
/// The number of rows in the table.
async fn count_table(table_name: &str, pool: &sqlx::Pool<sqlx::Sqlite>) -> i64 {
    let count_query = format!("SELECT COUNT(*) FROM [{}]", table_name);
    let count: (i64,) = sqlx::query_as(&count_query).fetch_one(pool).await.unwrap();

    count.0
}
