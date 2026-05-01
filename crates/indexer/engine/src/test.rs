use std::hash::{DefaultHasher, Hash, Hasher};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cainome::cairo_serde::{ByteArray, CairoSerde, ContractAddress};
use dojo_test_utils::migration::copy_spawn_and_move_db;
use dojo_test_utils::setup::TestSetup;
use dojo_types::primitive::Primitive;
use dojo_types::schema::{Member, Struct, Ty};
use dojo_utils::{TransactionExt, TransactionWaiter, TxnConfig};
use dojo_world::contracts::abigen::model::Layout;
use dojo_world::contracts::naming::{compute_bytearray_hash, compute_selector_from_names};
use dojo_world::contracts::world::WorldContract;
use katana_runner::RunnerCtx;
use num_traits::ToPrimitive;
use scarb_interop::Profile;
use scarb_metadata_ext::MetadataDojoExt;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Row;
use starknet::accounts::Account;
use starknet::core::types::{BlockId, BlockTag, Call, Event, Felt, FunctionCall, U256};
use starknet::core::utils::get_selector_from_name;
use starknet::macros::selector;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider, Url};
use starknet_crypto::poseidon_hash_many;
use tempfile::NamedTempFile;
use tokio::sync::broadcast;
use torii_cache::{Cache, InMemoryCache, ReadOnlyCache};
use torii_sqlite::executor::Executor;
use torii_sqlite::types::Token;
use torii_sqlite::utils::{felt_and_u256_to_sql_string, felt_to_sql_string, u256_to_sql_string};
use torii_sqlite::Sql;
use torii_storage::proto::{ContractDefinition, ContractType};
use torii_storage::utils::format_world_scoped_id;
use torii_storage::Storage;

use crate::engine::{Engine, EngineConfig};
use torii_indexer_fetcher::{
    FetchRangeBlock, FetchRangeResult, FetchResult, FetchTransaction, Fetcher, FetcherConfig,
};
use torii_processors::error::Error as ProcessorError;
use torii_processors::{
    EventProcessor, EventProcessorContext, Processors, Result as ProcessorResult,
};

pub async fn bootstrap_engine<P>(
    db: Sql,
    cache: Arc<dyn Cache>,
    provider: P,
    contracts: &[ContractDefinition],
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
    );

    let cursors = contracts
        .iter()
        .map(|c| (c.address, Default::default()))
        .collect();

    let fetcher = Fetcher::new(Arc::new(provider), FetcherConfig::default());

    let data = fetcher.fetch(&cursors).await.unwrap();
    engine
        .process(
            &data,
            &contracts.iter().map(|c| (c.address, c.r#type)).collect(),
        )
        .await
        .unwrap();

    db.apply_balances_diff(
        cache.balances_diff().await,
        cache.total_supply_diff().await,
        cursors,
    )
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

    let contracts = vec![ContractDefinition {
        address: world_address,
        r#type: ContractType::WORLD,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

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
        format_world_scoped_id(
            &world_address,
            &compute_selector_from_names("ns", "Position")
        )
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
        format_world_scoped_id(&world_address, &compute_selector_from_names("ns", "Moves"))
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
        format_world_scoped_id(
            &world_address,
            &compute_selector_from_names("ns", "PlayerConfig")
        )
    );
    assert_eq!(name, "PlayerConfig");
    assert_eq!(namespace, "ns");
    assert_eq!(packed_size, 0);
    assert_eq!(unpacked_size, 0);

    assert_eq!(count_table("entities", &pool).await, 2);
    assert_eq!(count_table("event_messages", &pool).await, 2);

    let (id, keys): (String, String) = sqlx::query_as(
        format!(
            "SELECT id, keys FROM entities WHERE id = '{}'",
            format_world_scoped_id(&world_address, &poseidon_hash_many(&[account.address()]))
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        id,
        format_world_scoped_id(&world_address, &poseidon_hash_many(&[account.address()]))
    );
    assert_eq!(keys, format!("{}/", felt_to_sql_string(&account.address())));
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

    let contracts = vec![ContractDefinition {
        address: token_address,
        r#type: ContractType::ERC20,
        starting_block: None,
    }];

    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    // first check if we indexed the token
    let token = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * from tokens where contract_address = '{}'",
            felt_to_sql_string(&token_address)
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
            "SELECT balance FROM token_balances WHERE account_address = '{}' AND \
             contract_address = '{}'",
            felt_to_sql_string(&account.address()),
            felt_to_sql_string(&token_address)
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

    let contracts = vec![ContractDefinition {
        address: badge_address,
        r#type: ContractType::ERC721,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());
    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    // Check if we indexed all tokens
    let tokens = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * from tokens where contract_address = '{}' AND token_id IS NOT NULL ORDER BY token_id",
            felt_to_sql_string(&badge_address)
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
                "SELECT balance FROM token_balances WHERE account_address = '{}' AND \
                 contract_address = '{}' AND token_id = '{}'",
                felt_to_sql_string(&Felt::ONE),
                felt_to_sql_string(&badge_address),
                felt_and_u256_to_sql_string(&badge_address, &U256::from(token_id as u32))
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
                "SELECT balance FROM token_balances WHERE account_address = '{}' AND \
                 contract_address = '{}' AND token_id = '{}'",
                felt_to_sql_string(&account.address()),
                felt_to_sql_string(&badge_address),
                felt_and_u256_to_sql_string(&badge_address, &U256::from(token_id as u32))
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

    let contracts = vec![ContractDefinition {
        address: rewards_address,
        r#type: ContractType::ERC1155,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());
    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    // Check if we indexed all tokens
    let tokens = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * from tokens where contract_address = '{}' AND token_id IS NOT NULL ORDER BY token_id",
            felt_to_sql_string(&rewards_address)
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
                "SELECT balance FROM token_balances WHERE account_address = '{}' AND \
                 contract_address = '{}' AND token_id = '{}'",
                felt_to_sql_string(&other_account.address()),
                felt_to_sql_string(&rewards_address),
                felt_and_u256_to_sql_string(&rewards_address, &U256::from(token_id))
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
                "SELECT balance FROM token_balances WHERE account_address = '{}' AND \
                 contract_address = '{}' AND token_id = '{}'",
                felt_to_sql_string(&account.address()),
                felt_to_sql_string(&rewards_address),
                felt_and_u256_to_sql_string(&rewards_address, &U256::from(token_id))
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

    let contracts = vec![ContractDefinition {
        address: world_address,
        r#type: ContractType::WORLD,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    assert_eq!(count_table("ns-PlayerConfig", &pool).await, 0);
    assert_eq!(count_table("ns-Position", &pool).await, 0);
    assert_eq!(count_table("ns-Moves", &pool).await, 0);

    // our entity model relations should be deleted for our player entity
    let entity_model_count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM entity_model WHERE entity_id = '{}'",
            felt_to_sql_string(&poseidon_hash_many(&[account.address()]))
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
            "SELECT COUNT(*) FROM entities WHERE id = '{}'",
            format_world_scoped_id(&world_address, &poseidon_hash_many(&[account.address()]))
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

    let contracts = vec![ContractDefinition {
        address: world_address,
        r#type: ContractType::WORLD,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

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

    let contracts = vec![ContractDefinition {
        address: world_address,
        r#type: ContractType::WORLD,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    let name: String = sqlx::query_scalar(
        format!(
            "SELECT name FROM [ns-PlayerConfig] WHERE internal_id = '{}'",
            format_world_scoped_id(&world_address, &poseidon_hash_many(&[account.address()]))
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

    let contracts = vec![ContractDefinition {
        address: rewards_address,
        r#type: ContractType::ERC1155,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    let token = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * from tokens where contract_address = '{}' AND token_id IS NOT NULL ORDER BY token_id",
            felt_to_sql_string(&rewards_address)
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

    let contracts = vec![ContractDefinition {
        address: rewards_address,
        r#type: ContractType::ERC1155,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    let _ = bootstrap_engine(db.clone(), cache, provider, &contracts)
        .await
        .unwrap();

    // Check only the contract-level metadata (token_id = 0)
    let contract_token = sqlx::query_as::<_, Token>(
        format!(
            "SELECT * FROM tokens WHERE contract_address = '{}' AND token_id IS NULL",
            felt_to_sql_string(&rewards_address)
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

fn hashed_task_identifier(from_address: Felt, discriminator: Felt) -> u64 {
    let mut hasher = DefaultHasher::new();
    from_address.hash(&mut hasher);
    discriminator.hash(&mut hasher);
    hasher.finish()
}

fn schema_has_member(schema: &Ty, member_name: &str) -> bool {
    match schema {
        Ty::Struct(struct_ty) => struct_ty
            .children
            .iter()
            .any(|member| member.name == member_name),
        _ => false,
    }
}

async fn table_has_column(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    table_name: &str,
    column_name: &str,
) -> bool {
    let query = format!("PRAGMA table_info([{table_name}])");
    let rows = sqlx::query(&query).fetch_all(pool).await.unwrap();

    rows.into_iter()
        .any(|row| row.try_get::<String, _>("name").unwrap() == column_name)
}

#[derive(Debug)]
struct SyntheticModelUpgradeProcessor {
    selector: Felt,
    added_member: &'static str,
}

#[async_trait]
impl<P> EventProcessor<P> for SyntheticModelUpgradeProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "SyntheticModelUpgrade".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        event.keys.len() == 2 && event.keys[1] == self.selector
    }

    fn task_identifier(&self, event: &Event) -> u64 {
        hashed_task_identifier(event.from_address, event.keys[1])
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> ProcessorResult<()> {
        let current = ctx
            .storage
            .model_optional(ctx.contract_address, self.selector)
            .await?
            .expect("seeded model must exist");

        let mut upgraded_schema = current.schema.clone();
        let struct_ty = match &mut upgraded_schema {
            Ty::Struct(struct_ty) => struct_ty,
            _ => panic!("synthetic test expects a struct model"),
        };

        if struct_ty
            .children
            .iter()
            .all(|member| member.name != self.added_member)
        {
            struct_ty.children.push(Member {
                name: self.added_member.to_string(),
                ty: Ty::Primitive(Primitive::U32(None)),
                key: false,
            });
        }

        let Some(schema_diff) = upgraded_schema.diff(&current.schema) else {
            return Ok(());
        };
        let upgrade_diff = current.schema.diff(&upgraded_schema);
        let packed_size = current.packed_size.saturating_add(1);
        let unpacked_size = current.unpacked_size.saturating_add(1);

        ctx.storage
            .register_model(
                ctx.contract_address,
                self.selector,
                &upgraded_schema,
                &current.layout,
                current.class_hash,
                current.contract_address,
                packed_size,
                unpacked_size,
                ctx.block_timestamp,
                Some(&schema_diff),
                upgrade_diff.as_ref(),
                current.use_legacy_store,
            )
            .await?;

        ctx.cache
            .register_model(
                ctx.contract_address,
                self.selector,
                torii_storage::proto::Model {
                    world_address: ctx.contract_address,
                    namespace: current.namespace,
                    name: current.name,
                    selector: self.selector,
                    class_hash: current.class_hash,
                    contract_address: current.contract_address,
                    packed_size,
                    unpacked_size,
                    layout: current.layout,
                    schema: upgraded_schema,
                    use_legacy_store: current.use_legacy_store,
                },
            )
            .await;

        Ok(())
    }
}

#[derive(Debug)]
struct OneShotFailProcessor {
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl<P> EventProcessor<P> for OneShotFailProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "Fail".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        event.keys.len() == 3
    }

    fn task_identifier(&self, event: &Event) -> u64 {
        let mut hasher = DefaultHasher::new();
        event.from_address.hash(&mut hasher);
        let canonical_pair = std::cmp::max(event.keys[1], event.keys[2]);
        canonical_pair.hash(&mut hasher);
        hasher.finish()
    }

    async fn process(&self, _ctx: &EventProcessorContext<P>) -> ProcessorResult<()> {
        let attempt = self.invocations.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Err(ProcessorError::UriMalformed)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
struct OneShotModelFailProcessor {
    invocations: Arc<AtomicUsize>,
    selector: Felt,
}

#[async_trait]
impl<P> EventProcessor<P> for OneShotModelFailProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "SyntheticModelFail".to_string()
    }

    fn validate(&self, event: &Event) -> bool {
        event.keys.len() == 2 && event.keys[1] == self.selector
    }

    fn task_identifier(&self, event: &Event) -> u64 {
        hashed_task_identifier(event.from_address, event.keys[1])
    }

    async fn process(&self, _ctx: &EventProcessorContext<P>) -> ProcessorResult<()> {
        let attempt = self.invocations.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Err(ProcessorError::UriMalformed)
        } else {
            Ok(())
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rollback_replays_model_upgrade_after_cache_reset() {
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
        Url::parse("http://127.0.0.1:0").unwrap(),
    )));

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

    let world_address = Felt::from(0x111_u64);
    let model_selector = Felt::from(0x222_u64);
    let initial_schema = Ty::Struct(Struct {
        name: "ns-RollbackProbe".to_string(),
        children: vec![
            Member {
                name: "player".to_string(),
                ty: Ty::Primitive(Primitive::ContractAddress(None)),
                key: true,
            },
            Member {
                name: "score".to_string(),
                ty: Ty::Primitive(Primitive::U32(None)),
                key: false,
            },
        ],
    });
    let layout = Layout::Fixed(vec![]);

    let contracts = vec![ContractDefinition {
        address: world_address,
        r#type: ContractType::WORLD,
        starting_block: None,
    }];

    let db = Sql::new(pool.clone(), sender, &contracts).await.unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    db.register_model(
        world_address,
        model_selector,
        &initial_schema,
        &layout,
        Felt::from(0x333_u64),
        Felt::from(0x444_u64),
        1,
        2,
        1_715_000_000,
        None,
        None,
        true,
    )
    .await
    .unwrap();
    db.execute().await.unwrap();

    let table_name = initial_schema.name();
    let added_member = "rollback_probe";
    assert!(!table_has_column(&pool, &table_name, added_member).await);
    assert!(matches!(
        cache.model(world_address, model_selector).await,
        Err(torii_cache::error::Error::ModelNotFound(selector)) if selector == model_selector
    ));

    let fail_invocations = Arc::new(AtomicUsize::new(0));
    let mut processors = Processors::<Arc<JsonRpcClient<HttpTransport>>>::default();
    processors
        .event_processors
        .get_mut(&ContractType::WORLD)
        .unwrap()
        .entry(selector!("SyntheticModelUpgrade"))
        .or_default()
        .push(Box::new(SyntheticModelUpgradeProcessor {
            selector: model_selector,
            added_member,
        }));
    processors
        .event_processors
        .get_mut(&ContractType::WORLD)
        .unwrap()
        .entry(selector!("SyntheticModelFail"))
        .or_default()
        .push(Box::new(OneShotModelFailProcessor {
            invocations: fail_invocations.clone(),
            selector: model_selector,
        }));
    let processors = Arc::new(processors);

    let upgrade_event = Event {
        from_address: world_address,
        keys: vec![selector!("SyntheticModelUpgrade"), model_selector],
        data: vec![],
    };
    let fail_event = Event {
        from_address: world_address,
        keys: vec![selector!("SyntheticModelFail"), model_selector],
        data: vec![],
    };

    let tx_hash = Felt::from(0x999_u64);
    let block_number = 1_u64;
    let block_timestamp = 1_715_000_123_u64;
    let fetch_result = FetchResult {
        range: FetchRangeResult {
            blocks: std::collections::BTreeMap::from([(
                block_number,
                FetchRangeBlock {
                    block_hash: Some(Felt::from(0x1234_u64)),
                    timestamp: block_timestamp,
                    transactions: vec![(
                        tx_hash,
                        FetchTransaction {
                            transaction: None,
                            events: vec![upgrade_event.clone(), fail_event.clone()],
                            receipt: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            )]),
        },
        preconfirmed_block: None,
        cursors: torii_indexer_fetcher::Cursors {
            cursor_transactions: std::collections::HashMap::new(),
            cursors: std::collections::HashMap::from([(
                world_address,
                torii_storage::proto::ContractCursor {
                    contract_address: world_address,
                    head: Some(0),
                    last_block_timestamp: None,
                    last_pending_block_tx: None,
                },
            )]),
        },
    };
    let contract_types = std::collections::HashMap::from([(world_address, ContractType::WORLD)]);

    let mut engine = Engine::new(
        Arc::new(db.clone()),
        cache.clone(),
        provider.clone(),
        processors.clone(),
        EngineConfig::default(),
        shutdown_tx.clone(),
    );

    assert!(engine
        .process(&fetch_result, &contract_types)
        .await
        .is_err());

    let poisoned_model = cache.model(world_address, model_selector).await.unwrap();
    assert!(schema_has_member(&poisoned_model.schema, added_member));
    assert!(!table_has_column(&pool, &table_name, added_member).await);

    db.rollback().await.unwrap();
    cache.clear_balances_diff().await;
    cache.clear_models().await;
    cache.reset_token_registry().await.unwrap();

    assert!(matches!(
        cache.model(world_address, model_selector).await,
        Err(torii_cache::error::Error::ModelNotFound(selector)) if selector == model_selector
    ));

    let mut retry_engine = Engine::new(
        Arc::new(db.clone()),
        cache.clone(),
        provider,
        processors,
        EngineConfig::default(),
        shutdown_tx,
    );
    retry_engine
        .process(&fetch_result, &contract_types)
        .await
        .unwrap();
    db.execute().await.unwrap();

    assert!(table_has_column(&pool, &table_name, added_member).await);
    let upgraded_model = cache.model(world_address, model_selector).await.unwrap();
    assert!(schema_has_member(&upgraded_model.schema, added_member));
    assert_eq!(fail_invocations.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_rollback_resets_token_registry_for_retry(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();
    let token_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.tag == "ns-WoodToken")
        .unwrap()
        .address;

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

    let contracts = vec![ContractDefinition {
        address: token_address,
        r#type: ContractType::ERC20,
        starting_block: None,
    }];

    let db = Sql::new(pool.clone(), sender, &contracts).await.unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    let fail_invocations = Arc::new(AtomicUsize::new(0));
    let mut processors = Processors::<Arc<JsonRpcClient<HttpTransport>>>::default();
    processors
        .event_processors
        .get_mut(&ContractType::ERC20)
        .unwrap()
        .entry(selector!("Fail"))
        .or_default()
        .push(Box::new(OneShotFailProcessor {
            invocations: fail_invocations.clone(),
        }));
    let processors = Arc::new(processors);

    let transfer_event = Event {
        from_address: token_address,
        keys: vec![
            selector!("Transfer"),
            Felt::from(0xabc_u64),
            Felt::from(0xdef_u64),
        ],
        data: vec![Felt::from(12345_u64), Felt::ZERO],
    };
    let fail_event = Event {
        from_address: token_address,
        keys: vec![
            selector!("Fail"),
            Felt::from(0xabc_u64),
            Felt::from(0xdef_u64),
        ],
        data: vec![],
    };

    let tx_hash = Felt::from(0x999_u64);
    let block_number = 1_u64;
    let block_timestamp = 1_715_000_000_u64;
    let fetch_result = FetchResult {
        range: FetchRangeResult {
            blocks: std::collections::BTreeMap::from([(
                block_number,
                FetchRangeBlock {
                    block_hash: Some(Felt::from(0x1234_u64)),
                    timestamp: block_timestamp,
                    transactions: vec![(
                        tx_hash,
                        FetchTransaction {
                            transaction: None,
                            events: vec![transfer_event.clone(), fail_event.clone()],
                            receipt: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            )]),
        },
        preconfirmed_block: None,
        cursors: torii_indexer_fetcher::Cursors {
            cursor_transactions: std::collections::HashMap::new(),
            cursors: std::collections::HashMap::from([(
                token_address,
                torii_storage::proto::ContractCursor {
                    contract_address: token_address,
                    head: Some(0),
                    last_block_timestamp: None,
                    last_pending_block_tx: None,
                },
            )]),
        },
    };
    let contract_types = std::collections::HashMap::from([(token_address, ContractType::ERC20)]);

    let mut engine = Engine::new(
        Arc::new(db.clone()),
        cache.clone(),
        provider.clone(),
        processors.clone(),
        EngineConfig::default(),
        shutdown_tx.clone(),
    );

    assert!(engine
        .process(&fetch_result, &contract_types)
        .await
        .is_err());
    assert_eq!(cache.erc_cache.token_id_registry.len(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tokens WHERE contract_address = ?")
            .bind(felt_to_sql_string(&token_address))
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );

    db.rollback().await.unwrap();
    cache.clear_balances_diff().await;
    cache.clear_models().await;
    cache.reset_token_registry().await.unwrap();

    assert_eq!(cache.erc_cache.token_id_registry.len(), 0);

    let mut retry_engine = Engine::new(
        Arc::new(db.clone()),
        cache.clone(),
        provider,
        processors,
        EngineConfig::default(),
        shutdown_tx,
    );
    retry_engine
        .process(&fetch_result, &contract_types)
        .await
        .unwrap();
    db.execute().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tokens WHERE contract_address = ?")
            .bind(felt_to_sql_string(&token_address))
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(fail_invocations.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_erc20_total_supply_tracking(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let account2 = sequencer.account(1);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let world_address = world_local.deterministic_world_address().unwrap();

    // Get ERC20 token contract address
    let erc20_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.contract_name == "ERC20Token")
        .unwrap()
        .address;

    let world = WorldContract::new(world_address, &account);

    // Grant writer permission
    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(erc20_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint 1000 tokens to account 1
    let mint_amount = Felt::from(1000);
    let tx = &account
        .execute_v3(vec![Call {
            to: erc20_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![mint_amount, Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint 500 more tokens to account 2
    let mint_amount2 = Felt::from(500);
    let tx = &account2
        .execute_v3(vec![Call {
            to: erc20_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![mint_amount2, Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Burn 200 tokens from account 1
    let burn_amount = Felt::from(200);
    let tx = &account
        .execute_v3(vec![Call {
            to: erc20_address,
            selector: get_selector_from_name("burn").unwrap(),
            calldata: vec![burn_amount, Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Setup database and indexer
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

    let contracts = vec![ContractDefinition {
        address: erc20_address,
        r#type: ContractType::ERC20,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    let _ = bootstrap_engine(db.clone(), cache, provider.clone(), &contracts)
        .await
        .unwrap();

    // Give the indexer some time to process all events
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Fetch the actual total supply from the contract via RPC
    let total_supply_result = provider
        .call(
            FunctionCall {
                contract_address: erc20_address,
                entry_point_selector: get_selector_from_name("total_supply").unwrap(),
                calldata: vec![],
            },
            BlockId::Tag(BlockTag::Latest),
        )
        .await
        .unwrap();

    let expected_total_supply = U256::from_words(
        total_supply_result[0].try_into().unwrap(),
        total_supply_result[1].try_into().unwrap(),
    );

    let token: Token = sqlx::query_as(
        format!(
            "SELECT * FROM tokens WHERE contract_address = '{}' AND (token_id IS NULL OR token_id = '')",
            felt_to_sql_string(&erc20_address)
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        token.total_supply.unwrap(),
        u256_to_sql_string(&expected_total_supply)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_erc721_total_supply_tracking(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let account2 = sequencer.account(1);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let world_address = world_local.deterministic_world_address().unwrap();

    // Get ERC721 token contract address
    let erc721_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.contract_name == "ERC721Token")
        .unwrap()
        .address;

    let world = WorldContract::new(world_address, &account);

    // Grant writer permission
    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(erc721_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint NFT token ID 1
    let tx = &account
        .execute_v3(vec![Call {
            to: erc721_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(1), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint NFT token ID 2
    let tx = &account2
        .execute_v3(vec![Call {
            to: erc721_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(2), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint NFT token ID 3
    let tx = &account
        .execute_v3(vec![Call {
            to: erc721_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(3), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Burn NFT token ID 2
    let tx = &account2
        .execute_v3(vec![Call {
            to: erc721_address,
            selector: get_selector_from_name("burn").unwrap(),
            calldata: vec![
                Felt::from(2), // token_id
                Felt::ZERO,    // token_id high
            ],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Setup database and indexer
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

    let contracts = vec![ContractDefinition {
        address: erc721_address,
        r#type: ContractType::ERC721,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    let _ = bootstrap_engine(db.clone(), cache, provider.clone(), &contracts)
        .await
        .unwrap();

    // Give the indexer some time to process all events
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check contract-level total supply (should be 3 - represents unique token IDs registered, not circulating supply)
    // Contract-level total supply = number of unique token IDs that have been registered, regardless of burning
    // We track this ourselves since ERC721 doesn't have a total_supply entrypoint
    let contract_token: Token = sqlx::query_as(
        format!(
            "SELECT * FROM tokens WHERE contract_address = '{}' AND (token_id IS NULL OR token_id = '')",
            felt_to_sql_string(&erc721_address)
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        contract_token.total_supply.unwrap(),
        u256_to_sql_string(&U256::from(3u64)) // 3 unique token IDs registered (1, 2, 3)
    );

    // Check individual NFT token supplies for existing tokens (should each be 1)
    for token_id in [1, 3] {
        // Token ID 2 was burned
        let nft_token: Token = sqlx::query_as(
            format!(
                "SELECT * FROM tokens WHERE id = '{}'",
                felt_and_u256_to_sql_string(&erc721_address, &U256::from(token_id as u64))
            )
            .as_str(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            nft_token.total_supply.unwrap(),
            u256_to_sql_string(&U256::from(1u64))
        );
    }

    // Check that burned token ID 2 has supply 0
    let burned_token: Token = sqlx::query_as(
        format!(
            "SELECT * FROM tokens WHERE id = '{}'",
            felt_and_u256_to_sql_string(&erc721_address, &U256::from(2u64))
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        burned_token.total_supply.unwrap(),
        u256_to_sql_string(&U256::from(0u64))
    );
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_erc1155_total_supply_tracking(sequencer: &RunnerCtx) {
    let setup = TestSetup::from_examples("/tmp", "../../../examples/");
    let metadata = setup.load_metadata("spawn-and-move", Profile::DEV);

    let account = sequencer.account(0);
    let account2 = sequencer.account(1);
    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world_local = metadata.load_dojo_world_local().unwrap();
    let manifest = metadata.read_dojo_manifest_profile().unwrap().unwrap();

    let world_address = world_local.deterministic_world_address().unwrap();

    // Get ERC1155 token contract address
    let erc1155_address = manifest
        .external_contracts
        .iter()
        .find(|c| c.contract_name == "ERC1155Token")
        .unwrap()
        .address;

    let world = WorldContract::new(world_address, &account);

    // Grant writer permission
    let res = world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(erc1155_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();

    TransactionWaiter::new(res.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint 100 of token ID 1
    let tx = &account
        .execute_v3(vec![Call {
            to: erc1155_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(1), Felt::ZERO, Felt::from(100), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint 50 more of token ID 1 to account2
    let tx = &account2
        .execute_v3(vec![Call {
            to: erc1155_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(1), Felt::ZERO, Felt::from(50), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Mint 200 of token ID 2
    let tx = &account
        .execute_v3(vec![Call {
            to: erc1155_address,
            selector: get_selector_from_name("mint").unwrap(),
            calldata: vec![Felt::from(2), Felt::ZERO, Felt::from(200), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Burn 30 of token ID 1 from account
    let tx = &account
        .execute_v3(vec![Call {
            to: erc1155_address,
            selector: get_selector_from_name("burn").unwrap(),
            calldata: vec![Felt::from(1), Felt::ZERO, Felt::from(30), Felt::ZERO],
        }])
        .send()
        .await
        .unwrap();

    TransactionWaiter::new(tx.transaction_hash, &provider)
        .await
        .unwrap();

    // Setup database and indexer
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

    let contracts = vec![ContractDefinition {
        address: erc1155_address,
        r#type: ContractType::ERC1155,
        starting_block: None,
    }];
    let db = Sql::new(pool.clone(), sender.clone(), &contracts)
        .await
        .unwrap();
    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
    let db = db.with_cache(cache.clone());

    let _ = bootstrap_engine(db.clone(), cache, provider.clone(), &contracts)
        .await
        .unwrap();

    // Give the indexer some time to process all events
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Calculate expected supplies based on our transactions
    // Token ID 1: 100 + 50 - 30 = 120
    // Token ID 2: 200
    let expected_token1_supply = U256::from(120u64);
    let expected_token2_supply = U256::from(200u64);

    // Check contract-level total supply (count of unique token IDs registered)
    // Contract-level total supply = number of unique token IDs that have been registered, regardless of burning
    // We have 2 unique token IDs registered (1 and 2)
    let expected_contract_total = U256::from(2u64);

    let contract_token: Token = sqlx::query_as(
        format!(
            "SELECT * FROM tokens WHERE contract_address = '{}' AND (token_id IS NULL OR token_id = '')",
            felt_to_sql_string(&erc1155_address)
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        contract_token.total_supply.unwrap(),
        u256_to_sql_string(&expected_contract_total)
    );

    // Check token ID 1 total supply
    let token1: Token = sqlx::query_as(
        format!(
            "SELECT * FROM tokens WHERE id = '{}'",
            felt_and_u256_to_sql_string(&erc1155_address, &U256::from(1u64))
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        token1.total_supply.unwrap(),
        u256_to_sql_string(&expected_token1_supply)
    );

    // Check token ID 2 total supply
    let token2: Token = sqlx::query_as(
        format!(
            "SELECT * FROM tokens WHERE id = '{}'",
            felt_and_u256_to_sql_string(&erc1155_address, &U256::from(2u64))
        )
        .as_str(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        token2.total_supply.unwrap(),
        u256_to_sql_string(&expected_token2_supply)
    );
}
