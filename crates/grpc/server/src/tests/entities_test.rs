use std::str::FromStr;
use std::sync::Arc;

use cainome::cairo_serde::ContractAddress;
use dojo_test_utils::compiler::CompilerTestSetup;
use dojo_test_utils::migration::copy_spawn_and_move_db;
use dojo_types::naming::compute_selector_from_names;
use dojo_utils::{TransactionExt, TransactionWaiter, TxnConfig};
use dojo_world::contracts::naming::compute_bytearray_hash;
use dojo_world::contracts::{WorldContract, WorldContractReader};
use katana_runner::RunnerCtx;
use scarb::compiler::Profile;
use scarb::ops;
use sozo_scarbext::WorkspaceExt;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use starknet::accounts::Account;
use starknet::core::types::Call;
use starknet::core::utils::get_selector_from_name;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::JsonRpcClient;
use starknet_crypto::poseidon_hash_many;
use tempfile::NamedTempFile;
use tokio::sync::broadcast;
use tonic::Request;
use torii_cache::InMemoryCache;
use torii_indexer::engine::{Engine, EngineConfig};
use torii_indexer_fetcher::{Fetcher, FetcherConfig};
use torii_processors::processors::Processors;
use torii_proto::proto::world::world_server::World;
use torii_proto::proto::world::RetrieveEntitiesRequest;
use torii_proto::{Clause, KeysClause, PatternMatching, Query};

use torii_proto::schema::Entity;
use torii_sqlite::executor::Executor;
use torii_sqlite::Sql;
use torii_storage::proto::{Contract, ContractType};
use torii_storage::Storage;

use crate::{DojoWorld, GrpcConfig};

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10, db_dir = copy_spawn_and_move_db().as_str())]
async fn test_entities_queries(sequencer: &RunnerCtx) {
    let tempfile = NamedTempFile::new().unwrap();
    let path = tempfile.path().to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path)
        .unwrap()
        .create_if_missing(true)
        .with_regexp();
    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    let setup = CompilerTestSetup::from_examples("/tmp", "../../../examples/");
    let config = setup.build_test_config("spawn-and-move", Profile::DEV);

    let ws = ops::read_workspace(config.manifest_path(), &config)
        .unwrap_or_else(|op| panic!("Error building workspace: {op:?}"));

    let account = sequencer.account(0);

    let world_local = ws.load_world_local().unwrap();
    let world_address = world_local.deterministic_world_address().unwrap();

    let actions_address = world_local
        .get_contract_address_local(compute_selector_from_names("ns", "actions"))
        .unwrap();

    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));

    let world = WorldContract::new(world_address, &account);
    let world_reader = WorldContractReader::new(world_address, Arc::clone(&provider));

    world
        .grant_writer(
            &compute_bytearray_hash("ns"),
            &ContractAddress(actions_address),
        )
        .send_with_cfg(&TxnConfig::init_wait())
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // spawn
    let tx = account
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

    let (shutdown_tx, _) = broadcast::channel(1);

    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let db = Sql::new(
        pool.clone(),
        sender,
        &[Contract {
            address: world_address,
            r#type: ContractType::WORLD,
        }],
    )
    .await
    .unwrap();

    let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());

    let (shutdown_tx, _) = broadcast::channel(1);

    let contracts = &[Contract {
        address: world_address,
        r#type: ContractType::WORLD,
    }];
    let mut engine = Engine::new(
        world_reader,
        Arc::new(db.clone()),
        cache.clone(),
        Arc::clone(&provider),
        Processors {
            ..Processors::default()
        },
        EngineConfig::default(),
        shutdown_tx,
        contracts,
    );

    let cursors = contracts
        .iter()
        .map(|c| (c.address, Default::default()))
        .collect();

    let fetcher = Fetcher::new(Arc::new(provider.clone()), FetcherConfig::default());

    let data = fetcher.fetch(&cursors).await.unwrap();
    engine.process(&data).await.unwrap();

    db.execute().await.unwrap();

    let grpc = DojoWorld::new(
        Arc::new(db),
        provider.clone(),
        world_address,
        None,
        GrpcConfig::default(),
    );

    let query = Query {
        clause: Some(Clause::Keys(KeysClause {
            keys: vec![Some(account.address())],
            pattern_matching: PatternMatching::FixedLen,
            models: vec![],
        })),
        ..Default::default()
    };
    let entities = grpc
        .retrieve_entities(Request::new(RetrieveEntitiesRequest {
            query: Some(query.into()),
        }))
        .await
        .unwrap()
        .into_inner()
        .entities;

    assert_eq!(entities.len(), 1);

    let entity: Entity = entities.first().unwrap().clone().try_into().unwrap();
    let model_names: Vec<&str> = entity.models.iter().map(|m| m.name.as_str()).collect();
    assert!(model_names.contains(&"ns-Moves"));
    assert!(model_names.contains(&"ns-Position"));
    assert_eq!(entity.hashed_keys, poseidon_hash_many(&[account.address()]));
}
