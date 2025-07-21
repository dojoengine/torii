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

#[tokio::test(flavor = "multi_thread")]
async fn test_entity_broker_multiple_subscriptions() {
    use crate::subscriptions::entity::{EntityManager, Service};
    use chrono::Utc;
    use std::time::Duration;
    use tokio::time::timeout;
    use torii_broker::{types::EntityUpdate, MemoryBroker};
    use torii_proto::schema::{Entity, EntityWithMetadata};

    // Create entity manager with default config
    let config = GrpcConfig::default();
    let entity_manager = Arc::new(EntityManager::new(config));

    // Create the service that will process broker updates and spawn it as a task
    let service = Service::new(entity_manager.clone());
    tokio::spawn(service);

    // Create multiple subscribers with NO clauses (all should receive all updates)
    let num_subscribers = 5;
    let mut subscribers = Vec::new();

    for i in 0..num_subscribers {
        let receiver = entity_manager.add_subscriber(None).await; // No clause = receive all
        subscribers.push((i, receiver));
    }

    // Wait a bit for subscriptions to be set up
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create and publish many entity updates
    let num_updates = 200;

    for update_id in 0..num_updates {
        let hashed_keys = starknet_crypto::Felt::from(update_id as u64);
        let keys = vec![hashed_keys];

        // Create a test entity
        let entity = Entity {
            hashed_keys,
            models: vec![],
        };

        let now = Utc::now();
        let entity_with_metadata = EntityWithMetadata {
            entity,
            event_id: format!("event_{}", update_id),
            keys,
            created_at: now,
            updated_at: now,
            executed_at: now,
        };

        // Publish the update to the broker
        let update = EntityUpdate::new(entity_with_metadata, false);
        MemoryBroker::publish(update);

        // Small delay every 50 updates to prevent overwhelming the system
        if update_id % 50 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Collect results from all subscribers
    let mut received_counts = Vec::new();

    for (subscriber_id, mut receiver) in subscribers {
        let mut count = 0;

        // Skip the initial empty response
        if let Ok(Some(response)) = timeout(Duration::from_secs(1), receiver.recv()).await {
            match response {
                Ok(resp) => {
                    assert!(resp.entity.is_none()); // Initial empty response
                }
                Err(e) => panic!("Subscriber {} received error: {:?}", subscriber_id, e),
            }
        }

        // Collect all updates (each subscriber should receive all num_updates)
        while count < num_updates {
            match timeout(Duration::from_secs(3), receiver.recv()).await {
                Ok(Some(response)) => match response {
                    Ok(resp) => {
                        if let Some(_entity) = resp.entity {
                            count += 1;
                        }
                    }
                    Err(e) => panic!("Subscriber {} received error: {:?}", subscriber_id, e),
                },
                Ok(None) => break,
                Err(_) => {
                    println!(
                        "Subscriber {} timed out after receiving {} updates (expected {})",
                        subscriber_id, count, num_updates
                    );
                    break;
                }
            }
        }

        received_counts.push(count);
    }

    // Verify that all subscribers received all updates
    println!("Published {} updates", num_updates);
    println!("Received counts: {:?}", received_counts);

    for (subscriber_id, received_count) in received_counts.iter().enumerate() {
        assert_eq!(
            *received_count, num_updates,
            "Subscriber {} expected {} updates, got {}",
            subscriber_id, num_updates, received_count
        );
    }

    println!(
        "✅ Successfully tested {} subscribers with {} updates each",
        num_subscribers, num_updates
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_entity_broker_stress_test() {
    use crate::subscriptions::entity::{EntityManager, Service};
    use chrono::Utc;
    use dojo_types::primitive::Primitive;
    use dojo_types::schema::{Member, Struct, Ty};
    use std::time::{Duration, Instant};
    use tokio::time::timeout;
    use torii_broker::{types::EntityUpdate, MemoryBroker};
    use torii_proto::schema::{Entity, EntityWithMetadata};

    println!("🚀 Starting broker stress test...");

    // Create entity manager with larger buffer size for stress test
    let config = GrpcConfig {
        subscription_buffer_size: 10000,
        ..Default::default()
    };
    let entity_manager = Arc::new(EntityManager::new(config));

    // Create the service that will process broker updates and spawn it as a task
    let service = Service::new(entity_manager.clone());
    tokio::spawn(service);

    // Create hundreds of subscribers with NO clauses (all should receive all updates)
    let num_subscribers = 500;
    let mut subscribers = Vec::new();

    println!("📡 Creating {} subscribers...", num_subscribers);

    for i in 0..num_subscribers {
        let receiver = entity_manager.add_subscriber(None).await; // No clause = receive all
        subscribers.push((i, receiver));

        // Progress indicator
        if i % 100 == 0 {
            println!("  Created {} subscribers", i + 1);
        }
    }

    // Wait for subscriptions to be set up
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create and publish thousands of entity updates
    let num_updates = 5000;
    let batch_size = 100;
    let start_time = Instant::now();

    println!(
        "📦 Publishing {} updates in batches of {}...",
        num_updates, batch_size
    );

    for batch in 0..(num_updates / batch_size) {
        let batch_start = batch * batch_size;
        let batch_end = std::cmp::min(batch_start + batch_size, num_updates);

        for update_id in batch_start..batch_end {
            let hashed_keys = starknet_crypto::Felt::from(update_id as u64);
            let keys = vec![hashed_keys];

            // Create realistic game models that cycle through different types
            let models = match update_id % 4 {
                0 => {
                    // Player update
                    vec![Struct {
                        name: "game-Player".to_string(),
                        children: vec![
                            Member {
                                name: "player_id".to_string(),
                                ty: Ty::Primitive(Primitive::ContractAddress(Some(hashed_keys))),
                                key: true,
                            },
                            Member {
                                name: "name".to_string(),
                                ty: Ty::ByteArray(format!("Player{}", update_id)),
                                key: false,
                            },
                            Member {
                                name: "level".to_string(),
                                ty: Ty::Primitive(Primitive::U32(Some(
                                    (update_id % 100 + 1) as u32,
                                ))),
                                key: false,
                            },
                            Member {
                                name: "experience".to_string(),
                                ty: Ty::Primitive(Primitive::U64(Some((update_id * 150) as u64))),
                                key: false,
                            },
                        ],
                    }]
                }
                1 => {
                    // Position update
                    vec![Struct {
                        name: "game-Position".to_string(),
                        children: vec![
                            Member {
                                name: "entity_id".to_string(),
                                ty: Ty::Primitive(Primitive::ContractAddress(Some(hashed_keys))),
                                key: true,
                            },
                            Member {
                                name: "x".to_string(),
                                ty: Ty::Primitive(Primitive::U32(Some((update_id % 1000) as u32))),
                                key: false,
                            },
                            Member {
                                name: "y".to_string(),
                                ty: Ty::Primitive(Primitive::U32(Some((update_id % 1000) as u32))),
                                key: false,
                            },
                            Member {
                                name: "z".to_string(),
                                ty: Ty::Primitive(Primitive::U32(Some((update_id % 100) as u32))),
                                key: false,
                            },
                        ],
                    }]
                }
                2 => {
                    // Health update
                    vec![Struct {
                        name: "game-Health".to_string(),
                        children: vec![
                            Member {
                                name: "entity_id".to_string(),
                                ty: Ty::Primitive(Primitive::ContractAddress(Some(hashed_keys))),
                                key: true,
                            },
                            Member {
                                name: "current_hp".to_string(),
                                ty: Ty::Primitive(Primitive::U32(Some(
                                    (update_id % 100 + 1) as u32,
                                ))),
                                key: false,
                            },
                            Member {
                                name: "max_hp".to_string(),
                                ty: Ty::Primitive(Primitive::U32(Some(100))),
                                key: false,
                            },
                            Member {
                                name: "last_damaged_at".to_string(),
                                ty: Ty::Primitive(Primitive::U64(Some(update_id as u64))),
                                key: false,
                            },
                        ],
                    }]
                }
                3 => {
                    // Inventory update
                    vec![Struct {
                        name: "game-Inventory".to_string(),
                        children: vec![
                            Member {
                                name: "owner".to_string(),
                                ty: Ty::Primitive(Primitive::ContractAddress(Some(hashed_keys))),
                                key: true,
                            },
                            Member {
                                name: "item_count".to_string(),
                                ty: Ty::Primitive(Primitive::U32(Some((update_id % 50) as u32))),
                                key: false,
                            },
                            Member {
                                name: "gold".to_string(),
                                ty: Ty::Primitive(Primitive::U64(Some((update_id * 10) as u64))),
                                key: false,
                            },
                            Member {
                                name: "last_updated".to_string(),
                                ty: Ty::Primitive(Primitive::U64(Some(update_id as u64))),
                                key: false,
                            },
                        ],
                    }]
                }
                _ => unreachable!(),
            };

            // Create entity with realistic models
            let entity = Entity {
                hashed_keys,
                models,
            };

            let now = Utc::now();
            let entity_with_metadata = EntityWithMetadata {
                entity,
                event_id: format!("stress_event_{}", update_id),
                keys,
                created_at: now,
                updated_at: now,
                executed_at: now,
            };

            // Publish the update to the broker
            let update = EntityUpdate::new(entity_with_metadata, false);
            MemoryBroker::publish(update);
        }

        // Small delay between batches to prevent overwhelming the system
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Progress indicator
        if batch % 10 == 0 {
            let elapsed = start_time.elapsed();
            let updates_sent = batch_end;
            let rate = updates_sent as f64 / elapsed.as_secs_f64();
            println!("  Sent {} updates ({:.1} updates/sec)", updates_sent, rate);
        }
    }

    let publish_duration = start_time.elapsed();
    println!(
        "✅ Published {} updates in {:.2}s ({:.1} updates/sec)",
        num_updates,
        publish_duration.as_secs_f64(),
        num_updates as f64 / publish_duration.as_secs_f64()
    );

    // Collect results from all subscribers with extended timeout
    let mut received_counts = Vec::new();
    let mut successful_subscribers = 0;
    let mut failed_subscribers = 0;

    println!(
        "📊 Collecting results from {} subscribers...",
        num_subscribers
    );

    for (subscriber_id, mut receiver) in subscribers {
        let mut count = 0;
        let subscriber_start = Instant::now();

        // Skip the initial empty response
        if let Ok(Some(response)) = timeout(Duration::from_secs(2), receiver.recv()).await {
            match response {
                Ok(resp) => {
                    assert!(resp.entity.is_none()); // Initial empty response
                }
                Err(e) => {
                    println!(
                        "⚠️  Subscriber {} received error during initial response: {:?}",
                        subscriber_id, e
                    );
                    failed_subscribers += 1;
                    received_counts.push(0);
                    continue;
                }
            }
        }

        // Collect all updates with generous timeout
        let mut consecutive_timeouts = 0;
        let max_consecutive_timeouts = 3;

        while count < num_updates {
            match timeout(Duration::from_secs(5), receiver.recv()).await {
                Ok(Some(response)) => {
                    match response {
                        Ok(resp) => {
                            if let Some(_entity) = resp.entity {
                                count += 1;
                                consecutive_timeouts = 0; // Reset timeout counter
                            }
                        }
                        Err(e) => {
                            println!("⚠️  Subscriber {} received error: {:?}", subscriber_id, e);
                            break;
                        }
                    }
                }
                Ok(None) => {
                    println!(
                        "⚠️  Subscriber {} channel closed unexpectedly",
                        subscriber_id
                    );
                    break;
                }
                Err(_) => {
                    consecutive_timeouts += 1;
                    if consecutive_timeouts >= max_consecutive_timeouts {
                        println!(
                            "⚠️  Subscriber {} timed out {} times consecutively, stopping",
                            subscriber_id, consecutive_timeouts
                        );
                        break;
                    }
                    // Continue waiting for more updates
                }
            }
        }

        let subscriber_duration = subscriber_start.elapsed();

        if count == num_updates {
            successful_subscribers += 1;
            if subscriber_id % 100 == 0 {
                println!(
                    "  ✅ Subscriber {} received all {} updates in {:.2}s",
                    subscriber_id,
                    count,
                    subscriber_duration.as_secs_f64()
                );
            }
        } else {
            failed_subscribers += 1;
            println!(
                "  ❌ Subscriber {} received only {} updates (expected {})",
                subscriber_id, count, num_updates
            );
        }

        received_counts.push(count);
    }

    // Calculate statistics
    let total_expected = num_subscribers * num_updates;
    let total_received: usize = received_counts.iter().sum();
    let success_rate = (successful_subscribers as f64 / num_subscribers as f64) * 100.0;
    let delivery_rate = (total_received as f64 / total_expected as f64) * 100.0;

    println!("\n📈 STRESS TEST RESULTS:");
    println!("  Subscribers: {}", num_subscribers);
    println!("  Updates per subscriber: {}", num_updates);
    println!("  Total expected deliveries: {}", total_expected);
    println!("  Total actual deliveries: {}", total_received);
    println!(
        "  Successful subscribers: {} ({:.1}%)",
        successful_subscribers, success_rate
    );
    println!("  Failed subscribers: {}", failed_subscribers);
    println!("  Overall delivery rate: {:.2}%", delivery_rate);

    // Calculate min, max, median received counts
    let mut sorted_counts = received_counts.clone();
    sorted_counts.sort();
    let min_received = sorted_counts[0];
    let max_received = sorted_counts[sorted_counts.len() - 1];
    let median_received = sorted_counts[sorted_counts.len() / 2];

    println!("  Min received: {}", min_received);
    println!("  Max received: {}", max_received);
    println!("  Median received: {}", median_received);

    // Assert that we have reasonable success rates
    assert!(
        success_rate >= 95.0,
        "Success rate too low: {:.1}%. Expected at least 95%",
        success_rate
    );
    assert!(
        delivery_rate >= 95.0,
        "Delivery rate too low: {:.2}%. Expected at least 95%",
        delivery_rate
    );

    println!("🎉 Stress test completed successfully! {} subscribers handled {} updates each with {:.1}% success rate", 
             num_subscribers, num_updates, success_rate);
}
