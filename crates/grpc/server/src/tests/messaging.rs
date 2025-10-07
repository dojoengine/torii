use std::str::FromStr;
use std::sync::Arc;

use dojo_types::naming::compute_selector_from_names;
use dojo_types::primitive::Primitive;
use dojo_types::schema::{Member, Struct, Ty};
use dojo_world::contracts::abigen::model::Layout;
use indexmap::IndexMap;
use katana_runner::RunnerCtx;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::types::chrono::Utc;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::JsonRpcClient;
use starknet::signers::SigningKey;
use starknet_crypto::Felt;
use tempfile::NamedTempFile;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tonic::Request;
use torii_libp2p_relay::Relay;
use torii_messaging::{Messaging, MessagingConfig};
use torii_proto::proto::world::PublishMessageRequest;
use torii_sqlite::executor::Executor;
use torii_sqlite::utils::felt_to_sql_string;
use torii_sqlite::Sql;
use torii_storage::proto::{ContractDefinition, ContractType};
use torii_storage::Storage;
use torii_typed_data::typed_data::{Domain, Field, SimpleField, TypedData};

use crate::{DojoWorld, GrpcConfig};

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10)]
async fn test_publish_message(sequencer: &RunnerCtx) {
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

    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));
    let account_data = sequencer.account_data(0);

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let db = Arc::new(
        Sql::new(
            pool.clone(),
            sender,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap(),
    );

    // Register the model for our Message
    db.register_model(
        compute_selector_from_names("types_test", "Message"),
        Felt::ZERO, // world_address
        &Ty::Struct(Struct {
            name: "types_test-Message".to_string(),
            children: vec![
                Member {
                    name: "identity".to_string(),
                    ty: Ty::Primitive(Primitive::ContractAddress(None)),
                    key: true,
                },
                Member {
                    name: "message".to_string(),
                    ty: Ty::ByteArray("".to_string()),
                    key: false,
                },
            ],
        }),
        &Layout::Fixed(vec![]),
        Felt::ZERO,
        Felt::ZERO,
        0,
        0,
        0,
        None,
        None,
        true,
    )
    .await
    .unwrap();
    db.execute().await.unwrap();

    // Create DojoWorld instance
    let messaging = Arc::new(Messaging::new(
        MessagingConfig::default(),
        db.clone(),
        provider.clone(),
    ));
    let grpc = DojoWorld::new(
        db.clone(),
        messaging,
        Felt::ZERO, // world_address
        None,
        pool.clone(),
        GrpcConfig::default(),
    );

    // Create typed data for the message
    let mut typed_data = TypedData::new(
        IndexMap::from_iter(vec![
            (
                "types_test-Message".to_string(),
                vec![
                    Field::SimpleType(SimpleField {
                        name: "identity".to_string(),
                        r#type: "ContractAddress".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "message".to_string(),
                        r#type: "string".to_string(),
                    }),
                ],
            ),
            (
                "StarknetDomain".to_string(),
                vec![
                    Field::SimpleType(SimpleField {
                        name: "name".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "version".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "chainId".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "revision".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                ],
            ),
        ]),
        "types_test-Message",
        Domain::new("types_test-Message", "1", "0x0", Some("1")),
        IndexMap::new(),
    );

    typed_data.message.insert(
        "identity".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String(account_data.address.to_string()),
    );

    typed_data.message.insert(
        "message".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String("test message".to_string()),
    );

    // Sign the message
    let message_hash = typed_data.encode(account_data.address).unwrap();
    let signature =
        SigningKey::from_secret_scalar(account_data.private_key.clone().unwrap().secret_scalar())
            .sign(&message_hash)
            .unwrap();

    // Create the publish message request
    let request = Request::new(PublishMessageRequest {
        message: serde_json::to_string(&typed_data).unwrap(),
        signature: vec![
            signature.r.to_bytes_be().to_vec(),
            signature.s.to_bytes_be().to_vec(),
        ],
    });

    // Publish the message using the gRPC service
    use torii_proto::proto::world::world_server::World;
    let response = grpc.publish_message(request).await.unwrap();
    let entity_id = response.into_inner().entity_id;

    // Verify the entity was created
    assert!(!entity_id.is_empty());

    // Verify the message was stored in the database by checking entities table
    let message: String =
        sqlx::query_scalar("SELECT message FROM [types_test-Message] WHERE internal_id = ?")
            .bind(felt_to_sql_string(&Felt::from_bytes_be_slice(&entity_id)))
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(message, "test message");

    // Publish again with another message
    typed_data.message.insert(
        "message".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String("test message 2".to_string()),
    );

    let message_hash = typed_data.encode(account_data.address).unwrap();
    let signature =
        SigningKey::from_secret_scalar(account_data.private_key.clone().unwrap().secret_scalar())
            .sign(&message_hash)
            .unwrap();

    let request = Request::new(PublishMessageRequest {
        message: serde_json::to_string(&typed_data).unwrap(),
        signature: vec![
            signature.r.to_bytes_be().to_vec(),
            signature.s.to_bytes_be().to_vec(),
        ],
    });

    // Publish the message using the gRPC service
    let response = grpc.publish_message(request).await.unwrap();
    let entity_id = response.into_inner().entity_id;

    // Verify the entity was created
    assert!(!entity_id.is_empty());

    // Verify the message was stored in the database by checking entities table
    let message: String =
        sqlx::query_scalar("SELECT message FROM [types_test-Message] WHERE internal_id = ?")
            .bind(felt_to_sql_string(&Felt::from_bytes_be_slice(&entity_id)))
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(message, "test message 2");

    // Check that message is not updated with bad signature
    let message_hash = typed_data.encode(account_data.address).unwrap();
    let signature = SigningKey::from_secret_scalar(Felt::ZERO)
        .sign(&message_hash)
        .unwrap();

    let request = Request::new(PublishMessageRequest {
        message: serde_json::to_string(&typed_data).unwrap(),
        signature: vec![
            signature.r.to_bytes_be().to_vec(),
            signature.s.to_bytes_be().to_vec(),
        ],
    });

    let result = grpc.publish_message(request).await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10)]
async fn test_cross_messaging_between_relay_servers(sequencer: &RunnerCtx) {
    // Setup first relay server database
    let tempfile1 = NamedTempFile::new().unwrap();
    let path1 = tempfile1.path().to_string_lossy();
    let options1 = SqliteConnectOptions::from_str(&path1)
        .unwrap()
        .create_if_missing(true)
        .with_regexp();
    let pool1 = SqlitePoolOptions::new()
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(options1)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations")
        .run(&pool1)
        .await
        .unwrap();

    // Setup second relay server database
    let tempfile2 = NamedTempFile::new().unwrap();
    let path2 = tempfile2.path().to_string_lossy();
    let options2 = SqliteConnectOptions::from_str(&path2)
        .unwrap()
        .create_if_missing(true)
        .with_regexp();
    let pool2 = SqlitePoolOptions::new()
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(options2)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations")
        .run(&pool2)
        .await
        .unwrap();

    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));
    let account_data = sequencer.account_data(0);

    // Setup first server components
    let (shutdown_tx1, _) = broadcast::channel(1);
    let (mut executor1, sender1) =
        Executor::new(pool1.clone(), shutdown_tx1.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor1.run().await.unwrap();
    });

    let mut db1 = Arc::new(
        Sql::new(
            pool1.clone(),
            sender1,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap(),
    );

    // Setup second server components
    let (shutdown_tx2, _) = broadcast::channel(1);
    let (mut executor2, sender2) =
        Executor::new(pool2.clone(), shutdown_tx2.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor2.run().await.unwrap();
    });

    let mut db2 = Arc::new(
        Sql::new(
            pool2.clone(),
            sender2,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap(),
    );

    // Register the message model on both databases
    let message_model = Ty::Struct(Struct {
        name: "types_test-Message".to_string(),
        children: vec![
            Member {
                name: "identity".to_string(),
                ty: Ty::Primitive(Primitive::ContractAddress(None)),
                key: true,
            },
            Member {
                name: "message".to_string(),
                ty: Ty::ByteArray("".to_string()),
                key: false,
            },
        ],
    });

    for db in [&mut db1, &mut db2] {
        db.register_model(
            compute_selector_from_names("types_test", "Message"),
            Felt::ZERO, // world_address
            &message_model,
            &Layout::Fixed(vec![]),
            Felt::ZERO,
            Felt::ZERO,
            0,
            0,
            0,
            None,
            None,
            true,
        )
        .await
        .unwrap();
        db.execute().await.unwrap();
    }

    let messaging1 = Arc::new(Messaging::new(
        MessagingConfig::default(),
        db1.clone(),
        provider.clone(),
    ));
    let messaging2 = Arc::new(Messaging::new(
        MessagingConfig::default(),
        db2.clone(),
        provider.clone(),
    ));

    // Create first relay server (will be the main server)
    let (mut relay_server1, cross_messaging_tx1) =
        Relay::new(messaging1.clone(), 9900, 9901, 9902, None, None).unwrap();

    // Create second relay server (peer server) - connect to first server
    let (mut relay_server2, _cross_messaging_tx2) = Relay::new_with_peers(
        messaging2.clone(),
        9903,
        9904,
        9905,
        None,
        None,
        vec!["/ip4/127.0.0.1/tcp/9900".to_string()],
    )
    .unwrap();

    // Start both relay servers
    tokio::spawn(async move {
        relay_server1.run().await;
    });

    tokio::spawn(async move {
        relay_server2.run().await;
    });

    // Wait for servers to start and connect
    sleep(Duration::from_secs(3)).await;

    // Create DojoWorld instance with cross messaging
    let grpc = DojoWorld::new(
        db1,
        messaging1,
        Felt::ZERO, // world_address
        Some(cross_messaging_tx1),
        pool1.clone(),
        GrpcConfig::default(),
    );

    // Create typed data for the message
    let mut typed_data = TypedData::new(
        IndexMap::from_iter(vec![
            (
                "types_test-Message".to_string(),
                vec![
                    Field::SimpleType(SimpleField {
                        name: "identity".to_string(),
                        r#type: "ContractAddress".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "message".to_string(),
                        r#type: "string".to_string(),
                    }),
                ],
            ),
            (
                "StarknetDomain".to_string(),
                vec![
                    Field::SimpleType(SimpleField {
                        name: "name".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "version".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "chainId".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "revision".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                ],
            ),
        ]),
        "types_test-Message",
        Domain::new("types_test-Message", "1", "0x0", Some("1")),
        IndexMap::new(),
    );

    typed_data.message.insert(
        "identity".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String(account_data.address.to_string()),
    );

    typed_data.message.insert(
        "message".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String("cross messaging test".to_string()),
    );

    // Sign the message
    let message_hash = typed_data.encode(account_data.address).unwrap();
    let signature =
        SigningKey::from_secret_scalar(account_data.private_key.clone().unwrap().secret_scalar())
            .sign(&message_hash)
            .unwrap();

    // Create the publish message request
    let request = Request::new(PublishMessageRequest {
        message: serde_json::to_string(&typed_data).unwrap(),
        signature: vec![
            signature.r.to_bytes_be().to_vec(),
            signature.s.to_bytes_be().to_vec(),
        ],
    });

    // Publish the message using the gRPC service
    use torii_proto::proto::world::world_server::World;
    let response = grpc.publish_message(request).await.unwrap();
    let entity_id = response.into_inner().entity_id;

    // Verify the entity was created on the first server
    assert!(!entity_id.is_empty());

    let entity_exists_server1: bool =
        sqlx::query_scalar("SELECT COUNT(*) > 0 FROM entities WHERE id = ?")
            .bind(felt_to_sql_string(&Felt::from_bytes_be_slice(&entity_id)))
            .fetch_one(&pool1)
            .await
            .unwrap();

    assert!(
        entity_exists_server1,
        "Entity should exist in first server database after publishing message"
    );

    // Wait for cross messaging to propagate
    sleep(Duration::from_secs(3)).await;

    // Verify the message was received and stored by the second server
    let entity_exists_server2: bool =
        sqlx::query_scalar("SELECT COUNT(*) > 0 FROM entities WHERE id = ?")
            .bind(felt_to_sql_string(&Felt::from_bytes_be_slice(&entity_id)))
            .fetch_one(&pool2)
            .await
            .unwrap();

    assert!(
        entity_exists_server2,
        "Entity should exist in second server database after cross messaging"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10)]
async fn test_publish_message_with_bad_signature_fails(sequencer: &RunnerCtx) {
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

    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));
    let account_data = sequencer.account_data(0);
    let wrong_account_data = sequencer.account_data(1); // Different account for wrong signature

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let db = Arc::new(
        Sql::new(
            pool.clone(),
            sender,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap(),
    );

    // Register the model for our Message
    db.register_model(
        compute_selector_from_names("types_test", "Message"),
        Felt::ZERO, // world_address
        &Ty::Struct(Struct {
            name: "types_test-Message".to_string(),
            children: vec![
                Member {
                    name: "identity".to_string(),
                    ty: Ty::Primitive(Primitive::ContractAddress(None)),
                    key: true,
                },
                Member {
                    name: "message".to_string(),
                    ty: Ty::ByteArray("".to_string()),
                    key: false,
                },
            ],
        }),
        &Layout::Fixed(vec![]),
        Felt::ZERO,
        Felt::ZERO,
        0,
        0,
        0,
        None,
        None,
        true,
    )
    .await
    .unwrap();
    db.execute().await.unwrap();

    // Create DojoWorld instance
    let messaging = Arc::new(Messaging::new(
        MessagingConfig::default(),
        db.clone(),
        provider.clone(),
    ));
    let grpc = DojoWorld::new(
        db.clone(),
        messaging,
        Felt::ZERO, // world_address
        None,
        pool.clone(),
        GrpcConfig::default(),
    );

    // Create typed data for the message
    let mut typed_data = TypedData::new(
        IndexMap::from_iter(vec![
            (
                "types_test-Message".to_string(),
                vec![
                    Field::SimpleType(SimpleField {
                        name: "identity".to_string(),
                        r#type: "ContractAddress".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "message".to_string(),
                        r#type: "string".to_string(),
                    }),
                ],
            ),
            (
                "StarknetDomain".to_string(),
                vec![
                    Field::SimpleType(SimpleField {
                        name: "name".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "version".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "chainId".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "revision".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                ],
            ),
        ]),
        "types_test-Message",
        Domain::new("types_test-Message", "1", "0x0", Some("1")),
        IndexMap::new(),
    );

    typed_data.message.insert(
        "identity".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String(account_data.address.to_string()),
    );

    typed_data.message.insert(
        "message".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String("malicious message".to_string()),
    );

    // Sign the message hash with the WRONG private key (simulating impersonation attempt)
    let message_hash = typed_data.encode(account_data.address).unwrap();
    let bad_signature = SigningKey::from_secret_scalar(
        wrong_account_data
            .private_key
            .clone()
            .unwrap()
            .secret_scalar(),
    )
    .sign(&message_hash)
    .unwrap();

    // Create the publish message request with bad signature
    let request = Request::new(PublishMessageRequest {
        message: serde_json::to_string(&typed_data).unwrap(),
        signature: vec![
            bad_signature.r.to_bytes_be().to_vec(),
            bad_signature.s.to_bytes_be().to_vec(),
        ],
    });

    // Attempt to publish the message using the gRPC service
    use torii_proto::proto::world::world_server::World;
    let result = grpc.publish_message(request).await;

    // Verify that the request failed due to invalid signature
    assert!(
        result.is_err(),
        "Publishing message with bad signature should fail"
    );

    // Verify that no entity was created in the database
    let entity_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM entities")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(
        entity_count, 0,
        "No entities should be created when signature verification fails"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[katana_runner::test(accounts = 10)]
async fn test_timestamp_validation_logic(sequencer: &RunnerCtx) {
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

    let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));
    let account_data = sequencer.account_data(0);

    let (shutdown_tx, _) = broadcast::channel(1);
    let (mut executor, sender) =
        Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
            .await
            .unwrap();
    tokio::spawn(async move {
        executor.run().await.unwrap();
    });

    let db = Arc::new(
        Sql::new(
            pool.clone(),
            sender,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap(),
    );

    // Register a model with timestamp support
    db.register_model(
        compute_selector_from_names("types_test", "TimestampedMessage"),
        Felt::ZERO, // world_address
        &Ty::Struct(Struct {
            name: "types_test-TimestampedMessage".to_string(),
            children: vec![
                Member {
                    name: "identity".to_string(),
                    ty: Ty::Primitive(Primitive::ContractAddress(None)),
                    key: true,
                },
                Member {
                    name: "timestamp".to_string(),
                    ty: Ty::Primitive(Primitive::U64(None)),
                    key: false,
                },
                Member {
                    name: "message".to_string(),
                    ty: Ty::ByteArray("".to_string()),
                    key: false,
                },
            ],
        }),
        &Layout::Fixed(vec![]),
        Felt::ZERO,
        Felt::ZERO,
        0,
        0,
        0,
        None,
        None,
        true,
    )
    .await
    .unwrap();
    db.execute().await.unwrap();

    let now = Utc::now().timestamp_millis() as u64;

    // Create DojoWorld instance with default messaging config
    let messaging = Arc::new(Messaging::new(
        MessagingConfig::default(),
        db.clone(),
        provider.clone(),
    ));
    let grpc = DojoWorld::new(
        db.clone(),
        messaging,
        Felt::ZERO,
        None,
        pool.clone(),
        GrpcConfig::default(),
    );

    // Test valid timestamp
    let mut typed_data = TypedData::new(
        IndexMap::from_iter(vec![
            (
                "types_test-TimestampedMessage".to_string(),
                vec![
                    Field::SimpleType(SimpleField {
                        name: "identity".to_string(),
                        r#type: "ContractAddress".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "timestamp".to_string(),
                        r#type: "u128".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "message".to_string(),
                        r#type: "string".to_string(),
                    }),
                ],
            ),
            (
                "StarknetDomain".to_string(),
                vec![
                    Field::SimpleType(SimpleField {
                        name: "name".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "version".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "chainId".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                    Field::SimpleType(SimpleField {
                        name: "revision".to_string(),
                        r#type: "shortstring".to_string(),
                    }),
                ],
            ),
        ]),
        "types_test-TimestampedMessage",
        Domain::new("types_test-TimestampedMessage", "1", "0x0", Some("1")),
        IndexMap::new(),
    );

    typed_data.message.insert(
        "identity".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String(account_data.address.to_string()),
    );

    typed_data.message.insert(
        "timestamp".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String(now.to_string()),
    );

    typed_data.message.insert(
        "message".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String("valid timestamp test".to_string()),
    );

    // Sign and publish valid message
    let message_hash = typed_data.encode(account_data.address).unwrap();
    let signature =
        SigningKey::from_secret_scalar(account_data.private_key.clone().unwrap().secret_scalar())
            .sign(&message_hash)
            .unwrap();

    let request = Request::new(PublishMessageRequest {
        message: serde_json::to_string(&typed_data).unwrap(),
        signature: vec![
            signature.r.to_bytes_be().to_vec(),
            signature.s.to_bytes_be().to_vec(),
        ],
    });

    use torii_proto::proto::world::world_server::World;
    let response = grpc.publish_message(request).await.unwrap();
    assert!(!response.into_inner().entity_id.is_empty());

    // Test timestamp too far in future (should fail)
    typed_data.message.insert(
        "timestamp".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String((now + 120_000).to_string()),
    );

    let message_hash = typed_data.encode(account_data.address).unwrap();
    let signature =
        SigningKey::from_secret_scalar(account_data.private_key.clone().unwrap().secret_scalar())
            .sign(&message_hash)
            .unwrap();

    let request = Request::new(PublishMessageRequest {
        message: serde_json::to_string(&typed_data).unwrap(),
        signature: vec![
            signature.r.to_bytes_be().to_vec(),
            signature.s.to_bytes_be().to_vec(),
        ],
    });

    let result = grpc.publish_message(request).await;
    assert!(result.is_err());
    // Check that it's specifically a TimestampTooFuture error
    let error = result.unwrap_err();
    assert!(error.message().contains("too far in the future"));

    // Test timestamp too old (should fail)
    typed_data.message.insert(
        "timestamp".to_string(),
        torii_typed_data::typed_data::PrimitiveType::String((now - 400_000).to_string()), // 400 seconds ago, exceeds max_age of 300 seconds
    );

    let message_hash = typed_data.encode(account_data.address).unwrap();
    let signature =
        SigningKey::from_secret_scalar(account_data.private_key.clone().unwrap().secret_scalar())
            .sign(&message_hash)
            .unwrap();

    let request = Request::new(PublishMessageRequest {
        message: serde_json::to_string(&typed_data).unwrap(),
        signature: vec![
            signature.r.to_bytes_be().to_vec(),
            signature.s.to_bytes_be().to_vec(),
        ],
    });

    let result = grpc.publish_message(request).await;
    assert!(result.is_err());
    // Check that it's specifically a TimestampTooOld error
    let error = result.unwrap_err();
    assert!(error.message().contains("too old"));

    println!("All timestamp validation tests passed!");
}
