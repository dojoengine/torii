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
use torii_sqlite::Sql;
use torii_storage::proto::{Contract, ContractType};
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
            &[Contract {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
            }],
        )
        .await
        .unwrap(),
    );

    // Register the model for our Message
    db.register_model(
        compute_selector_from_names("types_test", "Message"),
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
    )
    .await
    .unwrap();
    db.execute().await.unwrap();

    // Create DojoWorld instance
    let grpc = DojoWorld::new(
        db.clone(),
        provider.clone(),
        Felt::ZERO, // world_address
        None,
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
            .bind(format!("{:#x}", Felt::from_bytes_be_slice(&entity_id)))
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
            .bind(format!("{:#x}", Felt::from_bytes_be_slice(&entity_id)))
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
            &[Contract {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
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
            &[Contract {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
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
            &message_model,
            &Layout::Fixed(vec![]),
            Felt::ZERO,
            Felt::ZERO,
            0,
            0,
            0,
            None,
            None,
        )
        .await
        .unwrap();
        db.execute().await.unwrap();
    }

    // Create first relay server (will be the main server)
    let (mut relay_server1, cross_messaging_tx1) = Relay::new(
        db1.clone(),
        Arc::clone(&provider).as_ref().clone(),
        9900,
        9901,
        9902,
        None,
        None,
    )
    .unwrap();

    // Create second relay server (peer server) - connect to first server
    let (mut relay_server2, _cross_messaging_tx2) = Relay::new_with_peers(
        db2.clone(),
        Arc::clone(&provider).as_ref().clone(),
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
    let messaging = Arc::new(Messaging::default());
    let grpc = DojoWorld::new(
        db1,
        provider.clone(),
        messaging,
        Felt::ZERO, // world_address
        Some(cross_messaging_tx1),
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
            .bind(format!("{:#x}", Felt::from_bytes_be_slice(&entity_id)))
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
            .bind(format!("{:#x}", Felt::from_bytes_be_slice(&entity_id)))
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
            &[Contract {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
            }],
        )
        .await
        .unwrap(),
    );

    // Register the model for our Message
    db.register_model(
        compute_selector_from_names("types_test", "Message"),
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
    )
    .await
    .unwrap();
    db.execute().await.unwrap();

    // Create DojoWorld instance
    let messaging = Arc::new(Messaging::default());
    let grpc = DojoWorld::new(
        db.clone(),
        provider.clone(),
        messaging,
        Felt::ZERO, // world_address
        None,
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
            &[Contract {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
            }],
        )
        .await
        .unwrap(),
    );

    // Register a model with timestamp support
    db.register_model(
        compute_selector_from_names("types_test", "TimestampedMessage"),
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
    )
    .await
    .unwrap();
    db.execute().await.unwrap();

    // Test different messaging configurations

    // 1. Test with default config (max_age: 300s, future_tolerance: 60s, require_timestamp: false)
    let default_messaging = Arc::new(Messaging::new(MessagingConfig::default()));

    // 2. Test with strict config (require_timestamp: true)
    let strict_messaging = Arc::new(Messaging::new(MessagingConfig {
        max_age: 300,
        future_tolerance: 60,
        require_timestamp: true,
    }));

    // 3. Test with very restrictive config (max_age: 10s, future_tolerance: 5s)
    let restrictive_messaging = Arc::new(Messaging::new(MessagingConfig {
        max_age: 10,
        future_tolerance: 5,
        require_timestamp: false,
    }));

    let now = Utc::now().timestamp() as u64;

    // Helper function to create typed data with timestamp
    let create_typed_data_with_timestamp = |timestamp: Option<u64>, message_text: &str| {
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
                            r#type: "u64".to_string(),
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

        if let Some(ts) = timestamp {
            typed_data.message.insert(
                "timestamp".to_string(),
                torii_typed_data::typed_data::PrimitiveType::String(ts.to_string()),
            );
        }

        typed_data.message.insert(
            "message".to_string(),
            torii_typed_data::typed_data::PrimitiveType::String(message_text.to_string()),
        );

        typed_data
    };

    // Helper function to sign and validate message
    let sign_and_validate = |messaging: Arc<Messaging>, typed_data: TypedData| async move {
        let message_hash = typed_data.encode(account_data.address).unwrap();
        let signature = SigningKey::from_secret_scalar(
            account_data.private_key.clone().unwrap().secret_scalar(),
        )
        .sign(&message_hash)
        .unwrap();

        let signature_vec = vec![signature.r, signature.s];

        // Convert TypedData to starknet_core TypedData for validation
        let starknet_typed_data: starknet_core::types::TypedData =
            serde_json::from_str(&serde_json::to_string(&typed_data).unwrap()).unwrap();

        messaging
            .validate_and_set_entity(db.clone(), &starknet_typed_data, &signature_vec, &provider)
            .await
    };

    println!("Testing valid timestamp (current time)...");
    let valid_typed_data = create_typed_data_with_timestamp(Some(now), "valid timestamp");
    assert!(
        sign_and_validate(default_messaging.clone(), valid_typed_data)
            .await
            .is_ok()
    );

    println!("Testing timestamp too far in future...");
    let future_typed_data = create_typed_data_with_timestamp(Some(now + 120), "future timestamp");
    let result = sign_and_validate(default_messaging.clone(), future_typed_data).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("too far in the future"));

    println!("Testing timestamp too old...");
    let old_typed_data = create_typed_data_with_timestamp(Some(now - 400), "old timestamp");
    let result = sign_and_validate(default_messaging.clone(), old_typed_data).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("too old"));

    println!("Testing valid message without timestamp (should succeed with default config)...");
    let no_timestamp_data = create_typed_data_with_timestamp(None, "no timestamp");
    assert!(
        sign_and_validate(default_messaging.clone(), no_timestamp_data)
            .await
            .is_ok()
    );

    println!("Testing message without timestamp with strict config (should fail)...");
    let no_timestamp_strict = create_typed_data_with_timestamp(None, "no timestamp strict");
    let result = sign_and_validate(strict_messaging.clone(), no_timestamp_strict).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));

    println!("Testing restrictive timing config...");
    // Should fail with restrictive config (max_age: 10s, future_tolerance: 5s)
    let slightly_old = create_typed_data_with_timestamp(Some(now - 15), "slightly old");
    let result = sign_and_validate(restrictive_messaging.clone(), slightly_old).await;
    assert!(result.is_err());

    let slightly_future = create_typed_data_with_timestamp(Some(now + 8), "slightly future");
    let result = sign_and_validate(restrictive_messaging.clone(), slightly_future).await;
    assert!(result.is_err());

    println!("Testing entity timestamp ordering...");
    // First, create an entity with a timestamp
    let initial_typed_data = create_typed_data_with_timestamp(Some(now), "initial message");
    let entity_id = sign_and_validate(default_messaging.clone(), initial_typed_data)
        .await
        .unwrap();

    // Try to update with older timestamp (should fail)
    let older_update = create_typed_data_with_timestamp(Some(now - 10), "older update");
    let result = sign_and_validate(default_messaging.clone(), older_update).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("older than the entity timestamp")
            || result.unwrap_err().to_string().contains("Invalid")
    );

    // Try to update with newer timestamp (should succeed)
    let newer_update = create_typed_data_with_timestamp(Some(now + 30), "newer update");
    assert!(sign_and_validate(default_messaging.clone(), newer_update)
        .await
        .is_ok());

    // Try to update with no timestamp when entity has timestamp (should fail)
    let no_timestamp_update = create_typed_data_with_timestamp(None, "no timestamp update");
    let result = sign_and_validate(default_messaging.clone(), no_timestamp_update).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));

    println!("All timestamp validation tests passed!");
}
