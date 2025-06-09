use std::str::FromStr;
use std::sync::Arc;

use dojo_types::primitive::Primitive;
use dojo_types::schema::{Member, Struct, Ty};
use dojo_world::contracts::abigen::model::Layout;
use indexmap::IndexMap;
use katana_runner::RunnerCtx;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::JsonRpcClient;
use starknet::signers::SigningKey;
use starknet_crypto::Felt;
use tempfile::NamedTempFile;
use tokio::sync::broadcast;
use tonic::Request;
use torii_proto::proto::world::PublishMessageRequest;
use torii_sqlite::cache::ModelCache;
use torii_sqlite::executor::Executor;
use torii_sqlite::types::{Contract, ContractType};
use torii_sqlite::Sql;
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

    let model_cache = Arc::new(ModelCache::new(pool.clone()).await.unwrap());
    let mut db = Sql::new(
        pool.clone(),
        sender,
        &[Contract {
            address: Felt::ZERO,
            r#type: ContractType::WORLD,
        }],
        model_cache.clone(),
    )
    .await
    .unwrap();

    // Register the model for our Message
    db.register_model(
        "types_test",
        &Ty::Struct(Struct {
            name: "Message".to_string(),
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
        Layout::Fixed(vec![]),
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
        db,
        provider.clone(),
        Felt::ZERO, // world_address
        model_cache,
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
    let entity_exists: bool = sqlx::query_scalar("SELECT COUNT(*) > 0 FROM entities WHERE id = ?")
        .bind(format!("{:#x}", Felt::from_bytes_be_slice(&entity_id)))
        .fetch_one(&pool)
        .await
        .unwrap();

    assert!(
        entity_exists,
        "Entity should exist in database after publishing message"
    );
}
