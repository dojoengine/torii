#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use dojo_types::naming::compute_selector_from_names;
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
    use torii_messaging::{Messaging, MessagingConfig};
    use torii_sqlite::executor::Executor;
    use torii_sqlite::Sql;
    use torii_storage::proto::{ContractDefinition, ContractType};
    use torii_storage::Storage;
    use torii_typed_data::typed_data::{Domain, Field, SimpleField, TypedData};

    use crate::schema::build_schema;
    use crate::tests::run_graphql_query;

    #[tokio::test(flavor = "multi_thread")]
    #[katana_runner::test(accounts = 10)]
    async fn test_graphql_publish_message(sequencer: &RunnerCtx) {
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
        sqlx::migrate!("../migrations").run(&pool).await.unwrap();

        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(sequencer.url())));
        let account_data = sequencer.account_data(0);

        let (shutdown_tx, _) = broadcast::channel(1);
        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), provider.clone())
                .await
                .unwrap();
        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let storage = Arc::new(
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
        storage
            .register_model(
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
                true,
            )
            .await
            .unwrap();
        storage.execute().await.unwrap();

        let messaging = Arc::new(Messaging::new(
            MessagingConfig::default(),
            storage.clone(),
            provider.clone(),
        ));

        let schema = build_schema(messaging, storage).await.unwrap();

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
        let signature = SigningKey::from_secret_scalar(
            account_data.private_key.clone().unwrap().secret_scalar(),
        )
        .sign(&message_hash)
        .unwrap();

        // Create GraphQL mutation
        let message_json = serde_json::to_string(&typed_data)
            .unwrap()
            .replace('"', "\\\"");
        let signature_r = format!("0x{:064x}", signature.r);
        let signature_s = format!("0x{:064x}", signature.s);

        let mutation = format!(
            r#"
            mutation {{
                publishMessage(
                    message: "{}",
                    signature: ["{}", "{}"]
                ) {{
                    entityId
                }}
            }}
            "#,
            message_json, signature_r, signature_s
        );

        // Execute the mutation
        let result = run_graphql_query(&schema, &mutation).await;

        // Verify the response
        let publish_result = result
            .get("publishMessage")
            .ok_or("publishMessage not found")
            .unwrap();

        let entity_id = publish_result
            .get("entityId")
            .ok_or("entityId not found")
            .unwrap()
            .as_str()
            .unwrap();

        // Verify the entity was created
        assert!(!entity_id.is_empty());
        assert!(entity_id.starts_with("0x"));

        // Verify the message was stored in the database
        let stored_message: String =
            sqlx::query_scalar("SELECT message FROM [types_test-Message] WHERE internal_id = ?")
                .bind(entity_id)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(stored_message, "test message");
    }
}
