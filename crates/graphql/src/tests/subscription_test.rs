#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;

    use async_graphql::value;
    use dojo_types::naming::get_tag;
    use dojo_types::primitive::Primitive;
    use dojo_types::schema::{Enum, EnumOption, Member, Struct, Ty};
    use dojo_world::contracts::abigen::model::Layout;
    use dojo_world::contracts::naming::{compute_selector_from_names, compute_selector_from_tag};
    use serial_test::serial;
    use sqlx::SqlitePool;
    use starknet::core::types::Event;
    use starknet::providers::jsonrpc::HttpTransport;
    use starknet::providers::JsonRpcClient;
    use starknet_crypto::{poseidon_hash_many, Felt};
    use tokio::sync::{broadcast, mpsc};
    use torii_sqlite::executor::Executor;
    use torii_sqlite::utils::felt_to_sql_string;
    use torii_sqlite::Sql;
    use torii_storage::proto::{ContractDefinition, ContractType};
    use torii_storage::Storage;
    use url::Url;

    use crate::tests::{model_fixtures, run_graphql_subscription};
    use crate::utils;

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_entity_subscription(pool: SqlitePool) {
        let (shutdown_tx, _) = broadcast::channel(1);
        // used to fetch token_uri data for erc721 tokens so pass dummy for the test
        let url: Url = "https://www.example.com".parse().unwrap();
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), provider.clone())
                .await
                .unwrap();
        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let db = Sql::new(
            pool.clone(),
            sender,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap();

        model_fixtures(&db).await;
        // 0. Preprocess expected entity value
        let namespace = "types_test".to_string();
        let model_name = "Record".to_string();
        let key = vec![Felt::ONE];
        let entity_id = felt_to_sql_string(&poseidon_hash_many(&key));
        let keys_str = key
            .iter()
            .map(felt_to_sql_string)
            .collect::<Vec<String>>()
            .join(",");
        let block_timestamp = 1710754478_u64;
        let type_name = utils::type_name_from_names(&namespace, &model_name);

        let expected_value: async_graphql::Value = value!({
            "entityUpdated": {
                "id": entity_id,
                "keys":vec![keys_str],
                "models" : [{
                    "__typename": type_name,
                        "depth": "Zero",
                        "record_id": 0,
                        "typeU16": 1,
                        "type_u64": "0x1",
                        "typeBool": true,
                        "type_felt": format!("{:#x}", Felt::from(1u128)),
                        "typeContractAddress": format!("{:#x}", Felt::ONE)
                }]
            }
        });
        let (tx, mut rx) = mpsc::channel(10);

        let db_clone = db.clone();
        tokio::spawn(async move {
            // 1. Open process and sleep.Go to execute subscription
            tokio::time::sleep(Duration::from_secs(1)).await;
            let ty = Ty::Struct(Struct {
                name: get_tag(&namespace, &model_name),
                children: vec![
                    Member {
                        name: "depth".to_string(),
                        key: false,
                        ty: Ty::Enum(Enum {
                            name: "Depth".to_string(),
                            option: Some(0),
                            options: vec![
                                EnumOption {
                                    name: "Zero".to_string(),
                                    ty: Ty::Tuple(vec![]),
                                },
                                EnumOption {
                                    name: "One".to_string(),
                                    ty: Ty::Tuple(vec![]),
                                },
                                EnumOption {
                                    name: "Two".to_string(),
                                    ty: Ty::Tuple(vec![]),
                                },
                                EnumOption {
                                    name: "Three".to_string(),
                                    ty: Ty::Tuple(vec![]),
                                },
                            ],
                        }),
                    },
                    Member {
                        name: "record_id".to_string(),
                        key: false,
                        ty: Ty::Primitive(Primitive::U8(Some(0))),
                    },
                    Member {
                        name: "typeU16".to_string(),
                        key: false,
                        ty: Ty::Primitive(Primitive::U16(Some(1))),
                    },
                    Member {
                        name: "type_u64".to_string(),
                        key: false,
                        ty: Ty::Primitive(Primitive::U64(Some(1))),
                    },
                    Member {
                        name: "typeBool".to_string(),
                        key: false,
                        ty: Ty::Primitive(Primitive::Bool(Some(true))),
                    },
                    Member {
                        name: "type_felt".to_string(),
                        key: false,
                        ty: Ty::Primitive(Primitive::Felt252(Some(Felt::from(1u128)))),
                    },
                    Member {
                        name: "typeContractAddress".to_string(),
                        key: true,
                        ty: Ty::Primitive(Primitive::ContractAddress(Some(Felt::ONE))),
                    },
                ],
            });
            let keys = keys_from_ty(&ty).unwrap();
            let entity_id = poseidon_hash_many(&keys);
            let model_id = model_id_from_ty(&ty);

            // Set entity with one Record model
            db_clone
                .set_entity(
                    ty,
                    &format!("0x{:064x}:0x{:04x}:0x{:04x}", 0, 0, 0),
                    block_timestamp,
                    entity_id,
                    model_id,
                    Some(keys),
                )
                .await
                .unwrap();
            db_clone.execute().await.unwrap();

            tx.send(()).await.unwrap();
        });

        // 2. The subscription is executed and it is listening, waiting for publish() to be executed
        let response_value = run_graphql_subscription(
            &db,
            provider,
            r#"subscription {
                entityUpdated {
                    id
                    keys
                    models {
                        __typename
                        ... on types_test_Record {
                            depth
                            record_id
                            typeU16
                            type_u64
                            typeBool
                            type_felt
                            typeContractAddress
                        }
                    }
                }
            }"#,
        )
        .await;
        // 4. The subscription has received the message from publish()
        // 5. Compare values
        assert_eq!(expected_value, response_value);
        rx.recv().await.unwrap();
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_entity_subscription_with_id(pool: SqlitePool) {
        let (shutdown_tx, _) = broadcast::channel(1);

        // dummy provider since its required to query data for erc721 tokens
        let url: Url = "https://www.example.com".parse().unwrap();
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));

        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), provider.clone())
                .await
                .unwrap();
        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let db = Sql::new(
            pool.clone(),
            sender,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap();
        model_fixtures(&db).await;
        // 0. Preprocess expected entity value
        let namespace = "types_test".to_string();
        let model_name = "Record".to_string();
        let key = vec![Felt::ONE];
        let entity_id = felt_to_sql_string(&poseidon_hash_many(&key));
        let block_timestamp = 1710754478_u64;
        let keys_str = key
            .iter()
            .map(felt_to_sql_string)
            .collect::<Vec<String>>()
            .join(",");
        let type_name = utils::type_name_from_names(&namespace, &model_name);

        let expected_value: async_graphql::Value = value!({
            "entityUpdated": {
                "id": entity_id,
                "keys":vec![keys_str],
                "models" : [{
                    "__typename": type_name,
                        "depth": "Zero",
                        "record_id": 0,
                        "type_felt": felt_to_sql_string(&Felt::from(1u128)),
                        "typeContractAddress": felt_to_sql_string(&Felt::ONE)
                }]
            }
        });
        let (tx, mut rx) = mpsc::channel(10);

        let db_clone = db.clone();
        tokio::spawn(async move {
            // 1. Open process and sleep.Go to execute subscription
            tokio::time::sleep(Duration::from_secs(1)).await;
            let ty = Ty::Struct(Struct {
                name: get_tag(&namespace, &model_name),
                children: vec![
                    Member {
                        name: "depth".to_string(),
                        key: false,
                        ty: Ty::Enum(Enum {
                            name: "Depth".to_string(),
                            option: Some(0),
                            options: vec![
                                EnumOption {
                                    name: "Zero".to_string(),
                                    ty: Ty::Tuple(vec![]),
                                },
                                EnumOption {
                                    name: "One".to_string(),
                                    ty: Ty::Tuple(vec![]),
                                },
                                EnumOption {
                                    name: "Two".to_string(),
                                    ty: Ty::Tuple(vec![]),
                                },
                                EnumOption {
                                    name: "Three".to_string(),
                                    ty: Ty::Tuple(vec![]),
                                },
                            ],
                        }),
                    },
                    Member {
                        name: "record_id".to_string(),
                        key: false,
                        ty: Ty::Primitive(Primitive::U32(Some(0))),
                    },
                    Member {
                        name: "type_felt".to_string(),
                        key: false,
                        ty: Ty::Primitive(Primitive::Felt252(Some(Felt::from(1u128)))),
                    },
                    Member {
                        name: "typeContractAddress".to_string(),
                        key: true,
                        ty: Ty::Primitive(Primitive::ContractAddress(Some(Felt::ONE))),
                    },
                ],
            });

            let keys = keys_from_ty(&ty).unwrap();
            let entity_id = poseidon_hash_many(&keys);
            let model_id = model_id_from_ty(&ty);

            // Set entity with one Record model
            db_clone
                .set_entity(
                    ty,
                    &format!("0x{:064x}:0x{:04x}:0x{:04x}", 0, 0, 0),
                    block_timestamp,
                    entity_id,
                    model_id,
                    Some(keys),
                )
                .await
                .unwrap();
            db_clone.execute().await.unwrap();

            tx.send(()).await.unwrap();
        });

        // 2. The subscription is executed and it is listening, waiting for publish() to be executed
        // uise entity_id variable
        let response_value = run_graphql_subscription(
            &db,
            provider,
            &format!(
                r#"subscription {{
                entityUpdated(id: "{entity_id}") {{
                    id
                    keys
                    models {{
                        __typename
                        ... on types_test_Record {{
                            depth
                            record_id
                            type_felt
                            typeContractAddress
                        }}
                    }}
                }}
                }}
            }}"#
            ),
        )
        .await;
        // 4. The subscription has received the message from publish()
        // 5. Compare values
        assert_eq!(expected_value, response_value);
        rx.recv().await.unwrap();
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_model_subscription(pool: SqlitePool) {
        let (shutdown_tx, _) = broadcast::channel(1);

        let url: Url = "https://www.example.com".parse().unwrap();
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), provider.clone())
                .await
                .unwrap();
        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let db = Sql::new(
            pool.clone(),
            sender,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap();
        // 0. Preprocess model value
        let namespace = "types_test".to_string();
        let model_name = "Subrecord".to_string();
        let tag = get_tag(&namespace, &model_name);
        let selector = compute_selector_from_names(&tag, &model_name);
        let model_id = felt_to_sql_string(&selector);
        let class_hash = Felt::TWO;
        let contract_address = Felt::THREE;
        let block_timestamp: u64 = 1710754478_u64;
        let expected_value: async_graphql::Value = value!({
            "modelRegistered": { "id": model_id, "name": model_name }
        });

        let (tx, mut rx) = mpsc::channel(7);

        let db_clone = db.clone();
        tokio::spawn(async move {
            // 1. Open process and sleep.Go to execute subscription
            tokio::time::sleep(Duration::from_secs(1)).await;

            let model = Ty::Struct(Struct {
                name: tag,
                children: vec![Member {
                    name: "subrecordId".to_string(),
                    key: true,
                    ty: Ty::Primitive(Primitive::U32(None)),
                }],
            });
            db_clone
                .register_model(
                    selector,
                    &model,
                    &Layout::Fixed(vec![]),
                    class_hash,
                    contract_address,
                    0,
                    0,
                    block_timestamp,
                    None,
                    None,
                    true,
                )
                .await
                .unwrap();
            db_clone.execute().await.unwrap();

            // 3. fn publish() is called from state.set_entity()

            tx.send(()).await.unwrap();
        });

        // 2. The subscription is executed and it is listeing, waiting for publish() to be executed
        let response_value = run_graphql_subscription(
            &db,
            provider,
            r#"
                subscription {
                    modelRegistered {
                            id, name
                        }
                }"#,
        )
        .await;
        // 4. The subcription has received the message from publish()
        // 5. Compare values
        assert_eq!(expected_value, response_value);
        rx.recv().await.unwrap();
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_model_subscription_with_id(pool: SqlitePool) {
        let (shutdown_tx, _) = broadcast::channel(1);

        let url: Url = "https://www.example.com".parse().unwrap();
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), provider.clone())
                .await
                .unwrap();
        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let db = Sql::new(
            pool.clone(),
            sender,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap();
        // 0. Preprocess model value
        let namespace = "types_test".to_string();
        let model_name = "Subrecord".to_string();
        let selector = compute_selector_from_names(&namespace, &model_name);
        let model_id = felt_to_sql_string(&selector);
        let class_hash = Felt::TWO;
        let contract_address = Felt::THREE;
        let block_timestamp: u64 = 1710754478_u64;
        let expected_value: async_graphql::Value = value!({
         "modelRegistered": { "id": model_id, "name": model_name }
        });
        let (tx, mut rx) = mpsc::channel(7);

        let db_clone = db.clone();
        tokio::spawn(async move {
            // 1. Open process and sleep.Go to execute subscription
            tokio::time::sleep(Duration::from_secs(1)).await;

            let model = Ty::Struct(Struct {
                name: get_tag(&namespace, &model_name),
                children: vec![Member {
                    name: "type_u8".into(),
                    key: false,
                    ty: Ty::Primitive(Primitive::U8(None)),
                }],
            });
            db_clone
                .register_model(
                    selector,
                    &model,
                    &Layout::Fixed(vec![]),
                    class_hash,
                    contract_address,
                    0,
                    0,
                    block_timestamp,
                    None,
                    None,
                    true,
                )
                .await
                .unwrap();
            db_clone.execute().await.unwrap();
            // 3. fn publish() is called from state.set_entity()

            tx.send(()).await.unwrap();
        });

        // 2. The subscription is executed and it is listeing, waiting for publish() to be executed
        let response_value = run_graphql_subscription(
            &db,
            provider,
            &format!(
                r#"
            subscription {{
                modelRegistered(id: "{}") {{
                        id, name
                    }}
            }}"#,
                model_id
            ),
        )
        .await;
        // 4. The subcription has received the message from publish()
        // 5. Compare values
        assert_eq!(expected_value, response_value);
        rx.recv().await.unwrap();
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_event_emitted(pool: SqlitePool) {
        let (shutdown_tx, _) = broadcast::channel(1);

        let url: Url = "https://www.example.com".parse().unwrap();
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), provider.clone())
                .await
                .unwrap();
        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let db = Sql::new(
            pool.clone(),
            sender,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap();
        let block_timestamp: u64 = 1710754478_u64;
        let (tx, mut rx) = mpsc::channel(7);
        let db_clone = db.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;

            db_clone
                .store_event(
                    "0x0",
                    &Event {
                        from_address: Felt::ZERO,
                        keys: vec![
                            Felt::from_str("0xdead").unwrap(),
                            Felt::from_str("0xbeef").unwrap(),
                        ],
                        data: vec![
                            Felt::from_str("0xc0de").unwrap(),
                            Felt::from_str("0xface").unwrap(),
                        ],
                    },
                    Felt::ZERO,
                    block_timestamp,
                )
                .await
                .unwrap();
            db_clone.execute().await.unwrap();

            tx.send(()).await.unwrap();
        });

        let response_value = run_graphql_subscription(
            &db,
            provider,
            &format!(
                r#"
                    subscription {{
                        eventEmitted (keys: ["*", "{}"]) {{
                            keys
                            data
                            transactionHash
                        }}
                    }}
                "#,
                felt_to_sql_string(&Felt::from_str("0xbeef").unwrap())
            ),
        )
        .await;

        let expected_value: async_graphql::Value = value!({
         "eventEmitted": { "keys": vec![
            felt_to_sql_string(&Felt::from_str("0xdead").unwrap()),
            felt_to_sql_string(&Felt::from_str("0xbeef").unwrap())
         ], "data": vec![
            felt_to_sql_string(&Felt::from_str("0xc0de").unwrap()),
            felt_to_sql_string(&Felt::from_str("0xface").unwrap())
         ], "transactionHash": felt_to_sql_string(&Felt::ZERO)}
        });

        assert_eq!(response_value, expected_value);
        rx.recv().await.unwrap();
    }

    fn keys_from_ty(ty: &Ty) -> anyhow::Result<Vec<Felt>> {
        if let Ty::Struct(s) = &ty {
            let mut keys = Vec::new();
            for m in s.keys() {
                keys.extend(
                    m.serialize()
                        .map_err(|_| anyhow::anyhow!("Failed to serialize model key"))?,
                );
            }
            Ok(keys)
        } else {
            anyhow::bail!("Entity is not a struct")
        }
    }

    fn model_id_from_ty(ty: &Ty) -> Felt {
        let namespaced_name = ty.name();

        compute_selector_from_tag(&namespaced_name)
    }
}
