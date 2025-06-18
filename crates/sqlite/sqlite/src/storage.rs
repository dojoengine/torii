use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dojo_types::{
    naming::{compute_selector_from_names, get_tag},
    schema::{Struct, Ty},
};
use dojo_world::{config::WorldMetadata, contracts::abigen::model::Layout};
use starknet::{
    core::types::{Event, U256},
    providers::Provider,
};
use starknet_crypto::{poseidon_hash_many, Felt};
use torii_math::I256;
use torii_sqlite_types::HookEvent;
use torii_storage::{
    types::{Cursor, ParsedCall},
    Storage, StorageError,
};

use crate::constants::SQL_FELT_DELIMITER;
use crate::{
    erc::fetch_token_metadata,
    error::{Error, ParseError, TokenMetadataError},
    executor::{
        erc::UpdateNftMetadataQuery, error::ExecutorQueryError, ApplyBalanceDiffQuery, Argument,
        DeleteEntityQuery, EntityQuery, EventMessageQuery, QueryMessage, QueryType,
        StoreTransactionQuery, UpdateCursorsQuery,
    },
    utils::{
        felt_and_u256_to_sql_string, felt_to_sql_string, felts_to_sql_string,
        utc_dt_string_from_timestamp
    },
    Sql,
};

#[async_trait]
impl Storage for Sql {
    /// Updates the contract cursors with the storage.
    async fn update_cursors(
        &self,
        cursors: HashMap<Felt, Cursor>,
        cursor_transactions: HashMap<Felt, HashSet<Felt>>,
    ) -> Result<(), StorageError> {
        let (query, recv) = QueryMessage::new_recv(
            "".to_string(),
            vec![],
            QueryType::UpdateCursors(UpdateCursorsQuery {
                cursors,
                cursor_transactions,
            }),
        );

        self.executor
            .send(query)
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        recv.await
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::RecvError(e))))?
            .map_err(|e| Error::ExecutorQuery(Box::new(e)))?;

        Ok(())
    }

    /// Registers a model with the storage, along with its table.
    /// This is also used when a model is upgraded, which should
    /// update the model schema and its table.
    async fn register_model(
        &self,
        namespace: &str,
        model: &Ty,
        layout: Layout,
        class_hash: Felt,
        contract_address: Felt,
        packed_size: u32,
        unpacked_size: u32,
        block_timestamp: u64,
        schema_diff: Option<&Ty>,
        upgrade_diff: Option<&Ty>,
    ) -> Result<(), StorageError> {
        let selector = compute_selector_from_names(namespace, &model.name());
        let namespaced_name = get_tag(namespace, &model.name());
        let namespaced_schema = Ty::Struct(Struct {
            name: namespaced_name.clone(),
            children: model.as_struct().unwrap().children.clone(),
        });

        let insert_models =
            "INSERT INTO models (id, namespace, name, class_hash, contract_address, layout, \
             schema, packed_size, unpacked_size, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, \
             ?) ON CONFLICT(id) DO UPDATE SET contract_address=EXCLUDED.contract_address, \
             class_hash=EXCLUDED.class_hash, layout=EXCLUDED.layout, schema=EXCLUDED.schema, \
             packed_size=EXCLUDED.packed_size, unpacked_size=EXCLUDED.unpacked_size, \
             executed_at=EXCLUDED.executed_at RETURNING *";
        let arguments = vec![
            Argument::FieldElement(selector),
            Argument::String(namespace.to_string()),
            Argument::String(model.name().to_string()),
            Argument::FieldElement(class_hash),
            Argument::FieldElement(contract_address),
            Argument::String(
                serde_json::to_string(&layout)
                    .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?,
            ),
            Argument::String(
                serde_json::to_string(&namespaced_schema)
                    .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?,
            ),
            Argument::Int(packed_size as i64),
            Argument::Int(unpacked_size as i64),
            Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
        ];
        self.executor
            .send(QueryMessage::new(
                insert_models.to_string(),
                arguments,
                QueryType::RegisterModel,
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        self.build_model_query(
            vec![namespaced_name.clone()],
            model,
            schema_diff,
            upgrade_diff,
        )?;

        // we set the model in the cache directly
        // because entities might be using it before the query queue is processed
        self.cache.model_cache
            .set(
                selector,
                torii_cache::Model {
                    namespace: namespace.to_string(),
                    name: model.name().to_string(),
                    selector,
                    class_hash,
                    contract_address,
                    packed_size,
                    unpacked_size,
                    layout,
                    schema: namespaced_schema,
                },
            )
            .await;

        for hook in self.config.hooks.iter() {
            if let HookEvent::ModelRegistered { model_tag } = &hook.event {
                if namespaced_name == *model_tag {
                    self.executor
                        .send(QueryMessage::other(
                            hook.statement.clone(),
                            vec![Argument::FieldElement(selector)],
                        ))
                        .map_err(|e| {
                            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e)))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Sets an entity with the storage.
    /// It should insert or update the entity if it already exists.
    /// Along with its model state in the model table.
    async fn set_entity(
        &self,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
        entity_id: Felt,
        model_selector: Felt,
        keys_str: Option<&str>,
    ) -> Result<(), StorageError> {
        let namespaced_name = entity.name();

        let entity_id = format!("{:#x}", entity_id);
        let model_id = format!("{:#x}", model_selector);

        let insert_entities = if keys_str.is_some() {
            "INSERT INTO entities (id, event_id, executed_at, keys) VALUES (?, ?, ?, ?) ON \
             CONFLICT(id) DO UPDATE SET updated_at=CURRENT_TIMESTAMP, \
             executed_at=EXCLUDED.executed_at, event_id=EXCLUDED.event_id, keys=EXCLUDED.keys \
             RETURNING *"
        } else {
            "INSERT INTO entities (id, event_id, executed_at) VALUES (?, ?, ?) ON CONFLICT(id) DO \
             UPDATE SET updated_at=CURRENT_TIMESTAMP, executed_at=EXCLUDED.executed_at, \
             event_id=EXCLUDED.event_id RETURNING *"
        };

        let mut arguments = vec![
            Argument::String(entity_id.clone()),
            Argument::String(event_id.to_string()),
            Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
        ];

        if let Some(keys) = keys_str {
            arguments.push(Argument::String(keys.to_string()));
        }

        self.executor
            .send(QueryMessage::new(
                insert_entities.to_string(),
                arguments,
                QueryType::SetEntity(EntityQuery {
                    event_id: event_id.to_string(),
                    block_timestamp: utc_dt_string_from_timestamp(block_timestamp),
                    entity_id: entity_id.clone(),
                    model_id: model_id.clone(),
                    keys_str: keys_str.map(|s| s.to_string()),
                    ty: entity.clone(),
                    is_historical: self.config.is_historical(&model_selector),
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        self.executor.send(QueryMessage::other(
            "INSERT INTO entity_model (entity_id, model_id) VALUES (?, ?) ON CONFLICT(entity_id, \
             model_id) DO NOTHING"
                .to_string(),
            vec![
                Argument::String(entity_id.clone()),
                Argument::String(model_id.clone()),
            ],
        )).map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        self.set_entity_model(
            &namespaced_name,
            event_id,
            &entity_id,
            &entity,
            block_timestamp,
        )?;

        for hook in self.config.hooks.iter() {
            if let HookEvent::ModelUpdated { model_tag } = &hook.event {
                if namespaced_name == *model_tag {
                    self.executor
                        .send(QueryMessage::other(
                            hook.statement.clone(),
                            vec![Argument::String(entity_id.clone())],
                        ))
                        .map_err(|e| {
                            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e)))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Sets an event message with the storage.
    /// It should insert or update the event message if it already exists.
    /// Along with its model state in the model table.
    async fn set_event_message(
        &self,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
    ) -> Result<(), StorageError> {
        let keys = if let Ty::Struct(s) = &entity {
            let mut keys = Vec::new();
            for m in s.keys() {
                keys.extend(m.serialize()?);
            }
            keys
        } else {
            return Err(Box::new(Error::Parse(ParseError::InvalidTyEntity)));
        };

        let namespaced_name = entity.name();
        let (model_namespace, model_name) = namespaced_name.split_once('-').unwrap();

        let entity_id = format!("{:#x}", poseidon_hash_many(&keys));
        let model_selector = compute_selector_from_names(model_namespace, model_name);
        let model_id = format!("{:#x}", model_selector);

        let keys_str = felts_to_sql_string(&keys);
        let block_timestamp_str = utc_dt_string_from_timestamp(block_timestamp);

        let insert_entities = "INSERT INTO event_messages (id, keys, event_id, executed_at) \
                               VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET \
                               updated_at=CURRENT_TIMESTAMP, executed_at=EXCLUDED.executed_at, \
                               event_id=EXCLUDED.event_id RETURNING *";
        self.executor
            .send(QueryMessage::new(
                insert_entities.to_string(),
                vec![
                    Argument::String(entity_id.clone()),
                    Argument::String(keys_str.clone()),
                    Argument::String(event_id.to_string()),
                    Argument::String(block_timestamp_str.clone()),
                ],
                QueryType::EventMessage(EventMessageQuery {
                    entity_id: entity_id.clone(),
                    model_id: model_id.clone(),
                    keys_str: keys_str.clone(),
                    event_id: event_id.to_string(),
                    block_timestamp: block_timestamp_str.clone(),
                    ty: entity.clone(),
                    is_historical: self.config.is_historical(&model_selector),
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        self.set_entity_model(
            &namespaced_name,
            event_id,
            &format!("event:{}", entity_id),
            &entity,
            block_timestamp,
        )?;

        for hook in self.config.hooks.iter() {
            if let HookEvent::ModelUpdated { model_tag } = &hook.event {
                if namespaced_name == *model_tag {
                    self.executor
                        .send(QueryMessage::other(
                            hook.statement.clone(),
                            vec![Argument::String(entity_id.clone())],
                        ))
                        .map_err(|e| {
                            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e)))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Deletes an entity with the storage.
    /// It should delete the entity from the entity table.
    /// Along with its model state in the model table.
    async fn delete_entity(
        &self,
        entity_id: Felt,
        model_id: Felt,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
    ) -> Result<(), StorageError> {
        let entity_id = format!("{:#x}", entity_id);
        let model_id = format!("{:#x}", model_id);
        let model_table = entity.name();

        self.executor
            .send(QueryMessage::new(
                format!("DELETE FROM [{model_table}] WHERE internal_id = ?").to_string(),
                vec![Argument::String(entity_id.clone())],
                QueryType::DeleteEntity(DeleteEntityQuery {
                    model_id: model_id.clone(),
                    entity_id: entity_id.clone(),
                    event_id: event_id.to_string(),
                    block_timestamp: utc_dt_string_from_timestamp(block_timestamp),
                    ty: entity.clone(),
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        for hook in self.config.hooks.iter() {
            if let HookEvent::ModelDeleted { model_tag } = &hook.event {
                if model_table == *model_tag {
                    self.executor
                        .send(QueryMessage::other(
                            hook.statement.clone(),
                            vec![Argument::String(entity_id.clone())],
                        ))
                        .map_err(|e| {
                            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e)))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Sets the metadata for a resource with the storage.
    /// It should insert or update the metadata if it already exists.
    /// Along with its model state in the model table.
    fn set_metadata(
        &self,
        resource: &Felt,
        uri: &str,
        block_timestamp: u64,
    ) -> Result<(), StorageError> {
        let resource = Argument::FieldElement(*resource);
        let uri = Argument::String(uri.to_string());
        let executed_at = Argument::String(utc_dt_string_from_timestamp(block_timestamp));

        self.executor
            .send(QueryMessage::other(
                "INSERT INTO metadata (id, uri, executed_at) VALUES (?, ?, ?) ON CONFLICT(id) DO \
             UPDATE SET id=excluded.id, executed_at=excluded.executed_at, \
             updated_at=CURRENT_TIMESTAMP"
                    .to_string(),
                vec![resource, uri, executed_at],
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }

    /// Updates the metadata for a resource with the storage.
    /// It should update the metadata if it already exists.
    /// Along with its model state in the model table.
    fn update_metadata(
        &self,
        resource: &Felt,
        uri: &str,
        metadata: &WorldMetadata,
        icon_img: &Option<String>,
        cover_img: &Option<String>,
    ) -> Result<(), StorageError> {
        let json = serde_json::to_string(metadata).unwrap(); // safe unwrap

        let mut update = vec!["uri=?", "json=?", "updated_at=CURRENT_TIMESTAMP"];
        let mut arguments = vec![Argument::String(uri.to_string()), Argument::String(json)];

        if let Some(icon) = icon_img {
            update.push("icon_img=?");
            arguments.push(Argument::String(icon.clone()));
        }

        if let Some(cover) = cover_img {
            update.push("cover_img=?");
            arguments.push(Argument::String(cover.clone()));
        }

        let statement = format!("UPDATE metadata SET {} WHERE id = ?", update.join(","));
        arguments.push(Argument::FieldElement(*resource));

        self.executor
            .send(QueryMessage::other(statement, arguments))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }

    /// Stores a transaction with the storage.
    /// It should insert or ignore the transaction if it already exists.
    /// And store all the relevant calls made in the transaction.
    /// Along with the unique models if any used in the transaction.
    fn store_transaction(
        &self,
        transaction_hash: Felt,
        sender_address: Felt,
        calldata: &[Felt],
        max_fee: Felt,
        signature: &[Felt],
        nonce: Felt,
        block_number: u64,
        contract_addresses: &HashSet<Felt>,
        transaction_type: &str,
        block_timestamp: u64,
        calls: &[ParsedCall],
        unique_models: &HashSet<Felt>,
    ) -> Result<(), StorageError> {
        // Store the transaction in the transactions table
        self.executor
            .send(QueryMessage::new(
                "INSERT INTO transactions (id, transaction_hash, sender_address, calldata, \
             max_fee, signature, nonce, transaction_type, executed_at, block_number) VALUES (?, \
             ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET transaction_hash=excluded.transaction_hash RETURNING *"
                    .to_string(),
                vec![
                    Argument::FieldElement(transaction_hash),
                    Argument::FieldElement(transaction_hash),
                    Argument::FieldElement(sender_address),
                    Argument::String(felts_to_sql_string(calldata)),
                    Argument::FieldElement(max_fee),
                    Argument::String(felts_to_sql_string(signature)),
                    Argument::FieldElement(nonce),
                    Argument::String(transaction_type.to_string()),
                    Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
                    Argument::String(block_number.to_string()),
                ],
                QueryType::StoreTransaction(StoreTransactionQuery {
                    contract_addresses: contract_addresses.clone(),
                    calls: calls.to_vec(),
                    unique_models: unique_models.clone(),
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }

    /// Stores an event with the storage.
    fn store_event(
        &self,
        event_id: &str,
        event: &Event,
        transaction_hash: Felt,
        block_timestamp: u64,
    ) -> Result<(), StorageError> {
        let id = Argument::String(event_id.to_string());
        let keys = Argument::String(felts_to_sql_string(&event.keys));
        let data = Argument::String(felts_to_sql_string(&event.data));
        let hash = Argument::FieldElement(transaction_hash);
        let executed_at = Argument::String(utc_dt_string_from_timestamp(block_timestamp));

        self.executor
            .send(QueryMessage::new(
                "INSERT INTO events (id, keys, data, transaction_hash, executed_at) VALUES \
             (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING *"
                    .to_string(),
                vec![id, keys, data, hash, executed_at],
                QueryType::StoreEvent,
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }

    /// Adds a controller to the storage.
    async fn add_controller(
        &self,
        username: &str,
        address: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let insert_controller = "
            INSERT INTO controllers (id, username, address, deployed_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                username=EXCLUDED.username,
                address=EXCLUDED.address,
                deployed_at=EXCLUDED.deployed_at
            RETURNING *";

        let arguments = vec![
            Argument::String(username.to_string()),
            Argument::String(username.to_string()),
            Argument::String(address.to_string()),
            Argument::String(timestamp.to_rfc3339()),
        ];

        self.executor
            .send(QueryMessage::other(
                insert_controller.to_string(),
                arguments,
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }

    /// Handles ERC20 token transfers, updating balances and registering tokens as needed.
    async fn handle_erc20_transfer<P: Provider + Sync>(
        &self,
        provider: &P,
        contract_address: Felt,
        from_address: Felt,
        to_address: Felt,
        amount: U256,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<(), StorageError> {
        // contract_address
        let token_id = felt_to_sql_string(&contract_address);

        // optimistically add the token_id to cache
        // this cache is used while applying the cache diff
        // so we need to make sure that all RegisterErc*Token queries
        // are applied before the cache diff is applied
        self.try_register_erc20_token_metadata(contract_address, &token_id, provider)
            .await?;

        self.store_erc_transfer_event(
            contract_address,
            from_address,
            to_address,
            amount,
            &token_id,
            block_timestamp,
            event_id,
        )?;

        if from_address != Felt::ZERO {
            // from_address/contract_address/
            let from_balance_id = felts_to_sql_string(&[from_address, contract_address]);
            let mut from_balance = self
                .cache
                .erc_cache
                .balances_diff
                .entry(from_balance_id)
                .or_default();
            *from_balance -= I256::from(amount);
        }

        if to_address != Felt::ZERO {
            let to_balance_id = felts_to_sql_string(&[to_address, contract_address]);
            let mut to_balance = self
                .cache
                .erc_cache
                .balances_diff
                .entry(to_balance_id)
                .or_default();
            *to_balance += I256::from(amount);
        }

        Ok(())
    }

    /// Handles NFT (ERC721/ERC1155) token transfers, updating balances and registering tokens as needed.
    #[allow(clippy::too_many_arguments)]
    async fn handle_nft_transfer<P: Provider + Sync>(
        &self,
        provider: &P,
        contract_address: Felt,
        from_address: Felt,
        to_address: Felt,
        token_id: U256,
        amount: U256,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<(), StorageError> {
        // contract_address:id
        let id = felt_and_u256_to_sql_string(&contract_address, &token_id);
        // optimistically add the token_id to cache
        // this cache is used while applying the cache diff
        // so we need to make sure that all RegisterErc*Token queries
        // are applied before the cache diff is applied
        self.try_register_nft_token_metadata(&id, contract_address, token_id, provider)
            .await?;

        self.store_erc_transfer_event(
            contract_address,
            from_address,
            to_address,
            amount,
            &id,
            block_timestamp,
            event_id,
        )?;

        // from_address/contract_address:id
        if from_address != Felt::ZERO {
            let from_balance_id = format!(
                "{}{SQL_FELT_DELIMITER}{}",
                felt_to_sql_string(&from_address),
                &id
            );
            let mut from_balance = self
                .cache
                .erc_cache
                .balances_diff
                .entry(from_balance_id)
                .or_default();
            *from_balance -= I256::from(amount);
        }

        if to_address != Felt::ZERO {
            let to_balance_id = format!(
                "{}{SQL_FELT_DELIMITER}{}",
                felt_to_sql_string(&to_address),
                &id
            );
            let mut to_balance = self.cache.erc_cache.balances_diff.entry(to_balance_id).or_default();
            *to_balance += I256::from(amount);
        }

        Ok(())
    }

    /// Updates NFT metadata for a specific token.
    async fn update_nft_metadata<P: Provider + Sync>(
        &self,
        provider: &P,
        contract_address: Felt,
        token_id: U256,
    ) -> Result<(), StorageError> {
        let id = felt_and_u256_to_sql_string(&contract_address, &token_id);
        if !self.cache.erc_cache.is_token_registered(&id).await {
            return Ok(());
        }

        let _permit = self
            .nft_metadata_semaphore
            .acquire()
            .await
            .map_err(|e| Error::TokenMetadata(TokenMetadataError::AcquireError(e)))?;
        let metadata = fetch_token_metadata(contract_address, token_id, provider).await?;

        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::UpdateNftMetadata(UpdateNftMetadataQuery {
                    id,
                    contract_address,
                    token_id,
                    metadata,
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }

    /// Applies cached balance differences to the storage.
    async fn apply_cache_diff(&self, cursors: HashMap<Felt, Cursor>) -> Result<(), StorageError> {
        if !self.cache.erc_cache.balances_diff.is_empty() {
            let erc_cache = self
                .cache
                .erc_cache
                .balances_diff
                .iter()
                .map(|t| (t.key().clone(), *t.value()))
                .collect::<HashMap<String, I256>>();
            self.cache.erc_cache.balances_diff.clear();
            self.cache.erc_cache.balances_diff.shrink_to_fit();

            self.executor
                .send(QueryMessage::new(
                    "".to_string(),
                    vec![],
                    QueryType::ApplyBalanceDiff(ApplyBalanceDiffQuery { erc_cache, cursors }),
                ))
                .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;
        }
        Ok(())
    }

    /// Executes pending operations and commits the current transaction.
    async fn execute(&self) -> Result<(), StorageError> {
        let (execute, recv) = QueryMessage::execute_recv();
        self.executor
            .send(execute)
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;
        let res = recv
            .await
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::RecvError(e))))?;
        res.map_err(|e| Error::ExecutorQuery(Box::new(e)))?;
        Ok(())
    }

    /// Rolls back the current transaction and starts a new one.
    async fn rollback(&self) -> Result<(), StorageError> {
        let (rollback, recv) = QueryMessage::rollback_recv();
        self.executor
            .send(rollback)
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;
        let res = recv
            .await
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::RecvError(e))))?;
        res.map_err(|e| Error::ExecutorQuery(Box::new(e)))?;
        Ok(())
    }
}
