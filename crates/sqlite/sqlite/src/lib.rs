use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dojo_types::naming::get_tag;
use dojo_types::primitive::SqlType;
use dojo_types::schema::{Struct, Ty};
use dojo_world::config::WorldMetadata;
use dojo_world::contracts::abigen::model::Layout;
use dojo_world::contracts::naming::compute_selector_from_names;
use executor::{EntityQuery, StoreTransactionQuery};
use sqlx::{Pool, Sqlite};
use starknet::core::types::{Event, Felt};
use starknet_crypto::poseidon_hash_many;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Semaphore;
use torii_sqlite_types::{ContractCursor, HookEvent, ParsedCall};
use utils::felts_to_sql_string;

use crate::constants::SQL_FELT_DELIMITER;
use crate::error::{Error, ParseError};
use crate::executor::error::ExecutorError;
use crate::executor::{
    Argument, DeleteEntityQuery, EventMessageQuery, QueryMessage, QueryType, UpdateCursorsQuery,
};
use crate::utils::utc_dt_string_from_timestamp;
use torii_sqlite_types::{Contract, Hook, ModelIndices};

pub mod cache;
pub mod constants;
pub mod controllers;
pub mod erc;
pub mod error;
pub mod executor;
pub mod model;
pub mod simple_broker;
pub mod utils;

use cache::{LocalCache, Model, ModelCache};
pub use torii_sqlite_types as types;

#[derive(Debug, Clone)]
pub struct SqlConfig {
    pub all_model_indices: bool,
    pub model_indices: Vec<ModelIndices>,
    pub historical_models: HashSet<Felt>,
    pub hooks: Vec<Hook>,
    pub max_metadata_tasks: usize,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            all_model_indices: false,
            model_indices: vec![],
            historical_models: HashSet::new(),
            hooks: vec![],
            max_metadata_tasks: 10,
        }
    }
}

impl SqlConfig {
    pub fn is_historical(&self, selector: &Felt) -> bool {
        self.historical_models.contains(selector)
    }
}

#[derive(Debug, Clone)]
pub struct Sql {
    pub pool: Pool<Sqlite>,
    pub executor: UnboundedSender<QueryMessage>,
    nft_metadata_semaphore: Arc<Semaphore>,
    model_cache: Arc<ModelCache>,
    local_cache: Arc<LocalCache>,
    pub config: SqlConfig,
}

#[derive(Default, Debug, Clone)]
pub struct Cursor {
    pub last_pending_block_tx: Option<Felt>,
    pub head: Option<u64>,
    pub last_block_timestamp: Option<u64>,
}

impl Sql {
    pub async fn new(
        pool: Pool<Sqlite>,
        executor: UnboundedSender<QueryMessage>,
        contracts: &[Contract],
        model_cache: Arc<ModelCache>,
    ) -> Result<Self, Error> {
        Self::new_with_config(pool, executor, contracts, model_cache, Default::default()).await
    }

    pub async fn new_with_config(
        pool: Pool<Sqlite>,
        executor: UnboundedSender<QueryMessage>,
        contracts: &[Contract],
        model_cache: Arc<ModelCache>,
        config: SqlConfig,
    ) -> Result<Self, Error> {
        for contract in contracts {
            executor.send(QueryMessage::other(
                "INSERT OR IGNORE INTO contracts (id, contract_address, contract_type) VALUES (?, \
                 ?, ?)"
                    .to_string(),
                vec![
                    Argument::FieldElement(contract.address),
                    Argument::FieldElement(contract.address),
                    Argument::String(contract.r#type.to_string()),
                ],
            )).map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
        }

        let local_cache = LocalCache::new(pool.clone()).await;
        let nft_metadata_semaphore = Arc::new(Semaphore::new(config.max_metadata_tasks));
        let db = Self {
            pool: pool.clone(),
            executor,
            model_cache,
            local_cache: Arc::new(local_cache),
            config,
            nft_metadata_semaphore,
        };

        db.execute().await?;

        Ok(db)
    }

    pub fn set_last_pending_block_contract_tx(
        &mut self,
        contract: Felt,
        last_pending_block_contract_tx: Option<Felt>,
    ) -> Result<(), Error> {
        let last_pending_block_contract_tx = if let Some(f) = last_pending_block_contract_tx {
            Argument::String(format!("{:#x}", f))
        } else {
            Argument::Null
        };

        let id = Argument::FieldElement(contract);

        self.executor
            .send(QueryMessage::other(
                "UPDATE contracts SET last_pending_block_contract_tx = ? WHERE id = ?".to_string(),
                vec![last_pending_block_contract_tx, id],
            ))
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        Ok(())
    }

    pub fn set_last_pending_block_tx(
        &mut self,
        last_pending_block_tx: Option<Felt>,
    ) -> Result<(), Error> {
        let last_pending_block_tx = if let Some(f) = last_pending_block_tx {
            Argument::String(format!("{:#x}", f))
        } else {
            Argument::Null
        };

        self.executor
            .send(QueryMessage::other(
                "UPDATE contracts SET last_pending_block_tx = ? WHERE 1=1".to_string(),
                vec![last_pending_block_tx],
            ))
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        Ok(())
    }

    pub async fn cursors(&self) -> Result<HashMap<Felt, Cursor>, Error> {
        let cursors = sqlx::query_as::<_, ContractCursor>("SELECT * FROM contracts")
            .fetch_all(&self.pool)
            .await?;

        let mut cursors_map = HashMap::new();
        for c in cursors {
            let contract_address = Felt::from_str(&c.contract_address)
                .map_err(|e| Error::Parse(ParseError::FromStr(e)))?;
            let last_pending_block_tx = c
                .last_pending_block_tx
                .map(|tx| Felt::from_str(&tx).map_err(|e| Error::Parse(ParseError::FromStr(e))))
                .transpose()?;
            let cursor = Cursor {
                last_pending_block_tx,
                head: c.head.map(|h| h as u64),
                last_block_timestamp: c.last_block_timestamp.map(|t| t as u64),
            };
            cursors_map.insert(contract_address, cursor);
        }
        Ok(cursors_map)
    }

    pub fn update_cursors(
        &mut self,
        cursors: HashMap<Felt, Cursor>,
        num_transactions: HashMap<Felt, u64>,
    ) -> Result<(), Error> {
        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::UpdateCursors(UpdateCursorsQuery {
                    cursors,
                    num_transactions,
                }),
            ))
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn register_model(
        &mut self,
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
    ) -> Result<(), Error> {
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
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        self.build_model_query(
            vec![namespaced_name.clone()],
            model,
            schema_diff,
            upgrade_diff,
        )?;

        // we set the model in the cache directly
        // because entities might be using it before the query queue is processed
        self.model_cache
            .set(
                selector,
                Model {
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
                        .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_entity(
        &self,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
        entity_id: Felt,
        model_selector: Felt,
        keys_str: Option<&str>,
    ) -> Result<(), Error> {
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
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        self.executor.send(QueryMessage::other(
            "INSERT INTO entity_model (entity_id, model_id) VALUES (?, ?) ON CONFLICT(entity_id, \
             model_id) DO NOTHING"
                .to_string(),
            vec![
                Argument::String(entity_id.clone()),
                Argument::String(model_id.clone()),
            ],
        )).map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

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
                        .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
                }
            }
        }

        Ok(())
    }

    pub async fn set_event_message(
        &mut self,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
    ) -> Result<(), Error> {
        let keys = if let Ty::Struct(s) = &entity {
            let mut keys = Vec::new();
            for m in s.keys() {
                keys.extend(m.serialize()?);
            }
            keys
        } else {
            return Err(Error::Parse(ParseError::InvalidTyEntity));
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
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

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
                        .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
                }
            }
        }

        Ok(())
    }

    pub async fn delete_entity(
        &mut self,
        entity_id: Felt,
        model_id: Felt,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
    ) -> Result<(), Error> {
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
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        for hook in self.config.hooks.iter() {
            if let HookEvent::ModelDeleted { model_tag } = &hook.event {
                if model_table == *model_tag {
                    self.executor
                        .send(QueryMessage::other(
                            hook.statement.clone(),
                            vec![Argument::String(entity_id.clone())],
                        ))
                        .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
                }
            }
        }

        Ok(())
    }

    pub fn set_metadata(
        &mut self,
        resource: &Felt,
        uri: &str,
        block_timestamp: u64,
    ) -> Result<(), Error> {
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
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        Ok(())
    }

    pub fn update_metadata(
        &mut self,
        resource: &Felt,
        uri: &str,
        metadata: &WorldMetadata,
        icon_img: &Option<String>,
        cover_img: &Option<String>,
    ) -> Result<(), Error> {
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
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        Ok(())
    }

    pub async fn model(&self, selector: Felt) -> Result<Model, Error> {
        self.model_cache.model(&selector).await
    }

    #[allow(clippy::too_many_arguments)]
    pub fn store_transaction(
        &mut self,
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
    ) -> Result<(), Error> {
        // Store the transaction in the transactions table
        self.executor
            .send(QueryMessage::new(
                "INSERT INTO transactions (id, transaction_hash, sender_address, calldata, \
             max_fee, signature, nonce, transaction_type, executed_at, block_number) VALUES (?, \
             ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING *"
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
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        Ok(())
    }

    pub fn store_event(
        &mut self,
        event_id: &str,
        event: &Event,
        transaction_hash: Felt,
        block_timestamp: u64,
    ) -> Result<(), Error> {
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
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        Ok(())
    }

    fn set_entity_model(
        &self,
        model_name: &str,
        event_id: &str,
        entity_id: &str,
        entity: &Ty,
        block_timestamp: u64,
    ) -> Result<(), Error> {
        let mut columns = vec![
            "internal_id".to_string(),
            "internal_event_id".to_string(),
            "internal_executed_at".to_string(),
            "internal_updated_at".to_string(),
            if entity_id.starts_with("event:") {
                "internal_event_message_id".to_string()
            } else {
                "internal_entity_id".to_string()
            },
        ];

        let mut arguments = vec![
            Argument::String(entity_id.to_string()),
            Argument::String(event_id.to_string()),
            Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
            Argument::String(chrono::Utc::now().to_rfc3339()),
            Argument::String(entity_id.trim_start_matches("event:").to_string()),
        ];

        fn collect_members(
            prefix: &str,
            ty: &Ty,
            columns: &mut Vec<String>,
            arguments: &mut Vec<Argument>,
        ) -> Result<(), Error> {
            match ty {
                Ty::Struct(s) => {
                    for member in &s.children {
                        let column_name = if prefix.is_empty() {
                            member.name.clone()
                        } else {
                            format!("{}.{}", prefix, member.name)
                        };
                        collect_members(&column_name, &member.ty, columns, arguments)?;
                    }
                }
                Ty::Enum(e) => {
                    columns.push(format!("\"{}\"", prefix));
                    arguments.push(Argument::String(e.to_sql_value()));

                    if let Some(option_idx) = e.option {
                        let option = &e.options[option_idx as usize];
                        if let Ty::Tuple(t) = &option.ty {
                            if t.is_empty() {
                                return Ok(());
                            }
                        }
                        let variant_path = format!("{}.{}", prefix, option.name);
                        collect_members(&variant_path, &option.ty, columns, arguments)?;
                    }
                }
                Ty::Tuple(t) => {
                    for (idx, member) in t.iter().enumerate() {
                        let column_name = if prefix.is_empty() {
                            format!("{}", idx)
                        } else {
                            format!("{}.{}", prefix, idx)
                        };
                        collect_members(&column_name, member, columns, arguments)?;
                    }
                }
                Ty::Array(array) => {
                    columns.push(format!("\"{}\"", prefix));
                    let values = array
                        .iter()
                        .map(|v| v.to_json_value())
                        .collect::<Result<Vec<_>, _>>()?;
                    arguments.push(Argument::String(
                        serde_json::to_string(&values)
                            .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?,
                    ));
                }
                Ty::Primitive(ty) => {
                    columns.push(format!("\"{}\"", prefix));
                    arguments.push(Argument::String(ty.to_sql_value()));
                }
                Ty::ByteArray(b) => {
                    columns.push(format!("\"{}\"", prefix));
                    arguments.push(Argument::String(b.clone()));
                }
            }
            Ok(())
        }

        // Collect all columns and arguments recursively
        collect_members("", entity, &mut columns, &mut arguments)?;

        // Try to insert first - this will only succeed if the entity doesn't exist
        let placeholders: Vec<&str> = arguments.iter().map(|_| "?").collect();
        let insert_statement = format!(
            "INSERT INTO [{}] ({}) VALUES ({}) ON CONFLICT(internal_id) DO UPDATE SET {}",
            model_name,
            columns.join(","),
            placeholders.join(","),
            columns
                .iter()
                .skip(1) // Skip internal_id which is the primary key
                .map(|col| format!("{0}=excluded.{0}", col))
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Execute the single query
        self.executor
            .send(QueryMessage::other(insert_statement, arguments))
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_model_query(
        &mut self,
        path: Vec<String>,
        model: &Ty,
        schema_diff: Option<&Ty>,
        upgrade_diff: Option<&Ty>,
    ) -> Result<(), Error> {
        let table_id = path[0].clone(); // Use only the root path component
        let mut columns = Vec::new();
        let mut indices = Vec::new();
        let mut alter_table_queries = Vec::new();

        // Start building the create table query with internal columns
        let mut create_table_query = format!(
            "CREATE TABLE IF NOT EXISTS [{table_id}] (internal_id TEXT NOT NULL PRIMARY KEY, \
             internal_event_id TEXT NOT NULL, internal_entity_id TEXT, internal_event_message_id \
             TEXT, "
        );

        indices.push(format!(
            "CREATE INDEX IF NOT EXISTS [idx_{table_id}_internal_entity_id] ON [{table_id}] \
             ([internal_entity_id]);"
        ));
        indices.push(format!(
            "CREATE INDEX IF NOT EXISTS [idx_{table_id}_internal_event_message_id] ON \
             [{table_id}] ([internal_event_message_id]);"
        ));

        // Recursively add columns for all nested type
        self.add_columns_recursive(
            &path,
            model,
            &mut columns,
            &mut alter_table_queries,
            &mut indices,
            &table_id,
            schema_diff,
            upgrade_diff,
            false,
        )?;

        // Add all columns to the create table query
        for column in columns {
            create_table_query.push_str(&format!("{}, ", column));
        }

        // Add internal timestamps
        create_table_query.push_str("internal_executed_at DATETIME NOT NULL, ");
        create_table_query
            .push_str("internal_created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ");
        create_table_query
            .push_str("internal_updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ");

        // Add foreign key constraints
        create_table_query.push_str("FOREIGN KEY (internal_entity_id) REFERENCES entities(id), ");
        create_table_query
            .push_str("FOREIGN KEY (internal_event_message_id) REFERENCES event_messages(id));");

        // Execute the queries
        if upgrade_diff.is_some() || schema_diff.is_some() {
            for alter_query in alter_table_queries {
                self.executor
                    .send(QueryMessage::other(alter_query, vec![]))
                    .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
            }
        } else {
            self.executor
                .send(QueryMessage::other(create_table_query, vec![]))
                .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
        }

        // Create indices
        for index_query in indices {
            self.executor
                .send(QueryMessage::other(index_query, vec![]))
                .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn add_columns_recursive(
        &self,
        path: &[String],
        ty: &Ty,
        columns: &mut Vec<String>,
        alter_table_queries: &mut Vec<String>,
        indices: &mut Vec<String>,
        table_id: &str,
        schema_diff: Option<&Ty>,
        upgrade_diff: Option<&Ty>,
        is_key: bool,
    ) -> Result<(), Error> {
        let column_prefix = if path.len() > 1 {
            path[1..].join(".")
        } else {
            String::new()
        };

        let add_index = |indices: &mut Vec<String>, name: &str| {
            let model_indices = self
                .config
                .model_indices
                .iter()
                .find(|m| m.model_tag == table_id);

            if model_indices.is_some_and(|m| m.fields.contains(&name.to_string()))
                || (model_indices.is_none() && (self.config.all_model_indices || is_key))
            {
                indices.push(format!(
                    "CREATE INDEX IF NOT EXISTS [idx_{table_id}_{name}] ON [{table_id}] ([{name}]);"
                ));
            }
        };

        let mut add_column = |name: &str, sql_type: &str, indices: &mut Vec<String>| {
            columns.push(format!("[{name}] {sql_type}"));
            add_index(indices, name);
        };

        let mut alter_column = |name: &str, sql_type: &str, indices: &mut Vec<String>| {
            alter_table_queries.push(format!(
                "ALTER TABLE [{table_id}] ADD COLUMN [{name}] {sql_type}"
            ));
            add_index(indices, name);
        };

        let modify_column = |alter_table_queries: &mut Vec<String>,
                             name: &str,
                             sql_type: &str,
                             sql_value: &str| {
            alter_table_queries.push(format!(
            "CREATE TEMPORARY TABLE [tmp_values_{name}] AS SELECT internal_id, [{name}] FROM [{table_id}]"
        ));
            alter_table_queries.push(format!("DROP INDEX IF EXISTS [idx_{table_id}_{name}]"));
            alter_table_queries.push(format!("ALTER TABLE [{table_id}] DROP COLUMN [{name}]"));
            alter_table_queries.push(format!(
                "ALTER TABLE [{table_id}] ADD COLUMN [{name}] {sql_type}"
            ));
            alter_table_queries.push(format!(
                "UPDATE [{table_id}] SET [{name}] = (SELECT {sql_value} FROM [tmp_values_{name}] \
             WHERE [tmp_values_{name}].internal_id = [{table_id}].internal_id)"
            ));
            alter_table_queries.push(format!("DROP TABLE [tmp_values_{name}]"));
            alter_table_queries.push(format!(
                "CREATE INDEX IF NOT EXISTS [idx_{table_id}_{name}] ON [{table_id}] ([{name}]);"
            ));
        };

        match ty {
            Ty::Struct(s) => {
                let struct_upgrade_diff = upgrade_diff.and_then(|d| d.as_struct());
                let struct_schema_diff = schema_diff.and_then(|d| d.as_struct());

                for member in &s.children {
                    let member_upgrade_diff = struct_upgrade_diff
                        .and_then(|diff| diff.children.iter().find(|m| m.name == member.name))
                        .map(|m| &m.ty);
                    let member_schema_diff = struct_schema_diff
                        .and_then(|diff| diff.children.iter().find(|m| m.name == member.name))
                        .map(|m| &m.ty);

                    let mut new_path = path.to_vec();
                    new_path.push(member.name.clone());

                    self.add_columns_recursive(
                        &new_path,
                        &member.ty,
                        columns,
                        alter_table_queries,
                        indices,
                        table_id,
                        member_schema_diff,
                        member_upgrade_diff,
                        member.key,
                    )?;
                }
            }
            Ty::Tuple(t) => {
                let tuple_upgrade_diff = upgrade_diff.and_then(|d| d.as_tuple());
                let tuple_schema_diff = schema_diff.and_then(|d| d.as_tuple());

                for (idx, member) in t.iter().enumerate() {
                    let member_upgrade_diff = tuple_upgrade_diff.and_then(|diff| diff.get(idx));
                    let member_schema_diff = tuple_schema_diff.and_then(|diff| diff.get(idx));

                    let mut new_path = path.to_vec();
                    new_path.push(idx.to_string());

                    self.add_columns_recursive(
                        &new_path,
                        member,
                        columns,
                        alter_table_queries,
                        indices,
                        table_id,
                        member_schema_diff,
                        member_upgrade_diff,
                        is_key,
                    )?;
                }
            }
            Ty::Array(_) => {
                let column_name = if column_prefix.is_empty() {
                    "value".to_string()
                } else {
                    column_prefix
                };

                if upgrade_diff.is_some() {
                    modify_column(
                        alter_table_queries,
                        &column_name,
                        "TEXT",
                        &format!("[{column_name}]"),
                    );
                } else if schema_diff.is_some() {
                    alter_column(&column_name, "TEXT", indices);
                } else {
                    add_column(&column_name, "TEXT", indices);
                }
            }
            Ty::Enum(e) => {
                let enum_upgrade_diff = upgrade_diff.and_then(|d| d.as_enum());
                let enum_schema_diff = schema_diff.and_then(|d| d.as_enum());

                let column_name = if column_prefix.is_empty() {
                    "option".to_string()
                } else {
                    column_prefix
                };

                let all_options = e
                    .options
                    .iter()
                    .map(|c| format!("'{}'", c.name))
                    .collect::<Vec<_>>()
                    .join(", ");

                let sql_type = format!(
                "TEXT CONSTRAINT [{column_name}_check] CHECK([{column_name}] IN ({all_options}))"
            );

                // If new variants of an enum are added, without the enum itself being added through an upgrade
                // to the model, then we should consider this as an upgrade. The reason is that for this specific
                // case, we only need to modify the column and its constraints. Not add it.
                if enum_upgrade_diff.is_some()
                    || (schema_diff.is_some() && schema_diff.unwrap() != ty)
                {
                    modify_column(
                        alter_table_queries,
                        &column_name,
                        &sql_type,
                        &format!("[{column_name}]"),
                    );
                } else if enum_schema_diff.is_some() {
                    // In the case where the enum is being added to the model, we need to add the column and its constraints
                    alter_column(&column_name, &sql_type, indices);
                } else {
                    // And in the default case where we have no upgrade at all (nor to the model, nor to the enum)
                    // just add the column and its constraints as part of the create query.
                    add_column(&column_name, &sql_type, indices);
                }

                for child in &e.options {
                    let variant_upgrade_diff = enum_upgrade_diff
                        .and_then(|diff| diff.options.iter().find(|v| v.name == child.name))
                        .map(|v| &v.ty);
                    let variant_schema_diff = enum_schema_diff
                        .and_then(|diff| diff.options.iter().find(|v| v.name == child.name))
                        .map(|v| &v.ty);

                    if let Ty::Tuple(tuple) = &child.ty {
                        if tuple.is_empty() {
                            continue;
                        }
                    }

                    let mut new_path = path.to_vec();
                    new_path.push(child.name.clone());

                    self.add_columns_recursive(
                        &new_path,
                        &child.ty,
                        columns,
                        alter_table_queries,
                        indices,
                        table_id,
                        variant_schema_diff,
                        variant_upgrade_diff,
                        is_key,
                    )?;
                }
            }
            Ty::ByteArray(_) => {
                let column_name = if column_prefix.is_empty() {
                    "value".to_string()
                } else {
                    column_prefix
                };

                if upgrade_diff.is_some() {
                    modify_column(
                        alter_table_queries,
                        &column_name,
                        "TEXT",
                        &format!("[{column_name}]"),
                    );
                } else if schema_diff.is_some() {
                    alter_column(&column_name, "TEXT", indices);
                } else {
                    add_column(&column_name, "TEXT", indices);
                }
            }
            Ty::Primitive(p) => {
                let column_name = if column_prefix.is_empty() {
                    "value".to_string()
                } else {
                    column_prefix
                };

                if let Some(upgrade_diff) = upgrade_diff {
                    if let Some(old_primitive) = upgrade_diff.as_primitive() {
                        let sql_value = if old_primitive.to_sql_type() == SqlType::Integer
                            && p.to_sql_type() == SqlType::Text
                        {
                            format!("'0x' || printf('%064x', [{column_name}])")
                        } else {
                            format!("[{column_name}]")
                        };
                        modify_column(
                            alter_table_queries,
                            &column_name,
                            p.to_sql_type().as_ref(),
                            &sql_value,
                        );
                    }
                } else if schema_diff.is_some() {
                    alter_column(&column_name, p.to_sql_type().as_ref(), indices);
                } else {
                    add_column(&column_name, p.to_sql_type().as_ref(), indices);
                }
            }
        }

        Ok(())
    }

    pub async fn execute(&self) -> Result<(), Error> {
        let (execute, recv) = QueryMessage::execute_recv();
        self.executor
            .send(execute)
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
        let res = recv
            .await
            .map_err(|e| Error::Executor(ExecutorError::RecvError(e)))?;
        res.map_err(Error::Executor)
    }

    pub async fn rollback(&self) -> Result<(), Error> {
        let (rollback, recv) = QueryMessage::rollback_recv();
        self.executor
            .send(rollback)
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;
        let res = recv
            .await
            .map_err(|e| Error::Executor(ExecutorError::RecvError(e)))?;
        res.map_err(Error::Executor)
    }

    pub async fn add_controller(
        &self,
        username: &str,
        address: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<(), Error> {
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
            .map_err(|e| Error::Executor(ExecutorError::SendError(e)))?;

        Ok(())
    }
}
