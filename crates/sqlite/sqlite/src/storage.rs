use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dojo_types::{naming::compute_selector_from_names, schema::Ty};
use dojo_world::{config::WorldMetadata, contracts::abigen::model::Layout};
use sqlx::sqlite::SqliteRow;
use starknet::core::types::U256;
use starknet_crypto::{poseidon_hash_many, Felt};
use torii_math::I256;
use torii_proto::{
    schema::Entity, Clause, CompositeClause, Controller, Event, EventQuery, LogicalOperator, Model,
    Page, Query, Token, TokenBalance, TokenCollection,
};
use torii_sqlite_types::{ContractCursor, HookEvent, Model as SQLModel};
use torii_storage::{
    types::{Cursor, ParsedCall},
    ReadOnlyStorage, Storage, StorageError,
};
use tracing::warn;

use crate::{
    constants::{
        ENTITIES_ENTITY_RELATION_COLUMN, ENTITIES_HISTORICAL_TABLE, ENTITIES_MODEL_RELATION_TABLE,
        ENTITIES_TABLE, EVENT_MESSAGES_ENTITY_RELATION_COLUMN, EVENT_MESSAGES_HISTORICAL_TABLE,
        EVENT_MESSAGES_MODEL_RELATION_TABLE, EVENT_MESSAGES_TABLE, TOKEN_TRANSFER_TABLE,
    },
    cursor::{decode_cursor, encode_cursor},
    executor::{RegisterErc20TokenQuery, RegisterNftTokenQuery},
    model::map_row_to_ty,
    utils::{build_keys_pattern, u256_to_sql_string},
};
use crate::{
    error::{Error, ParseError},
    executor::{
        erc::UpdateNftMetadataQuery, error::ExecutorQueryError, ApplyBalanceDiffQuery, Argument,
        DeleteEntityQuery, EntityQuery, EventMessageQuery, QueryMessage, QueryType,
        StoreTransactionQuery, UpdateCursorsQuery,
    },
    utils::{
        felt_and_u256_to_sql_string, felt_to_sql_string, felts_to_sql_string,
        utc_dt_string_from_timestamp,
    },
    Sql,
};

pub const LOG_TARGET: &str = "torii::sqlite::storage";

#[async_trait]
impl ReadOnlyStorage for Sql {
    fn as_read_only(&self) -> &dyn ReadOnlyStorage {
        self
    }

    /// Returns the cursors for all contracts.
    async fn cursors(&self) -> Result<HashMap<Felt, Cursor>, StorageError> {
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
                tps: c.tps.map(|t| t as u64),
            };
            cursors_map.insert(contract_address, cursor);
        }
        Ok(cursors_map)
    }

    /// Returns the model metadata for the storage.
    async fn model(&self, selector: Felt) -> Result<Model, StorageError> {
        if let Some(cache) = &self.cache {
            if let Ok(model) = cache.model(selector).await {
                return Ok(model);
            } else {
                warn!(
                    target: LOG_TARGET,
                    model_selector = %format!("{:#x}", selector),
                    "Failed to get model from cache, falling back to database."
                );
            }
        }

        let model = sqlx::query_as::<_, SQLModel>("SELECT * FROM models WHERE id = ?")
            .bind(format!("{:#x}", selector))
            .fetch_one(&self.pool)
            .await?;

        let layout = serde_json::from_str(&model.layout)
            .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?;
        let schema = serde_json::from_str(&model.schema)
            .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?;

        let model_metadata = Model {
            selector: Felt::from_str(&model.id)?,
            name: model.name,
            namespace: model.namespace,
            schema,
            packed_size: model.packed_size,
            unpacked_size: model.unpacked_size,
            class_hash: Felt::from_str(&model.class_hash)?,
            contract_address: Felt::from_str(&model.contract_address)?,
            layout,
        };
        Ok(model_metadata)
    }

    /// Returns the models for the storage.
    /// If selectors is empty, returns all models.
    async fn models(&self, selectors: &[Felt]) -> Result<Vec<Model>, StorageError> {
        if let Some(cache) = &self.cache {
            if let Ok(models) = cache.models(selectors).await {
                return Ok(models);
            } else {
                warn!(
                    target: LOG_TARGET,
                    selectors = ?selectors.iter().map(|s| format!("{:#x}", s)).collect::<Vec<_>>(),
                    "Failed to get models from cache, falling back to database.",
                );
            }
        }

        let mut query = "SELECT * FROM models".to_string();
        let mut bind_values = vec![];
        if !selectors.is_empty() {
            let placeholders = vec!["?"; selectors.len()].join(", ");
            query += &format!(" WHERE id IN ({})", placeholders);
            bind_values.extend(selectors.iter().map(|s| format!("{:#x}", s)));
        }

        let mut query = sqlx::query_as::<_, SQLModel>(&query);
        for value in bind_values {
            query = query.bind(value);
        }
        let models = query.fetch_all(&self.pool).await?;

        let mut models_metadata = Vec::with_capacity(models.len());
        for model in models {
            let layout = serde_json::from_str(&model.layout)
                .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?;
            let schema = serde_json::from_str(&model.schema)
                .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?;

            let model_metadata = Model {
                selector: Felt::from_str(&model.id)?,
                name: model.name,
                namespace: model.namespace,
                schema,
                packed_size: model.packed_size,
                unpacked_size: model.unpacked_size,
                class_hash: Felt::from_str(&model.class_hash)?,
                contract_address: Felt::from_str(&model.contract_address)?,
                layout,
            };

            models_metadata.push(model_metadata);
        }

        Ok(models_metadata)
    }

    async fn token_ids(&self) -> Result<HashSet<String>, StorageError> {
        let token_ids = sqlx::query_scalar::<_, String>("SELECT id FROM tokens")
            .fetch_all(&self.pool)
            .await?;
        Ok(token_ids.into_iter().collect())
    }

    /// Returns the controllers for the storage.
    async fn controllers(
        &self,
        contract_addresses: &[Felt],
        usernames: &[String],
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<Page<Controller>, StorageError> {
        let mut query =
            "SELECT address, username, deployed_at FROM controllers WHERE 1=1".to_string();
        let mut args = vec![];

        if !usernames.is_empty() {
            let placeholders = vec!["?"; usernames.len()].join(", ");
            query += &format!(" AND id IN ({})", placeholders);
            args.extend(usernames.iter().cloned());
        }

        if !contract_addresses.is_empty() {
            let placeholders = vec!["?"; contract_addresses.len()].join(", ");
            query += &format!(" AND address IN ({})", placeholders);
            args.extend(contract_addresses.iter().map(|a| format!("{:#064x}", a)));
        }

        if let Some(cursor) = cursor {
            query += " AND deployed_at < ?";
            args.push(decode_cursor(&cursor)?);
        }

        query += " ORDER BY deployed_at DESC";

        if let Some(limit) = limit {
            query += " LIMIT ?";
            args.push((limit + 1).to_string());
        }

        let mut query = sqlx::query_as::<_, torii_sqlite_types::Controller>(&query);
        for arg in args {
            query = query.bind(arg);
        }

        let mut controllers = query.fetch_all(&self.pool).await?;
        let next_cursor = if limit.is_some() && controllers.len() > limit.unwrap() {
            Some(encode_cursor(
                &controllers.pop().unwrap().deployed_at.to_rfc3339(),
            )?)
        } else {
            None
        };

        Ok(Page {
            items: controllers.into_iter().map(|c| c.into()).collect(),
            next_cursor,
        })
    }

    async fn tokens(
        &self,
        contract_addresses: &[Felt],
        token_ids: &[U256],
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<Page<Token>, StorageError> {
        let mut query = "SELECT * FROM tokens".to_string();
        let mut bind_values = Vec::new();
        let mut conditions = Vec::new();

        if !contract_addresses.is_empty() {
            let placeholders = vec!["?"; contract_addresses.len()].join(", ");
            conditions.push(format!("contract_address IN ({})", placeholders));
            bind_values.extend(contract_addresses.iter().map(|addr| format!("{:#x}", addr)));
        }
        if !token_ids.is_empty() {
            let placeholders = vec!["?"; token_ids.len()].join(", ");
            conditions.push(format!("token_id IN ({})", placeholders));
            bind_values.extend(token_ids.iter().map(u256_to_sql_string));
        }

        if let Some(cursor) = cursor {
            bind_values.push(decode_cursor(&cursor)?);
            conditions.push("id >= ?".to_string());
        }

        if !conditions.is_empty() {
            query += &format!(" WHERE {}", conditions.join(" AND "));
        }

        query += " ORDER BY id";

        if let Some(limit) = limit {
            query += " LIMIT ?";
            bind_values.push((limit + 1).to_string());
        }

        let mut query = sqlx::query_as(&query);
        for value in bind_values {
            query = query.bind(value);
        }

        let mut tokens: Vec<torii_sqlite_types::Token> = query.fetch_all(&self.pool).await?;
        let next_cursor = if limit.is_some() && tokens.len() > limit.unwrap() {
            Some(encode_cursor(&tokens.pop().unwrap().id)?)
        } else {
            None
        };

        let tokens = tokens.into_iter().map(|token| token.into()).collect();
        Ok(Page {
            items: tokens,
            next_cursor,
        })
    }

    async fn token_balances(
        &self,
        account_addresses: &[Felt],
        contract_addresses: &[Felt],
        token_ids: &[U256],
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<Page<TokenBalance>, StorageError> {
        let mut query = "SELECT * FROM token_balances".to_string();
        let mut bind_values = Vec::new();
        let mut conditions = Vec::new();

        if !account_addresses.is_empty() {
            let placeholders = vec!["?"; account_addresses.len()].join(", ");
            conditions.push(format!("account_address IN ({})", placeholders));
            bind_values.extend(account_addresses.iter().map(|addr| format!("{:#x}", addr)));
        }

        if !contract_addresses.is_empty() {
            let placeholders = vec!["?"; contract_addresses.len()].join(", ");
            conditions.push(format!("contract_address IN ({})", placeholders));
            bind_values.extend(contract_addresses.iter().map(|addr| format!("{:#x}", addr)));
        }

        if !token_ids.is_empty() {
            let placeholders = vec!["?"; token_ids.len()].join(", ");
            conditions.push(format!(
                "SUBSTR(token_id, INSTR(token_id, ':') + 1) IN ({})",
                placeholders
            ));
            bind_values.extend(token_ids.iter().map(u256_to_sql_string));
        }

        if let Some(cursor) = cursor {
            bind_values.push(decode_cursor(&cursor)?);
            conditions.push("id >= ?".to_string());
        }

        if !conditions.is_empty() {
            query += &format!(" WHERE {}", conditions.join(" AND "));
        }

        query += " ORDER BY id";
        if let Some(limit) = limit {
            query += " LIMIT ?";
            bind_values.push((limit + 1).to_string());
        }

        let mut query = sqlx::query_as(&query);
        for value in bind_values {
            query = query.bind(value);
        }

        let mut balances: Vec<torii_sqlite_types::TokenBalance> =
            query.fetch_all(&self.pool).await?;
        let next_cursor = if limit.is_some() && balances.len() > limit.unwrap() {
            Some(encode_cursor(&balances.pop().unwrap().id)?)
        } else {
            None
        };

        let balances = balances.into_iter().map(|balance| balance.into()).collect();
        Ok(Page {
            items: balances,
            next_cursor,
        })
    }

    async fn token_collections(
        &self,
        account_addresses: &[Felt],
        contract_addresses: &[Felt],
        token_ids: &[U256],
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<Page<TokenCollection>, StorageError> {
        let mut query =
            "SELECT t.contract_address as contract_address, t.name as name, t.symbol as symbol, t.decimals as decimals, t.metadata as metadata, count(t.contract_address) as count FROM tokens t".to_owned();

        let mut bind_values = Vec::new();
        let mut conditions = Vec::new();

        if !account_addresses.is_empty() {
            query += "  JOIN token_balances tb ON tb.token_id = CONCAT(t.contract_address, ':', t.token_id)";

            let placeholders = vec!["?"; account_addresses.len()].join(", ");
            conditions.push(format!("tb.account_address IN ({})", placeholders));
            bind_values.extend(account_addresses.iter().map(|addr| format!("{:#x}", addr)));
        }

        if !contract_addresses.is_empty() {
            let placeholders = vec!["?"; contract_addresses.len()].join(", ");
            conditions.push(format!("t.contract_address IN ({})", placeholders));
            bind_values.extend(contract_addresses.iter().map(|addr| format!("{:#x}", addr)));
        }
        if !token_ids.is_empty() {
            let placeholders = vec!["?"; token_ids.len()].join(", ");
            conditions.push(format!("t.token_id IN ({})", placeholders));
            bind_values.extend(token_ids.iter().map(u256_to_sql_string));
        }

        if let Some(cursor) = cursor {
            bind_values.push(decode_cursor(&cursor)?);
            conditions.push("t.id >= ?".to_string());
        }

        if !conditions.is_empty() {
            query += &format!(" WHERE {}", conditions.join(" AND "));
        }

        query += " GROUP BY t.contract_address ORDER BY t.id";
        if let Some(limit) = limit {
            query += " LIMIT ?";
            bind_values.push((limit + 1).to_string());
        }

        let mut query = sqlx::query_as(&query);
        for value in bind_values {
            query = query.bind(value);
        }

        let mut tokens: Vec<torii_sqlite_types::TokenCollection> =
            query.fetch_all(&self.pool).await?;
        let next_cursor = if limit.is_some() && tokens.len() > limit.unwrap() {
            Some(encode_cursor(&tokens.pop().unwrap().contract_address)?)
        } else {
            None
        };

        let tokens = tokens.into_iter().map(|token| token.into()).collect();
        Ok(Page {
            items: tokens,
            next_cursor,
        })
    }

    async fn events(&self, query: EventQuery) -> Result<Page<Event>, StorageError> {
        let mut bind_values = Vec::new();
        let mut conditions = Vec::new();

        let keys_pattern = if let Some(keys) = &query.keys {
            build_keys_pattern(keys)
        } else {
            "".to_string()
        };

        if !keys_pattern.is_empty() {
            conditions.push("keys REGEXP ?");
            bind_values.push(keys_pattern);
        }

        if let Some(cursor) = query.cursor {
            conditions.push("id >= ?");
            bind_values.push(decode_cursor(&cursor)?);
        }

        let mut events_query = r#"
            SELECT id, keys, data, transaction_hash, executed_at, created_at
            FROM events
        "#
        .to_string();

        if !conditions.is_empty() {
            events_query = format!("{} WHERE {}", events_query, conditions.join(" AND "));
        }

        events_query = format!("{} ORDER BY id", events_query);
        if let Some(limit) = query.limit {
            events_query += &format!(" LIMIT {}", limit);
        }

        let mut row_events = sqlx::query_as(&events_query);
        for value in &bind_values {
            row_events = row_events.bind(value);
        }
        let mut row_events: Vec<torii_sqlite_types::Event> =
            row_events.fetch_all(&self.pool).await?;

        let next_cursor = if query.limit.is_some() && row_events.len() > query.limit.unwrap() {
            Some(encode_cursor(&row_events.pop().unwrap().id)?)
        } else {
            None
        };

        let events = row_events.into_iter().map(|event| event.into()).collect();

        Ok(Page {
            items: events,
            next_cursor,
        })
    }

    /// Queries the entities from the storage.
    async fn entities(&self, query: &Query) -> Result<Page<Entity>, StorageError> {
        // Map other clauses to a composite clause
        let composite = match &query.clause {
            Some(Clause::Composite(composite)) => composite.clone(),
            _ => {
                let mut composite = CompositeClause {
                    operator: LogicalOperator::And,
                    clauses: vec![],
                };
                if let Some(clause) = &query.clause {
                    composite.clauses.push(clause.clone());
                }
                composite
            }
        };

        let table = if query.historical {
            ENTITIES_HISTORICAL_TABLE
        } else {
            ENTITIES_TABLE
        };
        let model_relation_table = ENTITIES_MODEL_RELATION_TABLE;
        let entity_relation_column = ENTITIES_ENTITY_RELATION_COLUMN;

        let page = self
            .query_by_composite(
                table,
                model_relation_table,
                entity_relation_column,
                &composite,
                query.pagination.clone(),
                query.no_hashed_keys,
                query.models.clone(),
                query.historical,
            )
            .await?;

        Ok(page)
    }

    /// Queries the event messages from the storage.
    async fn event_messages(&self, query: &Query) -> Result<Page<Entity>, StorageError> {
        // Map other clauses to a composite clause
        let composite = match &query.clause {
            Some(Clause::Composite(composite)) => composite.clone(),
            _ => {
                let mut composite = CompositeClause {
                    operator: LogicalOperator::And,
                    clauses: vec![],
                };
                if let Some(clause) = &query.clause {
                    composite.clauses.push(clause.clone());
                }
                composite
            }
        };

        let table = if query.historical {
            EVENT_MESSAGES_HISTORICAL_TABLE
        } else {
            EVENT_MESSAGES_TABLE
        };
        let model_relation_table = EVENT_MESSAGES_MODEL_RELATION_TABLE;
        let entity_relation_column = EVENT_MESSAGES_ENTITY_RELATION_COLUMN;

        let page = self
            .query_by_composite(
                table,
                model_relation_table,
                entity_relation_column,
                &composite,
                query.pagination.clone(),
                query.no_hashed_keys,
                query.models.clone(),
                query.historical,
            )
            .await?;

        Ok(page)
    }

    /// Returns the model data of an entity.
    async fn entity_model(
        &self,
        entity_id: Felt,
        model_selector: Felt,
    ) -> Result<Option<Ty>, StorageError> {
        let mut schema = self.model(model_selector).await?.schema;
        let query = format!("SELECT * FROM [{}] WHERE internal_id = ?", schema.name());
        let mut query = sqlx::query(&query);
        query = query.bind(format!("{:#x}", entity_id));
        let row: Option<SqliteRow> = query.fetch_optional(&self.pool).await?;
        match row {
            Some(row) => {
                map_row_to_ty("", "", &mut schema, &row)?;
                Ok(Some(schema))
            }
            None => Ok(None),
        }
    }
}

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
        selector: Felt,
        model: &Ty,
        layout: &Layout,
        class_hash: Felt,
        contract_address: Felt,
        packed_size: u32,
        unpacked_size: u32,
        block_timestamp: u64,
        schema_diff: Option<&Ty>,
        upgrade_diff: Option<&Ty>,
    ) -> Result<(), StorageError> {
        let namespaced_name = model.name();
        let (namespace, name) = namespaced_name.split_once('-').unwrap();

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
            Argument::String(name.to_string()),
            Argument::FieldElement(class_hash),
            Argument::FieldElement(contract_address),
            Argument::String(
                serde_json::to_string(&layout)
                    .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?,
            ),
            Argument::String(
                serde_json::to_string(&model)
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
        keys: Option<Vec<Felt>>,
    ) -> Result<(), StorageError> {
        let namespaced_name = entity.name();

        let entity_id = format!("{:#x}", entity_id);
        let model_id = format!("{:#x}", model_selector);

        let keys_str = keys.map(|keys| felts_to_sql_string(&keys));

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

        if let Some(keys) = keys_str.clone() {
            arguments.push(Argument::String(keys));
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
                    keys_str: keys_str.clone(),
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
    async fn set_metadata(
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
    async fn update_metadata(
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
    async fn store_transaction(
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
    async fn store_event(
        &self,
        event_id: &str,
        event: &starknet::core::types::Event,
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

    /// Registers an ERC20 token with the storage.
    async fn register_erc20_token(
        &self,
        contract_address: Felt,
        name: String,
        symbol: String,
        decimals: u8,
    ) -> Result<(), StorageError> {
        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::RegisterErc20Token(RegisterErc20TokenQuery {
                    contract_address,
                    name,
                    symbol,
                    decimals,
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;
        Ok(())
    }

    /// Registers an NFT token with the storage.
    async fn register_nft_token(
        &self,
        contract_address: Felt,
        token_id: U256,
        metadata: String,
    ) -> Result<(), StorageError> {
        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::RegisterNftToken(RegisterNftTokenQuery {
                    contract_address,
                    token_id,
                    metadata,
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }

    /// Updates NFT metadata for a specific token.
    async fn update_nft_metadata(
        &self,
        contract_address: Felt,
        token_id: U256,
        metadata: String,
    ) -> Result<(), StorageError> {
        let id = felt_and_u256_to_sql_string(&contract_address, &token_id);

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

    /// Stores an ERC transfer event with the storage.
    #[allow(clippy::too_many_arguments)]
    async fn store_erc_transfer_event(
        &self,
        contract_address: Felt,
        from: Felt,
        to: Felt,
        amount: U256,
        token_id: Option<U256>,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<(), StorageError> {
        let token_id = if let Some(token_id) = token_id {
            felt_and_u256_to_sql_string(&contract_address, &token_id)
        } else {
            felt_to_sql_string(&contract_address)
        };

        let id = format!("{}:{}", event_id, token_id);

        let insert_query = format!(
            "INSERT INTO {TOKEN_TRANSFER_TABLE} (id, contract_address, from_address, to_address, \
             amount, token_id, event_id, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"
        );

        self.executor
            .send(QueryMessage::new(
                insert_query.to_string(),
                vec![
                    Argument::String(id),
                    Argument::FieldElement(contract_address),
                    Argument::FieldElement(from),
                    Argument::FieldElement(to),
                    Argument::String(u256_to_sql_string(&amount)),
                    Argument::String(token_id.to_string()),
                    Argument::String(event_id.to_string()),
                    Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
                ],
                QueryType::Other,
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }

    /// Applies cached balance differences to the storage.
    async fn apply_balances_diff(
        &self,
        balances_diff: HashMap<String, I256>,
        cursors: HashMap<Felt, Cursor>,
    ) -> Result<(), StorageError> {
        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::ApplyBalanceDiff(ApplyBalanceDiffQuery {
                    balances_diff,
                    cursors,
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;
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
