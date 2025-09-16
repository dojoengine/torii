use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use cainome::cairo_serde::{ByteArray, CairoSerde};
use dojo_types::schema::{Struct, Ty};
use erc::{update_contract_traits_from_metadata, UpdateTokenMetadataQuery};
use metrics::{counter, histogram};
use serde_json;
use sqlx::{Executor as SqlxExecutor, FromRow, Pool, Sqlite, Transaction as SqlxTransaction};
use starknet::core::types::requests::CallRequest;
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall, U256};
use starknet::core::utils::parse_cairo_short_string;
use starknet::macros::selector;
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::time::Instant;
use torii_broker::types::{
    ContractUpdate, EntityUpdate, EventMessageUpdate, EventUpdate, InnerType, ModelUpdate,
    TokenBalanceUpdate, TokenTransferUpdate, TokenUpdate, TransactionUpdate, Update,
};
use torii_math::I256;
use torii_proto::{ContractCursor, TransactionCall};
use torii_sqlite_types::TokenTransfer as SQLTokenTransfer;
use tracing::{debug, error, info, warn};

use crate::constants::TOKENS_TABLE;
use crate::error::ParseError;
use crate::executor::error::{ExecutorError, ExecutorQueryError};
use crate::utils::{
    felt_and_u256_to_sql_string, felt_to_sql_string, felts_to_sql_string, u256_to_sql_string,
};
use torii_broker::MemoryBroker;

pub mod erc;
pub mod error;
pub use erc::{RegisterNftTokenQuery, RegisterTokenContractQuery};

pub(crate) const LOG_TARGET: &str = "torii::sqlite::executor";

pub type Result<T> = std::result::Result<T, ExecutorError>;
pub type QueryResult<T> = std::result::Result<T, ExecutorQueryError>;

#[derive(Debug, Clone)]
pub enum Argument {
    Null,
    Int(i64),
    Bool(bool),
    String(String),
    FieldElement(Felt),
}

#[derive(Debug, Clone)]
pub enum BrokerMessage {
    ContractUpdate(<ContractUpdate as InnerType>::Inner),
    ModelRegistered(<ModelUpdate as InnerType>::Inner),
    EntityUpdate(<EntityUpdate as InnerType>::Inner),
    EventMessageUpdate(<EventMessageUpdate as InnerType>::Inner),
    EventEmitted(<EventUpdate as InnerType>::Inner),
    TokenRegistered(<TokenUpdate as InnerType>::Inner),
    TokenBalanceUpdated(<TokenBalanceUpdate as InnerType>::Inner),
    TokenTransfer(<TokenTransferUpdate as InnerType>::Inner),
    Transaction(<TransactionUpdate as InnerType>::Inner),
}

#[derive(Debug, Clone)]
pub struct DeleteEntityQuery {
    pub entity_id: String,
    pub model_id: String,
    pub event_id: String,
    pub block_timestamp: String,
    pub ty: Ty,
}

#[derive(Debug, Clone)]
pub struct ApplyBalanceDiffQuery {
    pub balances_diff: HashMap<String, I256>,
    pub total_supply_diff: HashMap<String, I256>,
    pub cursors: HashMap<Felt, ContractCursor>,
}

#[derive(Debug, Clone)]
pub struct EventMessageQuery {
    pub entity_id: String,
    pub model_id: String,
    pub keys_str: String,
    pub event_id: String,
    pub block_timestamp: String,
    pub is_historical: bool,
    pub ty: Ty,
}

#[derive(Debug, Clone)]
pub struct StoreTransactionQuery {
    pub contract_addresses: HashSet<Felt>,
    pub calls: Vec<TransactionCall>,
    pub unique_models: HashSet<Felt>,
}

#[derive(Debug, Clone)]
pub struct EntityQuery {
    pub entity_id: String,
    pub model_id: String,
    pub keys_str: Option<String>,
    pub event_id: String,
    pub block_timestamp: String,
    pub is_historical: bool,
    pub ty: Ty,
}

#[derive(Debug, Clone)]
pub struct UpdateCursorsQuery {
    pub cursors: HashMap<Felt, ContractCursor>,
    pub cursor_transactions: HashMap<Felt, HashSet<Felt>>,
}

#[derive(Debug, Clone)]
pub enum QueryType {
    StoreTransaction(StoreTransactionQuery),
    UpdateCursors(UpdateCursorsQuery),
    SetEntity(EntityQuery),
    DeleteEntity(DeleteEntityQuery),
    EventMessage(EventMessageQuery),
    ApplyBalanceDiff(ApplyBalanceDiffQuery),
    RegisterNftToken(RegisterNftTokenQuery),
    RegisterTokenContract(RegisterTokenContractQuery),
    RegisterModel,
    RegisterContract,
    StoreEvent,
    StoreTokenTransfer,
    UpdateTokenMetadata(UpdateTokenMetadataQuery),
    Execute,
    Rollback,
    Other,
}

impl std::fmt::Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                QueryType::StoreTransaction(_) => "StoreTransaction",
                QueryType::UpdateCursors(_) => "UpdateCursors",
                QueryType::SetEntity(_) => "SetEntity",
                QueryType::DeleteEntity(_) => "DeleteEntity",
                QueryType::EventMessage(_) => "EventMessage",
                QueryType::ApplyBalanceDiff(_) => "ApplyBalanceDiff",
                QueryType::RegisterNftToken(_) => "RegisterNftToken",
                QueryType::RegisterTokenContract(_) => "RegisterTokenContract",
                QueryType::RegisterModel => "RegisterModel",
                QueryType::RegisterContract => "RegisterContract",
                QueryType::StoreEvent => "StoreEvent",
                QueryType::StoreTokenTransfer => "StoreTokenTransfer",
                QueryType::UpdateTokenMetadata(_) => "UpdateTokenMetadata",
                QueryType::Execute => "Execute",
                QueryType::Rollback => "Rollback",
                QueryType::Other => "Other",
            }
        )
    }
}

#[derive(Debug)]
pub struct Executor<'c, P: Provider + Sync + Send + Clone + 'static> {
    // Queries should use `transaction` instead of `pool`
    // This `pool` is only used to create a new `transaction`
    pool: Pool<Sqlite>,
    transaction: Option<SqlxTransaction<'c, Sqlite>>,
    publish_queue: Vec<BrokerMessage>,
    rx: UnboundedReceiver<QueryMessage>,
    shutdown_rx: Receiver<()>,
    // It is used to make RPC calls to fetch erc contracts
    provider: P,
}

#[derive(Debug)]
pub struct QueryMessage {
    pub statement: String,
    pub arguments: Vec<Argument>,
    pub query_type: QueryType,
    tx: Option<oneshot::Sender<QueryResult<()>>>,
}

impl QueryMessage {
    pub fn new(statement: String, arguments: Vec<Argument>, query_type: QueryType) -> Self {
        Self {
            statement,
            arguments,
            query_type,
            tx: None,
        }
    }

    pub fn new_recv(
        statement: String,
        arguments: Vec<Argument>,
        query_type: QueryType,
    ) -> (Self, oneshot::Receiver<QueryResult<()>>) {
        let (tx, rx) = oneshot::channel();
        (
            QueryMessage {
                statement,
                arguments,
                query_type,
                tx: Some(tx),
            },
            rx,
        )
    }

    pub fn other(statement: String, arguments: Vec<Argument>) -> Self {
        QueryMessage::new(statement, arguments, QueryType::Other)
    }

    pub fn other_recv(
        statement: String,
        arguments: Vec<Argument>,
    ) -> (Self, oneshot::Receiver<QueryResult<()>>) {
        QueryMessage::new_recv(statement, arguments, QueryType::Other)
    }

    pub fn execute() -> Self {
        QueryMessage::new("".to_string(), vec![], QueryType::Execute)
    }

    pub fn execute_recv() -> (Self, oneshot::Receiver<QueryResult<()>>) {
        QueryMessage::new_recv("".to_string(), vec![], QueryType::Execute)
    }

    pub fn rollback_recv() -> (Self, oneshot::Receiver<QueryResult<()>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                statement: "".to_string(),
                arguments: vec![],
                query_type: QueryType::Rollback,
                tx: Some(tx),
            },
            rx,
        )
    }
}

impl<P: Provider + Sync + Send + Clone + 'static> Executor<'_, P> {
    pub async fn new(
        pool: Pool<Sqlite>,
        shutdown_tx: Sender<()>,
        provider: P,
    ) -> Result<(Self, UnboundedSender<QueryMessage>)> {
        let (tx, rx) = unbounded_channel();
        let transaction = pool.begin().await?;
        let publish_queue = Vec::new();
        let shutdown_rx = shutdown_tx.subscribe();

        Ok((
            Executor {
                pool,
                transaction: Some(transaction),
                publish_queue,
                rx,
                shutdown_rx,
                provider,
            },
            tx,
        ))
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    debug!(target: LOG_TARGET, "Shutting down executor");
                    break Ok(());
                }
                Some(mut msg) = self.rx.recv() => {
                    let query_type = msg.query_type.clone();
                    let statement = msg.statement.clone();
                    let arguments = msg.arguments.clone();
                    let tx = msg.tx.take();
                    let res = self.handle_query_message(msg).await;

                    if let Err(e) = &res {
                        error!(target: LOG_TARGET, r#type = %query_type, error = ?e, "Failed to execute query.");
                        debug!(target: LOG_TARGET, query = ?statement, arguments = ?arguments, query_type = ?query_type, "Failed to execute query.");
                    }

                    if let Some(tx) = tx {
                        if let Err(e) = tx.send(res) {
                            error!(target: LOG_TARGET, error = ?e, "Failed to send query result.");
                        }
                    }
                }
            }
        }
    }

    async fn handle_query_message(&mut self, query_message: QueryMessage) -> QueryResult<()> {
        let start_time = Instant::now();
        let query_type_str = format!("{}", query_message.query_type);

        let tx = self.transaction.as_mut().unwrap();

        let mut query = sqlx::query(&query_message.statement);

        for arg in &query_message.arguments {
            query = match arg {
                Argument::Null => query.bind(None::<String>),
                Argument::Int(integer) => query.bind(integer),
                Argument::Bool(bool) => query.bind(bool),
                Argument::String(string) => query.bind(string),
                Argument::FieldElement(felt) => query.bind(format!("{:#x}", felt)),
            }
        }

        match query_message.query_type {
            QueryType::UpdateCursors(update_cursors) => {
                // Read all cursors from db
                let mut contracts: Vec<torii_sqlite_types::Contract> =
                    sqlx::query_as("SELECT * FROM contracts")
                        .fetch_all(&mut **tx)
                        .await?;

                let mut updates = Vec::with_capacity(update_cursors.cursors.len());

                for cursor in &mut contracts {
                    let new_cursor = match update_cursors
                        .cursors
                        .get(&Felt::from_str(&cursor.contract_address).unwrap())
                    {
                        Some(cursor) => cursor,
                        None => continue, // Skip if no cursor found
                    };
                    let num_transactions = update_cursors
                        .cursor_transactions
                        .get(&Felt::from_str(&cursor.contract_address).unwrap())
                        .unwrap_or(&HashSet::new())
                        .len() as u64;

                    let new_head = new_cursor.head.unwrap_or_default();
                    let new_timestamp = new_cursor.last_block_timestamp.unwrap_or_default();
                    let cursor_timestamp = cursor.last_block_timestamp.unwrap_or_default() as u64;

                    let new_tps = if new_timestamp - cursor_timestamp != 0 {
                        num_transactions / (new_timestamp - cursor_timestamp)
                    } else {
                        let current_time = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();

                        let diff = current_time
                            .checked_sub(cursor_timestamp)
                            .unwrap_or_default();

                        if diff > 0 {
                            num_transactions / diff
                        } else {
                            num_transactions
                        }
                    };

                    cursor.last_pending_block_tx = new_cursor
                        .last_pending_block_tx
                        .map(|tx| format!("{:#x}", tx));
                    cursor.tps = Some(new_tps.try_into().expect("does't fit in i64"));
                    cursor.last_block_timestamp =
                        Some(new_timestamp.try_into().expect("doesn't fit in i64"));
                    cursor.head = Some(new_head as i64);

                    sqlx::query(
                        "UPDATE contracts SET head = ?, last_block_timestamp = ?, \
                         last_pending_block_tx = ?, updated_at = CURRENT_TIMESTAMP WHERE id = \
                         ?",
                    )
                    .bind(cursor.head)
                    .bind(cursor.last_block_timestamp)
                    .bind(&cursor.last_pending_block_tx)
                    .bind(&cursor.contract_address)
                    .execute(&mut **tx)
                    .await?;

                    // Send appropriate ContractUpdated publish message
                    updates.push(BrokerMessage::ContractUpdate(cursor.clone().into()));
                }

                for update in updates {
                    self.publish_optimistic_and_queue(update);
                }
            }
            QueryType::StoreTransaction(store_transaction) => {
                // Our transaction has alraedy been added by another contract probably.
                let mut transaction =
                    torii_sqlite_types::Transaction::from_row(&query.fetch_one(&mut **tx).await?)?;

                for contract_address in &store_transaction.contract_addresses {
                    sqlx::query(
                        "INSERT INTO transaction_contract (transaction_hash, \
                         contract_address) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    )
                    .bind(transaction.transaction_hash.clone())
                    .bind(felt_to_sql_string(contract_address))
                    .execute(&mut **tx)
                    .await?;
                }

                for unique_model in &store_transaction.unique_models {
                    sqlx::query(
                        "INSERT INTO transaction_models (transaction_hash, \
                         model_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    )
                    .bind(transaction.transaction_hash.clone())
                    .bind(felt_to_sql_string(unique_model))
                    .execute(&mut **tx)
                    .await?;
                }

                // Store each call in the transaction_calls table
                for call in &store_transaction.calls {
                    sqlx::query(
                        "INSERT INTO transaction_calls (transaction_hash, \
                         contract_address, entrypoint, calldata, call_type, caller_address) \
                         VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    )
                    .bind(transaction.transaction_hash.clone())
                    .bind(felt_to_sql_string(&call.contract_address))
                    .bind(call.entrypoint.clone())
                    .bind(felts_to_sql_string(&call.calldata))
                    .bind(call.call_type.to_string())
                    .bind(felt_to_sql_string(&call.caller_address))
                    .execute(&mut **tx)
                    .await?;
                }

                transaction.contract_addresses = store_transaction.contract_addresses;
                transaction.calls = store_transaction.calls;
                transaction.unique_models = store_transaction.unique_models;

                let transaction: torii_proto::Transaction = transaction.into();
                self.publish_optimistic_and_queue(BrokerMessage::Transaction(transaction));
            }
            QueryType::SetEntity(entity) => {
                let row = query.fetch_one(&mut **tx).await?;
                let mut entity_updated = torii_sqlite_types::Entity::from_row(&row)?;
                entity_updated.updated_model = Some(entity.ty.clone());

                if entity_updated.keys.is_empty() {
                    warn!(target: LOG_TARGET, "Entity has been updated without being set before. Keys are not known and non-updated values will be NULL.");
                }

                // Handle historical entities similar to historical event messages
                let mut entity_counter: i64 = sqlx::query_scalar::<_, i64>(
                    "SELECT historical_counter FROM entity_model WHERE entity_id = ? AND model_id \
                     = ?",
                )
                .bind(entity.entity_id.clone())
                .bind(entity.model_id.clone())
                .fetch_optional(&mut **tx)
                .await
                .map_or(0, |counter| counter.unwrap_or(0));

                if entity.is_historical {
                    entity_counter += 1;

                    let data = serde_json::to_string(&entity.ty.to_json_value()?)
                        .map_err(|e| ExecutorQueryError::Parse(ParseError::FromJsonStr(e)))?;
                    if let Some(keys) = entity.keys_str {
                        sqlx::query(
                            "INSERT INTO entities_historical (id, keys, event_id, data, model_id, \
                             executed_at) VALUES (?, ?, ?, ?, ?, ?) RETURNING *",
                        )
                        .bind(entity.entity_id.clone())
                        .bind(keys)
                        .bind(entity.event_id.clone())
                        .bind(data)
                        .bind(entity.model_id.clone())
                        .bind(entity.block_timestamp.clone())
                        .fetch_one(&mut **tx)
                        .await?;
                    } else {
                        sqlx::query(
                            "INSERT INTO entities_historical (id, event_id, data, model_id, \
                             executed_at) VALUES (?, ?, ?, ?, ?) RETURNING *",
                        )
                        .bind(entity.entity_id.clone())
                        .bind(entity.event_id.clone())
                        .bind(data)
                        .bind(entity.model_id.clone())
                        .bind(entity.block_timestamp.clone())
                        .fetch_one(&mut **tx)
                        .await?;
                    }
                }

                sqlx::query(
                    "INSERT INTO entity_model (entity_id, model_id, historical_counter) VALUES \
                     (?, ?, ?) ON CONFLICT(entity_id, model_id) DO UPDATE SET \
                     historical_counter=EXCLUDED.historical_counter",
                )
                .bind(entity.entity_id.clone())
                .bind(entity.model_id.clone())
                .bind(entity_counter)
                .execute(&mut **tx)
                .await?;

                self.publish_optimistic_and_queue(BrokerMessage::EntityUpdate(
                    entity_updated.into(),
                ));
            }
            QueryType::DeleteEntity(entity) => {
                let delete_model = query.execute(&mut **tx).await?;
                if delete_model.rows_affected() == 0 {
                    return Ok(());
                }

                sqlx::query("DELETE FROM entity_model WHERE entity_id = ? AND model_id = ?")
                    .bind(entity.entity_id.clone())
                    .bind(entity.model_id)
                    .execute(&mut **tx)
                    .await?;

                let row = sqlx::query(
                    "UPDATE entities SET updated_at=CURRENT_TIMESTAMP, executed_at=?, event_id=? \
                     WHERE id = ? RETURNING *",
                )
                .bind(entity.block_timestamp)
                .bind(entity.event_id)
                .bind(entity.entity_id)
                .fetch_one(&mut **tx)
                .await?;
                let mut entity_updated = torii_sqlite_types::Entity::from_row(&row)?;
                entity_updated.updated_model = Some(Ty::Struct(Struct {
                    name: entity.ty.name(),
                    children: vec![],
                }));

                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM entity_model WHERE entity_id = ?",
                )
                .bind(entity_updated.id.clone())
                .fetch_one(&mut **tx)
                .await?;

                // Delete entity if all of its models are deleted
                if count == 0 {
                    sqlx::query("DELETE FROM entities WHERE id = ?")
                        .bind(entity_updated.id.clone())
                        .execute(&mut **tx)
                        .await?;
                    entity_updated.deleted = true;
                }

                self.publish_optimistic_and_queue(BrokerMessage::EntityUpdate(
                    entity_updated.into(),
                ));
            }
            QueryType::RegisterModel => {
                let row = query.fetch_one(&mut **tx).await?;
                let model_registered = torii_sqlite_types::Model::from_row(&row)?;
                self.publish_optimistic_and_queue(BrokerMessage::ModelRegistered(
                    model_registered.into(),
                ));
            }
            QueryType::RegisterContract => {
                let row = query.fetch_one(&mut **tx).await?;
                let contract_registered = torii_sqlite_types::Contract::from_row(&row)?;
                self.publish_optimistic_and_queue(BrokerMessage::ContractUpdate(
                    contract_registered.into(),
                ));
            }
            QueryType::EventMessage(em_query) => {
                // Must be executed first since other tables have foreign keys on event_messages.id.
                let event_messages_row = query.fetch_one(&mut **tx).await?;
                let mut event_counter: i64 = sqlx::query_scalar::<_, i64>(
                    "SELECT historical_counter FROM event_model WHERE entity_id = ? AND model_id \
                     = ?",
                )
                .bind(em_query.entity_id.clone())
                .bind(em_query.model_id.clone())
                .fetch_optional(&mut **tx)
                .await
                .map_or(0, |counter| counter.unwrap_or(0));

                if em_query.is_historical {
                    event_counter += 1;

                    let data = serde_json::to_string(&em_query.ty.to_json_value()?)
                        .map_err(|e| ExecutorQueryError::Parse(ParseError::FromJsonStr(e)))?;
                    sqlx::query(
                        "INSERT INTO event_messages_historical (id, keys, event_id, data, \
                         model_id, executed_at) VALUES (?, ?, ?, ?, ?, ?) RETURNING *",
                    )
                    .bind(em_query.entity_id.clone())
                    .bind(em_query.keys_str.clone())
                    .bind(em_query.event_id.clone())
                    .bind(data)
                    .bind(em_query.model_id.clone())
                    .bind(em_query.block_timestamp.clone())
                    .fetch_one(&mut **tx)
                    .await?;
                }

                sqlx::query(
                    "INSERT INTO event_model (entity_id, model_id, historical_counter) VALUES (?, \
                     ?, ?) ON CONFLICT(entity_id, model_id) DO UPDATE SET \
                     historical_counter=EXCLUDED.historical_counter",
                )
                .bind(em_query.entity_id.clone())
                .bind(em_query.model_id.clone())
                .bind(event_counter)
                .execute(&mut **tx)
                .await?;

                let mut event_message = torii_sqlite_types::Entity::from_row(&event_messages_row)?;
                event_message.updated_model = Some(em_query.ty.clone());

                self.publish_optimistic_and_queue(BrokerMessage::EventMessageUpdate(
                    event_message.into(),
                ));
            }
            QueryType::StoreEvent => {
                let row = query.fetch_one(&mut **tx).await?;
                let event = torii_sqlite_types::Event::from_row(&row)?;
                self.publish_optimistic_and_queue(BrokerMessage::EventEmitted(event.into()));
            }
            QueryType::StoreTokenTransfer => {
                let row = query.fetch_one(&mut **tx).await?;
                let token_transfer = SQLTokenTransfer::from_row(&row)?;
                self.publish_optimistic_and_queue(BrokerMessage::TokenTransfer(
                    token_transfer.into(),
                ));
            }
            QueryType::ApplyBalanceDiff(apply_balance_diff) => {
                debug!(target: LOG_TARGET, "Applying balance diff.");
                let instant = Instant::now();
                self.apply_balance_diff(apply_balance_diff, self.provider.clone())
                    .await?;
                debug!(target: LOG_TARGET, duration = ?instant.elapsed(), "Applied balance diff.");
            }
            QueryType::RegisterNftToken(register_nft_token) => {
                // Check if we already have the metadata for this contract
                let res = sqlx::query_as::<_, (String, String)>(&format!(
                    "SELECT name, symbol FROM {TOKENS_TABLE} WHERE contract_address = ? LIMIT 1"
                ))
                .bind(felt_to_sql_string(&register_nft_token.contract_address))
                .fetch_one(&mut **tx)
                .await;

                // If we find a token already registered for this contract_address we dont need to
                // refetch the data since its same for all tokens of this contract
                let (name, symbol) = match res {
                    Ok((name, symbol)) => {
                        debug!(
                            target: LOG_TARGET,
                            contract_address = %felt_to_sql_string(&register_nft_token.contract_address),
                            "Token already registered for contract_address, so reusing fetched data",
                        );
                        (name, symbol)
                    }
                    Err(_) => {
                        // Prepare batch requests for name and symbol
                        let block_id = BlockId::Tag(BlockTag::PreConfirmed);
                        let requests = vec![
                            ProviderRequestData::Call(CallRequest {
                                request: FunctionCall {
                                    contract_address: register_nft_token.contract_address,
                                    entry_point_selector: selector!("name"),
                                    calldata: vec![],
                                },
                                block_id,
                            }),
                            ProviderRequestData::Call(CallRequest {
                                request: FunctionCall {
                                    contract_address: register_nft_token.contract_address,
                                    entry_point_selector: selector!("symbol"),
                                    calldata: vec![],
                                },
                                block_id,
                            }),
                        ];

                        let results = self.provider.batch_requests(requests).await;
                        match results {
                            Ok(results) => {
                                // Parse name
                                let name = match &results[0] {
                                    ProviderResponseData::Call(name) if name.len() == 1 => {
                                        parse_cairo_short_string(&name[0]).map_err(|e| {
                                            ExecutorQueryError::Parse(
                                                ParseError::ParseCairoShortString(e),
                                            )
                                        })?
                                    }
                                    ProviderResponseData::Call(name) => {
                                        ByteArray::cairo_deserialize(name, 0)
                                            .map_err(|e| {
                                                ExecutorQueryError::Parse(
                                                    ParseError::CairoSerdeError(e),
                                                )
                                            })?
                                            .to_string()
                                            .map_err(|e| {
                                                ExecutorQueryError::Parse(ParseError::FromUtf8(e))
                                            })?
                                    }
                                    _ => String::new(),
                                };

                                // Parse symbol
                                let symbol = match &results[1] {
                                    ProviderResponseData::Call(symbol) if symbol.len() == 1 => {
                                        parse_cairo_short_string(&symbol[0]).map_err(|e| {
                                            ExecutorQueryError::Parse(
                                                ParseError::ParseCairoShortString(e),
                                            )
                                        })?
                                    }
                                    ProviderResponseData::Call(symbol) => {
                                        ByteArray::cairo_deserialize(symbol, 0)
                                            .map_err(|e| {
                                                ExecutorQueryError::Parse(
                                                    ParseError::CairoSerdeError(e),
                                                )
                                            })?
                                            .to_string()
                                            .map_err(|e| {
                                                ExecutorQueryError::Parse(ParseError::FromUtf8(e))
                                            })?
                                    }
                                    _ => String::new(),
                                };

                                (name, symbol)
                            }
                            _ => (String::new(), String::new()),
                        }
                    }
                };

                let query = sqlx::query_as::<_, torii_sqlite_types::Token>(
                    "INSERT INTO tokens (id, contract_address, token_id, name, symbol, decimals, \
                     metadata, total_supply, traits) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING *",
                )
                .bind(felt_and_u256_to_sql_string(
                    &register_nft_token.contract_address,
                    &register_nft_token.token_id,
                ))
                .bind(felt_to_sql_string(&register_nft_token.contract_address))
                .bind(u256_to_sql_string(&register_nft_token.token_id))
                .bind(&name)
                .bind(&symbol)
                .bind(0)
                .bind(&register_nft_token.metadata)
                .bind(u256_to_sql_string(&U256::from(0u8))) // Default to 0, will be updated on mint
                .bind("{}"); // Initialize traits as empty JSON object for individual tokens

                let token = query.fetch_one(&mut **tx).await?;

                // Extract traits from metadata and update the token contract's traits
                update_contract_traits_from_metadata(
                    &register_nft_token.metadata,
                    &register_nft_token.contract_address,
                    &mut *tx,
                )
                .await?;

                info!(target: LOG_TARGET, name = %name, symbol = %symbol, contract_address = %token.contract_address, token_id = %register_nft_token.token_id, "NFT token registered.");
                self.publish_optimistic_and_queue(BrokerMessage::TokenRegistered(token.into()));
            }
            QueryType::RegisterTokenContract(register_token_contract) => {
                let query = sqlx::query_as::<_, torii_sqlite_types::Token>(
                    "INSERT INTO tokens (id, contract_address, name, symbol, decimals, metadata, total_supply, traits) VALUES (?, \
                     ?, ?, ?, ?, ?, ?, ?) RETURNING *",
                )
                .bind(felt_to_sql_string(&register_token_contract.contract_address))
                .bind(felt_to_sql_string(&register_token_contract.contract_address))
                .bind(&register_token_contract.name)
                .bind(&register_token_contract.symbol)
                .bind(register_token_contract.decimals)
                .bind(&register_token_contract.metadata)
                .bind(u256_to_sql_string(&U256::from(0u8))) // Initialize total_supply to 0 for all contracts
                .bind("{}"); // Initialize traits as empty JSON object

                let token = query.fetch_one(&mut **tx).await?;
                info!(target: LOG_TARGET, name = %register_token_contract.name, symbol = %register_token_contract.symbol, contract_address = %token.contract_address, "Registered token contract.");

                self.publish_optimistic_and_queue(BrokerMessage::TokenRegistered(token.into()));
            }
            QueryType::Execute => {
                debug!(target: LOG_TARGET, "Executing query.");
                let instant = Instant::now();
                self.execute().await?;
                debug!(target: LOG_TARGET, duration = ?instant.elapsed(), "Executed query.");
            }
            QueryType::Rollback => {
                debug!(target: LOG_TARGET, "Rolling back the transaction.");
                // rollback's the current transaction and starts a new one
                self.rollback().await?;
                debug!(target: LOG_TARGET, "Rolled back the transaction.");
            }
            QueryType::UpdateTokenMetadata(update_metadata) => {
                let id = if let Some(token_id) = update_metadata.token_id {
                    felt_and_u256_to_sql_string(&update_metadata.contract_address, &token_id)
                } else {
                    felt_to_sql_string(&update_metadata.contract_address)
                };

                // Update metadata and timestamp in database
                let token = sqlx::query_as::<_, torii_sqlite_types::Token>(
                    "UPDATE tokens SET metadata = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? RETURNING *",
                )
                .bind(&update_metadata.metadata)
                .bind(&id)
                .fetch_one(&mut **tx)
                .await?;

                // If this is an individual token (has token_id), update the contract's traits
                if update_metadata.token_id.is_some() {
                    update_contract_traits_from_metadata(
                        &update_metadata.metadata,
                        &update_metadata.contract_address,
                        &mut *tx,
                    )
                    .await?;
                }

                info!(target: LOG_TARGET, name = %token.name, symbol = %token.symbol, contract_address = %token.contract_address, token_id = ?update_metadata.token_id, "Token metadata updated.");
                self.publish_optimistic_and_queue(BrokerMessage::TokenRegistered(token.into()));
            }
            QueryType::Other => {
                query.execute(&mut **tx).await?;
            }
        }

        // Record metrics
        let duration = start_time.elapsed();
        histogram!(
            "torii_executor_query_duration_seconds",
            "query_type" => query_type_str.clone()
        )
        .record(duration.as_secs_f64());
        counter!(
            "torii_executor_queries_total",
            "query_type" => query_type_str,
            "status" => "success"
        )
        .increment(1);

        Ok(())
    }

    async fn execute(&mut self) -> Result<()> {
        if let Some(transaction) = self.transaction.take() {
            transaction.commit().await?;
        }
        self.pool
            .execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await?;

        self.transaction = Some(self.pool.begin().await?);

        for message in self.publish_queue.drain(..) {
            send_broker_message(message, false);
        }

        // Record metrics
        counter!("torii_executor_transaction_operations_total", "operation" => "execute", "status" => "success")
            .increment(1);

        Ok(())
    }

    async fn rollback(&mut self) -> Result<()> {
        if let Some(transaction) = self.transaction.take() {
            transaction.rollback().await?;
        }
        self.pool
            .execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await?;

        self.transaction = Some(self.pool.begin().await?);

        self.publish_queue.clear();

        // Record metrics
        counter!("torii_executor_transaction_operations_total", "operation" => "rollback", "status" => "success")
            .increment(1);

        Ok(())
    }

    fn publish_optimistic_and_queue(&mut self, message: BrokerMessage) {
        send_broker_message(message.clone(), true);
        self.publish_queue.push(message);
    }
}

fn send_broker_message(message: BrokerMessage, optimistic: bool) {
    match message {
        BrokerMessage::ContractUpdate(contract) => {
            MemoryBroker::publish(Update::new(contract, optimistic))
        }
        BrokerMessage::ModelRegistered(model) => {
            MemoryBroker::publish(Update::new(model, optimistic))
        }
        BrokerMessage::EntityUpdate(entity) => {
            MemoryBroker::publish(Update::new(entity, optimistic))
        }
        BrokerMessage::EventMessageUpdate(event) => {
            MemoryBroker::publish(Update::new(event, optimistic))
        }
        BrokerMessage::EventEmitted(event) => MemoryBroker::publish(Update::new(event, optimistic)),
        BrokerMessage::TokenRegistered(token) => {
            MemoryBroker::publish(Update::new(token, optimistic))
        }
        BrokerMessage::TokenBalanceUpdated(token_balance) => {
            MemoryBroker::publish(Update::new(token_balance, optimistic))
        }
        BrokerMessage::TokenTransfer(token_transfer) => {
            MemoryBroker::publish(Update::new(token_transfer, optimistic))
        }
        BrokerMessage::Transaction(transaction) => {
            MemoryBroker::publish(Update::new(transaction, optimistic))
        }
    }
}
