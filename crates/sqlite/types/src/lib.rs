use core::fmt;
use std::collections::HashSet;

use chrono::{DateTime, Utc};
use dojo_types::schema::Ty;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use starknet::core::types::Felt;
use torii_storage::types::{ContractType, ParsedCall};

#[derive(Debug, Serialize, Deserialize)]
pub struct SQLFelt(pub Felt);

impl From<SQLFelt> for Felt {
    fn from(field_element: SQLFelt) -> Self {
        field_element.0
    }
}

impl TryFrom<String> for SQLFelt {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(SQLFelt(Felt::from_hex(&value)?))
    }
}

impl fmt::LowerHex for SQLFelt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Entity {
    pub id: String,
    pub keys: String,
    pub event_id: String,
    pub executed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    // this should never be None
    #[sqlx(skip)]
    pub updated_model: Option<Ty>,
    #[sqlx(skip)]
    pub deleted: bool,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OptimisticEntity {
    pub id: String,
    pub keys: String,
    pub event_id: String,
    pub executed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    // this should never be None
    #[sqlx(skip)]
    pub updated_model: Option<Ty>,
    #[sqlx(skip)]
    pub deleted: bool,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventMessage {
    pub id: String,
    pub keys: String,
    pub event_id: String,
    pub executed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    // this should never be None
    #[sqlx(skip)]
    pub updated_model: Option<Ty>,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OptimisticEventMessage {
    pub id: String,
    pub keys: String,
    pub event_id: String,
    pub executed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    // this should never be None
    #[sqlx(skip)]
    pub updated_model: Option<Ty>,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Model {
    pub id: String,
    pub namespace: String,
    pub name: String,
    pub class_hash: String,
    pub contract_address: String,
    pub layout: String,
    pub schema: String,
    pub packed_size: u32,
    pub unpacked_size: u32,
    pub executed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: String,
    pub keys: String,
    pub data: String,
    pub transaction_hash: String,
    pub executed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OptimisticToken {
    pub id: String,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub metadata: String,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Token {
    pub id: String,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub metadata: String,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TokenCollection {
    pub contract_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub count: u32,
    pub metadata: String,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OptimisticTokenBalance {
    pub id: String,
    pub balance: String,
    pub account_address: String,
    pub contract_address: String,
    pub token_id: String,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalance {
    pub id: String,
    pub balance: String,
    pub account_address: String,
    pub contract_address: String,
    pub token_id: String,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct Contract {
    pub address: Felt,
    pub r#type: ContractType,
}

impl std::fmt::Display for Contract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:#x}", self.r#type, self.address)
    }
}

#[derive(FromRow, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ContractCursor {
    pub head: Option<i64>,
    pub tps: Option<i64>,
    pub last_block_timestamp: Option<i64>,
    pub contract_address: String,
    pub last_pending_block_tx: Option<String>,
}

#[derive(FromRow, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub id: String,
    pub transaction_hash: String,
    pub sender_address: String,
    pub calldata: String,
    pub max_fee: String,
    pub signature: String,
    pub nonce: String,
    pub executed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub transaction_type: String,
    pub block_number: u64,

    #[sqlx(skip)]
    pub calls: Vec<ParsedCall>,
    #[sqlx(skip)]
    pub contract_addresses: HashSet<Felt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelIndices {
    pub model_tag: String,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Hook {
    pub event: HookEvent,
    pub statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HookEvent {
    ModelRegistered { model_tag: String },
    ModelUpdated { model_tag: String },
    ModelDeleted { model_tag: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PaginationDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Pagination {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub direction: PaginationDirection,
    pub order_by: Vec<OrderBy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderBy {
    pub model: String,
    pub member: String,
    pub direction: OrderDirection,
}
