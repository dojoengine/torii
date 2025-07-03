use core::fmt;
use std::collections::HashSet;

use chrono::{DateTime, Utc};
use crypto_bigint::U256;
use dojo_types::schema::Ty;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use starknet::core::types::Felt;
use std::str::FromStr;
use torii_storage::types::ParsedCall;

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

impl From<Event> for torii_proto::Event {
    fn from(value: Event) -> Self {
        Self {
            keys: value
                .keys
                .trim_end_matches('/')
                .split('/')
                .filter(|k| !k.is_empty())
                .map(|k| Felt::from_str(k).unwrap())
                .collect(),
            data: value
                .data
                .trim_end_matches('/')
                .split('/')
                .filter(|d| !d.is_empty())
                .map(|d| Felt::from_str(d).unwrap())
                .collect(),
            transaction_hash: Felt::from_str(&value.transaction_hash).unwrap(),
        }
    }
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

impl From<Token> for torii_proto::Token {
    fn from(value: Token) -> Self {
        Self {
            token_id: if value.token_id.is_empty() {
                U256::ZERO
            } else {
                U256::from_be_hex(value.token_id.trim_start_matches("0x"))
            },
            contract_address: Felt::from_str(&value.contract_address).unwrap(),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals,
            metadata: value.metadata,
        }
    }
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

impl From<TokenCollection> for torii_proto::TokenCollection {
    fn from(value: TokenCollection) -> Self {
        Self {
            contract_address: Felt::from_str(&value.contract_address).unwrap(),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals,
            count: value.count,
            metadata: value.metadata,
        }
    }
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

impl From<TokenBalance> for torii_proto::TokenBalance {
    fn from(value: TokenBalance) -> Self {
        let id = value.token_id.split(':').collect::<Vec<&str>>();

        Self {
            balance: U256::from_be_hex(value.balance.trim_start_matches("0x")),
            account_address: Felt::from_str(&value.account_address).unwrap(),
            contract_address: Felt::from_str(&value.contract_address).unwrap(),
            token_id: if id.len() == 2 {
                U256::from_be_hex(id[1].trim_start_matches("0x"))
            } else {
                U256::ZERO
            },
        }
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

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Controller {
    pub address: String,
    pub username: String,
    pub deployed_at: DateTime<Utc>,
}

impl From<Controller> for torii_proto::Controller {
    fn from(value: Controller) -> Self {
        Self {
            address: Felt::from_str(&value.address).unwrap(),
            username: value.username,
            deployed_at: value.deployed_at,
        }
    }
}
