#[cfg(target_arch = "wasm32")]
extern crate wasm_prost as prost;
#[cfg(target_arch = "wasm32")]
extern crate wasm_tonic as tonic;

pub mod proto {
    pub mod world {
        tonic::include_proto!("world");

        #[cfg(feature = "server")]
        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("world_descriptor");
    }

    pub mod types {
        tonic::include_proto!("types");
    }
}

pub mod error;
pub mod schema;

use core::fmt;
use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use crypto_bigint::{Encoding, U256};
use dojo_types::primitive::Primitive;
use dojo_types::schema::Ty;
use dojo_world::contracts::abigen::model::Layout;
use error::ProtoError;
use serde::{Deserialize, Serialize};
use starknet::core::types::Felt;
use strum_macros::{AsRefStr, EnumIter, FromRepr};

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub enum PaginationDirection {
    Forward,
    Backward,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct Message {
    pub signature: Vec<Felt>,
    // The raw TypedData. Should be deserializable to a TypedData struct.
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct Pagination {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub direction: PaginationDirection,
    pub order_by: Vec<OrderBy>,
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: None,
            direction: PaginationDirection::Forward,
            order_by: vec![],
        }
    }
}

impl From<Pagination> for proto::types::Pagination {
    fn from(value: Pagination) -> Self {
        Self {
            cursor: value.cursor.unwrap_or_default(),
            limit: value.limit.unwrap_or_default(),
            direction: value.direction as i32,
            order_by: value.order_by.into_iter().map(|o| o.into()).collect(),
        }
    }
}

impl From<proto::types::Pagination> for Pagination {
    fn from(value: proto::types::Pagination) -> Self {
        Self {
            cursor: if value.cursor.is_empty() {
                None
            } else {
                Some(value.cursor)
            },
            limit: if value.limit == 0 {
                None
            } else {
                Some(value.limit)
            },
            direction: match value.direction {
                0 => PaginationDirection::Forward,
                1 => PaginationDirection::Backward,
                _ => unreachable!(),
            },
            order_by: value.order_by.into_iter().map(|o| o.into()).collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct Controller {
    pub address: Felt,
    pub username: String,
    pub deployed_at: DateTime<Utc>,
}

impl From<Controller> for proto::types::Controller {
    fn from(value: Controller) -> Self {
        Self {
            address: value.address.to_bytes_be().into(),
            username: value.username,
            deployed_at_timestamp: value.deployed_at.timestamp() as u64,
        }
    }
}

impl TryFrom<proto::types::Controller> for Controller {
    type Error = ProtoError;
    fn try_from(value: proto::types::Controller) -> Result<Self, Self::Error> {
        Ok(Self {
            address: Felt::from_bytes_be_slice(&value.address),
            username: value.username,
            deployed_at: DateTime::from_timestamp(value.deployed_at_timestamp as i64, 0).unwrap(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct Token {
    pub token_id: U256,
    pub contract_address: Felt,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub metadata: String,
}

impl From<Token> for proto::types::Token {
    fn from(value: Token) -> Self {
        Self {
            token_id: value.token_id.to_be_bytes().to_vec(),
            contract_address: value.contract_address.to_bytes_be().into(),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals as u32,
            metadata: value.metadata.into_bytes(),
        }
    }
}

impl TryFrom<proto::types::Token> for Token {
    type Error = ProtoError;
    fn try_from(value: proto::types::Token) -> Result<Self, Self::Error> {
        Ok(Self {
            token_id: U256::from_be_slice(&value.token_id),
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals as u8,
            metadata: String::from_utf8(value.metadata).map_err(ProtoError::FromUtf8)?,
        })
    }
}
impl TryFrom<proto::types::TokenCollection> for Token {
    type Error = ProtoError;
    fn try_from(value: proto::types::TokenCollection) -> Result<Self, Self::Error> {
        Ok(Self {
            token_id: U256::ZERO,
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals as u8,
            metadata: String::from_utf8(value.metadata).map_err(ProtoError::FromUtf8)?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct TokenCollection {
    pub contract_address: Felt,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub count: u32,
    pub metadata: String,
}

impl From<TokenCollection> for proto::types::TokenCollection {
    fn from(value: TokenCollection) -> Self {
        Self {
            contract_address: value.contract_address.to_bytes_be().into(),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals as u32,
            count: value.count,
            metadata: value.metadata.into_bytes(),
        }
    }
}

impl TryFrom<proto::types::TokenCollection> for TokenCollection {
    type Error = ProtoError;
    fn try_from(value: proto::types::TokenCollection) -> Result<Self, Self::Error> {
        Ok(Self {
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals as u8,
            count: value.count,
            metadata: String::from_utf8(value.metadata).map_err(ProtoError::FromUtf8)?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct TokenBalance {
    pub balance: U256,
    pub account_address: Felt,
    pub contract_address: Felt,
    pub token_id: U256,
}

impl From<TokenBalance> for proto::types::TokenBalance {
    fn from(value: TokenBalance) -> Self {
        Self {
            balance: value.balance.to_be_bytes().to_vec(),
            account_address: value.account_address.to_bytes_be().into(),
            contract_address: value.contract_address.to_bytes_be().into(),
            token_id: value.token_id.to_be_bytes().to_vec(),
        }
    }
}

impl TryFrom<proto::types::TokenBalance> for TokenBalance {
    type Error = ProtoError;
    fn try_from(value: proto::types::TokenBalance) -> Result<Self, Self::Error> {
        Ok(Self {
            balance: U256::from_be_slice(&value.balance),
            account_address: Felt::from_bytes_be_slice(&value.account_address),
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
            token_id: U256::from_be_slice(&value.token_id),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct IndexerUpdate {
    pub head: i64,
    pub tps: i64,
    pub last_block_timestamp: i64,
    pub contract_address: Felt,
}

impl From<proto::world::SubscribeIndexerResponse> for IndexerUpdate {
    fn from(value: proto::world::SubscribeIndexerResponse) -> Self {
        Self {
            head: value.head,
            tps: value.tps,
            last_block_timestamp: value.last_block_timestamp,
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct OrderBy {
    pub model: String,
    pub member: String,
    pub direction: OrderDirection,
}

impl From<OrderBy> for proto::types::OrderBy {
    fn from(value: OrderBy) -> Self {
        Self {
            model: value.model,
            member: value.member,
            direction: value.direction as i32,
        }
    }
}

impl From<proto::types::OrderBy> for OrderBy {
    fn from(value: proto::types::OrderBy) -> Self {
        Self {
            model: value.model,
            member: value.member,
            direction: match value.direction {
                0 => OrderDirection::Asc,
                1 => OrderDirection::Desc,
                _ => unreachable!(),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct Query {
    pub clause: Option<Clause>,
    pub pagination: Pagination,
    /// Whether or not to include the hashed keys (entity id) of the entities.
    /// This is useful for large queries compressed with GZIP to reduce the size of the response.
    pub no_hashed_keys: bool,
    /// If the array is not empty, only the given models are retrieved.
    /// All entities that don't have a model in the array are excluded.
    pub models: Vec<String>,
    /// Whether or not we should retrieve historical entities.
    pub historical: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub enum Clause {
    HashedKeys(Vec<Felt>),
    Keys(KeysClause),
    Member(MemberClause),
    Composite(CompositeClause),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct KeysClause {
    pub keys: Vec<Option<Felt>>,
    pub pattern_matching: PatternMatching,
    pub models: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub enum PatternMatching {
    FixedLen,
    VariableLen,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub enum MemberValue {
    Primitive(Primitive),
    String(String),
    List(Vec<MemberValue>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct MemberClause {
    pub model: String,
    pub member: String,
    pub operator: ComparisonOperator,
    pub value: MemberValue,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct CompositeClause {
    pub operator: LogicalOperator,
    pub clauses: Vec<Clause>,
}

#[derive(
    Debug, AsRefStr, Serialize, Deserialize, EnumIter, FromRepr, PartialEq, Hash, Eq, Clone,
)]
#[strum(serialize_all = "UPPERCASE")]
pub enum LogicalOperator {
    And,
    Or,
}

impl From<LogicalOperator> for proto::types::LogicalOperator {
    fn from(value: LogicalOperator) -> Self {
        match value {
            LogicalOperator::And => proto::types::LogicalOperator::And,
            LogicalOperator::Or => proto::types::LogicalOperator::Or,
        }
    }
}

impl From<proto::types::LogicalOperator> for LogicalOperator {
    fn from(value: proto::types::LogicalOperator) -> Self {
        match value {
            proto::types::LogicalOperator::And => LogicalOperator::And,
            proto::types::LogicalOperator::Or => LogicalOperator::Or,
        }
    }
}
#[derive(
    Debug, AsRefStr, Serialize, Deserialize, EnumIter, FromRepr, PartialEq, Hash, Eq, Clone,
)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ComparisonOperator {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    NotIn,
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonOperator::Gt => write!(f, ">"),
            ComparisonOperator::Gte => write!(f, ">="),
            ComparisonOperator::Lt => write!(f, "<"),
            ComparisonOperator::Lte => write!(f, "<="),
            ComparisonOperator::Neq => write!(f, "!="),
            ComparisonOperator::Eq => write!(f, "="),
            ComparisonOperator::In => write!(f, "IN"),
            ComparisonOperator::NotIn => write!(f, "NOT IN"),
        }
    }
}

impl From<ComparisonOperator> for proto::types::ComparisonOperator {
    fn from(operator: ComparisonOperator) -> Self {
        match operator {
            ComparisonOperator::Eq => proto::types::ComparisonOperator::Eq,
            ComparisonOperator::Neq => proto::types::ComparisonOperator::Neq,
            ComparisonOperator::Gt => proto::types::ComparisonOperator::Gt,
            ComparisonOperator::Gte => proto::types::ComparisonOperator::Gte,
            ComparisonOperator::Lt => proto::types::ComparisonOperator::Lt,
            ComparisonOperator::Lte => proto::types::ComparisonOperator::Lte,
            ComparisonOperator::In => proto::types::ComparisonOperator::In,
            ComparisonOperator::NotIn => proto::types::ComparisonOperator::NotIn,
        }
    }
}

impl From<proto::types::ComparisonOperator> for ComparisonOperator {
    fn from(operator: proto::types::ComparisonOperator) -> Self {
        match operator {
            proto::types::ComparisonOperator::Eq => ComparisonOperator::Eq,
            proto::types::ComparisonOperator::Gte => ComparisonOperator::Gte,
            proto::types::ComparisonOperator::Gt => ComparisonOperator::Gt,
            proto::types::ComparisonOperator::Lt => ComparisonOperator::Lt,
            proto::types::ComparisonOperator::Lte => ComparisonOperator::Lte,
            proto::types::ComparisonOperator::Neq => ComparisonOperator::Neq,
            proto::types::ComparisonOperator::In => ComparisonOperator::In,
            proto::types::ComparisonOperator::NotIn => ComparisonOperator::NotIn,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct Value {
    pub primitive_type: Primitive,
    pub value_type: ValueType,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub enum ValueType {
    String(String),
    Int(i64),
    UInt(u64),
    Bool(bool),
    Bytes(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Model {
    /// Namespace of the model
    pub namespace: String,
    /// The name of the model
    pub name: String,
    /// The selector of the model
    pub selector: Felt,
    /// The class hash of the model
    pub class_hash: Felt,
    /// The contract address of the model
    pub contract_address: Felt,
    pub packed_size: u32,
    pub unpacked_size: u32,
    pub layout: Layout,
    pub schema: Ty,
}

impl TryFrom<proto::types::Model> for Model {
    type Error = ProtoError;
    fn try_from(value: proto::types::Model) -> Result<Self, Self::Error> {
        let schema: Ty = serde_json::from_slice(&value.schema).map_err(ProtoError::FromJson)?;
        let layout: Layout = serde_json::from_slice(&value.layout).map_err(ProtoError::FromJson)?;
        Ok(Self {
            selector: Felt::from_bytes_be_slice(&value.selector),
            schema,
            layout,
            name: value.name,
            namespace: value.namespace,
            packed_size: value.packed_size,
            unpacked_size: value.unpacked_size,
            class_hash: Felt::from_bytes_be_slice(&value.class_hash),
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct World {
    pub world_address: Felt,
    pub models: HashMap<Felt, Model>,
}

impl TryFrom<proto::types::World> for World {
    type Error = ProtoError;
    fn try_from(value: proto::types::World) -> Result<Self, Self::Error> {
        let models = value
            .models
            .into_iter()
            .map(|component| {
                Ok((
                    Felt::from_bytes_be_slice(&component.selector),
                    component.try_into()?,
                ))
            })
            .collect::<Result<HashMap<_, Model>, ProtoError>>()?;

        Ok(World {
            models,
            world_address: Felt::from_str(&value.world_address)?,
        })
    }
}

impl TryFrom<proto::types::Query> for Query {
    type Error = ProtoError;
    fn try_from(value: proto::types::Query) -> Result<Self, Self::Error> {
        let clause = value.clause.map(|c| c.try_into()).transpose()?;
        Ok(Self {
            clause,
            pagination: value.pagination.map(|p| p.into()).unwrap_or_default(),
            no_hashed_keys: value.no_hashed_keys,
            models: value.models,
            historical: value.historical,
        })
    }
}

impl From<Query> for proto::types::Query {
    fn from(value: Query) -> Self {
        Self {
            clause: value.clause.map(|c| c.into()),
            no_hashed_keys: value.no_hashed_keys,
            models: value.models,
            pagination: Some(value.pagination.into()),
            historical: value.historical,
        }
    }
}

impl From<proto::types::PatternMatching> for PatternMatching {
    fn from(value: proto::types::PatternMatching) -> Self {
        match value {
            proto::types::PatternMatching::FixedLen => PatternMatching::FixedLen,
            proto::types::PatternMatching::VariableLen => PatternMatching::VariableLen,
        }
    }
}

impl From<KeysClause> for proto::types::KeysClause {
    fn from(value: KeysClause) -> Self {
        Self {
            keys: value
                .keys
                .iter()
                .map(|k| k.map_or(Vec::new(), |k| k.to_bytes_be().into()))
                .collect(),
            pattern_matching: value.pattern_matching as i32,
            models: value.models,
        }
    }
}

impl From<proto::types::KeysClause> for KeysClause {
    fn from(value: proto::types::KeysClause) -> Self {
        let keys = value
            .keys
            .iter()
            .map(|k| {
                if k.is_empty() {
                    None
                } else {
                    Some(Felt::from_bytes_be_slice(k))
                }
            })
            .collect::<Vec<Option<Felt>>>();

        Self {
            keys,
            pattern_matching: value.pattern_matching().into(),
            models: value.models,
        }
    }
}

impl From<Clause> for proto::types::Clause {
    fn from(value: Clause) -> Self {
        match value {
            Clause::HashedKeys(hashed_keys) => Self {
                clause_type: Some(proto::types::clause::ClauseType::HashedKeys(
                    proto::types::HashedKeysClause {
                        hashed_keys: hashed_keys.iter().map(|k| k.to_bytes_be().into()).collect(),
                    },
                )),
            },
            Clause::Keys(clause) => Self {
                clause_type: Some(proto::types::clause::ClauseType::Keys(clause.into())),
            },
            Clause::Member(clause) => Self {
                clause_type: Some(proto::types::clause::ClauseType::Member(clause.into())),
            },
            Clause::Composite(clause) => Self {
                clause_type: Some(proto::types::clause::ClauseType::Composite(clause.into())),
            },
        }
    }
}

impl TryFrom<proto::types::Clause> for Clause {
    type Error = ProtoError;
    fn try_from(value: proto::types::Clause) -> Result<Self, Self::Error> {
        let value = value
            .clause_type
            .ok_or(ProtoError::MissingExpectedData("clause_type".to_string()))?;

        match value {
            proto::types::clause::ClauseType::HashedKeys(clause) => Ok(Clause::HashedKeys(
                clause
                    .hashed_keys
                    .iter()
                    .map(|k| Felt::from_bytes_be_slice(k))
                    .collect(),
            )),
            proto::types::clause::ClauseType::Keys(clause) => Ok(Clause::Keys(clause.into())),
            proto::types::clause::ClauseType::Member(clause) => {
                Ok(Clause::Member(clause.try_into()?))
            }
            proto::types::clause::ClauseType::Composite(clause) => {
                Ok(Clause::Composite(clause.try_into()?))
            }
        }
    }
}

impl From<MemberClause> for proto::types::MemberClause {
    fn from(value: MemberClause) -> Self {
        Self {
            model: value.model,
            member: value.member,
            operator: value.operator as i32,
            value: Some(proto::types::MemberValue {
                value_type: Some(value.value.into()),
            }),
        }
    }
}

impl TryFrom<proto::types::MemberClause> for MemberClause {
    type Error = ProtoError;
    fn try_from(value: proto::types::MemberClause) -> Result<Self, Self::Error> {
        let operator = value.operator().into();
        let model = value.model;
        let member = value.member;
        let value = value
            .value
            .ok_or(ProtoError::MissingExpectedData("value".to_string()))?
            .try_into()?;

        Ok(Self {
            model,
            member,
            operator,
            value,
        })
    }
}

impl TryFrom<proto::types::MemberValue> for MemberValue {
    type Error = ProtoError;
    fn try_from(value: proto::types::MemberValue) -> Result<Self, Self::Error> {
        let value_type = value
            .value_type
            .ok_or(ProtoError::MissingExpectedData("value_type".to_string()))?;

        match value_type {
            proto::types::member_value::ValueType::Primitive(primitive) => {
                Ok(Self::Primitive(primitive.try_into()?))
            }
            proto::types::member_value::ValueType::String(string) => Ok(Self::String(string)),
            proto::types::member_value::ValueType::List(list) => Ok(Self::List(
                list.values
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        }
    }
}

impl From<CompositeClause> for proto::types::CompositeClause {
    fn from(value: CompositeClause) -> Self {
        Self {
            operator: value.operator as i32,
            clauses: value
                .clauses
                .into_iter()
                .map(|clause| clause.into())
                .collect(),
        }
    }
}

impl TryFrom<proto::types::CompositeClause> for CompositeClause {
    type Error = ProtoError;
    fn try_from(value: proto::types::CompositeClause) -> Result<Self, Self::Error> {
        let operator = value.operator().into();
        let clauses = value
            .clauses
            .into_iter()
            .map(|clause| clause.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { operator, clauses })
    }
}

impl From<MemberValue> for proto::types::member_value::ValueType {
    fn from(value: MemberValue) -> Self {
        match value {
            MemberValue::Primitive(primitive) => {
                proto::types::member_value::ValueType::Primitive(primitive.into())
            }
            MemberValue::String(string) => proto::types::member_value::ValueType::String(string),
            MemberValue::List(list) => {
                proto::types::member_value::ValueType::List(proto::types::MemberValueList {
                    values: list
                        .into_iter()
                        .map(|v| proto::types::MemberValue {
                            value_type: Some(v.into()),
                        })
                        .collect(),
                })
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct Event {
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
    pub transaction_hash: Felt,
}

impl From<Event> for proto::types::Event {
    fn from(value: Event) -> Self {
        Self {
            keys: value
                .keys
                .into_iter()
                .map(|k| k.to_bytes_be().into())
                .collect(),
            data: value
                .data
                .into_iter()
                .map(|d| d.to_bytes_be().into())
                .collect(),
            transaction_hash: value.transaction_hash.to_bytes_be().into(),
        }
    }
}

impl From<proto::types::Event> for Event {
    fn from(value: proto::types::Event) -> Self {
        let keys = value
            .keys
            .into_iter()
            .map(|k| Felt::from_bytes_be_slice(&k))
            .collect();
        let data = value
            .data
            .into_iter()
            .map(|d| Felt::from_bytes_be_slice(&d))
            .collect();
        let transaction_hash = Felt::from_bytes_be_slice(&value.transaction_hash);
        Self {
            keys,
            data,
            transaction_hash,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct EventQuery {
    pub keys: Option<KeysClause>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

impl From<EventQuery> for proto::types::EventQuery {
    fn from(value: EventQuery) -> Self {
        Self {
            keys: value.keys.map(|k| k.into()),
            cursor: value.cursor.unwrap_or_default(),
            limit: value.limit.unwrap_or_default() as u32,
        }
    }
}

impl From<proto::types::EventQuery> for EventQuery {
    fn from(value: proto::types::EventQuery) -> Self {
        Self {
            keys: value.keys.map(|k| k.into()),
            cursor: if value.cursor.is_empty() {
                None
            } else {
                Some(value.cursor)
            },
            limit: if value.limit == 0 {
                None
            } else {
                Some(value.limit as usize)
            },
        }
    }
}
