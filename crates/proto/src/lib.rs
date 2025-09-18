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

/// Represents a cursor for tracking blockchain state
#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct ContractCursor {
    pub contract_address: Felt,
    pub head: Option<u64>,
    pub last_block_timestamp: Option<u64>,
    pub last_pending_block_tx: Option<Felt>,
    pub contract_type: ContractType,
}

#[derive(Debug, Clone, Deserialize, Serialize, Copy, Hash, PartialEq, Eq, Default)]
pub enum ContractType {
    #[default]
    WORLD,
    ERC20,
    ERC721,
    ERC1155,
    UDC,
    OTHER,
}

impl From<proto::types::ContractType> for ContractType {
    fn from(value: proto::types::ContractType) -> Self {
        match value {
            proto::types::ContractType::World => ContractType::WORLD,
            proto::types::ContractType::Erc20 => ContractType::ERC20,
            proto::types::ContractType::Erc721 => ContractType::ERC721,
            proto::types::ContractType::Erc1155 => ContractType::ERC1155,
            proto::types::ContractType::Udc => ContractType::UDC,
            proto::types::ContractType::Other => ContractType::OTHER,
        }
    }
}

impl TryFrom<i32> for ContractType {
    type Error = ProtoError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ContractType::WORLD),
            1 => Ok(ContractType::ERC20),
            2 => Ok(ContractType::ERC721),
            3 => Ok(ContractType::ERC1155),
            4 => Ok(ContractType::UDC),
            5 => Ok(ContractType::OTHER),
            _ => Err(ProtoError::InvalidContractType(value.to_string())),
        }
    }
}

impl FromStr for ContractType {
    type Err = ProtoError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_lowercase().trim_end_matches("token") {
            "world" => Ok(ContractType::WORLD),
            "erc20" => Ok(ContractType::ERC20),
            "erc721" => Ok(ContractType::ERC721),
            "erc1155" => Ok(ContractType::ERC1155),
            "udc" => Ok(ContractType::UDC),
            "other" => Ok(ContractType::OTHER),
            _ => Err(ProtoError::InvalidContractType(input.to_string())),
        }
    }
}

impl std::fmt::Display for ContractType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContractType::WORLD => write!(f, "WORLD"),
            ContractType::ERC20 => write!(f, "ERC20"),
            ContractType::ERC721 => write!(f, "ERC721"),
            ContractType::ERC1155 => write!(f, "ERC1155"),
            ContractType::UDC => write!(f, "UDC"),
            ContractType::OTHER => write!(f, "OTHER"),
        }
    }
}

/// Simple contract representation for CLI and basic usage
#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct ContractDefinition {
    pub address: Felt,
    pub r#type: ContractType,
    pub starting_block: Option<u64>,
}

/// Full contract representation with metadata for storage/indexing
#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct Contract {
    pub contract_address: Felt,
    pub contract_type: ContractType,
    pub head: Option<u64>,
    pub tps: Option<u64>,
    pub last_block_timestamp: Option<u64>,
    pub last_pending_block_tx: Option<Felt>,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl std::fmt::Display for ContractDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(starting_block) = self.starting_block {
            write!(f, "{}:{:#x}:{}", self.r#type, self.address, starting_block)
        } else {
            write!(f, "{}:{:#x}", self.r#type, self.address)
        }
    }
}

impl std::fmt::Display for Contract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:#x}", self.contract_type, self.contract_address)
    }
}

impl From<Contract> for ContractCursor {
    fn from(contract: Contract) -> Self {
        Self {
            contract_address: contract.contract_address,
            head: contract.head,
            last_block_timestamp: contract.last_block_timestamp,
            last_pending_block_tx: contract.last_pending_block_tx,
            contract_type: contract.contract_type,
        }
    }
}

impl From<Contract> for ContractDefinition {
    fn from(contract: Contract) -> Self {
        Self {
            address: contract.contract_address,
            r#type: contract.contract_type,
            starting_block: contract.head,
        }
    }
}

impl From<Contract> for proto::types::Contract {
    fn from(value: Contract) -> Self {
        Self {
            contract_address: value.contract_address.to_bytes_be().into(),
            contract_type: value.contract_type as i32,
            head: value.head,
            tps: value.tps,
            last_block_timestamp: value.last_block_timestamp,
            last_pending_block_tx: value
                .last_pending_block_tx
                .map(|tx| tx.to_bytes_be().into()),
            updated_at: value.updated_at.timestamp() as u64,
            created_at: value.created_at.timestamp() as u64,
        }
    }
}

impl TryFrom<proto::types::Contract> for Contract {
    type Error = ProtoError;
    fn try_from(value: proto::types::Contract) -> Result<Self, Self::Error> {
        Ok(Self {
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
            contract_type: value.contract_type().into(),
            head: value.head,
            tps: value.tps,
            last_block_timestamp: value.last_block_timestamp,
            last_pending_block_tx: value
                .last_pending_block_tx
                .map(|tx| Felt::from_bytes_be_slice(&tx)),
            updated_at: DateTime::from_timestamp(value.updated_at as i64, 0).unwrap(),
            created_at: DateTime::from_timestamp(value.created_at as i64, 0).unwrap(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub enum PaginationDirection {
    #[default]
    Forward,
    Backward,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct Message {
    pub signature: Vec<Felt>,
    // The raw TypedData. Should be deserializable to a TypedData struct.
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct Pagination {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub direction: PaginationDirection,
    pub order_by: Vec<OrderBy>,
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct Token {
    pub token_id: Option<U256>,
    pub contract_address: Felt,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub metadata: String,
    pub total_supply: Option<U256>,
}

impl From<Token> for proto::types::Token {
    fn from(value: Token) -> Self {
        Self {
            token_id: value.token_id.map(|id| id.to_be_bytes().to_vec()),
            contract_address: value.contract_address.to_bytes_be().into(),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals as u32,
            metadata: value.metadata.into_bytes(),
            total_supply: value.total_supply.map(|s| s.to_be_bytes().to_vec()),
        }
    }
}

impl TryFrom<proto::types::Token> for Token {
    type Error = ProtoError;
    fn try_from(value: proto::types::Token) -> Result<Self, Self::Error> {
        Ok(Self {
            token_id: value.token_id.map(|id| U256::from_be_slice(&id)),
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals as u8,
            metadata: String::from_utf8(value.metadata).map_err(ProtoError::FromUtf8)?,
            total_supply: value.total_supply.map(|s| U256::from_be_slice(&s)),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct TokenContract {
    pub contract_address: Felt,
    pub r#type: ContractType,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub metadata: String,
    pub total_supply: Option<U256>,
}

impl From<TokenContract> for proto::types::TokenContract {
    fn from(value: TokenContract) -> Self {
        Self {
            contract_address: value.contract_address.to_bytes_be().into(),
            contract_type: value.r#type as i32,
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals as u32,
            metadata: value.metadata.into_bytes(),
            total_supply: value.total_supply.map(|s| s.to_be_bytes().to_vec()),
        }
    }
}

impl TryFrom<proto::types::TokenContract> for TokenContract {
    type Error = ProtoError;
    fn try_from(value: proto::types::TokenContract) -> Result<Self, Self::Error> {
        Ok(Self {
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
            r#type: value.contract_type().into(),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals as u8,
            metadata: String::from_utf8(value.metadata).map_err(ProtoError::FromUtf8)?,
            total_supply: value.total_supply.map(|s| U256::from_be_slice(&s)),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct TokenBalance {
    pub balance: U256,
    pub account_address: Felt,
    pub contract_address: Felt,
    pub token_id: Option<U256>,
}

impl From<TokenBalance> for proto::types::TokenBalance {
    fn from(value: TokenBalance) -> Self {
        Self {
            balance: value.balance.to_be_bytes().to_vec(),
            account_address: value.account_address.to_bytes_be().into(),
            contract_address: value.contract_address.to_bytes_be().into(),
            token_id: value.token_id.map(|id| id.to_be_bytes().to_vec()),
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
            token_id: value.token_id.map(|id| U256::from_be_slice(&id)),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct TokenTransfer {
    pub id: String,
    pub contract_address: Felt,
    pub from_address: Felt,
    pub to_address: Felt,
    pub amount: U256,
    pub token_id: Option<U256>,
    pub executed_at: DateTime<Utc>,
    pub event_id: Option<String>,
}

impl From<TokenTransfer> for proto::types::TokenTransfer {
    fn from(value: TokenTransfer) -> Self {
        Self {
            id: value.id,
            contract_address: value.contract_address.to_bytes_be().into(),
            from_address: value.from_address.to_bytes_be().into(),
            to_address: value.to_address.to_bytes_be().into(),
            amount: value.amount.to_be_bytes().to_vec(),
            token_id: value.token_id.map(|id| id.to_be_bytes().to_vec()),
            executed_at: value.executed_at.timestamp() as u64,
            event_id: value.event_id,
        }
    }
}

impl TryFrom<proto::types::TokenTransfer> for TokenTransfer {
    type Error = ProtoError;
    fn try_from(value: proto::types::TokenTransfer) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
            from_address: Felt::from_bytes_be_slice(&value.from_address),
            to_address: Felt::from_bytes_be_slice(&value.to_address),
            amount: U256::from_be_slice(&value.amount),
            token_id: value.token_id.map(|id| U256::from_be_slice(&id)),
            executed_at: DateTime::from_timestamp(value.executed_at as i64, 0).unwrap(),
            event_id: value.event_id,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct TokenTransferQuery {
    pub account_addresses: Vec<Felt>,
    pub contract_addresses: Vec<Felt>,
    pub token_ids: Vec<U256>,
    pub pagination: Pagination,
}

impl From<TokenTransferQuery> for proto::types::TokenTransferQuery {
    fn from(value: TokenTransferQuery) -> Self {
        Self {
            account_addresses: value
                .account_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            token_ids: value
                .token_ids
                .into_iter()
                .map(|id| id.to_be_bytes().to_vec())
                .collect(),
            pagination: Some(value.pagination.into()),
        }
    }
}

impl TryFrom<proto::types::TokenTransferQuery> for TokenTransferQuery {
    type Error = ProtoError;
    fn try_from(value: proto::types::TokenTransferQuery) -> Result<Self, Self::Error> {
        Ok(Self {
            account_addresses: value
                .account_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            token_ids: value
                .token_ids
                .into_iter()
                .map(|id| U256::from_be_slice(&id))
                .collect(),
            pagination: value.pagination.map(|p| p.into()).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct ControllerQuery {
    pub contract_addresses: Vec<Felt>,
    pub usernames: Vec<String>,
    pub pagination: Pagination,
}

impl From<ControllerQuery> for proto::types::ControllerQuery {
    fn from(value: ControllerQuery) -> Self {
        Self {
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            usernames: value.usernames,
            pagination: Some(value.pagination.into()),
        }
    }
}

impl TryFrom<proto::types::ControllerQuery> for ControllerQuery {
    type Error = ProtoError;
    fn try_from(value: proto::types::ControllerQuery) -> Result<Self, Self::Error> {
        Ok(Self {
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            usernames: value.usernames,
            pagination: value.pagination.map(|p| p.into()).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct TokenQuery {
    pub contract_addresses: Vec<Felt>,
    pub token_ids: Vec<U256>,
    pub pagination: Pagination,
}

impl From<TokenQuery> for proto::types::TokenQuery {
    fn from(value: TokenQuery) -> Self {
        Self {
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            token_ids: value
                .token_ids
                .into_iter()
                .map(|id| id.to_be_bytes().to_vec())
                .collect(),
            pagination: Some(value.pagination.into()),
        }
    }
}

impl TryFrom<proto::types::TokenQuery> for TokenQuery {
    type Error = ProtoError;
    fn try_from(value: proto::types::TokenQuery) -> Result<Self, Self::Error> {
        Ok(Self {
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            token_ids: value
                .token_ids
                .into_iter()
                .map(|id| U256::from_be_slice(&id))
                .collect(),
            pagination: value.pagination.map(|p| p.into()).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct TokenBalanceQuery {
    pub account_addresses: Vec<Felt>,
    pub contract_addresses: Vec<Felt>,
    pub token_ids: Vec<U256>,
    pub pagination: Pagination,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct TokenContractQuery {
    pub contract_addresses: Vec<Felt>,
    pub contract_types: Vec<ContractType>,
    pub pagination: Pagination,
}

impl From<TokenBalanceQuery> for proto::types::TokenBalanceQuery {
    fn from(value: TokenBalanceQuery) -> Self {
        Self {
            account_addresses: value
                .account_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            token_ids: value
                .token_ids
                .into_iter()
                .map(|id| id.to_be_bytes().to_vec())
                .collect(),
            pagination: Some(value.pagination.into()),
        }
    }
}

impl TryFrom<proto::types::TokenBalanceQuery> for TokenBalanceQuery {
    type Error = ProtoError;
    fn try_from(value: proto::types::TokenBalanceQuery) -> Result<Self, Self::Error> {
        Ok(Self {
            account_addresses: value
                .account_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            token_ids: value
                .token_ids
                .into_iter()
                .map(|id| U256::from_be_slice(&id))
                .collect(),
            pagination: value.pagination.map(|p| p.into()).unwrap_or_default(),
        })
    }
}

impl From<TokenContractQuery> for proto::types::TokenContractQuery {
    fn from(value: TokenContractQuery) -> Self {
        Self {
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            contract_types: value.contract_types.into_iter().map(|t| t as i32).collect(),
            pagination: Some(value.pagination.into()),
        }
    }
}

impl TryFrom<proto::types::TokenContractQuery> for TokenContractQuery {
    type Error = ProtoError;
    fn try_from(value: proto::types::TokenContractQuery) -> Result<Self, Self::Error> {
        let contract_types = value
            .contract_types
            .into_iter()
            .map(|t| t.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            contract_types,
            pagination: value.pagination.map(|p| p.into()).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct ContractQuery {
    pub contract_addresses: Vec<Felt>,
    pub contract_types: Vec<ContractType>,
}

impl From<ContractQuery> for proto::types::ContractQuery {
    fn from(value: ContractQuery) -> Self {
        Self {
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            contract_types: value.contract_types.into_iter().map(|t| t as i32).collect(),
        }
    }
}

impl TryFrom<proto::types::ContractQuery> for ContractQuery {
    type Error = ProtoError;
    fn try_from(value: proto::types::ContractQuery) -> Result<Self, Self::Error> {
        let contract_types = value
            .contract_types
            .into_iter()
            .map(|t| t.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            contract_types,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct OrderBy {
    pub field: String,
    pub direction: OrderDirection,
}

impl From<OrderBy> for proto::types::OrderBy {
    fn from(value: OrderBy) -> Self {
        Self {
            field: value.field,
            direction: value.direction as i32,
        }
    }
}

impl From<proto::types::OrderBy> for OrderBy {
    fn from(value: proto::types::OrderBy) -> Self {
        Self {
            field: value.field,
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
    // Array-specific operators
    Contains,      // Array contains value
    ContainsAll,   // Array contains all values
    ContainsAny,   // Array contains any of the values
    ArrayLengthEq, // Array length equals
    ArrayLengthGt, // Array length greater than
    ArrayLengthLt, // Array length less than
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
            // Array operators don't use simple SQL operators,
            // they require special JSON function handling
            ComparisonOperator::Contains => write!(f, "CONTAINS"),
            ComparisonOperator::ContainsAll => write!(f, "CONTAINS_ALL"),
            ComparisonOperator::ContainsAny => write!(f, "CONTAINS_ANY"),
            ComparisonOperator::ArrayLengthEq => write!(f, "ARRAY_LENGTH_EQ"),
            ComparisonOperator::ArrayLengthGt => write!(f, "ARRAY_LENGTH_GT"),
            ComparisonOperator::ArrayLengthLt => write!(f, "ARRAY_LENGTH_LT"),
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
            ComparisonOperator::Contains => proto::types::ComparisonOperator::Contains,
            ComparisonOperator::ContainsAll => proto::types::ComparisonOperator::ContainsAll,
            ComparisonOperator::ContainsAny => proto::types::ComparisonOperator::ContainsAny,
            ComparisonOperator::ArrayLengthEq => proto::types::ComparisonOperator::ArrayLengthEq,
            ComparisonOperator::ArrayLengthGt => proto::types::ComparisonOperator::ArrayLengthGt,
            ComparisonOperator::ArrayLengthLt => proto::types::ComparisonOperator::ArrayLengthLt,
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
            proto::types::ComparisonOperator::Contains => ComparisonOperator::Contains,
            proto::types::ComparisonOperator::ContainsAll => ComparisonOperator::ContainsAll,
            proto::types::ComparisonOperator::ContainsAny => ComparisonOperator::ContainsAny,
            proto::types::ComparisonOperator::ArrayLengthEq => ComparisonOperator::ArrayLengthEq,
            proto::types::ComparisonOperator::ArrayLengthGt => ComparisonOperator::ArrayLengthGt,
            proto::types::ComparisonOperator::ArrayLengthLt => ComparisonOperator::ArrayLengthLt,
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
    pub use_legacy_store: bool,
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
            use_legacy_store: value.use_legacy_store,
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct Event {
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
    pub transaction_hash: Felt,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct EventWithMetadata {
    pub id: String,
    pub event: Event,
    pub created_at: DateTime<Utc>,
    pub executed_at: DateTime<Utc>,
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
    pub pagination: Pagination,
}

impl From<EventQuery> for proto::types::EventQuery {
    fn from(value: EventQuery) -> Self {
        Self {
            keys: value.keys.map(|k| k.into()),
            pagination: Some(value.pagination.into()),
        }
    }
}

impl From<proto::types::EventQuery> for EventQuery {
    fn from(value: proto::types::EventQuery) -> Self {
        Self {
            keys: value.keys.map(|k| k.into()),
            pagination: value.pagination.map(|p| p.into()).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub enum CallType {
    Execute,
    ExecuteFromOutside,
}

impl std::fmt::Display for CallType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallType::Execute => write!(f, "EXECUTE"),
            CallType::ExecuteFromOutside => write!(f, "EXECUTE_FROM_OUTSIDE"),
        }
    }
}

impl FromStr for CallType {
    type Err = ProtoError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "EXECUTE" => Ok(CallType::Execute),
            "EXECUTE_FROM_OUTSIDE" => Ok(CallType::ExecuteFromOutside),
            _ => Err(ProtoError::InvalidCallType(s.to_string())),
        }
    }
}

impl From<proto::types::CallType> for CallType {
    fn from(value: proto::types::CallType) -> Self {
        match value {
            proto::types::CallType::Execute => CallType::Execute,
            proto::types::CallType::ExecuteFromOutside => CallType::ExecuteFromOutside,
        }
    }
}

impl From<CallType> for proto::types::CallType {
    fn from(value: CallType) -> Self {
        match value {
            CallType::Execute => proto::types::CallType::Execute,
            CallType::ExecuteFromOutside => proto::types::CallType::ExecuteFromOutside,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct TransactionCall {
    pub contract_address: Felt,
    pub entrypoint: String,
    pub calldata: Vec<Felt>,
    pub call_type: CallType,
    pub caller_address: Felt,
}

impl From<TransactionCall> for proto::types::TransactionCall {
    fn from(value: TransactionCall) -> Self {
        Self {
            contract_address: value.contract_address.to_bytes_be().into(),
            entrypoint: value.entrypoint,
            calldata: value
                .calldata
                .into_iter()
                .map(|d| d.to_bytes_be().into())
                .collect(),
            call_type: value.call_type as i32,
            caller_address: value.caller_address.to_bytes_be().into(),
        }
    }
}

impl TryFrom<proto::types::TransactionCall> for TransactionCall {
    type Error = ProtoError;
    fn try_from(value: proto::types::TransactionCall) -> Result<Self, Self::Error> {
        let call_type = value.call_type().into();
        Ok(Self {
            contract_address: Felt::from_bytes_be_slice(&value.contract_address),
            entrypoint: value.entrypoint,
            calldata: value
                .calldata
                .into_iter()
                .map(|d| Felt::from_bytes_be_slice(&d))
                .collect(),
            call_type,
            caller_address: Felt::from_bytes_be_slice(&value.caller_address),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone, Default)]
pub struct Transaction {
    pub transaction_hash: Felt,
    pub sender_address: Felt,
    pub calldata: Vec<Felt>,
    pub max_fee: Felt,
    pub signature: Vec<Felt>,
    pub nonce: Felt,
    pub block_number: u64,
    pub transaction_type: String,
    pub block_timestamp: DateTime<Utc>,
    pub calls: Vec<TransactionCall>,
    pub unique_models: Vec<Felt>,
}

impl From<Transaction> for proto::types::Transaction {
    fn from(value: Transaction) -> Self {
        Self {
            transaction_hash: value.transaction_hash.to_bytes_be().into(),
            sender_address: value.sender_address.to_bytes_be().into(),
            calldata: value
                .calldata
                .into_iter()
                .map(|d| d.to_bytes_be().into())
                .collect(),
            max_fee: value.max_fee.to_bytes_be().into(),
            signature: value
                .signature
                .into_iter()
                .map(|s| s.to_bytes_be().into())
                .collect(),
            nonce: value.nonce.to_bytes_be().into(),
            block_number: value.block_number,
            transaction_type: value.transaction_type,
            block_timestamp: value.block_timestamp.timestamp() as u64,
            calls: value.calls.into_iter().map(|c| c.into()).collect(),
            unique_models: value
                .unique_models
                .into_iter()
                .map(|m| m.to_bytes_be().into())
                .collect(),
        }
    }
}

impl TryFrom<proto::types::Transaction> for Transaction {
    type Error = ProtoError;
    fn try_from(value: proto::types::Transaction) -> Result<Self, Self::Error> {
        Ok(Self {
            transaction_hash: Felt::from_bytes_be_slice(&value.transaction_hash),
            sender_address: Felt::from_bytes_be_slice(&value.sender_address),
            calldata: value
                .calldata
                .into_iter()
                .map(|d| Felt::from_bytes_be_slice(&d))
                .collect(),
            max_fee: Felt::from_bytes_be_slice(&value.max_fee),
            signature: value
                .signature
                .into_iter()
                .map(|s| Felt::from_bytes_be_slice(&s))
                .collect(),
            nonce: Felt::from_bytes_be_slice(&value.nonce),
            block_number: value.block_number,
            transaction_type: value.transaction_type,
            block_timestamp: DateTime::from_timestamp(value.block_timestamp as i64, 0).unwrap(),
            calls: value
                .calls
                .into_iter()
                .map(|c| c.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            unique_models: value
                .unique_models
                .into_iter()
                .map(|m| Felt::from_bytes_be_slice(&m))
                .collect(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct TransactionFilter {
    pub transaction_hashes: Vec<Felt>,
    pub caller_addresses: Vec<Felt>,
    pub contract_addresses: Vec<Felt>,
    pub entrypoints: Vec<String>,
    pub model_selectors: Vec<Felt>,
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
}

impl From<TransactionFilter> for proto::types::TransactionFilter {
    fn from(value: TransactionFilter) -> Self {
        Self {
            transaction_hashes: value
                .transaction_hashes
                .into_iter()
                .map(|h| h.to_bytes_be().into())
                .collect(),
            caller_addresses: value
                .caller_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().into())
                .collect(),
            entrypoints: value.entrypoints,
            model_selectors: value
                .model_selectors
                .into_iter()
                .map(|m| m.to_bytes_be().into())
                .collect(),
            from_block: value.from_block,
            to_block: value.to_block,
        }
    }
}

impl TryFrom<proto::types::TransactionFilter> for TransactionFilter {
    type Error = ProtoError;
    fn try_from(value: proto::types::TransactionFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            transaction_hashes: value
                .transaction_hashes
                .into_iter()
                .map(|h| Felt::from_bytes_be_slice(&h))
                .collect(),
            caller_addresses: value
                .caller_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            contract_addresses: value
                .contract_addresses
                .into_iter()
                .map(|a| Felt::from_bytes_be_slice(&a))
                .collect(),
            entrypoints: value.entrypoints,
            model_selectors: value
                .model_selectors
                .into_iter()
                .map(|m| Felt::from_bytes_be_slice(&m))
                .collect(),
            from_block: value.from_block,
            to_block: value.to_block,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct TransactionQuery {
    pub filter: Option<TransactionFilter>,
    pub pagination: Pagination,
}

impl From<TransactionQuery> for proto::types::TransactionQuery {
    fn from(value: TransactionQuery) -> Self {
        Self {
            filter: value.filter.map(|f| f.into()),
            pagination: Some(value.pagination.into()),
        }
    }
}

impl TryFrom<proto::types::TransactionQuery> for TransactionQuery {
    type Error = ProtoError;
    fn try_from(value: proto::types::TransactionQuery) -> Result<Self, Self::Error> {
        Ok(Self {
            filter: value.filter.map(|f| f.try_into()).transpose()?,
            pagination: value.pagination.map(|p| p.into()).unwrap_or_default(),
        })
    }
}
