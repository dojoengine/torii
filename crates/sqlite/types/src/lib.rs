use core::fmt;
use std::collections::HashSet;

use chrono::{DateTime, Utc};
use crypto_bigint::U256;
use dojo_types::schema::Ty;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use starknet::core::types::Felt;
use std::str::FromStr;
use torii_proto::{schema::EntityWithMetadata, TransactionCall};

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
    pub id: String,        // Composite: "world_address:entity_id"
    pub entity_id: String, // Just the entity hash (for easy access)
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

impl<const EVENT_MESSAGE: bool> From<Entity> for torii_proto::schema::Entity<EVENT_MESSAGE> {
    fn from(value: Entity) -> Self {
        let models = if value.deleted {
            vec![]
        } else {
            vec![value.updated_model.unwrap().as_struct().unwrap().clone()]
        };

        // Use the dedicated entity_id column (no parsing needed!)
        Self {
            hashed_keys: Felt::from_str(&value.entity_id).unwrap(),
            models,
            created_at: value.created_at,
            updated_at: value.updated_at,
            executed_at: value.executed_at,
        }
    }
}

impl<const EVENT_MESSAGE: bool> From<Entity> for EntityWithMetadata<EVENT_MESSAGE> {
    fn from(value: Entity) -> Self {
        let keys = value
            .keys
            .split('/')
            .filter_map(|k| {
                if k.is_empty() {
                    None
                } else {
                    Some(Felt::from_str(k).unwrap())
                }
            })
            .collect();
        Self {
            event_id: value.event_id.clone(),
            entity: value.into(),
            keys,
        }
    }
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Model {
    pub id: String,             // Composite: "world_address:model_selector"
    pub model_selector: String, // Just the model selector (for easy access)
    pub world_address: String,  // The world address (for filtering)
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
    pub legacy_store: bool,
}

impl From<Model> for torii_proto::Model {
    fn from(value: Model) -> Self {
        Self {
            namespace: value.namespace,
            name: value.name,
            selector: Felt::from_str(&value.model_selector).unwrap(),
            class_hash: Felt::from_str(&value.class_hash).unwrap(),
            contract_address: Felt::from_str(&value.contract_address).unwrap(),
            layout: serde_json::from_str(&value.layout).unwrap(),
            schema: serde_json::from_str(&value.schema).unwrap(),
            packed_size: value.packed_size,
            unpacked_size: value.unpacked_size,
            use_legacy_store: value.legacy_store,
        }
    }
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
pub struct Activity {
    pub id: String,
    pub world_address: String,
    pub namespace: String,
    pub caller_address: String,
    pub session_start: DateTime<Utc>,
    pub session_end: DateTime<Utc>,
    pub action_count: i32,
    pub actions: String, // JSON string
    pub updated_at: DateTime<Utc>,
}

impl From<Activity> for torii_proto::Activity {
    fn from(value: Activity) -> Self {
        use std::collections::HashMap;
        let actions: HashMap<String, u32> =
            serde_json::from_str(&value.actions).unwrap_or_default();
        Self {
            id: value.id,
            world_address: Felt::from_hex(&value.world_address).unwrap(),
            namespace: value.namespace,
            caller_address: Felt::from_hex(&value.caller_address).unwrap(),
            session_start: value.session_start,
            session_end: value.session_end,
            action_count: value.action_count as u32,
            actions,
            updated_at: value.updated_at,
        }
    }
}

impl From<Event> for torii_proto::EventWithMetadata {
    fn from(value: Event) -> Self {
        Self {
            id: value.id.clone(),
            event: value.clone().into(),
            created_at: value.created_at,
            executed_at: value.executed_at,
        }
    }
}
impl From<Event> for torii_proto::Event {
    fn from(value: Event) -> Self {
        Self {
            keys: value
                .keys
                .split('/')
                .filter(|k| !k.is_empty())
                .map(|k| Felt::from_str(k).unwrap())
                .collect(),
            data: value
                .data
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
pub struct Token {
    pub id: String,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub metadata: String,
    pub total_supply: Option<String>,
}

impl From<Token> for torii_proto::Token {
    fn from(value: Token) -> Self {
        Self {
            token_id: if value.token_id.is_empty() {
                None
            } else {
                Some(U256::from_be_hex(value.token_id.trim_start_matches("0x")))
            },
            contract_address: Felt::from_str(&value.contract_address).unwrap(),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals,
            metadata: value.metadata,
            total_supply: value
                .total_supply
                .map(|s| U256::from_be_hex(s.trim_start_matches("0x"))),
        }
    }
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TokenContract {
    pub contract_address: String,
    pub contract_type: String,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub metadata: String,
    pub total_supply: Option<String>,
    pub traits: String,
    pub token_metadata: String,
}

impl From<TokenContract> for torii_proto::TokenContract {
    fn from(value: TokenContract) -> Self {
        Self {
            contract_address: Felt::from_str(&value.contract_address).unwrap(),
            r#type: value
                .contract_type
                .parse()
                .unwrap_or(torii_proto::ContractType::OTHER),
            name: value.name,
            symbol: value.symbol,
            decimals: value.decimals,
            metadata: value.metadata,
            total_supply: value
                .total_supply
                .map(|s| U256::from_be_hex(s.trim_start_matches("0x"))),
            traits: value.traits,
            token_metadata: value.token_metadata,
        }
    }
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
                Some(U256::from_be_hex(id[1].trim_start_matches("0x")))
            } else {
                None
            },
        }
    }
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TokenTransfer {
    pub id: String,
    pub contract_address: String,
    pub from_address: String,
    pub to_address: String,
    pub amount: String,
    pub token_id: String,
    pub executed_at: DateTime<Utc>,
    pub event_id: Option<String>,
}

impl From<TokenTransfer> for torii_proto::TokenTransfer {
    fn from(value: TokenTransfer) -> Self {
        let token_id_opt = value
            .token_id
            .split(':')
            .collect::<Vec<&str>>()
            .get(1)
            .map(|tid| U256::from_be_hex(tid.trim_start_matches("0x")));

        Self {
            id: value.id,
            contract_address: Felt::from_str(&value.contract_address).unwrap(),
            from_address: Felt::from_str(&value.from_address).unwrap(),
            to_address: Felt::from_str(&value.to_address).unwrap(),
            amount: U256::from_be_hex(value.amount.trim_start_matches("0x")),
            token_id: token_id_opt,
            executed_at: value.executed_at,
            event_id: value.event_id,
        }
    }
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
    pub calls: Vec<TransactionCall>,
    #[sqlx(skip)]
    pub contract_addresses: HashSet<Felt>,
    #[sqlx(skip)]
    pub unique_models: HashSet<Felt>,
}

impl From<Transaction> for torii_proto::Transaction {
    fn from(value: Transaction) -> Self {
        Self {
            transaction_hash: Felt::from_str(&value.transaction_hash).unwrap(),
            sender_address: Felt::from_str(&value.sender_address).unwrap(),
            calldata: value
                .calldata
                .split('/')
                .filter(|d| !d.is_empty())
                .map(|d| Felt::from_str(d).unwrap())
                .collect(),
            max_fee: Felt::from_str(&value.max_fee).unwrap(),
            signature: value
                .signature
                .split('/')
                .filter(|s| !s.is_empty())
                .map(|s| Felt::from_str(s).unwrap())
                .collect(),
            nonce: Felt::from_str(&value.nonce).unwrap(),
            block_number: value.block_number,
            transaction_type: value.transaction_type,
            block_timestamp: value.executed_at,
            calls: value.calls,
            unique_models: value.unique_models.into_iter().collect(),
        }
    }
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

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Contract {
    pub id: String,
    pub contract_address: String,
    pub contract_type: String,
    pub head: Option<i64>,
    pub tps: Option<i64>,
    pub last_block_timestamp: Option<i64>,
    pub last_pending_block_tx: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl From<Contract> for torii_proto::Contract {
    fn from(value: Contract) -> Self {
        let contract_type = torii_proto::ContractType::from_str(&value.contract_type)
            .unwrap_or(torii_proto::ContractType::OTHER);

        Self {
            contract_address: Felt::from_str(&value.contract_address).unwrap(),
            contract_type,
            head: value.head.map(|h| h as u64),
            tps: value.tps.map(|t| t as u64),
            last_block_timestamp: value.last_block_timestamp.map(|t| t as u64),
            last_pending_block_tx: value
                .last_pending_block_tx
                .map(|tx| Felt::from_str(&tx).unwrap()),
            updated_at: value.updated_at,
            created_at: value.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AggregatorConfig {
    pub id: String,
    pub model_tag: String,
    #[serde(deserialize_with = "deserialize_group_by")]
    pub group_by: Vec<String>,
    pub aggregation: Aggregation,
    pub order: SortOrder,
}

// Custom deserializer to handle both single string and array for group_by
fn deserialize_group_by<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    use serde_json::Value;

    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(s) => Ok(vec![s]),
        Value::Array(arr) => arr
            .into_iter()
            .map(|v| match v {
                Value::String(s) => Ok(s),
                _ => Err(D::Error::custom("group_by array must contain only strings")),
            })
            .collect(),
        _ => Err(D::Error::custom(
            "group_by must be a string or array of strings",
        )),
    }
}

impl<'de> Deserialize<'de> for AggregatorConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct AggregatorConfigHelper {
            id: String,
            model_tag: String,
            #[serde(deserialize_with = "deserialize_group_by")]
            group_by: Vec<String>,
            aggregation: Aggregation,
            order: SortOrder,
        }

        let helper = AggregatorConfigHelper::deserialize(deserializer)?;
        Ok(AggregatorConfig {
            id: helper.id,
            model_tag: helper.model_tag,
            group_by: helper.group_by,
            aggregation: helper.aggregation,
            order: helper.order,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Aggregation {
    /// Count occurrences by incrementing by 1 for each update (useful for counting events like wins, kills, etc.)
    Count,
    /// Keep the latest value from the field (useful for current score, level, rank)
    Latest(String),
    /// Keep the maximum value seen from the field (useful for high scores, best combo)
    Max(String),
    /// Keep the minimum value seen from the field (useful for fastest times, speedruns)
    Min(String),
    /// Sum/accumulate values from the field (useful for total XP, total gold earned)
    Sum(String),
    /// Calculate the average value from the field (useful for average score, average time)
    Avg(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SortOrder {
    Desc,
    Asc,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AggregationEntry {
    pub id: String,
    pub aggregator_id: String,
    pub entity_id: String,
    pub value: String,
    pub display_value: String,
    pub model_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Only used for Avg aggregation to track sum and count
    #[sqlx(default)]
    pub metadata: Option<String>,
}

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AggregationEntryWithPosition {
    pub id: String,
    pub aggregator_id: String,
    pub entity_id: String,
    pub value: String,
    pub display_value: String,
    pub model_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub position: i64,
}

impl From<AggregationEntryWithPosition> for torii_proto::AggregationEntry {
    fn from(value: AggregationEntryWithPosition) -> Self {
        Self {
            id: value.id,
            aggregator_id: value.aggregator_id,
            entity_id: value.entity_id,
            value: U256::from_be_hex(value.value.trim_start_matches("0x")),
            display_value: value.display_value,
            model_id: Felt::from_str(&value.model_id).unwrap(),
            created_at: value.created_at,
            updated_at: value.updated_at,
            position: value.position as u64,
        }
    }
}
