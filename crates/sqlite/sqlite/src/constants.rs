pub const QUERY_QUEUE_BATCH_SIZE: usize = 1000;
pub const TOKEN_BALANCE_TABLE: &str = "token_balances";
pub const TOKEN_TRANSFER_TABLE: &str = "token_transfers";
pub const TOKENS_TABLE: &str = "tokens";
pub const WORLD_CONTRACT_TYPE: &str = "WORLD";
pub const SQL_FELT_DELIMITER: &str = "/";
pub const REQ_MAX_RETRIES: u8 = 3;

pub const SQL_DEFAULT_LIMIT: u64 = 10000;
pub const SQL_MAX_JOINS: usize = 64;

pub const ENTITIES_TABLE: &str = "entities";
pub const ENTITIES_MODEL_RELATION_TABLE: &str = "entity_model";
pub const ENTITIES_ENTITY_RELATION_COLUMN: &str = "internal_entity_id";

pub const ENTITIES_HISTORICAL_TABLE: &str = "entities_historical";

pub const EVENT_MESSAGES_TABLE: &str = "event_messages";
pub const EVENT_MESSAGES_MODEL_RELATION_TABLE: &str = "event_model";
pub const EVENT_MESSAGES_ENTITY_RELATION_COLUMN: &str = "internal_event_message_id";

pub const EVENT_MESSAGES_HISTORICAL_TABLE: &str = "event_messages_historical";

pub const EVENTS_TABLE: &str = "events";
