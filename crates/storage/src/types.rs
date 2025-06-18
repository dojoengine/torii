use serde::Deserialize;
use starknet::core::types::Felt;

/// Represents a cursor for tracking blockchain state
#[derive(Default, Debug, Clone)]
pub struct Cursor {
    pub last_pending_block_tx: Option<Felt>,
    pub head: Option<u64>,
    pub last_block_timestamp: Option<u64>,
}

/// Represents a parsed call within a transaction
#[derive(Debug, Clone, Deserialize)]
pub struct ParsedCall {
    pub contract_address: Felt,
    pub entrypoint: String,
    pub calldata: Vec<Felt>,
    pub call_type: CallType,
    pub caller_address: Felt,
}

/// Type of call made in a transaction
#[derive(Debug, Clone, Deserialize)]
pub enum CallType {
    Call,
    Invoke,
    Deploy,
}

impl std::fmt::Display for CallType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallType::Call => write!(f, "call"),
            CallType::Invoke => write!(f, "invoke"),
            CallType::Deploy => write!(f, "deploy"),
        }
    }
}