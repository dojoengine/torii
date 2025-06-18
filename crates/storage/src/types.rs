use serde::Deserialize;
use starknet::core::types::Felt;

#[derive(Debug, Clone, Deserialize)]
pub enum CallType {
    Execute,
    ExecuteFromOutside,
}

impl std::fmt::Display for CallType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallType::Execute => write!(f, "EXECUTE"),
            CallType::ExecuteFromOutside => write!(f, "EXECUTE_FROM_OUTSIDE"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParsedCall {
    pub contract_address: Felt,
    pub entrypoint: String,
    pub calldata: Vec<Felt>,
    pub call_type: CallType,
    pub caller_address: Felt,
}