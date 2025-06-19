use std::str::FromStr;

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

#[derive(Debug, Clone, Deserialize, Copy)]
pub enum ContractType {
    WORLD,
    ERC20,
    ERC721,
    ERC1155,
    UDC,
}

impl FromStr for ContractType {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_lowercase().as_str() {
            "world" => Ok(ContractType::WORLD),
            "erc20" => Ok(ContractType::ERC20),
            "erc721" => Ok(ContractType::ERC721),
            "erc1155" => Ok(ContractType::ERC1155),
            "udc" => Ok(ContractType::UDC),
            _ => Err(anyhow::anyhow!("Invalid ERC type: {}", input)),
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
        }
    }
}