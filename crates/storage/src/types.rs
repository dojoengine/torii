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

#[derive(Debug, Clone, Deserialize, Copy, Hash, PartialEq, Eq)]
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
