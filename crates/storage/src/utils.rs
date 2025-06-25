use starknet::core::types::Felt;
use std::str::FromStr;

pub fn format_event_id(
    block_number: u64,
    transaction_hash: &Felt,
    contract_address: &Felt,
    event_idx: u64,
) -> String {
    format!(
        "{:#064x}:{:#x}:{:#x}:{:#04x}",
        block_number, transaction_hash, contract_address, event_idx
    )
}

type BlockNumber = u64;
type TransactionHash = Felt;
type ContractAddress = Felt;
type EventIdx = u64;

pub fn parse_event_id(event_id: &str) -> (BlockNumber, TransactionHash, ContractAddress, EventIdx) {
    let parts: Vec<&str> = event_id.split(':').collect();
    (
        u64::from_str_radix(parts[0].trim_start_matches("0x"), 16).unwrap(),
        Felt::from_str(parts[1]).unwrap(),
        Felt::from_str(parts[2]).unwrap(),
        u64::from_str_radix(parts[3].trim_start_matches("0x"), 16).unwrap(),
    )
}
