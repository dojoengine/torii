use starknet::core::types::Felt;
use std::str::FromStr;

pub fn format_event_id(
    block_number: u64,
    transaction_hash: &Felt,
    contract_address: &Felt,
    event_idx: u64,
) -> String {
    format!(
        "{:#064x}:{:#064x}:{:#064x}:{:#04x}",
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

/// Formats a world-scoped identifier by combining world address with any identifier.
/// This ensures that identifiers with the same value in different worlds remain unique.
///
/// # Arguments
/// * `world_address` - The address of the world contract
/// * `identifier` - The identifier (entity ID, model selector, etc.)
///
/// # Returns
/// A formatted string in the format "world_address:identifier" with zero-padded Felts
///
/// # Example
/// ```ignore
/// let world_addr = Felt::from_hex("0x1234").unwrap();
/// let entity_id = Felt::from_hex("0x5678").unwrap();
/// let scoped_id = format_world_scoped_id(&world_addr, &entity_id);
/// // Returns: "0x0000...1234:0x0000...5678" (with full padding)
/// ```
pub fn format_world_scoped_id(world_address: &Felt, selector: &Felt) -> String {
    format!("{:#064x}:{:#064x}", world_address, selector)
}

/// Parses a world-scoped identifier into its components.
///
/// # Arguments
/// * `scoped_id` - A world-scoped identifier string in the format "world_address:selector"
///
/// # Returns
/// A tuple of (world_address, selector)
///
/// # Example
/// ```ignore
/// let (world_addr, selector) = parse_world_scoped_id("0x1234:0x5678")?;
/// ```
pub fn parse_world_scoped_id(scoped_id: &str) -> Result<(Felt, Felt), Box<dyn std::error::Error>> {
    let parts: Vec<&str> = scoped_id.split(':').collect();
    if parts.len() != 2 {
        return Err("Invalid world-scoped ID format: expected 'world_address:selector'".into());
    }
    Ok((Felt::from_str(parts[0])?, Felt::from_str(parts[1])?))
}
