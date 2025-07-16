#[cfg(test)]
mod test;

pub mod error;
use std::collections::{BTreeMap, HashMap, HashSet};

pub use error::Error;

pub mod json_rpc;
use bitflags::bitflags;
use hashlink::LinkedHashMap;
pub use json_rpc::Fetcher;
use torii_storage::proto::ContractCursor;
use starknet::core::types::{Event, TransactionContent};
use starknet_crypto::Felt;

bitflags! {
    #[derive(Debug, Clone)]
    pub struct FetchingFlags: u32 {
        const TRANSACTIONS = 0b00000001;
        const PENDING_BLOCKS = 0b00000010;
    }
}

#[derive(Debug, Clone)]
pub struct FetcherConfig {
    pub batch_chunk_size: usize,
    pub blocks_chunk_size: u64,
    pub events_chunk_size: u64,
    pub world_block: u64,
    pub flags: FetchingFlags,
}

impl Default for FetcherConfig {
    fn default() -> Self {
        Self {
            batch_chunk_size: 1024,
            blocks_chunk_size: 10240,
            events_chunk_size: 1024,
            world_block: 0,
            flags: FetchingFlags::empty(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchRangeBlock {
    // For pending blocks, this is None.
    // We check the parent hash of the pending block to the latest block
    // to see if we need to re fetch the pending block.
    pub block_hash: Option<Felt>,
    pub timestamp: u64,
    pub transactions: LinkedHashMap<Felt, FetchTransaction>,
}

#[derive(Debug, Clone)]
pub struct FetchTransaction {
    // this is Some if the transactions indexing flag
    // is enabled
    pub transaction: Option<TransactionContent>,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone)]
pub struct FetchRangeResult {
    // block_number -> block and transactions
    pub blocks: BTreeMap<u64, FetchRangeBlock>,
    // contract_address -> transaction count
    pub cursor_transactions: HashMap<Felt, HashSet<Felt>>,
    // new updated cursors
    pub cursors: HashMap<Felt, ContractCursor>,
}

#[derive(Debug, Clone)]
pub struct FetchPendingResult {
    pub block_number: u64,
    pub timestamp: u64,
    pub cursors: HashMap<Felt, ContractCursor>,
    pub transactions: LinkedHashMap<Felt, FetchTransaction>,
    pub cursor_transactions: HashMap<Felt, HashSet<Felt>>,
}

#[derive(Debug, Clone)]
pub struct FetchResult {
    pub range: FetchRangeResult,
    pub pending: Option<FetchPendingResult>,
}
