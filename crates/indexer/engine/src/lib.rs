mod constants;

#[cfg(test)]
#[path = "test.rs"]
mod test;

pub mod engine;
pub mod error;
pub use engine::Engine;
pub use torii_indexer_fetcher::{FetcherConfig, FetchingFlags};

use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone)]
    pub struct IndexingFlags: u32 {
        const RAW_EVENTS = 0b00000001;
    }
}