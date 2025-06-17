#[cfg(test)]
#[path = "fetcher_test.rs"]
mod fetcher_test;

pub mod fetcher_json_rpc;
pub use fetcher_json_rpc::{FetchPendingResult, FetchRangeResult, FetchResult, Fetcher};
