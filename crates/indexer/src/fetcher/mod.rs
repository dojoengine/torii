#[cfg(test)]
#[path = "fetcher_test.rs"]
mod fetcher_test;

pub mod fetcher;
pub use fetcher::{FetchPendingResult, FetchRangeResult, FetchResult, Fetcher};
