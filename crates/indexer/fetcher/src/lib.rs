#[cfg(test)]
mod test;

pub mod json_rpc;
pub use json_rpc::{FetchPendingResult, FetchRangeResult, FetchResult, Fetcher};
