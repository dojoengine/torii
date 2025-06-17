#[cfg(test)]
mod test;

pub mod error;
pub use error::Error;

pub mod json_rpc;
pub use json_rpc::{FetchPendingResult, FetchRangeResult, FetchResult, Fetcher};
