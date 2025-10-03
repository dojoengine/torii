use starknet::core::types::{Felt, FromStrError};
use starknet::providers::ProviderError;
use thiserror::Error;
use torii_storage::StorageError;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error("Model not found in cache: {0:#x}")]
    ModelNotFound(Felt),
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error(transparent)]
    FromStr(#[from] FromStrError),
    #[error(transparent)]
    FromJsonStr(#[from] serde_json::Error),
}
