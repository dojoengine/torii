use starknet::{core::types::FromStrError, providers::ProviderError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Provider(#[from] ProviderError),
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error(transparent)]
    FromStr(#[from] FromStrError),
    #[error(transparent)]
    FromJsonStr(#[from] serde_json::Error),
}