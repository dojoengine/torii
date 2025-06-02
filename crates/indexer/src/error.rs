use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    FetchError(#[from] Box<Error>),
    #[error(transparent)]
    SqliteError(#[from] torii_sqlite::error::Error),
    #[error(transparent)]
    ProviderError(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub enum FetchError {
    #[error(transparent)]
    ProviderError(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub enum ProcessError {
    #[error(transparent)]
    ProviderError(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),
    #[error(transparent)]
    SqliteError(#[from] torii_sqlite::error::Error),
    #[error(transparent)]
    ProcessorsError(#[from] torii_processors::error::Error),
}
