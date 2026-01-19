use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Fetcher(#[from] torii_indexer_fetcher::Error),
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error(transparent)]
    Cache(#[from] torii_cache::error::Error),
    #[error(transparent)]
    Storage(#[from] torii_storage::StorageError),
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
}

#[derive(Error, Debug)]
pub enum ProcessError {
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    Processors(#[from] torii_processors::error::Error),
    #[error(transparent)]
    Cache(#[from] torii_cache::error::Error),
    #[error(transparent)]
    Storage(#[from] torii_storage::StorageError),
}
