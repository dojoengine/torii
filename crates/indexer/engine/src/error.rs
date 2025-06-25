use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Fetcher(#[from] torii_indexer_fetcher::Error),
    #[error(transparent)]
    ProcessError(#[from] ProcessError),
    #[error(transparent)]
    Cache(torii_cache::CacheError),
    #[error(transparent)]
    Storage(torii_storage::StorageError),
    #[error(transparent)]
    ProviderError(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),
    #[error(transparent)]
    ControllerSync(#[from] torii_controllers::error::Error),
}

#[derive(Error, Debug)]
pub enum ProcessError {
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    Processors(#[from] torii_processors::error::Error),
    #[error(transparent)]
    Cache(torii_cache::CacheError),
    #[error(transparent)]
    Storage(torii_storage::StorageError),
}
