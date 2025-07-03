use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Fetcher(#[from] torii_indexer_fetcher::Error),
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error(transparent)]
    SqliteError(#[from] torii_sqlite::error::Error),
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    ControllerSync(#[from] torii_sqlite::error::ControllerSyncError),
}

#[derive(Error, Debug)]
pub enum ProcessError {
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    Sqlite(#[from] torii_sqlite::error::Error),
    #[error(transparent)]
    Processors(#[from] torii_processors::error::Error),
}
