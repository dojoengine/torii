#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("API error: {0}")]
    ApiError(String),
    #[error(transparent)]
    Storage(#[from] torii_storage::StorageError),
}
