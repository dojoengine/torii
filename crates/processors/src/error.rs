use thiserror::Error;
use torii_sqlite::error::ParseError;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    ProviderError(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    SqliteError(#[from] torii_sqlite::error::Error),
    #[error(transparent)]
    TaskNetworkError(#[from] torii_task_network::TaskNetworkError),
    #[error(transparent)]
    ModelError(#[from] dojo_world::contracts::model::ModelError),
    #[error(transparent)]
    PrimitiveError(#[from] dojo_types::primitive::PrimitiveError),
    #[error("Model member not found: {0}")]
    ModelMemberNotFound(String),
    #[error("Uri is malformed")]
    UriMalformed,
    #[error(transparent)]
    IpfsError(#[from] ipfs_api_backend_hyper::Error),
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error(transparent)]
    CairoSerdeError(#[from] cainome::cairo_serde::Error),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
    #[error(transparent)]
    ControllerProcessorError(#[from] crate::processors::controller::ControllerProcessorError),
    #[error(transparent)]
    TokenMetadataError(#[from] TokenMetadataError),
}

#[derive(Error, Debug)]
pub enum TokenMetadataError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Ipfs(#[from] ipfs_api_backend_hyper::Error),
    #[error(transparent)]
    DataUrl(#[from] data_url::DataUrlError),
    #[error(transparent)]
    InvalidBase64(#[from] data_url::forgiving_base64::InvalidBase64),
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error("Invalid mime type: {0}")]
    InvalidMimeType(String),
    #[error("Unsupported URI scheme: {0}")]
    UnsupportedUriScheme(String),
    #[error(transparent)]
    AcquireError(#[from] tokio::sync::AcquireError),
    #[error("Invalid token name")]
    InvalidTokenName,
    #[error("Invalid token symbol")]
    InvalidTokenSymbol,
    #[error("Invalid token decimals")]
    InvalidTokenDecimals,
}