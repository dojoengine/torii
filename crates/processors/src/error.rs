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
}
