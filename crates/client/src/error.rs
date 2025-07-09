use dojo_world::contracts::model::ModelError;
use starknet::core::types::Felt;
use starknet::core::utils::{CairoShortStringToFeltError, ParseCairoShortStringError};
use torii_proto::error::ProtoError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Subscription service uninitialized")]
    SubscriptionUninitialized,
    #[error("Invalid model name: {0}. Expected format is \"namespace-model\"")]
    InvalidModelName(String),
    #[error("Unknown model: {0}")]
    UnknownModel(Felt),
    #[error("Parsing error: {0}")]
    Parse(#[from] ParseError),
    #[error(transparent)]
    GrpcClient(#[from] torii_grpc_client::Error),
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error("Unsupported query")]
    UnsupportedQuery,
    #[error(transparent)]
    Proto(#[from] ProtoError),
    #[error(transparent)]
    Sql(#[from] SqlError),
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    FeltFromStr(#[from] starknet::core::types::FromStrError),
    #[error(transparent)]
    CairoShortStringToFelt(#[from] CairoShortStringToFeltError),
    #[error(transparent)]
    ParseCairoShortString(#[from] ParseCairoShortStringError),
}

#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error(transparent)]
    Http(#[from] hyper::http::Error),
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error("SQL query error: {0}")]
    Query(String),
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),
}
