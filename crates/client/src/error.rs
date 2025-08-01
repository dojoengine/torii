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
