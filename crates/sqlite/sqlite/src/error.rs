use std::num::ParseIntError;

use dojo_types::primitive::PrimitiveError;
use dojo_types::schema::EnumError;
use starknet::core::types::FromStrError;
use starknet::core::utils::{
    CairoShortStringToFeltError, NonAsciiNameError, ParseCairoShortStringError,
};
use starknet::providers::ProviderError;

use crate::executor::QueryMessage;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Parsing error: {0}")]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    PrimitiveError(#[from] PrimitiveError),
    #[error(transparent)]
    EnumError(#[from] EnumError),
    #[error(transparent)]
    ProviderError(#[from] ProviderError),
    #[error(transparent)]
    ExecutorSendError(#[from] tokio::sync::mpsc::error::SendError<QueryMessage>),
    #[error(transparent)]
    ExecutorRecvError(#[from] tokio::sync::oneshot::error::RecvError),
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error(transparent)]
    IpfsError(#[from] ipfs_api_backend_hyper::Error),
    #[error(transparent)]
    DataUrlError(#[from] data_url::DataUrlError),
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

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("Unsuccessful status code: {0} with body: {1}")]
    StatusCode(reqwest::StatusCode, String),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error(transparent)]
    NonAsciiName(#[from] NonAsciiNameError),
    #[error(transparent)]
    FromStr(#[from] FromStrError),
    #[error(transparent)]
    ParseCairoShortString(#[from] ParseCairoShortStringError),
    #[error(transparent)]
    CairoShortStringToFelt(#[from] CairoShortStringToFeltError),
    #[error(transparent)]
    ParseIntError(#[from] ParseIntError),
    #[error(transparent)]
    CairoSerdeError(#[from] cainome::cairo_serde::Error),
    #[error(transparent)]
    FromJsonStr(#[from] serde_json::Error),
    #[error(transparent)]
    FromSlice(#[from] std::array::TryFromSliceError),
    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error("Entity is not a struct")]
    InvalidTyEntity,
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("Unsupported query: {0}")]
    UnsupportedQuery(String),
    #[error("Missing param: {0}")]
    MissingParam(String),
    #[error("Unsupported value for primitive: {0}")]
    UnsupportedValue(String),
    #[error("Model not found: {0}")]
    ModelNotFound(String),
    #[error("Exceeds sqlite `JOIN` limit (64)")]
    SqliteJoinLimit,
    #[error("Invalid namespaced model: {0}")]
    InvalidNamespacedModel(String),
    #[error("Invalid cursor: {0}")]
    InvalidCursor(String),
}
