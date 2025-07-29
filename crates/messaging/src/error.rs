use dojo_types::primitive::PrimitiveError;
use dojo_types::schema::EnumError;
use starknet::core::types::FromStrError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MessagingError {
    #[error("Invalid type: {0}")]
    InvalidType(String),

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Field not found: {0}")]
    FieldNotFound(String),

    #[error("Invalid tuple length mismatch")]
    InvalidTupleLength,

    #[error(transparent)]
    ParseFeltError(#[from] FromStrError),

    #[error("Invalid model tag: {0}")]
    InvalidModelTag(String),

    #[error("Model not found: {0}")]
    ModelNotFound(String),

    #[error("Message is not a struct")]
    MessageNotStruct,

    #[error("Failed to serialize model key: {0}")]
    SerializeModelKeyError(#[from] PrimitiveError),

    #[error(transparent)]
    EnumError(#[from] EnumError),

    #[error(transparent)]
    StorageError(#[from] torii_storage::StorageError),

    #[error(transparent)]
    SqliteError(#[from] torii_sqlite::error::Error),

    #[error(transparent)]
    ProviderError(#[from] starknet::providers::ProviderError),

    #[error(transparent)]
    TypedDataError(#[from] starknet_core::types::typed_data::TypedDataError),

    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Message timestamp is too far in the future")]
    TimestampTooFuture,

    #[error("Message timestamp is too old")]
    TimestampTooOld,

    #[error("Message timestamp is not found")]
    TimestampNotFound,

    #[error("Timestamp is older than the entity timestamp")]
    InvalidTimestamp,
}
