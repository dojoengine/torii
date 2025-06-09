use dojo_types::primitive::PrimitiveError;
use dojo_types::schema::EnumError;
use starknet_core::types::FromStrError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MessageError {
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
}
