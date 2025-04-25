use starknet::core::types::FromStrError;

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("Missing expected data: {0}")]
    MissingExpectedData(String),
    #[error("Unsupported primitive type for {0}")]
    UnsupportedType(String),
    #[error("Invalid byte length: {0}. Expected: {1}")]
    InvalidByteLength(usize, usize),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error(transparent)]
    FromSlice(#[from] std::array::TryFromSliceError),
    #[error(transparent)]
    FromStr(#[from] FromStrError),
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    FromJson(#[from] serde_json::Error),
}