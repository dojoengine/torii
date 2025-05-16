use thiserror::Error;

/// Errors using [`HttpTransport`].
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum HttpTransportError {
    /// HTTP-related errors.
    Reqwest(reqwest::Error),
    /// JSON serialization/deserialization errors.
    Json(serde_json::Error),
    /// Unexpected response ID.
    #[error("unexpected response ID: {0}")]
    UnexpectedResponseId(u64),
    /// Retries exhausted.
    #[error("retries exhausted after {max_retries} attempts: {last_error}")]
    RetriesExhausted { max_retries: u32, last_error: Box<HttpTransportError> },
}