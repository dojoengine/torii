use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    SendError(#[from] tokio::sync::mpsc::error::SendError<crate::executor::QueryMessage>),
    #[error(transparent)]
    RecvError(#[from] tokio::sync::oneshot::error::RecvError),
}

#[derive(Error, Debug)]
pub enum ExecutorQueryError {
    #[error(transparent)]
    Sqlite(#[from] crate::error::Error),
    #[error(transparent)]
    Provider(#[from] starknet::providers::ProviderError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Parse(#[from] crate::error::ParseError),
    #[error(transparent)]
    Primitive(#[from] dojo_types::primitive::PrimitiveError),
    #[error(transparent)]
    Executor(#[from] ExecutorError),
}
