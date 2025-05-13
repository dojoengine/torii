use thiserror::Error;

#[derive(Debug, Error)]
pub enum TaskNetworkError {
    #[error("Graph error: {0}")]
    GraphError(#[from] torii_adigraphmap::error::AcyclicDigraphMapError),

    #[error("Semaphore error: {0}")]
    SemaphoreError(#[from] tokio::sync::AcquireError),

    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Task error: {0}")]
    TaskError(anyhow::Error),
}
