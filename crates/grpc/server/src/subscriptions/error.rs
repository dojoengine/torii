use starknet::providers::ProviderError;
use torii_proto::error::ProtoError;
use torii_storage::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Proto(#[from] ProtoError),
    #[error(transparent)]
    Provider(ProviderError),
}
