use starknet::providers::ProviderError;
use torii_storage::StorageError;
use torii_proto::error::ProtoError;

#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Proto(#[from] ProtoError),
    #[error(transparent)]
    Provider(ProviderError),
}
