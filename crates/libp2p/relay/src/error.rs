use std::convert::Infallible;
use std::io;

use libp2p::gossipsub::{PublishError, SubscriptionError};
use libp2p::noise;
use starknet::providers::ProviderError;
use thiserror::Error;
use torii_typed_data::error::Error as TypedDataError;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    MultiaddrParseError(#[from] libp2p::core::multiaddr::Error),

    #[error(transparent)]
    NoiseUpgradeError(#[from] noise::Error),

    #[error(transparent)]
    DialError(#[from] libp2p::swarm::DialError),

    // accept any error
    #[error(transparent)]
    BehaviourError(#[from] Infallible),

    #[error(transparent)]
    GossipConfigError(#[from] libp2p::gossipsub::ConfigBuilderError),

    #[error(transparent)]
    TransportError(#[from] libp2p::TransportError<io::Error>),

    #[error(transparent)]
    SubscriptionError(#[from] SubscriptionError),

    #[error(transparent)]
    PublishError(#[from] PublishError),

    #[error("Failed to read identity: {0}")]
    ReadIdentityError(anyhow::Error),

    #[error("Failed to read certificate: {0}")]
    ReadCertificateError(anyhow::Error),

    #[error("Invalid type provided: {0}")]
    InvalidTypeError(String),

    #[error(transparent)]
    ProviderError(#[from] ProviderError),

    #[error(transparent)]
    TypedDataError(#[from] TypedDataError),

    #[error("Invalid message provided: {0}")]
    InvalidMessageError(String),
}
