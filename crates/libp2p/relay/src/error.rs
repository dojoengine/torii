use std::convert::Infallible;
use std::io;

use dojo_types::primitive::PrimitiveError;
use dojo_types::schema::EnumError;
use libp2p::gossipsub::{PublishError, SubscriptionError};
use libp2p::noise;
use starknet::providers::ProviderError;
use starknet_core::types::typed_data::TypedDataError;
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

    #[error(transparent)]
    ProviderError(#[from] ProviderError),

    #[error(transparent)]
    MessageError(#[from] MessageError),

    #[error(transparent)]
    TypedDataError(#[from] TypedDataError),
}
