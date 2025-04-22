use std::convert::Infallible;
use std::io;

use libp2p::gossipsub::PublishError;
#[cfg(not(target_arch = "wasm32"))]
use libp2p::noise;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    MultiaddrParseError(#[from] libp2p::core::multiaddr::Error),

    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NoiseUpgradeError(#[from] noise::Error),

    #[error(transparent)]
    DialError(#[from] libp2p::swarm::DialError),

    // accept any error
    #[error(transparent)]
    BehaviourError(#[from] Infallible),

    #[error(transparent)]
    TransportError(#[from] libp2p::TransportError<io::Error>),

    #[error(transparent)]
    PublishError(#[from] PublishError),
}
