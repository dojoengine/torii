use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;
use std::path::Path;
use std::time::Duration;
use std::{fs, io};

use events::BehaviourEvent;
use futures::StreamExt;
use libp2p::core::multiaddr::Protocol;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::upgrade::Version;
use libp2p::core::Multiaddr;
use libp2p::gossipsub::{self, IdentTopic, PublishError};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{
    dns, identify, identity, noise, ping, relay, tcp, websocket, yamux, PeerId, Swarm, Transport,
};
use libp2p_webrtc as webrtc;
use rand::thread_rng;
use starknet::providers::Provider;
use starknet_core::types::TypedData;
use torii_libp2p_types::Message;
use torii_messaging::validate_and_set_entity;
use torii_sqlite::Sql;
use tracing::{info, trace, warn};
use webrtc::tokio::Certificate;

mod constants;
pub mod error;
mod events;

use crate::error::Error;

pub(crate) const LOG_TARGET: &str = "torii::relay::server";

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "BehaviourEvent")]
#[allow(missing_debug_implementations)]
pub struct Behaviour {
    relay: relay::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    gossipsub: gossipsub::Behaviour,
}

#[allow(missing_debug_implementations)]
pub struct Relay<P: Provider + Sync> {
    swarm: Swarm<Behaviour>,
    db: Sql,
    provider: Box<P>,
}

impl<P: Provider + Sync> Relay<P> {
    pub fn new(
        pool: Sql,
        provider: P,
        port: u16,
        port_webrtc: u16,
        port_websocket: u16,
        local_key_path: Option<String>,
        cert_path: Option<String>,
    ) -> Result<Self, Error> {
        Self::new_with_peers(
            pool,
            provider,
            port,
            port_webrtc,
            port_websocket,
            local_key_path,
            cert_path,
            vec![],
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_peers(
        pool: Sql,
        provider: P,
        port: u16,
        port_webrtc: u16,
        port_websocket: u16,
        local_key_path: Option<String>,
        cert_path: Option<String>,
        peers: Vec<String>,
    ) -> Result<Self, Error> {
        let local_key = if let Some(path) = local_key_path {
            let path = Path::new(&path);
            read_or_create_identity(path).map_err(Error::ReadIdentityError)?
        } else {
            identity::Keypair::generate_ed25519()
        };

        let cert = if let Some(path) = cert_path {
            let path = Path::new(&path);
            read_or_create_certificate(path).map_err(Error::ReadCertificateError)?
        } else {
            Certificate::generate(&mut thread_rng()).unwrap()
        };

        info!(target: LOG_TARGET, peer_id = %PeerId::from(local_key.public()), "Relay peer id.");

        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_quic()
            .with_other_transport(|key| {
                webrtc::tokio::Transport::new(key.clone(), cert)
                    .map(|(peer_id, conn), _| (peer_id, StreamMuxerBox::new(conn)))
            })
            .expect("Failed to create WebRTC transport")
            .with_other_transport(|key| {
                let transport = websocket::Config::new(
                    dns::tokio::Transport::system(tcp::tokio::Transport::new(
                        tcp::Config::default(),
                    ))
                    .unwrap(),
                );

                transport
                    .upgrade(Version::V1)
                    .authenticate(noise::Config::new(key).unwrap())
                    .multiplex(yamux::Config::default())
            })
            .expect("Failed to create WebSocket transport")
            .with_dns()
            .expect("Failed to create DNS transport")
            .with_behaviour(|key| {
                // Hash messages by their content. No two messages of the same content will be
                // propagated.
                let _message_id_fn = |message: &gossipsub::Message| {
                    let mut s = DefaultHasher::new();
                    message.data.hash(&mut s);
                    gossipsub::MessageId::from(s.finish().to_string())
                };
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(
                        constants::GOSSIPSUB_HEARTBEAT_INTERVAL_SECS,
                    )) // This is set to aid debugging by not cluttering the log space
                    .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
                    // TODO: Use this once we incorporate nonces in the message model?
                    // .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                    .build()
                    .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))
                    .unwrap(); // Temporary hack because `build` does not return a proper `std::error::Error`.

                Behaviour {
                    relay: relay::Behaviour::new(key.public().to_peer_id(), Default::default()),
                    ping: ping::Behaviour::new(ping::Config::new()),
                    identify: identify::Behaviour::new(identify::Config::new(
                        format!("/{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
                        key.public(),
                    )),
                    gossipsub: gossipsub::Behaviour::new(
                        gossipsub::MessageAuthenticity::Signed(key.clone()),
                        gossipsub_config,
                    )
                    .unwrap(),
                }
            })?
            .with_swarm_config(|cfg| {
                cfg.with_idle_connection_timeout(Duration::from_secs(
                    constants::IDLE_CONNECTION_TIMEOUT_SECS,
                ))
            })
            .build();

        // TCP
        let listen_addr_tcp = Multiaddr::from(Ipv4Addr::UNSPECIFIED).with(Protocol::Tcp(port));
        swarm.listen_on(listen_addr_tcp.clone())?;

        // UDP QUIC
        let listen_addr_quic = Multiaddr::from(Ipv4Addr::UNSPECIFIED)
            .with(Protocol::Udp(port))
            .with(Protocol::QuicV1);
        swarm.listen_on(listen_addr_quic.clone())?;

        // WebRTC
        let listen_addr_webrtc = Multiaddr::from(Ipv4Addr::UNSPECIFIED)
            .with(Protocol::Udp(port_webrtc))
            .with(Protocol::WebRTCDirect);
        swarm.listen_on(listen_addr_webrtc.clone())?;

        // WS
        let listen_addr_wss = Multiaddr::from(Ipv4Addr::UNSPECIFIED)
            .with(Protocol::Tcp(port_websocket))
            .with(Protocol::Ws("/".to_string().into()));
        swarm.listen_on(listen_addr_wss.clone())?;

        // We dial all of our peers. Our server will then broadcast
        // all incoming offchain messages to all of our peers.
        for peer in peers {
            swarm.dial(peer.parse::<Multiaddr>().unwrap())?;
        }

        // Clients will send their messages to the "message" topic
        // with a room name as the message data.
        // and we will forward those messages to a specific room - in this case the topic
        // along with the message data.
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&IdentTopic::new(constants::MESSAGING_TOPIC))
            .unwrap();

        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&IdentTopic::new(constants::PEERS_MESSAGING_TOPIC))
            .unwrap();

        Ok(Self {
            swarm,
            db: pool,
            provider: Box::new(provider),
        })
    }

    pub async fn run(&mut self) {
        loop {
            match self.swarm.next().await.expect("Infinite Stream.") {
                SwarmEvent::Behaviour(event) => {
                    match &event {
                        BehaviourEvent::Gossipsub(gossipsub::Event::Message {
                            propagation_source: peer_id,
                            message_id,
                            message,
                        }) => {
                            // Ignore our own messages
                            if peer_id == self.swarm.local_peer_id() {
                                continue;
                            }

                            // Deserialize typed data.
                            // We shouldn't panic here
                            let data = match serde_json::from_slice::<Message>(&message.data) {
                                Ok(message) => message,
                                Err(e) => {
                                    warn!(
                                        target: LOG_TARGET,
                                        error = ?e,
                                        "Deserializing message."
                                    );
                                    continue;
                                }
                            };

                            info!(
                                target: LOG_TARGET,
                                message_id = %message_id,
                                peer_id = %peer_id,
                                "Received message."
                            );

                            let typed_data =
                                serde_json::from_str::<TypedData>(&data.message).unwrap();
                            if let Err(e) = validate_and_set_entity(
                                &mut self.db,
                                &typed_data,
                                &data.signature,
                                &self.provider,
                            )
                            .await
                            {
                                warn!(
                                    target: LOG_TARGET,
                                    error = ?e,
                                    "Validating and setting message."
                                );
                                continue;
                            }

                            info!(
                                target: LOG_TARGET,
                                message_id = %message_id,
                                peer_id = %peer_id,
                                "Message verified and set."
                            );

                            // We only want to publish messages from our clients. Not from our peers
                            // otherwise recursion hell :<
                            if message.topic != IdentTopic::new(constants::MESSAGING_TOPIC).hash() {
                                continue;
                            }

                            // Publish message to all peers if there are any
                            match self
                                .swarm
                                .behaviour_mut()
                                .gossipsub
                                .publish(
                                    IdentTopic::new(constants::PEERS_MESSAGING_TOPIC),
                                    serde_json::to_string(&data).unwrap(),
                                )
                                .map_err(Error::PublishError)
                            {
                                Ok(_) => info!(
                                    target: LOG_TARGET,
                                    "Forwarded message to peers."
                                ),
                                Err(Error::PublishError(
                                    PublishError::NoPeersSubscribedToTopic,
                                )) => {}
                                Err(e) => warn!(
                                    target: LOG_TARGET,
                                    error = ?e,
                                    "Publishing message to peers."
                                ),
                            }
                        }
                        BehaviourEvent::Gossipsub(gossipsub::Event::Subscribed {
                            peer_id,
                            topic,
                        }) => {
                            info!(
                                target: LOG_TARGET,
                                peer_id = %peer_id,
                                topic = %topic,
                                "Subscribed to topic."
                            );
                        }
                        BehaviourEvent::Gossipsub(gossipsub::Event::Unsubscribed {
                            peer_id,
                            topic,
                        }) => {
                            info!(
                                target: LOG_TARGET,
                                peer_id = %peer_id,
                                topic = %topic,
                                "Unsubscribed from topic."
                            );
                        }
                        BehaviourEvent::Identify(identify::Event::Received {
                            connection_id,
                            info: identify::Info { observed_addr, .. },
                            peer_id,
                        }) => {
                            info!(
                                target: LOG_TARGET,
                                connection_id = %connection_id,
                                peer_id = %peer_id,
                                observed_addr = %observed_addr,
                                "Received identify event."
                            );
                            self.swarm.add_external_address(observed_addr.clone());
                        }
                        BehaviourEvent::Ping(ping::Event { peer, result, .. }) => {
                            trace!(
                                target: LOG_TARGET,
                                peer_id = %peer,
                                result = ?result,
                                "Received ping event."
                            );
                        }
                        _ => {}
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    let mut is_localhost = false;
                    for protocol in address.iter() {
                        if let Protocol::Ip4(ip) = protocol {
                            if ip == Ipv4Addr::new(127, 0, 0, 1) {
                                is_localhost = true;
                                break;
                            }
                        }
                    }

                    // To declutter logs, we only log listen addresses that are localhost
                    if !is_localhost {
                        continue;
                    }
                    info!(target: LOG_TARGET, address = %address, "Serving libp2p Relay.");
                }
                event => {
                    trace!(target: LOG_TARGET, event = ?event, "Unhandled event.");
                }
            }
        }
    }
}

fn read_or_create_identity(path: &Path) -> anyhow::Result<identity::Keypair> {
    if path.exists() {
        let bytes = fs::read(path)?;

        info!(target: LOG_TARGET, path = %path.display(), "Using existing identity.");

        return Ok(identity::Keypair::from_protobuf_encoding(&bytes)?); // This only works for ed25519 but that is what we are using.
    }

    let identity = identity::Keypair::generate_ed25519();

    fs::write(path, identity.to_protobuf_encoding()?)?;

    info!(target: LOG_TARGET, path = %path.display(), "Generated new identity.");

    Ok(identity)
}

fn read_or_create_certificate(path: &Path) -> anyhow::Result<Certificate> {
    if path.exists() {
        let pem = fs::read_to_string(path)?;

        info!(target: LOG_TARGET, path = %path.display(), "Using existing certificate.");

        return Ok(Certificate::from_pem(&pem)?);
    }

    let cert = Certificate::generate(&mut rand::thread_rng())?;
    fs::write(path, cert.serialize_pem().as_bytes())?;

    info!(target: LOG_TARGET, path = %path.display(), "Generated new certificate.");

    Ok(cert)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_read_or_create_identity() {
        let dir = tempdir().unwrap();
        let identity_path = dir.path().join("identity");

        // Test identity creation
        let identity1 = read_or_create_identity(&identity_path).unwrap();
        assert!(identity_path.exists());

        // Test identity reading
        let identity2 = read_or_create_identity(&identity_path).unwrap();
        assert_eq!(identity1.public(), identity2.public());

        dir.close().unwrap();
    }

    #[test]
    fn test_read_or_create_certificate() {
        let dir = tempdir().unwrap();
        let cert_path = dir.path().join("certificate");

        // Test certificate creation
        let cert1 = read_or_create_certificate(&cert_path).unwrap();
        assert!(cert_path.exists());

        // Test certificate reading
        let cert2 = read_or_create_certificate(&cert_path).unwrap();
        assert_eq!(cert1.serialize_pem(), cert2.serialize_pem());

        dir.close().unwrap();
    }
}
