use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use std::{fs, io};

use chrono::Utc;
use dojo_types::naming::is_valid_tag;
use dojo_types::schema::Ty;
use dojo_world::contracts::naming::compute_selector_from_tag;
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
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall};
use starknet::core::utils::get_selector_from_name;
use starknet::providers::Provider;
use starknet_core::types::typed_data::TypeReference;
use starknet_core::types::TypedData;
use starknet_crypto::poseidon_hash_many;
use torii_sqlite::executor::QueryMessage;
use torii_sqlite::utils::felts_to_sql_string;
use torii_sqlite::Sql;
use tracing::{info, trace, warn};
use webrtc::tokio::Certificate;

mod constants;
pub mod error;
mod events;
mod mapping;

use crate::error::{Error, MessageError};
use crate::mapping::parse_value_to_ty;
use torii_libp2p_types::Message;

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

                            let typed_data =
                                serde_json::from_str::<TypedData>(&data.message).unwrap();
                            let ty = match validate_message(&self.db, &typed_data).await {
                                Ok(parsed_message) => parsed_message,
                                Err(e) => {
                                    warn!(
                                        target: LOG_TARGET,
                                        error = ?e,
                                        "Validating message."
                                    );
                                    continue;
                                }
                            };

                            info!(
                                target: LOG_TARGET,
                                message_id = %message_id,
                                peer_id = %peer_id,
                                data = ?data,
                                "Received message."
                            );

                            // retrieve entity identity from db
                            let mut pool = match self.db.pool.acquire().await {
                                Ok(pool) => pool,
                                Err(e) => {
                                    warn!(
                                        target: LOG_TARGET,
                                        error = ?e,
                                        "Acquiring pool."
                                    );
                                    continue;
                                }
                            };

                            let keys = match ty_keys(&ty) {
                                Ok(keys) => keys,
                                Err(e) => {
                                    warn!(
                                        target: LOG_TARGET,
                                        error = ?e,
                                        "Retrieving message model keys."
                                    );
                                    continue;
                                }
                            };
                            let keys_str = felts_to_sql_string(&keys);
                            let entity_id = poseidon_hash_many(&keys);
                            let model_id = ty_model_id(&ty).unwrap();

                            // select only identity field, if doesn't exist, empty string
                            let query = format!(
                                "SELECT identity FROM [{}] WHERE internal_id = ?",
                                ty.name()
                            );
                            let entity_identity: Option<String> = match sqlx::query_scalar(&query)
                                .bind(format!("{:#x}", entity_id))
                                .fetch_optional(&mut *pool)
                                .await
                            {
                                Ok(entity_identity) => entity_identity,
                                Err(e) => {
                                    warn!(
                                        target: LOG_TARGET,
                                        error = ?e,
                                        "Fetching entity."
                                    );
                                    continue;
                                }
                            };

                            let entity_identity = match entity_identity {
                                Some(identity) => match Felt::from_str(&identity) {
                                    Ok(identity) => identity,
                                    Err(e) => {
                                        warn!(
                                            target: LOG_TARGET,
                                            error = ?e,
                                            "Parsing identity."
                                        );
                                        continue;
                                    }
                                },
                                _ => match get_identity_from_ty(&ty) {
                                    Ok(identity) => identity,
                                    Err(e) => {
                                        warn!(
                                            target: LOG_TARGET,
                                            error = ?e,
                                            "Getting identity from message."
                                        );
                                        continue;
                                    }
                                },
                            };

                            // TODO: have a nonce in model to check
                            // against entity nonce and message nonce
                            // to prevent replay attacks.

                            // Verify the signature
                            if !match validate_signature(
                                &self.provider,
                                entity_identity,
                                &typed_data,
                                &data.signature,
                            )
                            .await
                            {
                                Ok(res) => res,
                                Err(e) => {
                                    warn!(
                                        target: LOG_TARGET,
                                        error = ?e,
                                        "Verifying signature."
                                    );
                                    continue;
                                }
                            } {
                                warn!(
                                    target: LOG_TARGET,
                                    message_id = %message_id,
                                    peer_id = %peer_id,
                                    "Invalid signature."
                                );
                                continue;
                            }

                            if let Err(e) = set_entity(
                                &mut self.db,
                                ty.clone(),
                                &message_id.to_string(),
                                Utc::now().timestamp() as u64,
                                entity_id,
                                model_id,
                                &keys_str,
                            )
                            .await
                            {
                                warn!(
                                    target: LOG_TARGET,
                                    error = ?e,
                                    "Setting message."
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

async fn validate_signature<P: Provider + Sync>(
    provider: &P,
    entity_identity: Felt,
    message: &TypedData,
    signature: &[Felt],
) -> Result<bool, Error> {
    let message_hash = message.message_hash(entity_identity)?;

    let mut calldata = vec![message_hash, Felt::from(signature.len())];
    calldata.extend(signature);
    provider
        .call(
            FunctionCall {
                contract_address: entity_identity,
                entry_point_selector: get_selector_from_name("is_valid_signature").unwrap(),
                calldata,
            },
            BlockId::Tag(BlockTag::Pending),
        )
        .await
        .map_err(Error::ProviderError)
        .map(|res| res[0] != Felt::ZERO)
}

fn ty_keys(ty: &Ty) -> Result<Vec<Felt>, Error> {
    if let Ty::Struct(s) = &ty {
        let mut keys = Vec::new();
        for m in s.keys() {
            keys.extend(
                m.serialize()
                    .map_err(|e| Error::MessageError(MessageError::SerializeModelKeyError(e)))?,
            );
        }
        Ok(keys)
    } else {
        Err(Error::MessageError(MessageError::InvalidType(
            "Message should be a struct".to_string(),
        )))
    }
}

fn ty_model_id(ty: &Ty) -> Result<Felt, Error> {
    let namespaced_name = ty.name();

    if !is_valid_tag(&namespaced_name) {
        return Err(Error::MessageError(MessageError::InvalidModelTag(
            namespaced_name,
        )));
    }

    let selector = compute_selector_from_tag(&namespaced_name);
    Ok(selector)
}

// Validates the message model
// and returns the identity and signature
async fn validate_message(db: &Sql, message: &TypedData) -> Result<Ty, Error> {
    let tag = message.primary_type().signature_ref_repr();
    if !is_valid_tag(&tag) {
        return Err(Error::MessageError(MessageError::InvalidModelTag(tag)));
    }

    let selector = compute_selector_from_tag(&tag);

    let mut ty = db
        .model(selector)
        .await
        .map_err(|e| Error::MessageError(MessageError::ModelNotFound(e.to_string())))?
        .schema;

    parse_value_to_ty(message.message(), &mut ty)?;

    Ok(ty)
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

fn get_identity_from_ty(ty: &Ty) -> Result<Felt, Error> {
    let identity = ty
        .as_struct()
        .ok_or_else(|| Error::MessageError(MessageError::MessageNotStruct))?
        .get("identity")
        .ok_or_else(|| Error::MessageError(MessageError::FieldNotFound("identity".to_string())))?
        .as_primitive()
        .ok_or_else(|| {
            Error::MessageError(MessageError::InvalidType(
                "Identity should be a primitive".to_string(),
            ))
        })?
        .as_contract_address()
        .ok_or_else(|| {
            Error::MessageError(MessageError::InvalidType(
                "Identity should be a contract address".to_string(),
            ))
        })?;
    Ok(identity)
}

#[allow(clippy::too_many_arguments)]
async fn set_entity(
    db: &mut Sql,
    ty: Ty,
    message_id: &str,
    block_timestamp: u64,
    entity_id: Felt,
    model_id: Felt,
    keys: &str,
) -> anyhow::Result<()> {
    db.set_entity(
        ty,
        message_id,
        block_timestamp,
        entity_id,
        model_id,
        Some(keys),
    )
    .await?;
    db.executor.send(QueryMessage::execute())?;
    Ok(())
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
