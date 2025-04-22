use gossipsub::Event as GossipsubEvent;
use libp2p::{gossipsub, identify, ping};

#[allow(clippy::large_enum_variant)]
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum BehaviourEvent {
    Gossipsub(GossipsubEvent),
    Identify(identify::Event),
    Ping(ping::Event),
}

impl From<GossipsubEvent> for BehaviourEvent {
    fn from(event: GossipsubEvent) -> Self {
        Self::Gossipsub(event)
    }
}

impl From<identify::Event> for BehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(event)
    }
}

impl From<ping::Event> for BehaviourEvent {
    fn from(event: ping::Event) -> Self {
        Self::Ping(event)
    }
}
