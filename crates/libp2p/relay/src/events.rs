use libp2p::gossipsub::Event as GossipsubEvent;
use libp2p::identify::Event as IdentifyEvent;
use libp2p::ping::Event as PingEvent;
use libp2p::relay::Event as RelayEvent;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum BehaviourEvent {
    Identify(IdentifyEvent),
    Ping(PingEvent),
    Relay(RelayEvent),
    Gossipsub(GossipsubEvent),
}

impl From<IdentifyEvent> for BehaviourEvent {
    fn from(event: IdentifyEvent) -> Self {
        Self::Identify(event)
    }
}

impl From<PingEvent> for BehaviourEvent {
    fn from(event: PingEvent) -> Self {
        Self::Ping(event)
    }
}

impl From<RelayEvent> for BehaviourEvent {
    fn from(event: RelayEvent) -> Self {
        Self::Relay(event)
    }
}

impl From<GossipsubEvent> for BehaviourEvent {
    fn from(event: GossipsubEvent) -> Self {
        Self::Gossipsub(event)
    }
}
