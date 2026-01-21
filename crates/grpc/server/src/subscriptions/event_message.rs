use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use dojo_types::schema::Ty;
use futures::Stream;
use futures_util::StreamExt;
use rand::Rng;
use starknet_crypto::Felt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use torii_broker::types::EventMessageUpdate;
use torii_broker::MemoryBroker;
use torii_proto::schema::EntityWithMetadata;
use torii_proto::Clause;
use tracing::{error, trace};

use torii_proto::proto::world::SubscribeEntityResponse;

use crate::GrpcConfig;

use super::match_entity;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::event_message";

#[derive(Debug)]
pub struct EventMessageSubscriber {
    /// The clause that the subscriber is interested in
    pub(crate) clause: Option<Clause>,
    /// The world addresses that the subscriber is interested in
    pub(crate) world_addresses: Vec<Felt>,
    /// The channel to send the response back to the subscriber.
    pub(crate) sender: Sender<Result<SubscribeEntityResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct EventMessageManager {
    subscribers: DashMap<u64, EventMessageSubscriber>,
    config: GrpcConfig,
}

impl EventMessageManager {
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            subscribers: DashMap::new(),
            config,
        }
    }

    pub async fn add_subscriber(
        &self,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) -> Receiver<Result<SubscribeEntityResponse, tonic::Status>> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(self.config.subscription_buffer_size);

        // NOTE: unlock issue with firefox/safari
        // initially send empty stream message to return from
        // initial subscribe call
        let _ = sender
            .send(Ok(SubscribeEntityResponse {
                entity: None,
                subscription_id,
            }))
            .await;

        self.subscribers.insert(
            subscription_id,
            EventMessageSubscriber {
                clause,
                world_addresses,
                sender,
            },
        );

        receiver
    }

    pub async fn update_subscriber(
        &self,
        id: u64,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) {
        if let Some(mut subscriber) = self.subscribers.get_mut(&id) {
            subscriber.clause = clause;
            subscriber.world_addresses = world_addresses;
        }
    }

    #[allow(dead_code)]
    pub(super) async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = EntityWithMetadata<true>> + Send>>,
    subs_manager: Arc<EventMessageManager>,
}

impl Service {
    pub fn new(subs_manager: Arc<EventMessageManager>) -> Self {
        Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<EventMessageUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<EventMessageUpdate>::subscribe())
            },
            subs_manager,
        }
    }

    // Process updates synchronously - no async overhead
    fn process_event_message(subs: &EventMessageManager, event: &EntityWithMetadata<true>) {
        let mut closed_stream = Vec::new();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            // Check if the subscriber is interested in this entity
            if let Some(clause) = &sub.clause {
                if !sub.world_addresses.is_empty()
                    && !sub.world_addresses.contains(&event.entity.world_address)
                {
                    continue;
                }

                if !match_entity(
                    event.entity.hashed_keys,
                    &event.keys,
                    &event.entity.models.first().map(|m| Ty::Struct(m.clone())),
                    clause,
                ) {
                    continue;
                }
            }

            let resp = SubscribeEntityResponse {
                entity: Some(event.entity.clone().into()),
                subscription_id: *idx,
            };

            // Use try_send to avoid blocking on slow subscribers
            match sub.sender.try_send(Ok(resp)) {
                Ok(_) => {
                    trace!(target = LOG_TARGET, subscription_id = %idx, "Event message update sent to subscriber");
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    error!(target = LOG_TARGET, subscription_id = %idx, entity_id = ?event.entity.hashed_keys, "Disconnecting slow subscriber - channel full");
                    closed_stream.push(*idx);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    trace!(target = LOG_TARGET, subscription_id = %idx, "Subscriber channel closed");
                    closed_stream.push(*idx);
                }
            }
        }

        // Clean up closed subscribers
        if !closed_stream.is_empty() {
            for id in closed_stream {
                trace!(target = LOG_TARGET, id = %id, "Removing closed subscriber.");
                subs.subscribers.remove(&id);
            }
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Process updates inline for minimum latency
        while let Poll::Ready(Some(event)) = this.simple_broker.poll_next_unpin(cx) {
            Self::process_event_message(&this.subs_manager, &event);
        }

        Poll::Pending
    }
}
