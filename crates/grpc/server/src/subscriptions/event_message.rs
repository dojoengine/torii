use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use dojo_types::schema::Ty;
use futures::Stream;
use futures_util::StreamExt;
use rand::Rng;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use torii_broker::types::EventMessageUpdate;
use torii_broker::MemoryBroker;
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

        self.subscribers
            .insert(subscription_id, EventMessageSubscriber { clause, sender });

        receiver
    }

    pub async fn update_subscriber(&self, id: u64, clause: Option<Clause>) {
        if let Some(mut subscriber) = self.subscribers.get_mut(&id) {
            subscriber.clause = clause;
        }
    }

    pub(super) async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = EventMessageUpdate> + Send>>,
    event_sender: UnboundedSender<EventMessageUpdate>,
}

impl Service {
    pub fn new(subs_manager: Arc<EventMessageManager>) -> Self {
        let (event_sender, event_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<EventMessageUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<EventMessageUpdate>::subscribe())
            },
            event_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, event_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<EventMessageManager>,
        mut event_receiver: UnboundedReceiver<EventMessageUpdate>,
    ) {
        while let Some(event) = event_receiver.recv().await {
            Self::process_event_update(&subs, &event).await;
        }
    }

    async fn process_event_update(subs: &Arc<EventMessageManager>, entity: &EventMessageUpdate) {
        let mut closed_stream = Vec::new();
        let event = entity.clone().into_inner();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            // Check if the subscriber is interested in this entity
            // If we have a clause of hashed keys, then check that the id of the entity
            // is in the list of hashed keys.

            // If we have a clause of keys, then check that the key pattern of the entity
            // matches the key pattern of the subscriber.
            if let Some(clause) = &sub.clause {
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
                    // Message sent successfully
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    // Channel is full, subscriber is too slow - disconnect them
                    trace!(target = LOG_TARGET, subscription_id = %idx, "Disconnecting slow subscriber - channel full");
                    closed_stream.push(*idx);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    // Channel is closed, subscriber has disconnected
                    closed_stream.push(*idx);
                }
            }
        }

        for id in closed_stream {
            trace!(target = LOG_TARGET, id = %id, "Closing entity stream.");
            subs.remove_subscriber(id).await
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(event)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.event_sender.send(event) {
                error!(target = LOG_TARGET, error = ?e, "Sending event update to processor.");
            }
        }

        Poll::Pending
    }
}
