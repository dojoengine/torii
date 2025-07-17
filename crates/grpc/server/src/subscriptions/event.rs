use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures::Stream;
use futures_util::StreamExt;
use rand::Rng;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use torii_broker::types::EventUpdate;
use torii_broker::MemoryBroker;
use torii_proto::KeysClause;
use torii_proto::EventWithMetadata;
use tracing::{error, trace};

use crate::GrpcConfig;

use super::match_keys;
use torii_proto::proto::types::Event as ProtoEvent;
use torii_proto::proto::world::SubscribeEventsResponse;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::event";

#[derive(Debug)]
pub struct EventSubscriber {
    /// Event keys that the subscriber is interested in
    keys: Vec<KeysClause>,
    /// The channel to send the response back to the subscriber.
    sender: Sender<Result<SubscribeEventsResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct EventManager {
    subscribers: DashMap<usize, EventSubscriber>,
    config: GrpcConfig,
}

impl EventManager {
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            subscribers: DashMap::new(),
            config,
        }
    }

    pub async fn add_subscriber(
        &self,
        keys: Vec<KeysClause>,
    ) -> Receiver<Result<SubscribeEventsResponse, tonic::Status>> {
        let id = rand::thread_rng().gen::<usize>();
        let (sender, receiver) = channel(self.config.subscription_buffer_size);

        // NOTE: unlock issue with firefox/safari
        // initially send empty stream message to return from
        // initial subscribe call
        let _ = sender
            .send(Ok(SubscribeEventsResponse { event: None }))
            .await;

        self.subscribers
            .insert(id, EventSubscriber { keys, sender });

        receiver
    }

    pub(super) async fn remove_subscriber(&self, id: usize) {
        self.subscribers.remove(&id);
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = EventWithMetadata> + Send>>,
    event_sender: UnboundedSender<EventWithMetadata>,
}

impl Service {
    pub fn new(subs_manager: Arc<EventManager>) -> Self {
        let (event_sender, event_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<EventUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<EventUpdate>::subscribe())
            },
            event_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, event_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<EventManager>,
        mut event_receiver: UnboundedReceiver<EventWithMetadata>,
    ) {
        while let Some(event) = event_receiver.recv().await {
            Self::process_event(&subs, &event).await;
        }
    }

    async fn process_event(subs: &Arc<EventManager>, event: &EventWithMetadata) {
        let mut closed_stream = Vec::new();

        let event = event.event.clone();
        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            if !match_keys(&event.keys, &sub.keys) {
                continue;
            }

            let resp = SubscribeEventsResponse {
                event: Some(ProtoEvent {
                    keys: event
                        .keys
                        .iter()
                        .map(|k| k.to_bytes_be().to_vec())
                        .collect(),
                    data: event
                        .data
                        .iter()
                        .map(|d| d.to_bytes_be().to_vec())
                        .collect(),
                    transaction_hash: event.transaction_hash.to_bytes_be().to_vec(),
                }),
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
            trace!(target = LOG_TARGET, id = %id, "Closing events stream.");
            subs.remove_subscriber(id).await
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let pin = self.get_mut();

        while let Poll::Ready(Some(event)) = pin.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = pin.event_sender.send(event) {
                error!(target = LOG_TARGET, error = ?e, "Sending event to processor.");
            }
        }

        Poll::Pending
    }
}
