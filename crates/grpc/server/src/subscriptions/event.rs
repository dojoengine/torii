use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures::Stream;
use futures_util::StreamExt;
use rand::Rng;
use starknet::core::types::Felt;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use torii_broker::MemoryBroker;
use torii_proto::KeysClause;
use torii_sqlite::constants::SQL_FELT_DELIMITER;
use torii_sqlite::error::{Error, ParseError};
use torii_sqlite::types::Event;
use tracing::{error, trace};

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
    subscription_buffer_size: usize,
}

impl EventManager {
    pub fn new(subscription_buffer_size: usize) -> Self {
        Self {
            subscribers: DashMap::new(),
            subscription_buffer_size,
        }
    }

    pub async fn add_subscriber(
        &self,
        keys: Vec<KeysClause>,
    ) -> Result<Receiver<Result<SubscribeEventsResponse, tonic::Status>>, Error> {
        let id = rand::thread_rng().gen::<usize>();
        let (sender, receiver) = channel(self.subscription_buffer_size);

        // NOTE: unlock issue with firefox/safari
        // initially send empty stream message to return from
        // initial subscribe call
        let _ = sender
            .send(Ok(SubscribeEventsResponse { event: None }))
            .await;

        self.subscribers
            .insert(id, EventSubscriber { keys, sender });

        Ok(receiver)
    }

    pub(super) async fn remove_subscriber(&self, id: usize) {
        self.subscribers.remove(&id);
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = Event> + Send>>,
    event_sender: UnboundedSender<Event>,
}

impl Service {
    pub fn new(subs_manager: Arc<EventManager>) -> Self {
        let (event_sender, event_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(MemoryBroker::<Event>::subscribe()),
            event_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, event_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<EventManager>,
        mut event_receiver: UnboundedReceiver<Event>,
    ) {
        while let Some(event) = event_receiver.recv().await {
            if let Err(e) = Self::process_event(&subs, &event).await {
                error!(target = LOG_TARGET, error = ?e, "Processing event update.");
            }
        }
    }

    async fn process_event(subs: &Arc<EventManager>, event: &Event) -> Result<(), Error> {
        let mut closed_stream = Vec::new();
        let keys = event
            .keys
            .trim_end_matches(SQL_FELT_DELIMITER)
            .split(SQL_FELT_DELIMITER)
            .filter(|s| !s.is_empty())
            .map(Felt::from_str)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ParseError::from)?;
        let data = event
            .data
            .trim_end_matches(SQL_FELT_DELIMITER)
            .split(SQL_FELT_DELIMITER)
            .filter(|s| !s.is_empty())
            .map(Felt::from_str)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ParseError::from)?;

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            if !match_keys(&keys, &sub.keys) {
                continue;
            }

            let resp = SubscribeEventsResponse {
                event: Some(ProtoEvent {
                    keys: keys.iter().map(|k| k.to_bytes_be().to_vec()).collect(),
                    data: data.iter().map(|d| d.to_bytes_be().to_vec()).collect(),
                    transaction_hash: Felt::from_str(&event.transaction_hash)
                        .map_err(ParseError::from)?
                        .to_bytes_be()
                        .to_vec(),
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

        Ok(())
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
