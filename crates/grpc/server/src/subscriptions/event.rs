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
use torii_proto::KeysClause;
use torii_sqlite::constants::SQL_FELT_DELIMITER;
use torii_sqlite::error::Error;
use torii_sqlite::simple_broker::SimpleBroker;
use torii_sqlite::types::Event;
use tracing::error;

use super::{
    broadcast_to_subscribers, match_keys, SubscriberSender, SubscriptionManager,
    SUBSCRIPTION_CHANNEL_SIZE,
};
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

impl SubscriberSender<SubscribeEventsResponse> for EventSubscriber {
    fn sender(&self) -> &Sender<Result<SubscribeEventsResponse, tonic::Status>> {
        &self.sender
    }
}

#[derive(Debug, Default)]
pub struct EventManager {
    subscribers: DashMap<u64, EventSubscriber>,
}

impl SubscriptionManager<Vec<KeysClause>> for EventManager {
    type Subscriber = EventSubscriber;
    type Response = SubscribeEventsResponse;
    type Item = Event;

    fn new() -> Self {
        Self {
            subscribers: DashMap::new(),
        }
    }

    fn subscribers(&self) -> &DashMap<u64, Self::Subscriber> {
        &self.subscribers
    }

    async fn add_subscriber(
        &self,
        keys: Vec<KeysClause>,
    ) -> Result<Receiver<Result<SubscribeEventsResponse, tonic::Status>>, Error> {
        let id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(SUBSCRIPTION_CHANNEL_SIZE);

        // NOTE: unlock issue with firefox/safari
        // initially send empty stream message to return from
        // initial subscribe call
        let _ = sender.send(Ok(Self::get_initial_response(id))).await;

        self.subscribers
            .insert(id, Self::create_subscriber(id, sender, keys));

        Ok(receiver)
    }

    async fn update_subscriber(&self, id: u64, keys: Vec<KeysClause>) {
        if let Some(subscriber) = self.subscribers.get(&id) {
            let sender = subscriber.sender.clone();
            self.subscribers
                .insert(id, EventSubscriber { keys, sender });
        }
    }

    async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
    }

    fn create_subscriber(
        _id: u64,
        sender: Sender<Result<SubscribeEventsResponse, tonic::Status>>,
        keys: Vec<KeysClause>,
    ) -> Self::Subscriber {
        EventSubscriber { keys, sender }
    }

    fn should_send_to_subscriber(subscriber: &Self::Subscriber, event: &Self::Item) -> bool {
        let keys = match event
            .keys
            .trim_end_matches(SQL_FELT_DELIMITER)
            .split(SQL_FELT_DELIMITER)
            .filter(|s| !s.is_empty())
            .map(Felt::from_str)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(k) => k,
            Err(_) => return false,
        };

        match_keys(&keys, &subscriber.keys)
    }

    fn create_response(_subscriber_id: u64, event: &Self::Item) -> Self::Response {
        let keys = event
            .keys
            .trim_end_matches(SQL_FELT_DELIMITER)
            .split(SQL_FELT_DELIMITER)
            .filter(|s| !s.is_empty())
            .map(Felt::from_str)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_default();

        let data = event
            .data
            .trim_end_matches(SQL_FELT_DELIMITER)
            .split(SQL_FELT_DELIMITER)
            .filter(|s| !s.is_empty())
            .map(Felt::from_str)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_default();

        let transaction_hash = Felt::from_str(&event.transaction_hash).unwrap_or_default();

        SubscribeEventsResponse {
            event: Some(ProtoEvent {
                keys: keys.iter().map(|k| k.to_bytes_be().to_vec()).collect(),
                data: data.iter().map(|d| d.to_bytes_be().to_vec()).collect(),
                transaction_hash: transaction_hash.to_bytes_be().to_vec(),
            }),
        }
    }

    fn get_initial_response(_subscriber_id: u64) -> Self::Response {
        SubscribeEventsResponse { event: None }
    }
}

#[allow(dead_code)]
impl EventManager {
    pub async fn add_subscriber(
        &self,
        keys: Vec<KeysClause>,
    ) -> Result<Receiver<Result<SubscribeEventsResponse, tonic::Status>>, Error> {
        <Self as SubscriptionManager<Vec<KeysClause>>>::add_subscriber(self, keys).await
    }

    pub(super) async fn remove_subscriber(&self, id: u64) {
        <Self as SubscriptionManager<Vec<KeysClause>>>::remove_subscriber(self, id).await
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
            simple_broker: Box::pin(SimpleBroker::<Event>::subscribe()),
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
                error!(target = LOG_TARGET, error = %e, "Processing event update.");
            }
        }
    }

    async fn process_event(subs: &Arc<EventManager>, event: &Event) -> Result<(), Error> {
        broadcast_to_subscribers::<EventManager, Vec<KeysClause>>(subs, event, LOG_TARGET).await
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let pin = self.get_mut();

        while let Poll::Ready(Some(event)) = pin.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = pin.event_sender.send(event) {
                error!(target = LOG_TARGET, error = %e, "Sending event to processor.");
            }
        }

        Poll::Pending
    }
}
