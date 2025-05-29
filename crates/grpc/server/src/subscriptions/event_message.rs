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
use torii_proto::proto::types::Entity;
use torii_proto::Clause;
use torii_sqlite::constants::SQL_FELT_DELIMITER;
use torii_sqlite::error::Error;
use torii_sqlite::simple_broker::SimpleBroker;
use torii_sqlite::types::OptimisticEventMessage;
use tracing::error;

use torii_proto::proto::world::SubscribeEntityResponse;

use super::{
    broadcast_to_subscribers, match_entity, SubscriptionManager, SUBSCRIPTION_CHANNEL_SIZE,
};

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::event_message";

#[derive(Debug)]
pub struct EventMessageSubscriber {
    /// The clause that the subscriber is interested in
    pub(crate) clause: Option<Clause>,
    /// The channel to send the response back to the subscriber.
    pub(crate) sender: Sender<Result<SubscribeEntityResponse, tonic::Status>>,
}

impl super::SubscriberSender<SubscribeEntityResponse> for EventMessageSubscriber {
    fn sender(&self) -> &Sender<Result<SubscribeEntityResponse, tonic::Status>> {
        &self.sender
    }
}

#[derive(Debug, Default)]
pub struct EventMessageManager {
    subscribers: DashMap<u64, EventMessageSubscriber>,
}

impl SubscriptionManager<Option<Clause>> for EventMessageManager {
    type Subscriber = EventMessageSubscriber;
    type Response = SubscribeEntityResponse;
    type Item = OptimisticEventMessage;

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
        clause: Option<Clause>,
    ) -> Result<Receiver<Result<SubscribeEntityResponse, tonic::Status>>, Error> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(SUBSCRIPTION_CHANNEL_SIZE);

        // NOTE: unlock issue with firefox/safari
        // initially send empty stream message to return from
        // initial subscribe call
        let _ = sender
            .send(Ok(Self::get_initial_response(subscription_id)))
            .await;

        self.subscribers.insert(
            subscription_id,
            Self::create_subscriber(subscription_id, sender, clause),
        );

        Ok(receiver)
    }

    async fn update_subscriber(&self, id: u64, clause: Option<Clause>) {
        let sender = if let Some(subscriber) = self.subscribers.get(&id) {
            subscriber.sender.clone()
        } else {
            return; // Subscriber not found, exit early
        };

        self.subscribers
            .insert(id, EventMessageSubscriber { clause, sender });
    }

    async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
    }

    fn create_subscriber(
        _id: u64,
        sender: Sender<Result<SubscribeEntityResponse, tonic::Status>>,
        clause: Option<Clause>,
    ) -> Self::Subscriber {
        EventMessageSubscriber { clause, sender }
    }

    fn should_send_to_subscriber(subscriber: &Self::Subscriber, entity: &Self::Item) -> bool {
        if let Some(clause) = &subscriber.clause {
            let hashed = match Felt::from_str(&entity.id) {
                Ok(h) => h,
                Err(_) => return false,
            };

            let keys = match entity
                .keys
                .trim_end_matches(SQL_FELT_DELIMITER)
                .split(SQL_FELT_DELIMITER)
                .map(Felt::from_str)
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(k) => k,
                Err(_) => return false,
            };

            match_entity(hashed, &keys, &entity.updated_model, clause)
        } else {
            true // No clause means subscriber wants all entities
        }
    }

    fn create_response(subscriber_id: u64, entity: &Self::Item) -> Self::Response {
        // This should NEVER be None
        let model = entity
            .updated_model
            .as_ref()
            .unwrap()
            .as_struct()
            .unwrap()
            .clone();

        SubscribeEntityResponse {
            entity: Some(Entity {
                hashed_keys: Felt::from_str(&entity.id)
                    .map(|f| f.to_bytes_be().to_vec())
                    .unwrap_or_default(),
                models: vec![model.into()],
            }),
            subscription_id: subscriber_id,
        }
    }

    fn get_initial_response(subscriber_id: u64) -> Self::Response {
        SubscribeEntityResponse {
            entity: None,
            subscription_id: subscriber_id,
        }
    }
}

#[allow(dead_code)]
impl EventMessageManager {
    pub async fn add_subscriber(
        &self,
        clause: Option<Clause>,
    ) -> Result<Receiver<Result<SubscribeEntityResponse, tonic::Status>>, Error> {
        <Self as SubscriptionManager<Option<Clause>>>::add_subscriber(self, clause).await
    }

    pub async fn update_subscriber(&self, id: u64, clause: Option<Clause>) {
        <Self as SubscriptionManager<Option<Clause>>>::update_subscriber(self, id, clause).await
    }

    pub(super) async fn remove_subscriber(&self, id: u64) {
        <Self as SubscriptionManager<Option<Clause>>>::remove_subscriber(self, id).await
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = OptimisticEventMessage> + Send>>,
    event_sender: UnboundedSender<OptimisticEventMessage>,
}

impl Service {
    pub fn new(subs_manager: Arc<EventMessageManager>) -> Self {
        let (event_sender, event_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(SimpleBroker::<OptimisticEventMessage>::subscribe()),
            event_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, event_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<EventMessageManager>,
        mut event_receiver: UnboundedReceiver<OptimisticEventMessage>,
    ) {
        while let Some(event) = event_receiver.recv().await {
            if let Err(e) = Self::process_event_update(&subs, &event).await {
                error!(target = LOG_TARGET, error = %e, "Processing event update.");
            }
        }
    }

    async fn process_event_update(
        subs: &Arc<EventMessageManager>,
        entity: &OptimisticEventMessage,
    ) -> Result<(), Error> {
        broadcast_to_subscribers::<EventMessageManager, Option<Clause>>(subs, entity, LOG_TARGET)
            .await
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(event)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.event_sender.send(event) {
                error!(target = LOG_TARGET, error = %e, "Sending event update to processor.");
            }
        }

        Poll::Pending
    }
}
