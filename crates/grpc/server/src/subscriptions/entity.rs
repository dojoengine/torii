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
use torii_sqlite::constants::SQL_FELT_DELIMITER;
use torii_sqlite::error::Error;
use torii_sqlite::simple_broker::SimpleBroker;
use torii_sqlite::types::OptimisticEntity;
use tracing::error;

use super::{
    broadcast_to_subscribers, match_entity, SubscriptionManager, SUBSCRIPTION_CHANNEL_SIZE,
};
use torii_proto::proto::world::SubscribeEntityResponse;
use torii_proto::Clause;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::entity";

#[derive(Debug)]
pub struct EntitiesSubscriber {
    /// The clause that the subscriber is interested in
    pub(crate) clause: Option<Clause>,
    /// The channel to send the response back to the subscriber.
    pub(crate) sender: Sender<Result<SubscribeEntityResponse, tonic::Status>>,
}

impl super::SubscriberSender<SubscribeEntityResponse> for EntitiesSubscriber {
    fn sender(&self) -> &Sender<Result<SubscribeEntityResponse, tonic::Status>> {
        &self.sender
    }
}

#[derive(Debug, Default)]
pub struct EntityManager {
    subscribers: DashMap<u64, EntitiesSubscriber>,
}

impl SubscriptionManager<Option<Clause>> for EntityManager {
    type Subscriber = EntitiesSubscriber;
    type Response = SubscribeEntityResponse;
    type Item = OptimisticEntity;

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
            .insert(id, EntitiesSubscriber { clause, sender });
    }

    async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
    }

    fn create_subscriber(
        _id: u64,
        sender: Sender<Result<SubscribeEntityResponse, tonic::Status>>,
        clause: Option<Clause>,
    ) -> Self::Subscriber {
        EntitiesSubscriber { clause, sender }
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
                .filter_map(|key| {
                    if key.is_empty() {
                        None
                    } else {
                        Some(Felt::from_str(key))
                    }
                })
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
        if entity.deleted {
            SubscribeEntityResponse {
                entity: Some(torii_proto::proto::types::Entity {
                    hashed_keys: Felt::from_str(&entity.id)
                        .map(|f| f.to_bytes_be().to_vec())
                        .unwrap_or_default(),
                    models: vec![],
                }),
                subscription_id: subscriber_id,
            }
        } else {
            // This should NEVER be None
            let model = entity
                .updated_model
                .as_ref()
                .unwrap()
                .as_struct()
                .unwrap()
                .clone();

            SubscribeEntityResponse {
                entity: Some(torii_proto::proto::types::Entity {
                    hashed_keys: Felt::from_str(&entity.id)
                        .map(|f| f.to_bytes_be().to_vec())
                        .unwrap_or_default(),
                    models: vec![model.into()],
                }),
                subscription_id: subscriber_id,
            }
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
impl EntityManager {
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
    simple_broker: Pin<Box<dyn Stream<Item = OptimisticEntity> + Send>>,
    entity_sender: UnboundedSender<OptimisticEntity>,
}

impl Service {
    pub fn new(subs_manager: Arc<EntityManager>) -> Self {
        let (entity_sender, entity_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(SimpleBroker::<OptimisticEntity>::subscribe()),
            entity_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, entity_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<EntityManager>,
        mut entity_receiver: UnboundedReceiver<OptimisticEntity>,
    ) {
        while let Some(entity) = entity_receiver.recv().await {
            if let Err(e) = Self::process_entity_update(&subs, &entity).await {
                error!(target = LOG_TARGET, error = %e, "Processing entity update.");
            }
        }
    }

    async fn process_entity_update(
        subs: &Arc<EntityManager>,
        entity: &OptimisticEntity,
    ) -> Result<(), Error> {
        broadcast_to_subscribers::<EntityManager, Option<Clause>>(subs, entity, LOG_TARGET).await
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(entity)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.entity_sender.send(entity) {
                error!(target = LOG_TARGET, error = %e, "Sending entity update to processor.");
            }
        }

        Poll::Pending
    }
}
