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
use torii_proto::schema::Entity;
use torii_sqlite::constants::SQL_FELT_DELIMITER;
use torii_sqlite::error::{Error, ParseError};
use torii_sqlite::simple_broker::SimpleBroker;
use tracing::{error, trace};

use super::match_entity;
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
#[derive(Debug, Default)]
pub struct EntityManager {
    subscribers: DashMap<u64, EntitiesSubscriber>,
    subscription_buffer_size: usize,
}

impl EntityManager {
    pub fn new(subscription_buffer_size: usize) -> Self {
        Self {
            subscribers: DashMap::new(),
            subscription_buffer_size,
        }
    }

    pub async fn add_subscriber(
        &self,
        clause: Option<Clause>,
    ) -> Result<Receiver<Result<SubscribeEntityResponse, tonic::Status>>, Error> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(self.subscription_buffer_size);

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
            .insert(subscription_id, EntitiesSubscriber { clause, sender });

        Ok(receiver)
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
    simple_broker: Pin<Box<dyn Stream<Item = Entity> + Send>>,
    entity_sender: UnboundedSender<Entity>,
}

impl Service {
    pub fn new(subs_manager: Arc<EntityManager>) -> Self {
        let (entity_sender, entity_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(SimpleBroker::<Entity>::subscribe()),
            entity_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, entity_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<EntityManager>,
        mut entity_receiver: UnboundedReceiver<Entity>,
    ) {
        while let Some(entity) = entity_receiver.recv().await {
            if let Err(e) = Self::process_entity_update(&subs, &entity).await {
                error!(target = LOG_TARGET, error = %e, "Processing entity update.");
            }
        }
    }

    async fn process_entity_update(
        subs: &Arc<EntityManager>,
        entity: &Entity,
    ) -> Result<(), Error> {
        let mut closed_stream = Vec::new();
        let hashed = Felt::from_bytes_be_slice(&entity.hashed_keys);

        let keys = if entity.models.is_empty() {
            vec![]
        } else {
            vec![]
        };

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            if let Some(clause) = &sub.clause {
                if !match_entity(hashed, &keys, &None, clause) {
                    continue;
                }
            }

            let resp = SubscribeEntityResponse {
                entity: Some(torii_proto::proto::types::Entity {
                    hashed_keys: entity.hashed_keys.clone(),
                    models: entity.models.clone(),
                }),
                subscription_id: *idx,
            };

            match sub.sender.try_send(Ok(resp)) {
                Ok(_) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    trace!(target = LOG_TARGET, subscription_id = %idx, "Disconnecting slow subscriber - channel full");
                    closed_stream.push(*idx);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    closed_stream.push(*idx);
                }
            }
        }

        for id in closed_stream {
            trace!(target = LOG_TARGET, id = %id, "Closing entity stream.");
            subs.remove_subscriber(id).await
        }

        Ok(())
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
