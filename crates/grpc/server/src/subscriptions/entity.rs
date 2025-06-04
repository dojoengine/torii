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
use torii_sqlite::error::{Error, ParseError};
use torii_sqlite::simple_broker::SimpleBroker;
use torii_sqlite::types::OptimisticEntity;
use tracing::{error, trace};

use super::{match_entity, SUBSCRIPTION_CHANNEL_SIZE};
use torii_proto::proto::world::SubscribeEntityResponse;
use torii_proto::Clause;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::entity";

#[derive(Debug)]
pub struct EntitiesSubscriber {
    clause: Option<Clause>,
    sender: Sender<Result<SubscribeEntityResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct EntityManager {
    subscribers: DashMap<u64, EntitiesSubscriber>,
}

impl EntityManager {
    pub async fn add_subscriber(
        &self,
        clause: Option<Clause>,
    ) -> Result<Receiver<Result<SubscribeEntityResponse, tonic::Status>>, Error> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(SUBSCRIPTION_CHANNEL_SIZE);

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
        if let Some(entry) = self.subscribers.get(&id) {
            let sender = entry.sender.clone();
            drop(entry);
            self.subscribers
                .insert(id, EntitiesSubscriber { clause, sender });
        }
    }

    pub async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
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
        let (entity_sender, mut entity_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(SimpleBroker::<OptimisticEntity>::subscribe()),
            entity_sender,
        };

        // Create a worker pool with separate channels for each worker
        let worker_count = num_cpus::get().min(4);
        let mut worker_senders = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (worker_sender, worker_receiver) = unbounded_channel();
            worker_senders.push(worker_sender);
            let subs = subs_manager.clone();
            tokio::spawn(async move {
                Self::publish_updates(subs, worker_receiver).await;
            });
        }

        // Spawn a task to distribute entities to workers
        tokio::spawn(async move {
            let mut worker_index = 0;
            while let Some(entity) = entity_receiver.recv().await {
                // Send to a worker in a round-robin fashion
                if let Err(e) = worker_senders[worker_index].send(entity) {
                    error!(target = LOG_TARGET, error = ?e, "Failed to send entity to worker.");
                }
                worker_index = (worker_index + 1) % worker_count;
            }
        });

        service
    }

    async fn publish_updates(
        subs: Arc<EntityManager>,
        mut entity_receiver: UnboundedReceiver<OptimisticEntity>,
    ) {
        while let Some(entity) = entity_receiver.recv().await {
            // Process each entity in a separate tarefa para permitir concorrência
            let subs = subs.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::process_entity_update(&subs, &entity).await {
                    error!(target = LOG_TARGET, error = ?e, "Processing entity update.");
                }
            });
        }
    }

    async fn process_entity_update(
        subs: &Arc<EntityManager>,
        entity: &OptimisticEntity,
    ) -> Result<(), Error> {
        let hashed = Felt::from_str(&entity.id).map_err(ParseError::FromStr)?;
        let keys = entity
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
            .map_err(ParseError::FromStr)?;

        // Take a snapshot of subscribers to minimize lock time
        let subscribers: Vec<_> = subs
            .subscribers
            .iter()
            .map(|entry| (*entry.key(), entry.clause.clone(), entry.sender.clone()))
            .collect();

        let mut closed_stream = Vec::new();

        // Process subscribers concurrently
        let tasks: Vec<_> = subscribers
            .into_iter()
            .filter_map(|(id, clause, sender)| {
                if let Some(clause) = &clause {
                    if !match_entity(hashed, &keys, &entity.updated_model, clause) {
                        return None;
                    }
                }

                let resp = if entity.deleted {
                    SubscribeEntityResponse {
                        entity: Some(torii_proto::proto::types::Entity {
                            hashed_keys: hashed.to_bytes_be().to_vec(),
                            models: vec![],
                        }),
                        subscription_id: id,
                    }
                } else {
                    let model = entity
                        .updated_model
                        .as_ref()
                        .unwrap()
                        .as_struct()
                        .unwrap()
                        .clone();
                    SubscribeEntityResponse {
                        entity: Some(torii_proto::proto::types::Entity {
                            hashed_keys: hashed.to_bytes_be().to_vec(),
                            models: vec![model.into()],
                        }),
                        subscription_id: id,
                    }
                };

                Some(tokio::spawn(async move {
                    match sender.send(Ok(resp)).await {
                        Ok(_) => Ok(id),
                        Err(_) => Err(id), // Channel closed or full
                    }
                }))
            })
            .collect();

        // Wait for all sends to complete and collect failed subscriber IDs
        for task in tasks {
            if let Ok(Err(id)) = task.await {
                closed_stream.push(id);
            }
        }

        // Remove disconnected subscribers
        for id in closed_stream {
            trace!(target = LOG_TARGET, id = %id, "Closing entity stream.");
            subs.remove_subscriber(id).await;
        }

        Ok(())
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Process in batches to reduce channel contention
        for _ in 0..100 {
            match this.simple_broker.poll_next_unpin(cx) {
                Poll::Ready(Some(entity)) => {
                    if let Err(e) = this.entity_sender.send(entity) {
                        error!(target = LOG_TARGET, error = ?e, "Sending entity update to processor.");
                    }
                }
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }

        Poll::Pending
    }
}
