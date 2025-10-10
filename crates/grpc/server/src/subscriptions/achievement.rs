use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures::Stream;
use futures_util::StreamExt;
use rand::Rng;
use starknet_crypto::Felt;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use torii_broker::types::AchievementProgressionUpdate;
use torii_broker::MemoryBroker;
use torii_proto::AchievementProgression;
use tracing::{error, trace};

use crate::GrpcConfig;
use torii_proto::proto::world::SubscribeAchievementProgressionsResponse;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::achievement";

#[derive(Debug, Default, Clone)]
pub struct AchievementProgressionFilter {
    pub world_addresses: Vec<Felt>,
    pub namespaces: Vec<String>,
    pub player_addresses: Vec<Felt>,
    pub achievement_ids: Vec<String>,
}

impl AchievementProgressionFilter {
    pub fn matches(&self, progression: &AchievementProgression) -> bool {
        // If no filters specified, match all
        if self.world_addresses.is_empty()
            && self.namespaces.is_empty()
            && self.player_addresses.is_empty()
            && self.achievement_ids.is_empty()
        {
            return true;
        }

        // Check world_address filter
        let world_match = self.world_addresses.is_empty()
            || self.world_addresses.contains(&progression.world_address);

        // Check namespace filter
        let namespace_match =
            self.namespaces.is_empty() || self.namespaces.contains(&progression.namespace);

        // Check player_address filter
        let player_match = self.player_addresses.is_empty()
            || self.player_addresses.contains(&progression.player_id);

        // Check achievement_id filter (derived from task_id or full achievement_id)
        let achievement_match = self.achievement_ids.is_empty()
            || self
                .achievement_ids
                .iter()
                .any(|aid| progression.id.contains(aid));

        world_match && namespace_match && player_match && achievement_match
    }
}

#[derive(Debug)]
pub struct AchievementProgressionSubscriber {
    pub(crate) filter: AchievementProgressionFilter,
    pub(crate) sender: Sender<Result<SubscribeAchievementProgressionsResponse, tonic::Status>>,
}

#[derive(Debug)]
pub struct AchievementProgressionManager {
    subscribers: DashMap<u64, AchievementProgressionSubscriber>,
    config: GrpcConfig,
}

impl AchievementProgressionManager {
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            subscribers: DashMap::new(),
            config,
        }
    }

    pub async fn add_subscriber(
        &self,
        filter: AchievementProgressionFilter,
    ) -> Receiver<Result<SubscribeAchievementProgressionsResponse, tonic::Status>> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(self.config.subscription_buffer_size);

        // Send initial empty message to establish stream
        let _ = sender
            .send(Ok(SubscribeAchievementProgressionsResponse {
                progression: None,
                subscription_id,
            }))
            .await;

        self.subscribers.insert(
            subscription_id,
            AchievementProgressionSubscriber { filter, sender },
        );

        receiver
    }

    pub async fn update_subscriber(&self, id: u64, filter: AchievementProgressionFilter) {
        if let Some(mut subscriber) = self.subscribers.get_mut(&id) {
            subscriber.filter = filter;
        }
    }

    pub(super) async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = AchievementProgression> + Send>>,
    progression_sender: UnboundedSender<AchievementProgression>,
}

impl Service {
    pub fn new(subs_manager: Arc<AchievementProgressionManager>) -> Self {
        let (progression_sender, progression_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<AchievementProgressionUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<AchievementProgressionUpdate>::subscribe())
            },
            progression_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, progression_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<AchievementProgressionManager>,
        mut progression_receiver: UnboundedReceiver<AchievementProgression>,
    ) {
        while let Some(update) = progression_receiver.recv().await {
            Self::process_progression_update(&subs, &update).await;
        }
    }

    async fn process_progression_update(
        subs: &Arc<AchievementProgressionManager>,
        progression: &AchievementProgression,
    ) {
        let mut closed_stream = Vec::new();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            // Check if the subscriber is interested in this progression
            if !sub.filter.matches(progression) {
                continue;
            }

            let resp = SubscribeAchievementProgressionsResponse {
                progression: Some(progression.clone().into()),
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
            trace!(target = LOG_TARGET, id = %id, "Closing achievement progression stream.");
            subs.remove_subscriber(id).await
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(progression)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.progression_sender.send(progression) {
                error!(target = LOG_TARGET, error = %e, "Sending achievement progression update to processor.");
            }
        }

        Poll::Pending
    }
}
