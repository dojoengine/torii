use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures::Stream;
use rand::Rng;
use starknet_crypto::Felt;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use torii_broker::types::ActivityUpdate;
use torii_broker::MemoryBroker;
use torii_proto::Activity;
use tracing::{error, trace};

use crate::GrpcConfig;
use torii_proto::proto::world::SubscribeActivitiesResponse;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::activity";

#[derive(Debug, Default, Clone)]
pub struct ActivityFilter {
    pub world_addresses: Vec<Felt>,
    pub namespaces: Vec<String>,
    pub caller_addresses: Vec<Felt>,
}

impl ActivityFilter {
    pub fn matches(&self, activity: &Activity) -> bool {
        // If no filters specified, match all
        if self.world_addresses.is_empty()
            && self.namespaces.is_empty()
            && self.caller_addresses.is_empty()
        {
            return true;
        }

        // Check world_address filter
        let world_match = self.world_addresses.is_empty()
            || self.world_addresses.contains(&activity.world_address);

        // Check namespace filter
        let namespace_match =
            self.namespaces.is_empty() || self.namespaces.contains(&activity.namespace);

        // Check caller_address filter
        let caller_match = self.caller_addresses.is_empty()
            || self.caller_addresses.contains(&activity.caller_address);

        world_match && namespace_match && caller_match
    }
}

#[derive(Debug)]
pub struct ActivitySubscriber {
    pub(crate) filter: ActivityFilter,
    pub(crate) sender: Sender<Result<SubscribeActivitiesResponse, tonic::Status>>,
}

#[derive(Debug)]
pub struct ActivityManager {
    subscribers: DashMap<u64, ActivitySubscriber>,
    config: GrpcConfig,
}

impl ActivityManager {
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            subscribers: DashMap::new(),
            config,
        }
    }

    pub async fn add_subscriber(
        &self,
        filter: ActivityFilter,
    ) -> Receiver<Result<SubscribeActivitiesResponse, tonic::Status>> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(self.config.subscription_buffer_size);

        // Send initial empty message to establish stream
        let _ = sender
            .send(Ok(SubscribeActivitiesResponse {
                activity: None,
                subscription_id,
            }))
            .await;

        self.subscribers
            .insert(subscription_id, ActivitySubscriber { filter, sender });

        receiver
    }

    pub async fn update_subscriber(&self, id: u64, filter: ActivityFilter) {
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
    simple_broker: Pin<Box<dyn Stream<Item = Activity> + Send>>,
    activity_sender: UnboundedSender<Activity>,
}

impl Service {
    pub fn new(subs_manager: Arc<ActivityManager>) -> impl Future<Output = ()> {
        let (activity_sender, activity_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<ActivityUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<ActivityUpdate>::subscribe())
            },
            activity_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, activity_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<ActivityManager>,
        mut activity_receiver: UnboundedReceiver<Activity>,
    ) {
        while let Some(update) = activity_receiver.recv().await {
            Self::process_activity_update(&subs, &update).await;
        }
    }

    async fn process_activity_update(subs: &ActivityManager, activity: &Activity) {
        let mut closed_stream_ids = Vec::new();

        for subscriber in subs.subscribers.iter() {
            let id = subscriber.key();
            let subscriber = subscriber.value();

            if !subscriber.filter.matches(activity) {
                continue;
            }

            trace!(
                target: LOG_TARGET,
                subscription_id = id,
                activity_id = %activity.id,
                "Processing activity update"
            );

            let resp = SubscribeActivitiesResponse {
                subscription_id: *id,
                activity: Some(activity.clone().into()),
            };

            if subscriber.sender.try_send(Ok(resp)).is_err() {
                closed_stream_ids.push(*id);
            }
        }

        for id in closed_stream_ids {
            subs.remove_subscriber(id).await;
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(activity)) = this.simple_broker.as_mut().poll_next(cx) {
            if let Err(e) = this.activity_sender.send(activity) {
                error!(
                    target: LOG_TARGET,
                    error = %e,
                    "Sending activity update to processor"
                );
            }
        }

        Poll::Pending
    }
}
