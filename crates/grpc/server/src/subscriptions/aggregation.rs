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
use torii_broker::{types::AggregationUpdate, MemoryBroker};
use torii_proto::AggregationEntry;
use tracing::{error, trace};

use crate::GrpcConfig;

use torii_proto::proto::world::SubscribeAggregationsResponse;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::aggregation";

#[derive(Debug, Clone)]
pub struct AggregationFilter {
    /// Filter by aggregator IDs (e.g., "top_scores", "most_wins")
    pub aggregator_ids: Vec<String>,
    /// Filter by entity IDs (e.g., specific player addresses)
    pub entity_ids: Vec<String>,
}

impl AggregationFilter {
    pub fn matches(&self, entry: &AggregationEntry) -> bool {
        // If no filters specified, match all
        if self.aggregator_ids.is_empty() && self.entity_ids.is_empty() {
            return true;
        }

        // Check aggregator_id filter
        let aggregator_match = self.aggregator_ids.is_empty()
            || self.aggregator_ids.contains(&entry.aggregator_id);

        // Check entity_id filter
        let entity_match =
            self.entity_ids.is_empty() || self.entity_ids.contains(&entry.entity_id);

        aggregator_match && entity_match
    }
}

#[derive(Debug)]
pub struct AggregationSubscriber {
    /// The filter that the subscriber is interested in
    pub(crate) filter: AggregationFilter,
    /// The channel to send the response back to the subscriber.
    pub(crate) sender: Sender<Result<SubscribeAggregationsResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct AggregationManager {
    subscribers: DashMap<u64, AggregationSubscriber>,
    config: GrpcConfig,
}

impl AggregationManager {
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            subscribers: DashMap::new(),
            config,
        }
    }

    pub async fn add_subscriber(
        &self,
        filter: AggregationFilter,
    ) -> Receiver<Result<SubscribeAggregationsResponse, tonic::Status>> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(self.config.subscription_buffer_size);

        // NOTE: unlock issue with firefox/safari
        // initially send empty stream message to return from
        // initial subscribe call
        let _ = sender
            .send(Ok(SubscribeAggregationsResponse {
                entry: None,
                subscription_id,
            }))
            .await;

        self.subscribers
            .insert(subscription_id, AggregationSubscriber { filter, sender });

        receiver
    }

    pub async fn update_subscriber(&self, id: u64, filter: AggregationFilter) {
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
    simple_broker: Pin<Box<dyn Stream<Item = AggregationEntry> + Send>>,
    aggregation_sender: UnboundedSender<AggregationEntry>,
}

impl Service {
    pub fn new(subs_manager: Arc<AggregationManager>) -> Self {
        let (aggregation_sender, aggregation_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<AggregationUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<AggregationUpdate>::subscribe())
            },
            aggregation_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, aggregation_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<AggregationManager>,
        mut aggregation_receiver: UnboundedReceiver<AggregationEntry>,
    ) {
        while let Some(update) = aggregation_receiver.recv().await {
            Self::process_aggregation_update(&subs, &update).await;
        }
    }

    async fn process_aggregation_update(
        subs: &Arc<AggregationManager>,
        entry: &AggregationEntry,
    ) {
        let mut closed_stream = Vec::new();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            // Check if the subscriber is interested in this aggregation
            if !sub.filter.matches(entry) {
                continue;
            }

            let resp = SubscribeAggregationsResponse {
                entry: Some(entry.clone().into()),
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
            trace!(target = LOG_TARGET, id = %id, "Closing aggregation stream.");
            subs.remove_subscriber(id).await
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(entry)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.aggregation_sender.send(entry) {
                error!(target = LOG_TARGET, error = %e, "Sending aggregation update to processor.");
            }
        }

        Poll::Pending
    }
}

