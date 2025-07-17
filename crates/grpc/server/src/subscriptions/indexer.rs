use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures::{Stream, StreamExt};
use rand::Rng;
use starknet::core::types::Felt;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use torii_broker::types::ContractUpdate;
use torii_broker::MemoryBroker;
use torii_storage::Storage;
use torii_storage::StorageError;
use tracing::{error, trace};

use torii_proto::proto::world::SubscribeIndexerResponse;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::indexer";

#[derive(Debug)]
pub struct IndexerSubscriber {
    /// Contract address that the subscriber is interested in
    contract_address: Felt,
    /// The channel to send the response back to the subscriber.
    sender: Sender<Result<SubscribeIndexerResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct IndexerManager {
    subscribers: DashMap<usize, IndexerSubscriber>,
    subscription_buffer_size: usize,
}

impl IndexerManager {
    pub fn new(subscription_buffer_size: usize) -> Self {
        Self {
            subscribers: DashMap::new(),
            subscription_buffer_size,
        }
    }

    pub async fn add_subscriber(
        &self,
        storage: Arc<dyn Storage>,
        contract_address: Felt,
    ) -> Result<Receiver<Result<SubscribeIndexerResponse, tonic::Status>>, StorageError> {
        let id = rand::thread_rng().gen::<usize>();
        let (sender, receiver) = channel(self.subscription_buffer_size);

        let contracts = storage.cursors().await?;
        for (contract_address, contract) in contracts {
            let _ = sender
                .send(Ok(SubscribeIndexerResponse {
                    head: contract.head.unwrap() as i64,
                    tps: contract.tps.unwrap() as i64,
                    last_block_timestamp: contract.last_block_timestamp.unwrap() as i64,
                    contract_address: contract_address.to_bytes_be().to_vec(),
                }))
                .await;
        }
        self.subscribers.insert(
            id,
            IndexerSubscriber {
                contract_address,
                sender,
            },
        );

        Ok(receiver)
    }

    pub(super) async fn remove_subscriber(&self, id: usize) {
        self.subscribers.remove(&id);
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = ContractUpdate> + Send>>,
    update_sender: UnboundedSender<ContractUpdate>,
}

impl Service {
    pub fn new(subs_manager: Arc<IndexerManager>) -> Self {
        let (update_sender, update_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(MemoryBroker::<ContractUpdate>::subscribe()),
            update_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, update_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<IndexerManager>,
        mut update_receiver: UnboundedReceiver<ContractUpdate>,
    ) {
        while let Some(update) = update_receiver.recv().await {
            Self::process_update(&subs, &update).await;
        }
    }

    async fn process_update(subs: &Arc<IndexerManager>, update: &ContractUpdate) {
        let mut closed_stream = Vec::new();
        let contract = update.clone().into_inner();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            if sub.contract_address != Felt::ZERO
                && sub.contract_address != contract.contract_address
            {
                continue;
            }

            let resp = SubscribeIndexerResponse {
                head: contract.head.unwrap() as i64,
                tps: contract.tps.unwrap() as i64,
                last_block_timestamp: contract.last_block_timestamp.unwrap() as i64,
                contract_address: contract.contract_address.to_bytes_be().to_vec(),
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
            trace!(target = LOG_TARGET, id = %id, "Closing indexer updates stream.");
            subs.remove_subscriber(id).await
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(update)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.update_sender.send(update) {
                error!(target = LOG_TARGET, error = ?e, "Sending indexer update to processor.");
            }
        }

        Poll::Pending
    }
}
