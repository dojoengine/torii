use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures::{Stream, StreamExt};
use rand::Rng;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use torii_broker::types::ContractUpdate;
use torii_broker::MemoryBroker;
use torii_proto::{Contract, ContractQuery};
use torii_storage::ReadOnlyStorage;
use torii_storage::StorageError;
use tracing::trace;

use torii_proto::proto::world::SubscribeContractsResponse;

use crate::GrpcConfig;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::contracts";

#[derive(Debug)]
pub struct ContractSubscriber {
    /// Contract address that the subscriber is interested in
    query: ContractQuery,
    /// The channel to send the response back to the subscriber.
    sender: Sender<Result<SubscribeContractsResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct ContractManager {
    subscribers: DashMap<usize, ContractSubscriber>,
    config: GrpcConfig,
}

impl ContractManager {
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            subscribers: DashMap::new(),
            config,
        }
    }

    pub async fn add_subscriber(
        &self,
        storage: Arc<dyn ReadOnlyStorage>,
        query: ContractQuery,
    ) -> Result<Receiver<Result<SubscribeContractsResponse, tonic::Status>>, StorageError> {
        let id = rand::thread_rng().gen::<usize>();
        let (sender, receiver) = channel(self.config.subscription_buffer_size);

        let contracts = storage.contracts(&query).await?;
        for contract in contracts {
            let _ = sender
                .send(Ok(SubscribeContractsResponse {
                    contract: Some(contract.clone().into()),
                }))
                .await;
        }
        self.subscribers
            .insert(id, ContractSubscriber { query, sender });

        Ok(receiver)
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = Contract> + Send>>,
    subs_manager: Arc<ContractManager>,
}

impl Service {
    pub fn new(subs_manager: Arc<ContractManager>) -> Self {
        Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<ContractUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<ContractUpdate>::subscribe())
            },
            subs_manager,
        }
    }

    // Process updates synchronously - no async overhead
    fn process_contract(subs: &ContractManager, contract: &Contract) {
        let mut closed_stream = Vec::new();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            if !sub.query.contract_addresses.is_empty()
                && !sub
                    .query
                    .contract_addresses
                    .contains(&contract.contract_address.clone())
            {
                continue;
            }

            if !sub.query.contract_types.is_empty()
                && !sub
                    .query
                    .contract_types
                    .contains(&contract.contract_type.clone())
            {
                continue;
            }

            let resp = SubscribeContractsResponse {
                contract: Some(contract.clone().into()),
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
            trace!(target = LOG_TARGET, id = %id, "Removing closed subscriber.");
            subs.subscribers.remove(&id);
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Process updates inline for minimum latency
        while let Poll::Ready(Some(contract)) = this.simple_broker.poll_next_unpin(cx) {
            Self::process_contract(&this.subs_manager, &contract);
        }

        Poll::Pending
    }
}
