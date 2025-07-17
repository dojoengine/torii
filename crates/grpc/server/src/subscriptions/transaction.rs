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
use torii_broker::types::TransactionUpdate;
use torii_broker::MemoryBroker;
use tracing::{error, trace};

use torii_proto::proto::world::SubscribeTransactionsResponse;
use torii_proto::TransactionFilter;
use torii_proto::Transaction;

use crate::GrpcConfig;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::transaction";

#[derive(Debug)]
pub struct TransactionSubscriber {
    /// The filter to apply to the subscription.
    filter: Option<TransactionFilter>,
    /// The channel to send the response back to the subscriber.
    sender: Sender<Result<SubscribeTransactionsResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct TransactionManager {
    subscribers: DashMap<usize, TransactionSubscriber>,
    config: GrpcConfig,
}

impl TransactionManager {
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            subscribers: DashMap::new(),
            config,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_subscriber(
        &self,
        filter: Option<TransactionFilter>,
    ) -> Receiver<Result<SubscribeTransactionsResponse, tonic::Status>> {
        let id = rand::thread_rng().gen::<usize>();
        let (sender, receiver) = channel(self.config.subscription_buffer_size);

        // NOTE: unlock issue with firefox/safari
        // initially send empty stream message to return from
        // initial subscribe call
        let _ = sender
            .send(Ok(SubscribeTransactionsResponse { transaction: None }))
            .await;

        self.subscribers
            .insert(id, TransactionSubscriber { filter, sender });

        receiver
    }

    pub(super) async fn remove_subscriber(&self, id: usize) {
        self.subscribers.remove(&id);
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = Transaction> + Send>>,
    transaction_sender: UnboundedSender<Transaction>,
}

impl Service {
    pub fn new(subs_manager: Arc<TransactionManager>) -> Self {
        let (transaction_sender, transaction_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<TransactionUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<TransactionUpdate>::subscribe())
            },
            transaction_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, transaction_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<TransactionManager>,
        mut transaction_receiver: UnboundedReceiver<Transaction>,
    ) {
        while let Some(transaction) = transaction_receiver.recv().await {
            Self::process_transaction(&subs, &transaction).await;
        }
    }

    async fn process_transaction(subs: &Arc<TransactionManager>, transaction: &Transaction) {
        let mut closed_stream = Vec::new();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            if let Some(filter) = &sub.filter {
                if !filter.transaction_hashes.is_empty()
                    && !filter
                        .transaction_hashes
                        .contains(&transaction.transaction_hash)
                {
                    continue;
                }

                if !filter.caller_addresses.is_empty() {
                    for caller_address in &transaction.calls {
                        if !filter
                            .caller_addresses
                            .contains(&caller_address.caller_address)
                        {
                            continue;
                        }
                    }
                }

                if !filter.contract_addresses.is_empty() {
                    for contract_address in &transaction.calls {
                        if !filter
                            .contract_addresses
                            .contains(&contract_address.contract_address)
                        {
                            continue;
                        }
                    }
                }

                if !filter.entrypoints.is_empty() {
                    for entrypoint in &transaction.calls {
                        if !filter.entrypoints.contains(&entrypoint.entrypoint) {
                            continue;
                        }
                    }
                }

                if !filter.model_selectors.is_empty() {
                    for model_selector in &transaction.unique_models {
                        if !filter.model_selectors.contains(model_selector) {
                            continue;
                        }
                    }
                }

                if filter.from_block.is_some()
                    && transaction.block_number < filter.from_block.unwrap()
                {
                    continue;
                }

                if filter.to_block.is_some() && transaction.block_number > filter.to_block.unwrap()
                {
                    continue;
                }
            }

            let resp: SubscribeTransactionsResponse = SubscribeTransactionsResponse {
                transaction: Some(transaction.clone().into()),
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
            trace!(target = LOG_TARGET, id = %id, "Closing events stream.");
            subs.remove_subscriber(id).await
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let pin = self.get_mut();

        while let Poll::Ready(Some(transaction)) = pin.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = pin.transaction_sender.send(transaction) {
                error!(target = LOG_TARGET, error = ?e, "Sending transaction to processor.");
            }
        }

        Poll::Pending
    }
}
