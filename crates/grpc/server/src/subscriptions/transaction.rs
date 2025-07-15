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
use torii_proto::Transaction;
use torii_sqlite::constants::SQL_FELT_DELIMITER;
use torii_sqlite::error::{Error, ParseError};
use torii_sqlite::simple_broker::SimpleBroker;
use tracing::{error, trace};

use torii_proto::proto::world::SubscribeTransactionsResponse;
use torii_proto::TransactionFilter;

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
    subscription_buffer_size: usize,
}

impl TransactionManager {
    pub fn new(subscription_buffer_size: usize) -> Self {
        Self {
            subscribers: DashMap::new(),
            subscription_buffer_size,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_subscriber(
        &self,
        filter: Option<TransactionFilter>,
    ) -> Result<Receiver<Result<SubscribeTransactionsResponse, tonic::Status>>, Error> {
        let id = rand::thread_rng().gen::<usize>();
        let (sender, receiver) = channel(self.subscription_buffer_size);

        // NOTE: unlock issue with firefox/safari
        // initially send empty stream message to return from
        // initial subscribe call
        let _ = sender
            .send(Ok(SubscribeTransactionsResponse { transaction: None }))
            .await;

        self.subscribers
            .insert(id, TransactionSubscriber { filter, sender });

        Ok(receiver)
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
            simple_broker: Box::pin(SimpleBroker::<Transaction>::subscribe()),
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
            if let Err(e) = Self::process_transaction(&subs, &transaction).await {
                error!(target = LOG_TARGET, error = ?e, "Processing transaction update.");
            }
        }
    }

    async fn process_transaction(
        subs: &Arc<TransactionManager>,
        transaction: &Transaction,
    ) -> Result<(), Error> {
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

                if !filter.caller_addresses.is_empty()
                    && !filter
                        .caller_addresses
                        .contains(&transaction.sender_address)
                {
                    continue;
                }

                if !filter.contract_addresses.is_empty() {
                    let has_matching_contract = transaction
                        .calls
                        .iter()
                        .any(|call| filter.contract_addresses.contains(&call.contract_address));
                    if !has_matching_contract {
                        continue;
                    }
                }

                if !filter.entrypoints.is_empty() {
                    let has_matching_entrypoint = transaction
                        .calls
                        .iter()
                        .any(|call| filter.entrypoints.contains(&call.entrypoint));
                    if !has_matching_entrypoint {
                        continue;
                    }
                }

                if !filter.model_selectors.is_empty() {
                    let has_matching_model = transaction
                        .unique_models
                        .iter()
                        .any(|model| filter.model_selectors.contains(model));
                    if !has_matching_model {
                        continue;
                    }
                }

                if let Some(from_block) = filter.from_block {
                    if transaction.block_number < from_block {
                        continue;
                    }
                }

                if let Some(to_block) = filter.to_block {
                    if transaction.block_number > to_block {
                        continue;
                    }
                }
            }

            let resp = SubscribeTransactionsResponse {
                transaction: Some(torii_proto::proto::types::Transaction {
                    transaction_hash: transaction.transaction_hash.to_bytes_be().to_vec(),
                    sender_address: transaction.sender_address.to_bytes_be().to_vec(),
                    calldata: transaction
                        .calldata
                        .iter()
                        .map(|d| d.to_bytes_be().to_vec())
                        .collect(),
                    max_fee: transaction.max_fee.to_bytes_be().to_vec(),
                    signature: transaction
                        .signature
                        .iter()
                        .map(|s| s.to_bytes_be().to_vec())
                        .collect(),
                    nonce: transaction.nonce.to_bytes_be().to_vec(),
                    block_number: transaction.block_number,
                    transaction_type: transaction.transaction_type.clone(),
                    block_timestamp: transaction.block_timestamp,
                    calls: transaction
                        .calls
                        .clone()
                        .into_iter()
                        .map(|c| c.into())
                        .collect(),
                    unique_models: transaction
                        .unique_models
                        .iter()
                        .map(|m| m.to_bytes_be().to_vec())
                        .collect(),
                }),
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

        Ok(())
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
