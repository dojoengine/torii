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
use torii_sqlite::types::Transaction;
use tracing::{error, trace};

use torii_proto::proto::world::SubscribeTransactionsResponse;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::transaction";

#[derive(Debug)]
pub struct TransactionSubscriber {
    // The list of transaction hashes to retrieve
    pub transaction_hashes: Vec<Felt>,
    // The list of caller addresses to filter by
    pub caller_addresses: Vec<Felt>,
    // The list of contract addresses to filter by (calls made to these contracts)
    pub contract_addresses: Vec<Felt>,
    // The list of entrypoints to filter by
    pub entrypoints: Vec<String>,
    // The list of model selectors to filter by
    pub model_selectors: Vec<Felt>,
    // The block number range to filter by
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
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

    pub async fn add_subscriber(
        &self,
        transaction_hashes: Vec<Felt>,
        caller_addresses: Vec<Felt>,
        contract_addresses: Vec<Felt>,
        entrypoints: Vec<String>,
        model_selectors: Vec<Felt>,
        from_block: Option<u64>,
        to_block: Option<u64>,
    ) -> Result<Receiver<Result<SubscribeTransactionsResponse, tonic::Status>>, Error> {
        let id = rand::thread_rng().gen::<usize>();
        let (sender, receiver) = channel(self.subscription_buffer_size);

        // NOTE: unlock issue with firefox/safari
        // initially send empty stream message to return from
        // initial subscribe call
        let _ = sender
            .send(Ok(SubscribeTransactionsResponse { transaction: None }))
            .await;

        self.subscribers.insert(
            id,
            TransactionSubscriber {
                transaction_hashes,
                caller_addresses,
                contract_addresses,
                entrypoints,
                model_selectors,
                from_block,
                to_block,
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

        let transaction_hash =
            Felt::from_str(&transaction.transaction_hash).map_err(ParseError::from)?;
        let sender_address =
            Felt::from_str(&transaction.sender_address).map_err(ParseError::from)?;
        let max_fee = Felt::from_str(&transaction.max_fee).map_err(ParseError::from)?;
        let nonce = Felt::from_str(&transaction.nonce).map_err(ParseError::from)?;
        let calldata = transaction
            .calldata
            .split(SQL_FELT_DELIMITER)
            .filter(|s| !s.is_empty())
            .map(Felt::from_str)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ParseError::from)?;
        let signature = transaction
            .signature
            .split(SQL_FELT_DELIMITER)
            .filter(|s| !s.is_empty())
            .map(Felt::from_str)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ParseError::from)?;

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            if !sub.transaction_hashes.is_empty()
                && !sub.transaction_hashes.contains(&transaction_hash)
            {
                continue;
            }

            if !sub.caller_addresses.is_empty() {
                for caller_address in &transaction.calls {
                    if !sub
                        .caller_addresses
                        .contains(&caller_address.caller_address)
                    {
                        continue;
                    }
                }
            }

            if !sub.contract_addresses.is_empty() {
                for contract_address in &transaction.calls {
                    if !sub
                        .contract_addresses
                        .contains(&contract_address.contract_address)
                    {
                        continue;
                    }
                }
            }

            if !sub.entrypoints.is_empty() {
                for entrypoint in &transaction.calls {
                    if !sub.entrypoints.contains(&entrypoint.entrypoint) {
                        continue;
                    }
                }
            }

            if !sub.model_selectors.is_empty() {
                for model_selector in &transaction.unique_models {
                    if !sub.model_selectors.contains(model_selector) {
                        continue;
                    }
                }
            }

            if sub.from_block.is_some() && transaction.block_number < sub.from_block.unwrap() {
                continue;
            }

            if sub.to_block.is_some() && transaction.block_number > sub.to_block.unwrap() {
                continue;
            }

            let resp: SubscribeTransactionsResponse = SubscribeTransactionsResponse {
                transaction: Some(torii_proto::Transaction {
                    transaction_hash,
                    sender_address,
                    calldata: calldata.clone(),
                    max_fee,
                    signature: signature.clone(),
                    nonce,
                    block_number: transaction.block_number,
                    block_timestamp: transaction.executed_at,
                    transaction_type: transaction.transaction_type.clone(),
                    calls: transaction.calls.clone(),
                    unique_models: transaction.unique_models.clone().into_iter().collect(),
                }.into()),
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
