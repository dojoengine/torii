use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crypto_bigint::U256;
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use rand::Rng;
use starknet_crypto::Felt;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use torii_broker::MemoryBroker;
use torii_broker::types::TokenBalanceUpdated;
use tracing::{error, trace};

use torii_proto::proto::world::SubscribeTokenBalancesResponse;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::balance";

#[derive(Debug)]
pub struct TokenBalanceSubscriber {
    /// Contract addresses that the subscriber is interested in
    /// If empty, subscriber receives updates for all contracts
    pub contract_addresses: HashSet<Felt>,
    /// Account addresses that the subscriber is interested in
    /// If empty, subscriber receives updates for all accounts
    pub account_addresses: HashSet<Felt>,
    /// Token IDs that the subscriber is interested in
    /// If empty, subscriber receives updates for all tokens
    pub token_ids: HashSet<U256>,
    /// The channel to send the response back to the subscriber.
    pub sender: Sender<Result<SubscribeTokenBalancesResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct TokenBalanceManager {
    subscribers: DashMap<u64, TokenBalanceSubscriber>,
    subscription_buffer_size: usize,
}

impl TokenBalanceManager {
    pub fn new(subscription_buffer_size: usize) -> Self {
        Self {
            subscribers: DashMap::new(),
            subscription_buffer_size,
        }
    }

    pub async fn add_subscriber(
        &self,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Receiver<Result<SubscribeTokenBalancesResponse, tonic::Status>> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(self.subscription_buffer_size);

        // Send initial empty response
        let _ = sender
            .send(Ok(SubscribeTokenBalancesResponse {
                subscription_id,
                balance: None,
            }))
            .await;

        self.subscribers.insert(
            subscription_id,
            TokenBalanceSubscriber {
                contract_addresses: contract_addresses.into_iter().collect(),
                account_addresses: account_addresses.into_iter().collect(),
                token_ids: token_ids.into_iter().collect(),
                sender,
            },
        );

        receiver
    }

    pub async fn update_subscriber(
        &self,
        id: u64,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) {
        if let Some(mut subscriber) = self.subscribers.get_mut(&id) {
            subscriber.contract_addresses = contract_addresses.into_iter().collect();
            subscriber.account_addresses = account_addresses.into_iter().collect();
            subscriber.token_ids = token_ids.into_iter().collect();
        }
    }

    pub(super) async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = TokenBalanceUpdated> + Send>>,
    balance_sender: UnboundedSender<TokenBalanceUpdated>,
}

impl Service {
    pub fn new(subs_manager: Arc<TokenBalanceManager>) -> Self {
        let (balance_sender, balance_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(MemoryBroker::<TokenBalanceUpdated>::subscribe()),
            balance_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, balance_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<TokenBalanceManager>,
        mut balance_receiver: UnboundedReceiver<TokenBalanceUpdated>,
    ) {
        while let Some(balance) = balance_receiver.recv().await {
            Self::process_balance_update(&subs, &balance).await;
        }
    }

    async fn process_balance_update(
        subs: &Arc<TokenBalanceManager>,
        balance: &TokenBalanceUpdated,
    ) {
        let mut closed_stream = Vec::new();
        let balance = balance.clone().into_inner();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            // Skip if contract address filter doesn't match
            if !sub.contract_addresses.is_empty()
                && !sub.contract_addresses.contains(&balance.contract_address)
            {
                continue;
            }

            // Skip if account address filter doesn't match
            if !sub.account_addresses.is_empty()
                && !sub.account_addresses.contains(&balance.account_address)
            {
                continue;
            }

            // Skip if token ID filter doesn't match
            if !sub.token_ids.is_empty() && !sub.token_ids.contains(&balance.token_id) {
                continue;
            }

            let resp = SubscribeTokenBalancesResponse {
                subscription_id: *idx,
                balance: Some(balance.clone().into()),
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
            trace!(target = LOG_TARGET, id = %id, "Closing balance stream.");
            subs.remove_subscriber(id).await
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(balance)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.balance_sender.send(balance) {
                error!(target = LOG_TARGET, error = ?e, "Sending balance update to processor.");
            }
        }

        Poll::Pending
    }
}
