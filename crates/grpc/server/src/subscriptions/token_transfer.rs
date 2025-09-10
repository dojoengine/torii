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
use torii_broker::types::TokenTransferUpdate;
use torii_broker::MemoryBroker;
use tracing::{error, trace};

use torii_proto::proto::world::SubscribeTokenTransfersResponse;
use torii_proto::TokenTransfer;

use crate::GrpcConfig;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::token_transfer";

#[derive(Debug)]
pub struct TokenTransferSubscriber {
    /// Contract addresses that the subscriber is interested in
    /// If empty, subscriber receives updates for all contracts
    pub contract_addresses: HashSet<Felt>,
    /// Account addresses that the subscriber is interested in (as sender or recipient)
    /// If empty, subscriber receives updates for all accounts
    pub account_addresses: HashSet<Felt>,
    /// Token IDs that the subscriber is interested in
    /// If empty, subscriber receives updates for all tokens
    pub token_ids: HashSet<U256>,
    /// The channel to send the response back to the subscriber.
    pub sender: Sender<Result<SubscribeTokenTransfersResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct TokenTransferManager {
    subscribers: DashMap<u64, TokenTransferSubscriber>,
    config: GrpcConfig,
}

impl TokenTransferManager {
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            subscribers: DashMap::new(),
            config,
        }
    }

    pub async fn add_subscriber(
        &self,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Receiver<Result<SubscribeTokenTransfersResponse, tonic::Status>> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(self.config.subscription_buffer_size);

        // Send initial empty response
        let _ = sender
            .send(Ok(SubscribeTokenTransfersResponse {
                subscription_id,
                transfer: None,
            }))
            .await;

        self.subscribers.insert(
            subscription_id,
            TokenTransferSubscriber {
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
    simple_broker: Pin<Box<dyn Stream<Item = TokenTransfer> + Send>>,
    token_transfer_sender: UnboundedSender<TokenTransfer>,
}

impl Service {
    pub fn new(subs_manager: Arc<TokenTransferManager>) -> Self {
        let (token_transfer_sender, token_transfer_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<TokenTransferUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<TokenTransferUpdate>::subscribe())
            },
            token_transfer_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, token_transfer_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<TokenTransferManager>,
        mut token_transfer_receiver: UnboundedReceiver<TokenTransfer>,
    ) {
        while let Some(token_transfer) = token_transfer_receiver.recv().await {
            Self::process_token_transfer_update(&subs, &token_transfer).await;
        }
    }

    async fn process_token_transfer_update(
        subs: &Arc<TokenTransferManager>,
        token_transfer: &TokenTransfer,
    ) {
        let mut closed_stream = Vec::new();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            // Skip if contract address filter doesn't match
            if !sub.contract_addresses.is_empty()
                && !sub
                    .contract_addresses
                    .contains(&token_transfer.contract_address)
            {
                continue;
            }

            // Skip if account address filter doesn't match (check both from and to)
            if !sub.account_addresses.is_empty()
                && !sub.account_addresses.contains(&token_transfer.from_address)
                && !sub.account_addresses.contains(&token_transfer.to_address)
            {
                continue;
            }

            // Skip if token ID filter doesn't match
            if !sub.token_ids.is_empty()
                && token_transfer.token_id.is_some()
                && !sub.token_ids.contains(&token_transfer.token_id.unwrap())
            {
                continue;
            }

            let resp = SubscribeTokenTransfersResponse {
                subscription_id: *idx,
                transfer: Some(token_transfer.clone().into()),
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
            trace!(target = LOG_TARGET, id = %id, "Closing token transfer stream.");
            subs.remove_subscriber(id).await
        }
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(token_transfer)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.token_transfer_sender.send(token_transfer) {
                error!(target = LOG_TARGET, error = ?e, "Sending token transfer update to processor.");
            }
        }

        Poll::Pending
    }
}
