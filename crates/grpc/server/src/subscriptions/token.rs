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
use tokio::sync::mpsc::{channel, Receiver, Sender};
use torii_broker::types::TokenUpdate;
use torii_broker::MemoryBroker;
use tracing::trace;

use torii_proto::proto::world::SubscribeTokensResponse;
use torii_proto::Token;

use crate::GrpcConfig;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::token";

#[derive(Debug)]
pub struct TokenSubscriber {
    /// Contract addresses that the subscriber is interested in
    /// If empty, subscriber receives updates for all contracts
    pub contract_addresses: HashSet<Felt>,
    /// Token IDs that the subscriber is interested in
    /// If empty, subscriber receives updates for all tokens
    pub token_ids: HashSet<U256>,
    /// The channel to send the response back to the subscriber.
    pub sender: Sender<Result<SubscribeTokensResponse, tonic::Status>>,
}

#[derive(Debug, Default)]
pub struct TokenManager {
    subscribers: DashMap<u64, TokenSubscriber>,
    config: GrpcConfig,
}

impl TokenManager {
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            subscribers: DashMap::new(),
            config,
        }
    }

    pub async fn add_subscriber(
        &self,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Receiver<Result<SubscribeTokensResponse, tonic::Status>> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(self.config.subscription_buffer_size);

        // Send initial empty response
        let _ = sender
            .send(Ok(SubscribeTokensResponse {
                subscription_id,
                token: None,
            }))
            .await;

        self.subscribers.insert(
            subscription_id,
            TokenSubscriber {
                contract_addresses: contract_addresses.into_iter().collect(),
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
        token_ids: Vec<U256>,
    ) {
        if let Some(mut subscriber) = self.subscribers.get_mut(&id) {
            subscriber.contract_addresses = contract_addresses.into_iter().collect();
            subscriber.token_ids = token_ids.into_iter().collect();
        }
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = Token> + Send>>,
    subs_manager: Arc<TokenManager>,
}

impl Service {
    pub fn new(subs_manager: Arc<TokenManager>) -> Self {
        Self {
            simple_broker: if subs_manager.config.optimistic {
                Box::pin(MemoryBroker::<TokenUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<TokenUpdate>::subscribe())
            },
            subs_manager,
        }
    }

    // Process updates synchronously - no async overhead
    fn process_token(subs: &TokenManager, token: &Token) {
        let mut closed_stream = Vec::new();

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            // Skip if contract address filter doesn't match
            if !sub.contract_addresses.is_empty()
                && !sub.contract_addresses.contains(&token.contract_address)
            {
                continue;
            }

            // Skip if token ID filter doesn't match
            if !sub.token_ids.is_empty()
                && token.token_id.is_some()
                && !sub.token_ids.contains(&token.token_id.unwrap())
            {
                continue;
            }

            let resp = SubscribeTokensResponse {
                subscription_id: *idx,
                token: Some(token.clone().into()),
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
        while let Poll::Ready(Some(token)) = this.simple_broker.poll_next_unpin(cx) {
            Self::process_token(&this.subs_manager, &token);
        }

        Poll::Pending
    }
}
