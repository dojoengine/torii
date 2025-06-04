use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use crypto_bigint::{Encoding, U256};
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use rand::Rng;
use starknet_crypto::Felt;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use torii_sqlite::error::{Error, ParseError};
use torii_sqlite::simple_broker::SimpleBroker;
use torii_sqlite::types::OptimisticToken;
use tracing::{error, trace};

use torii_proto::proto::types::Token;
use torii_proto::proto::world::SubscribeTokensResponse;

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
    subscription_buffer_size: usize,
}

impl TokenManager {
    pub fn new(subscription_buffer_size: usize) -> Self {
        Self {
            subscribers: DashMap::new(),
            subscription_buffer_size,
        }
    }

    pub async fn add_subscriber(
        &self,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<Receiver<Result<SubscribeTokensResponse, tonic::Status>>, Error> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(self.subscription_buffer_size);

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

        Ok(receiver)
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

    pub(super) async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = OptimisticToken> + Send>>,
    token_sender: UnboundedSender<OptimisticToken>,
}

impl Service {
    pub fn new(subs_manager: Arc<TokenManager>) -> Self {
        let (token_sender, token_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(SimpleBroker::<OptimisticToken>::subscribe()),
            token_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, token_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<TokenManager>,
        mut token_receiver: UnboundedReceiver<OptimisticToken>,
    ) {
        while let Some(token) = token_receiver.recv().await {
            if let Err(e) = Self::process_token_update(&subs, &token).await {
                error!(target = LOG_TARGET, error = ?e, "Processing token update.");
            }
        }
    }

    async fn process_token_update(
        subs: &Arc<TokenManager>,
        token: &OptimisticToken,
    ) -> Result<(), Error> {
        let mut closed_stream = Vec::new();
        let contract_address =
            Felt::from_str(&token.contract_address).map_err(ParseError::FromStr)?;
        let token_id = U256::from_be_hex(token.token_id.trim_start_matches("0x"));

        for sub in subs.subscribers.iter() {
            let idx = sub.key();
            let sub = sub.value();

            // Skip if contract address filter doesn't match
            if !sub.contract_addresses.is_empty()
                && !sub.contract_addresses.contains(&contract_address)
            {
                continue;
            }

            // Skip if token ID filter doesn't match
            if !sub.token_ids.is_empty() && !sub.token_ids.contains(&token_id) {
                continue;
            }

            let resp = SubscribeTokensResponse {
                subscription_id: *idx,
                token: Some(Token {
                    token_id: token_id.to_be_bytes().to_vec(),
                    contract_address: contract_address.to_bytes_be().to_vec(),
                    name: token.name.clone(),
                    symbol: token.symbol.clone(),
                    decimals: token.decimals as u32,
                    metadata: token.metadata.as_bytes().to_vec(),
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
            trace!(target = LOG_TARGET, id = %id, "Closing token stream.");
            subs.remove_subscriber(id).await
        }

        Ok(())
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(token)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.token_sender.send(token) {
                error!(target = LOG_TARGET, error = ?e, "Sending token update to processor.");
            }
        }

        Poll::Pending
    }
}
