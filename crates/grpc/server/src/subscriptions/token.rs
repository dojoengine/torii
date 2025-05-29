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
use torii_sqlite::error::Error;
use torii_sqlite::simple_broker::SimpleBroker;
use torii_sqlite::types::OptimisticToken;
use tracing::error;

use torii_proto::proto::types::Token;
use torii_proto::proto::world::SubscribeTokensResponse;

use super::{
    broadcast_to_subscribers, SubscriberSender, SubscriptionManager, SUBSCRIPTION_CHANNEL_SIZE,
};

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

impl SubscriberSender<SubscribeTokensResponse> for TokenSubscriber {
    fn sender(&self) -> &Sender<Result<SubscribeTokensResponse, tonic::Status>> {
        &self.sender
    }
}

#[derive(Debug, Default)]
pub struct TokenManager {
    subscribers: DashMap<u64, TokenSubscriber>,
}

impl SubscriptionManager<(Vec<Felt>, Vec<U256>)> for TokenManager {
    type Subscriber = TokenSubscriber;
    type Response = SubscribeTokensResponse;
    type Item = OptimisticToken;

    fn new() -> Self {
        Self {
            subscribers: DashMap::new(),
        }
    }

    fn subscribers(&self) -> &DashMap<u64, Self::Subscriber> {
        &self.subscribers
    }

    async fn add_subscriber(
        &self,
        params: (Vec<Felt>, Vec<U256>),
    ) -> Result<Receiver<Result<SubscribeTokensResponse, tonic::Status>>, Error> {
        let (contract_addresses, token_ids) = params;
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(SUBSCRIPTION_CHANNEL_SIZE);

        // Send initial empty response
        let _ = sender
            .send(Ok(Self::get_initial_response(subscription_id)))
            .await;

        self.subscribers.insert(
            subscription_id,
            Self::create_subscriber(subscription_id, sender, (contract_addresses, token_ids)),
        );

        Ok(receiver)
    }

    async fn update_subscriber(&self, id: u64, params: (Vec<Felt>, Vec<U256>)) {
        let (contract_addresses, token_ids) = params;
        if let Some(subscriber) = self.subscribers.get(&id) {
            let sender = subscriber.sender.clone();
            self.subscribers.insert(
                id,
                TokenSubscriber {
                    contract_addresses: contract_addresses.into_iter().collect(),
                    token_ids: token_ids.into_iter().collect(),
                    sender,
                },
            );
        }
    }

    async fn remove_subscriber(&self, id: u64) {
        self.subscribers.remove(&id);
    }

    fn create_subscriber(
        _id: u64,
        sender: Sender<Result<SubscribeTokensResponse, tonic::Status>>,
        params: (Vec<Felt>, Vec<U256>),
    ) -> Self::Subscriber {
        let (contract_addresses, token_ids) = params;
        TokenSubscriber {
            contract_addresses: contract_addresses.into_iter().collect(),
            token_ids: token_ids.into_iter().collect(),
            sender,
        }
    }

    fn should_send_to_subscriber(subscriber: &Self::Subscriber, token: &Self::Item) -> bool {
        let contract_address = match Felt::from_str(&token.contract_address) {
            Ok(addr) => addr,
            Err(_) => return false,
        };

        let token_id = U256::from_be_hex(token.token_id.trim_start_matches("0x"));

        // Skip if contract address filter doesn't match
        if !subscriber.contract_addresses.is_empty()
            && !subscriber.contract_addresses.contains(&contract_address)
        {
            return false;
        }

        // Skip if token ID filter doesn't match
        if !subscriber.token_ids.is_empty() && !subscriber.token_ids.contains(&token_id) {
            return false;
        }

        true
    }

    fn create_response(subscriber_id: u64, token: &Self::Item) -> Self::Response {
        let contract_address = Felt::from_str(&token.contract_address).unwrap_or_default();
        let token_id = U256::from_be_hex(token.token_id.trim_start_matches("0x"));

        SubscribeTokensResponse {
            subscription_id: subscriber_id,
            token: Some(Token {
                token_id: token_id.to_be_bytes().to_vec(),
                contract_address: contract_address.to_bytes_be().to_vec(),
                name: token.name.clone(),
                symbol: token.symbol.clone(),
                decimals: token.decimals as u32,
                metadata: token.metadata.as_bytes().to_vec(),
            }),
        }
    }

    fn get_initial_response(subscriber_id: u64) -> Self::Response {
        SubscribeTokensResponse {
            subscription_id: subscriber_id,
            token: None,
        }
    }
}

#[allow(dead_code)]
impl TokenManager {
    pub async fn add_subscriber(
        &self,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<Receiver<Result<SubscribeTokensResponse, tonic::Status>>, Error> {
        <Self as SubscriptionManager<(Vec<Felt>, Vec<U256>)>>::add_subscriber(
            self,
            (contract_addresses, token_ids),
        )
        .await
    }

    pub async fn update_subscriber(
        &self,
        id: u64,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) {
        <Self as SubscriptionManager<(Vec<Felt>, Vec<U256>)>>::update_subscriber(
            self,
            id,
            (contract_addresses, token_ids),
        )
        .await
    }

    pub(super) async fn remove_subscriber(&self, id: u64) {
        <Self as SubscriptionManager<(Vec<Felt>, Vec<U256>)>>::remove_subscriber(self, id).await
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
                error!(target = LOG_TARGET, error = %e, "Processing token update.");
            }
        }
    }

    async fn process_token_update(
        subs: &Arc<TokenManager>,
        token: &OptimisticToken,
    ) -> Result<(), Error> {
        broadcast_to_subscribers::<TokenManager, (Vec<Felt>, Vec<U256>)>(subs, token, LOG_TARGET)
            .await
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(token)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.token_sender.send(token) {
                error!(target = LOG_TARGET, error = %e, "Sending token update to processor.");
            }
        }

        Poll::Pending
    }
}
