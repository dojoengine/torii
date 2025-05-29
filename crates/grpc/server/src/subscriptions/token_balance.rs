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
use torii_sqlite::types::OptimisticTokenBalance;
use tracing::error;

use torii_proto::proto::types::TokenBalance;
use torii_proto::proto::world::SubscribeTokenBalancesResponse;

use super::{
    broadcast_to_subscribers, SubscriberSender, SubscriptionManager, SUBSCRIPTION_CHANNEL_SIZE,
};

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

impl SubscriberSender<SubscribeTokenBalancesResponse> for TokenBalanceSubscriber {
    fn sender(&self) -> &Sender<Result<SubscribeTokenBalancesResponse, tonic::Status>> {
        &self.sender
    }
}

#[derive(Debug, Default)]
pub struct TokenBalanceManager {
    subscribers: DashMap<u64, TokenBalanceSubscriber>,
}

impl SubscriptionManager<(Vec<Felt>, Vec<Felt>, Vec<U256>)> for TokenBalanceManager {
    type Subscriber = TokenBalanceSubscriber;
    type Response = SubscribeTokenBalancesResponse;
    type Item = OptimisticTokenBalance;

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
        params: (Vec<Felt>, Vec<Felt>, Vec<U256>),
    ) -> Result<Receiver<Result<SubscribeTokenBalancesResponse, tonic::Status>>, Error> {
        let (contract_addresses, account_addresses, token_ids) = params;
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(SUBSCRIPTION_CHANNEL_SIZE);

        // Send initial empty response
        let _ = sender
            .send(Ok(Self::get_initial_response(subscription_id)))
            .await;

        self.subscribers.insert(
            subscription_id,
            Self::create_subscriber(
                subscription_id,
                sender,
                (contract_addresses, account_addresses, token_ids),
            ),
        );

        Ok(receiver)
    }

    async fn update_subscriber(&self, id: u64, params: (Vec<Felt>, Vec<Felt>, Vec<U256>)) {
        let (contract_addresses, account_addresses, token_ids) = params;
        if let Some(subscriber) = self.subscribers.get(&id) {
            let sender = subscriber.sender.clone();
            self.subscribers.insert(
                id,
                TokenBalanceSubscriber {
                    contract_addresses: contract_addresses.into_iter().collect(),
                    account_addresses: account_addresses.into_iter().collect(),
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
        sender: Sender<Result<SubscribeTokenBalancesResponse, tonic::Status>>,
        params: (Vec<Felt>, Vec<Felt>, Vec<U256>),
    ) -> Self::Subscriber {
        let (contract_addresses, account_addresses, token_ids) = params;
        TokenBalanceSubscriber {
            contract_addresses: contract_addresses.into_iter().collect(),
            account_addresses: account_addresses.into_iter().collect(),
            token_ids: token_ids.into_iter().collect(),
            sender,
        }
    }

    fn should_send_to_subscriber(subscriber: &Self::Subscriber, balance: &Self::Item) -> bool {
        let contract_address = match Felt::from_str(&balance.contract_address) {
            Ok(addr) => addr,
            Err(_) => return false,
        };

        let account_address = match Felt::from_str(&balance.account_address) {
            Ok(addr) => addr,
            Err(_) => return false,
        };

        let id: Vec<&str> = balance.token_id.split(':').collect();
        let token_id: U256 = if id.len() == 2 {
            U256::from_be_hex(id[1].trim_start_matches("0x"))
        } else {
            U256::ZERO
        };

        // Skip if contract address filter doesn't match
        if !subscriber.contract_addresses.is_empty()
            && !subscriber.contract_addresses.contains(&contract_address)
        {
            return false;
        }

        // Skip if account address filter doesn't match
        if !subscriber.account_addresses.is_empty()
            && !subscriber.account_addresses.contains(&account_address)
        {
            return false;
        }

        // Skip if token ID filter doesn't match
        if !subscriber.token_ids.is_empty() && !subscriber.token_ids.contains(&token_id) {
            return false;
        }

        true
    }

    fn create_response(subscriber_id: u64, balance: &Self::Item) -> Self::Response {
        let contract_address = Felt::from_str(&balance.contract_address).unwrap_or_default();
        let account_address = Felt::from_str(&balance.account_address).unwrap_or_default();
        let id: Vec<&str> = balance.token_id.split(':').collect();
        let token_id: U256 = if id.len() == 2 {
            U256::from_be_hex(id[1].trim_start_matches("0x"))
        } else {
            U256::ZERO
        };
        let balance_value = U256::from_be_hex(balance.balance.trim_start_matches("0x"));

        SubscribeTokenBalancesResponse {
            subscription_id: subscriber_id,
            balance: Some(TokenBalance {
                balance: balance_value.to_be_bytes().to_vec(),
                account_address: account_address.to_bytes_be().to_vec(),
                contract_address: contract_address.to_bytes_be().to_vec(),
                token_id: token_id.to_be_bytes().to_vec(),
            }),
        }
    }

    fn get_initial_response(subscriber_id: u64) -> Self::Response {
        SubscribeTokenBalancesResponse {
            subscription_id: subscriber_id,
            balance: None,
        }
    }
}

#[allow(dead_code)]
impl TokenBalanceManager {
    pub async fn add_subscriber(
        &self,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<Receiver<Result<SubscribeTokenBalancesResponse, tonic::Status>>, Error> {
        <Self as SubscriptionManager<(Vec<Felt>, Vec<Felt>, Vec<U256>)>>::add_subscriber(
            self,
            (contract_addresses, account_addresses, token_ids),
        )
        .await
    }

    pub async fn update_subscriber(
        &self,
        id: u64,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) {
        <Self as SubscriptionManager<(Vec<Felt>, Vec<Felt>, Vec<U256>)>>::update_subscriber(
            self,
            id,
            (contract_addresses, account_addresses, token_ids),
        )
        .await
    }

    pub(super) async fn remove_subscriber(&self, id: u64) {
        <Self as SubscriptionManager<(Vec<Felt>, Vec<Felt>, Vec<U256>)>>::remove_subscriber(
            self, id,
        )
        .await
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = OptimisticTokenBalance> + Send>>,
    balance_sender: UnboundedSender<OptimisticTokenBalance>,
}

impl Service {
    pub fn new(subs_manager: Arc<TokenBalanceManager>) -> Self {
        let (balance_sender, balance_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(SimpleBroker::<OptimisticTokenBalance>::subscribe()),
            balance_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, balance_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<TokenBalanceManager>,
        mut balance_receiver: UnboundedReceiver<OptimisticTokenBalance>,
    ) {
        while let Some(balance) = balance_receiver.recv().await {
            if let Err(e) = Self::process_balance_update(&subs, &balance).await {
                error!(target = LOG_TARGET, error = %e, "Processing balance update.");
            }
        }
    }

    async fn process_balance_update(
        subs: &Arc<TokenBalanceManager>,
        balance: &OptimisticTokenBalance,
    ) -> Result<(), Error> {
        broadcast_to_subscribers::<TokenBalanceManager, (Vec<Felt>, Vec<Felt>, Vec<U256>)>(
            subs, balance, LOG_TARGET,
        )
        .await
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(balance)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.balance_sender.send(balance) {
                error!(target = LOG_TARGET, error = %e, "Sending balance update to processor.");
            }
        }

        Poll::Pending
    }
}
