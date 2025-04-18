use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use crypto_bigint::{Encoding, U256};
use futures::{Stream, StreamExt};
use rand::Rng;
use starknet_crypto::Felt;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::sync::RwLock;
use torii_sqlite::error::{Error, ParseError};
use torii_sqlite::simple_broker::SimpleBroker;
use torii_sqlite::types::OptimisticTokenBalance;
use tracing::{error, trace};

use torii_proto::proto::types::TokenBalance;
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
    subscribers: RwLock<HashMap<u64, TokenBalanceSubscriber>>,
}

impl TokenBalanceManager {
    pub async fn add_subscriber(
        &self,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<Receiver<Result<SubscribeTokenBalancesResponse, tonic::Status>>, Error> {
        let subscription_id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(1);

        // Send initial empty response
        let _ = sender
            .send(Ok(SubscribeTokenBalancesResponse {
                subscription_id,
                balance: None,
            }))
            .await;

        self.subscribers.write().await.insert(
            subscription_id,
            TokenBalanceSubscriber {
                contract_addresses: contract_addresses.into_iter().collect(),
                account_addresses: account_addresses.into_iter().collect(),
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
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) {
        let sender = {
            let subscribers = self.subscribers.read().await;
            if let Some(subscriber) = subscribers.get(&id) {
                subscriber.sender.clone()
            } else {
                return; // Subscriber not found, exit early
            }
        };

        self.subscribers.write().await.insert(
            id,
            TokenBalanceSubscriber {
                contract_addresses: contract_addresses.into_iter().collect(),
                account_addresses: account_addresses.into_iter().collect(),
                token_ids: token_ids.into_iter().collect(),
                sender,
            },
        );
    }

    pub(super) async fn remove_subscriber(&self, id: u64) {
        self.subscribers.write().await.remove(&id);
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
        let mut closed_stream = Vec::new();
        let contract_address =
            Felt::from_str(&balance.contract_address).map_err(ParseError::FromStr)?;
        let account_address =
            Felt::from_str(&balance.account_address).map_err(ParseError::FromStr)?;
        let id: Vec<&str> = balance.token_id.split(':').collect();
        let token_id: U256 = if id.len() == 2 {
            U256::from_be_hex(id[1].trim_start_matches("0x"))
        } else {
            U256::ZERO
        };
        let balance = U256::from_be_hex(balance.balance.trim_start_matches("0x"));

        for (idx, sub) in subs.subscribers.read().await.iter() {
            // Skip if contract address filter doesn't match
            if !sub.contract_addresses.is_empty()
                && !sub.contract_addresses.contains(&contract_address)
            {
                continue;
            }

            // Skip if account address filter doesn't match
            if !sub.account_addresses.is_empty()
                && !sub.account_addresses.contains(&account_address)
            {
                continue;
            }

            // Skip if token ID filter doesn't match
            if !sub.token_ids.is_empty() && !sub.token_ids.contains(&token_id) {
                continue;
            }

            let resp = SubscribeTokenBalancesResponse {
                subscription_id: *idx,
                balance: Some(TokenBalance {
                    balance: balance.to_be_bytes().to_vec(),
                    account_address: account_address.to_bytes_be().to_vec(),
                    contract_address: contract_address.to_bytes_be().to_vec(),
                    token_id: token_id.to_be_bytes().to_vec(),
                }),
            };

            if sub.sender.send(Ok(resp)).await.is_err() {
                closed_stream.push(*idx);
            }
        }

        for id in closed_stream {
            trace!(target = LOG_TARGET, id = %id, "Closing balance stream.");
            subs.remove_subscriber(id).await
        }

        Ok(())
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
