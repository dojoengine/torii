use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures::{Stream, StreamExt};
use rand::Rng;
use sqlx::{Pool, Sqlite};
use starknet::core::types::Felt;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use torii_sqlite::error::Error;
use torii_sqlite::simple_broker::SimpleBroker;
use torii_sqlite::types::ContractCursor as ContractUpdated;
use tracing::error;

use torii_proto::proto::world::SubscribeIndexerResponse;

use super::{
    broadcast_to_subscribers, SubscriberSender, SubscriptionManager, SUBSCRIPTION_CHANNEL_SIZE,
};

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::indexer";

#[derive(Debug)]
pub struct IndexerSubscriber {
    /// Contract address that the subscriber is interested in
    contract_address: Felt,
    /// The channel to send the response back to the subscriber.
    sender: Sender<Result<SubscribeIndexerResponse, tonic::Status>>,
}

impl SubscriberSender<SubscribeIndexerResponse> for IndexerSubscriber {
    fn sender(&self) -> &Sender<Result<SubscribeIndexerResponse, tonic::Status>> {
        &self.sender
    }
}

#[derive(Debug, Default)]
pub struct IndexerManager {
    subscribers: DashMap<u64, IndexerSubscriber>,
}

impl SubscriptionManager<Felt> for IndexerManager {
    type Subscriber = IndexerSubscriber;
    type Response = SubscribeIndexerResponse;
    type Item = ContractUpdated;

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
        contract_address: Felt,
    ) -> Result<Receiver<Result<SubscribeIndexerResponse, tonic::Status>>, Error> {
        let id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(SUBSCRIPTION_CHANNEL_SIZE);

        self.subscribers
            .insert(id, Self::create_subscriber(id, sender, contract_address));

        Ok(receiver)
    }

    async fn update_subscriber(&self, id: u64, contract_address: Felt) {
        if let Some(subscriber) = self.subscribers.get(&id) {
            let sender = subscriber.sender.clone();
            self.subscribers.insert(
                id,
                IndexerSubscriber {
                    contract_address,
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
        sender: Sender<Result<SubscribeIndexerResponse, tonic::Status>>,
        contract_address: Felt,
    ) -> Self::Subscriber {
        IndexerSubscriber {
            contract_address,
            sender,
        }
    }

    fn should_send_to_subscriber(subscriber: &Self::Subscriber, update: &Self::Item) -> bool {
        let contract_address = match Felt::from_str(&update.contract_address) {
            Ok(addr) => addr,
            Err(_) => return false,
        };

        subscriber.contract_address == Felt::ZERO || subscriber.contract_address == contract_address
    }

    fn create_response(_subscriber_id: u64, update: &Self::Item) -> Self::Response {
        let contract_address = Felt::from_str(&update.contract_address).unwrap_or_default();

        SubscribeIndexerResponse {
            head: update.head,
            tps: update.tps,
            last_block_timestamp: update.last_block_timestamp,
            contract_address: contract_address.to_bytes_be().to_vec(),
        }
    }

    fn get_initial_response(_subscriber_id: u64) -> Self::Response {
        SubscribeIndexerResponse {
            head: 0,
            tps: 0,
            last_block_timestamp: 0,
            contract_address: vec![],
        }
    }
}

#[allow(dead_code)]
impl IndexerManager {
    pub async fn add_subscriber(
        &self,
        pool: &Pool<Sqlite>,
        contract_address: Felt,
    ) -> Result<Receiver<Result<SubscribeIndexerResponse, tonic::Status>>, Error> {
        let id = rand::thread_rng().gen::<u64>();
        let (sender, receiver) = channel(SUBSCRIPTION_CHANNEL_SIZE);

        let mut statement = "SELECT * FROM contracts".to_string();

        let contracts: Vec<ContractUpdated> = if contract_address != Felt::ZERO {
            statement += " WHERE id = ?";

            sqlx::query_as(&statement)
                .bind(format!("{:#x}", contract_address))
                .fetch_all(pool)
                .await?
        } else {
            sqlx::query_as(&statement).fetch_all(pool).await?
        };

        for contract in contracts {
            let _ = sender
                .send(Ok(SubscribeIndexerResponse {
                    head: contract.head,
                    tps: contract.tps,
                    last_block_timestamp: contract.last_block_timestamp,
                    contract_address: contract_address.to_bytes_be().to_vec(),
                }))
                .await;
        }

        self.subscribers.insert(
            id,
            IndexerSubscriber {
                contract_address,
                sender,
            },
        );

        Ok(receiver)
    }

    pub(super) async fn remove_subscriber(&self, id: u64) {
        <Self as SubscriptionManager<Felt>>::remove_subscriber(self, id).await
    }
}

#[must_use = "Service does nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct Service {
    simple_broker: Pin<Box<dyn Stream<Item = ContractUpdated> + Send>>,
    update_sender: UnboundedSender<ContractUpdated>,
}

impl Service {
    pub fn new(subs_manager: Arc<IndexerManager>) -> Self {
        let (update_sender, update_receiver) = unbounded_channel();
        let service = Self {
            simple_broker: Box::pin(SimpleBroker::<ContractUpdated>::subscribe()),
            update_sender,
        };

        tokio::spawn(Self::publish_updates(subs_manager, update_receiver));

        service
    }

    async fn publish_updates(
        subs: Arc<IndexerManager>,
        mut update_receiver: UnboundedReceiver<ContractUpdated>,
    ) {
        while let Some(update) = update_receiver.recv().await {
            if let Err(e) = Self::process_update(&subs, &update).await {
                error!(target = LOG_TARGET, error = %e, "Processing indexer update.");
            }
        }
    }

    async fn process_update(
        subs: &Arc<IndexerManager>,
        update: &ContractUpdated,
    ) -> Result<(), Error> {
        broadcast_to_subscribers::<IndexerManager, Felt>(subs, update, LOG_TARGET).await
    }
}

impl Future for Service {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Poll::Ready(Some(update)) = this.simple_broker.poll_next_unpin(cx) {
            if let Err(e) = this.update_sender.send(update) {
                error!(target = LOG_TARGET, error = %e, "Sending indexer update to processor.");
            }
        }

        Poll::Pending
    }
}
