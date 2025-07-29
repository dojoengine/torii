pub mod subscriptions;

#[cfg(test)]
mod tests;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use crypto_bigint::U256;
use dojo_world::contracts::naming::compute_selector_from_names;
use futures::Stream;
use http::HeaderName;
use proto::world::{
    RetrieveEntitiesRequest, RetrieveEntitiesResponse, RetrieveEventsRequest,
    RetrieveEventsResponse, UpdateEntitiesSubscriptionRequest,
};
use starknet::core::types::Felt;
use starknet::providers::Provider;
use subscriptions::event::EventManager;
use subscriptions::indexer::IndexerManager;
use subscriptions::token::TokenManager;
use subscriptions::token_balance::TokenBalanceManager;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tonic_web::GrpcWebLayer;
use torii_messaging::Messaging;
use torii_proto::error::ProtoError;
use torii_storage::Storage;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::subscriptions::transaction::TransactionManager;

use self::subscriptions::entity::EntityManager;
use self::subscriptions::event_message::EventMessageManager;
use torii_proto::proto::world::world_server::WorldServer;
use torii_proto::proto::world::{
    PublishMessageBatchRequest, PublishMessageBatchResponse, PublishMessageRequest,
    PublishMessageResponse, RetrieveControllersRequest, RetrieveControllersResponse,
    RetrieveEventMessagesRequest, RetrieveTokenBalancesRequest, RetrieveTokenBalancesResponse,
    RetrieveTokenCollectionsRequest, RetrieveTokenCollectionsResponse, RetrieveTokensRequest,
    RetrieveTokensResponse, RetrieveTransactionsRequest, RetrieveTransactionsResponse,
    SubscribeEntitiesRequest, SubscribeEntityResponse, SubscribeEventMessagesRequest,
    SubscribeEventsResponse, SubscribeIndexerRequest, SubscribeIndexerResponse,
    SubscribeTokenBalancesRequest, SubscribeTokenBalancesResponse, SubscribeTokensRequest,
    SubscribeTokensResponse, SubscribeTransactionsRequest, SubscribeTransactionsResponse,
    UpdateEventMessagesSubscriptionRequest, UpdateTokenBalancesSubscriptionRequest,
    UpdateTokenSubscriptionRequest, WorldMetadataRequest, WorldMetadataResponse,
};
use torii_proto::proto::{self};
use torii_proto::Message;

use anyhow::{anyhow, Error};

// Shared subscription runtime for all DojoWorld instances
// This provides performance isolation from user-facing API requests
// Subscriptions involve heavy polling and should not starve API response threads
static SUBSCRIPTION_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let worker_threads = (num_cpus::get() / 2).clamp(2, 8);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .thread_name("torii-grpc-subscriptions")
        .enable_all()
        .build()
        .expect("Failed to create subscriptions runtime")
});

#[derive(Debug)]
pub struct DojoWorld<P: Provider + Sync> {
    storage: Arc<dyn Storage>,
    provider: P,
    messaging: Arc<Messaging>,
    world_address: Felt,
    cross_messaging_tx: Option<UnboundedSender<Message>>,
    entity_manager: Arc<EntityManager>,
    event_message_manager: Arc<EventMessageManager>,
    event_manager: Arc<EventManager>,
    indexer_manager: Arc<IndexerManager>,
    token_balance_manager: Arc<TokenBalanceManager>,
    token_manager: Arc<TokenManager>,
    transaction_manager: Arc<TransactionManager>,
    _config: GrpcConfig,
}

impl<P: Provider + Sync> DojoWorld<P> {
    pub fn new(
        storage: Arc<dyn Storage>,
        provider: P,
        messaging: Arc<Messaging>,
        world_address: Felt,
        cross_messaging_tx: Option<UnboundedSender<Message>>,
        config: GrpcConfig,
    ) -> Self {
        let entity_manager = Arc::new(EntityManager::new(config.clone()));
        let event_message_manager = Arc::new(EventMessageManager::new(config.clone()));
        let event_manager = Arc::new(EventManager::new(config.clone()));
        let indexer_manager = Arc::new(IndexerManager::new(config.clone()));
        let token_balance_manager = Arc::new(TokenBalanceManager::new(config.clone()));
        let token_manager = Arc::new(TokenManager::new(config.clone()));
        let transaction_manager = Arc::new(TransactionManager::new(config.clone()));

        // Spawn subscription services on the dedicated subscription runtime
        // These services do heavy polling and should be isolated from API request handling
        SUBSCRIPTION_RUNTIME.spawn(subscriptions::entity::Service::new(Arc::clone(
            &entity_manager,
        )));

        SUBSCRIPTION_RUNTIME.spawn(subscriptions::event_message::Service::new(Arc::clone(
            &event_message_manager,
        )));

        SUBSCRIPTION_RUNTIME.spawn(subscriptions::event::Service::new(Arc::clone(
            &event_manager,
        )));

        SUBSCRIPTION_RUNTIME.spawn(subscriptions::indexer::Service::new(Arc::clone(
            &indexer_manager,
        )));

        SUBSCRIPTION_RUNTIME.spawn(subscriptions::token_balance::Service::new(Arc::clone(
            &token_balance_manager,
        )));

        SUBSCRIPTION_RUNTIME.spawn(subscriptions::token::Service::new(Arc::clone(
            &token_manager,
        )));

        SUBSCRIPTION_RUNTIME.spawn(subscriptions::transaction::Service::new(Arc::clone(
            &transaction_manager,
        )));

        Self {
            storage,
            provider,
            messaging,
            world_address,
            cross_messaging_tx,
            entity_manager,
            event_message_manager,
            event_manager,
            indexer_manager,
            token_balance_manager,
            token_manager,
            transaction_manager,
            _config: config,
        }
    }
}

impl<P: Provider + Sync> DojoWorld<P> {
    pub async fn world(&self) -> Result<proto::types::World, Error> {
        let models = self
            .storage
            .models(&[])
            .await
            .map_err(|e| anyhow!("Failed to get models from cache: {}", e))?;

        let mut models_metadata = Vec::with_capacity(models.len());
        for model in models {
            models_metadata.push(proto::types::Model {
                selector: model.selector.to_bytes_be().to_vec(),
                namespace: model.namespace,
                name: model.name,
                class_hash: model.class_hash.to_bytes_be().to_vec(),
                contract_address: model.contract_address.to_bytes_be().to_vec(),
                packed_size: model.packed_size,
                unpacked_size: model.unpacked_size,
                layout: serde_json::to_vec(&model.layout).unwrap(),
                schema: serde_json::to_vec(&model.schema).unwrap(),
            });
        }

        Ok(proto::types::World {
            world_address: format!("{:#x}", self.world_address),
            models: models_metadata,
        })
    }

    pub async fn model_metadata(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<proto::types::Model, Error> {
        // selector
        let model = compute_selector_from_names(namespace, name);

        let model = self
            .storage
            .model(model)
            .await
            .map_err(|e| anyhow!("Failed to get model from cache: {}", e))?;

        Ok(proto::types::Model {
            selector: model.selector.to_bytes_be().to_vec(),
            namespace: namespace.to_string(),
            name: name.to_string(),
            class_hash: model.class_hash.to_bytes_be().to_vec(),
            contract_address: model.contract_address.to_bytes_be().to_vec(),
            packed_size: model.packed_size,
            unpacked_size: model.unpacked_size,
            layout: serde_json::to_vec(&model.layout).unwrap(),
            schema: serde_json::to_vec(&model.schema).unwrap(),
        })
    }
}

type ServiceResult<T> = Result<Response<T>, Status>;
type SubscribeEntitiesResponseStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeEntityResponse, Status>> + Send>>;
type SubscribeEventsResponseStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeEventsResponse, Status>> + Send>>;
type SubscribeIndexerResponseStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeIndexerResponse, Status>> + Send>>;
type SubscribeTokenBalancesResponseStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeTokenBalancesResponse, Status>> + Send>>;
type SubscribeTokensResponseStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeTokensResponse, Status>> + Send>>;
type SubscribeTransactionsResponseStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeTransactionsResponse, Status>> + Send>>;

#[tonic::async_trait]
impl<P: Provider + Sync + Send + 'static> proto::world::world_server::World for DojoWorld<P> {
    type SubscribeEntitiesStream = SubscribeEntitiesResponseStream;
    type SubscribeEventMessagesStream = SubscribeEntitiesResponseStream;
    type SubscribeEventsStream = SubscribeEventsResponseStream;
    type SubscribeIndexerStream = SubscribeIndexerResponseStream;
    type SubscribeTokenBalancesStream = SubscribeTokenBalancesResponseStream;
    type SubscribeTokensStream = SubscribeTokensResponseStream;
    type SubscribeTransactionsStream = SubscribeTransactionsResponseStream;

    async fn world_metadata(
        &self,
        _request: Request<WorldMetadataRequest>,
    ) -> Result<Response<WorldMetadataResponse>, Status> {
        let metadata = self
            .world()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(WorldMetadataResponse {
            world: Some(metadata),
        }))
    }

    async fn retrieve_transactions(
        &self,
        request: Request<RetrieveTransactionsRequest>,
    ) -> Result<Response<RetrieveTransactionsResponse>, Status> {
        let RetrieveTransactionsRequest { query } = request.into_inner();
        let query = query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?
            .try_into()
            .map_err(|e: ProtoError| Status::invalid_argument(e.to_string()))?;

        let transactions = self
            .storage
            .transactions(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RetrieveTransactionsResponse {
            transactions: transactions.items.into_iter().map(Into::into).collect(),
            next_cursor: transactions.next_cursor.unwrap_or_default(),
        }))
    }

    async fn subscribe_transactions(
        &self,
        request: Request<SubscribeTransactionsRequest>,
    ) -> ServiceResult<Self::SubscribeTransactionsStream> {
        let SubscribeTransactionsRequest { filter } = request.into_inner();

        let filter = filter
            .map(|f| f.try_into())
            .transpose()
            .map_err(|e: ProtoError| Status::internal(e.to_string()))?;

        let rx = self.transaction_manager.add_subscriber(filter).await;
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::SubscribeTransactionsStream
        ))
    }

    async fn retrieve_entities(
        &self,
        request: Request<RetrieveEntitiesRequest>,
    ) -> Result<Response<RetrieveEntitiesResponse>, Status> {
        let RetrieveEntitiesRequest { query } = request.into_inner();
        let query = query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?
            .try_into()
            .map_err(|e: ProtoError| Status::invalid_argument(e.to_string()))?;

        let entities = self
            .storage
            .entities(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RetrieveEntitiesResponse {
            entities: entities.items.into_iter().map(Into::into).collect(),
            next_cursor: entities.next_cursor.unwrap_or_default(),
        }))
    }

    async fn retrieve_event_messages(
        &self,
        request: Request<RetrieveEventMessagesRequest>,
    ) -> Result<Response<RetrieveEntitiesResponse>, Status> {
        let RetrieveEventMessagesRequest { query } = request.into_inner();
        let query = query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?
            .try_into()
            .map_err(|e: ProtoError| Status::invalid_argument(e.to_string()))?;

        let entities = self
            .storage
            .event_messages(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RetrieveEntitiesResponse {
            entities: entities.items.into_iter().map(Into::into).collect(),
            next_cursor: entities.next_cursor.unwrap_or_default(),
        }))
    }

    async fn retrieve_events(
        &self,
        request: Request<RetrieveEventsRequest>,
    ) -> Result<Response<RetrieveEventsResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;

        let events = self
            .storage
            .events(query.into())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RetrieveEventsResponse {
            events: events.items.into_iter().map(Into::into).collect(),
            next_cursor: events.next_cursor.unwrap_or_default(),
        }))
    }

    async fn retrieve_controllers(
        &self,
        request: Request<RetrieveControllersRequest>,
    ) -> Result<Response<RetrieveControllersResponse>, Status> {
        let RetrieveControllersRequest { query } = request.into_inner();
        let query = query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?
            .try_into()
            .map_err(|e: ProtoError| Status::invalid_argument(e.to_string()))?;

        let controllers = self
            .storage
            .controllers(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RetrieveControllersResponse {
            next_cursor: controllers.next_cursor.unwrap_or_default(),
            controllers: controllers.items.into_iter().map(|c| c.into()).collect(),
        }))
    }

    async fn retrieve_tokens(
        &self,
        request: Request<RetrieveTokensRequest>,
    ) -> Result<Response<RetrieveTokensResponse>, Status> {
        let RetrieveTokensRequest { query } = request.into_inner();
        let query = query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?
            .try_into()
            .map_err(|e: ProtoError| Status::invalid_argument(e.to_string()))?;

        let tokens = self
            .storage
            .tokens(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RetrieveTokensResponse {
            tokens: tokens.items.into_iter().map(Into::into).collect(),
            next_cursor: tokens.next_cursor.unwrap_or_default(),
        }))
    }

    async fn retrieve_token_collections(
        &self,
        request: Request<RetrieveTokenCollectionsRequest>,
    ) -> Result<Response<RetrieveTokenCollectionsResponse>, Status> {
        let RetrieveTokenCollectionsRequest { query } = request.into_inner();
        let query = query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?
            .try_into()
            .map_err(|e: ProtoError| Status::invalid_argument(e.to_string()))?;

        let token_collections = self
            .storage
            .token_collections(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RetrieveTokenCollectionsResponse {
            tokens: token_collections
                .items
                .into_iter()
                .map(Into::into)
                .collect(),
            next_cursor: token_collections.next_cursor.unwrap_or_default(),
        }))
    }

    async fn subscribe_tokens(
        &self,
        request: Request<SubscribeTokensRequest>,
    ) -> ServiceResult<Self::SubscribeTokensStream> {
        let SubscribeTokensRequest {
            contract_addresses,
            token_ids,
        } = request.into_inner();
        let contract_addresses = contract_addresses
            .iter()
            .map(|address| Felt::from_bytes_be_slice(address))
            .collect::<Vec<_>>();
        let token_ids = token_ids
            .iter()
            .map(|id| U256::from_be_slice(id))
            .collect::<Vec<_>>();

        let rx = self
            .token_manager
            .add_subscriber(contract_addresses, token_ids)
            .await;
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::SubscribeTokensStream
        ))
    }

    async fn update_tokens_subscription(
        &self,
        request: Request<UpdateTokenSubscriptionRequest>,
    ) -> ServiceResult<()> {
        let UpdateTokenSubscriptionRequest {
            subscription_id,
            contract_addresses,
            token_ids,
        } = request.into_inner();
        let contract_addresses = contract_addresses
            .iter()
            .map(|address| Felt::from_bytes_be_slice(address))
            .collect::<Vec<_>>();
        let token_ids = token_ids
            .iter()
            .map(|id| U256::from_be_slice(id))
            .collect::<Vec<_>>();

        self.token_manager
            .update_subscriber(subscription_id, contract_addresses, token_ids)
            .await;
        Ok(Response::new(()))
    }

    async fn retrieve_token_balances(
        &self,
        request: Request<RetrieveTokenBalancesRequest>,
    ) -> Result<Response<RetrieveTokenBalancesResponse>, Status> {
        let RetrieveTokenBalancesRequest { query } = request.into_inner();
        let query = query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?
            .try_into()
            .map_err(|e: ProtoError| Status::invalid_argument(e.to_string()))?;

        let balances = self
            .storage
            .token_balances(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RetrieveTokenBalancesResponse {
            balances: balances.items.into_iter().map(Into::into).collect(),
            next_cursor: balances.next_cursor.unwrap_or_default(),
        }))
    }

    async fn subscribe_indexer(
        &self,
        request: Request<SubscribeIndexerRequest>,
    ) -> ServiceResult<Self::SubscribeIndexerStream> {
        let SubscribeIndexerRequest { contract_address } = request.into_inner();
        let rx = self
            .indexer_manager
            .add_subscriber(
                self.storage.clone(),
                Felt::from_bytes_be_slice(&contract_address),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::SubscribeIndexerStream
        ))
    }

    async fn subscribe_entities(
        &self,
        request: Request<SubscribeEntitiesRequest>,
    ) -> ServiceResult<Self::SubscribeEntitiesStream> {
        let SubscribeEntitiesRequest { clause } = request.into_inner();
        let clause = clause
            .map(|c| c.try_into())
            .transpose()
            .map_err(|e: ProtoError| Status::internal(e.to_string()))?;

        let rx = self.entity_manager.add_subscriber(clause).await;

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::SubscribeEntitiesStream
        ))
    }

    async fn update_entities_subscription(
        &self,
        request: Request<UpdateEntitiesSubscriptionRequest>,
    ) -> ServiceResult<()> {
        let UpdateEntitiesSubscriptionRequest {
            subscription_id,
            clause,
        } = request.into_inner();
        let clause = clause
            .map(|c| c.try_into())
            .transpose()
            .map_err(|e: ProtoError| Status::internal(e.to_string()))?;
        self.entity_manager
            .update_subscriber(subscription_id, clause)
            .await;

        Ok(Response::new(()))
    }

    async fn subscribe_token_balances(
        &self,
        request: Request<SubscribeTokenBalancesRequest>,
    ) -> ServiceResult<Self::SubscribeTokenBalancesStream> {
        let SubscribeTokenBalancesRequest {
            contract_addresses,
            account_addresses,
            token_ids,
        } = request.into_inner();
        let contract_addresses = contract_addresses
            .iter()
            .map(|address| Felt::from_bytes_be_slice(address))
            .collect::<Vec<_>>();
        let account_addresses = account_addresses
            .iter()
            .map(|address| Felt::from_bytes_be_slice(address))
            .collect::<Vec<_>>();
        let token_ids = token_ids
            .iter()
            .map(|id| U256::from_be_slice(id))
            .collect::<Vec<_>>();

        let rx = self
            .token_balance_manager
            .add_subscriber(contract_addresses, account_addresses, token_ids)
            .await;

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::SubscribeTokenBalancesStream
        ))
    }

    async fn update_token_balances_subscription(
        &self,
        request: Request<UpdateTokenBalancesSubscriptionRequest>,
    ) -> ServiceResult<()> {
        let UpdateTokenBalancesSubscriptionRequest {
            subscription_id,
            contract_addresses,
            account_addresses,
            token_ids,
        } = request.into_inner();
        let contract_addresses = contract_addresses
            .iter()
            .map(|address| Felt::from_bytes_be_slice(address))
            .collect::<Vec<_>>();
        let account_addresses = account_addresses
            .iter()
            .map(|address| Felt::from_bytes_be_slice(address))
            .collect::<Vec<_>>();
        let token_ids = token_ids
            .iter()
            .map(|id| U256::from_be_slice(id))
            .collect::<Vec<_>>();

        self.token_balance_manager
            .update_subscriber(
                subscription_id,
                contract_addresses,
                account_addresses,
                token_ids,
            )
            .await;
        Ok(Response::new(()))
    }

    async fn subscribe_event_messages(
        &self,
        request: Request<SubscribeEventMessagesRequest>,
    ) -> ServiceResult<Self::SubscribeEntitiesStream> {
        let SubscribeEventMessagesRequest { clause } = request.into_inner();
        let clause = clause
            .map(|c| c.try_into())
            .transpose()
            .map_err(|e: ProtoError| Status::internal(e.to_string()))?;
        let rx = self.event_message_manager.add_subscriber(clause).await;

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::SubscribeEntitiesStream
        ))
    }

    async fn update_event_messages_subscription(
        &self,
        request: Request<UpdateEventMessagesSubscriptionRequest>,
    ) -> ServiceResult<()> {
        let UpdateEventMessagesSubscriptionRequest {
            subscription_id,
            clause,
        } = request.into_inner();
        let clause = clause
            .map(|c| c.try_into())
            .transpose()
            .map_err(|e: ProtoError| Status::internal(e.to_string()))?;
        self.event_message_manager
            .update_subscriber(subscription_id, clause)
            .await;

        Ok(Response::new(()))
    }

    async fn subscribe_events(
        &self,
        request: Request<proto::world::SubscribeEventsRequest>,
    ) -> ServiceResult<Self::SubscribeEventsStream> {
        let keys = request.into_inner().keys;
        let rx = self
            .event_manager
            .add_subscriber(keys.into_iter().map(|keys| keys.into()).collect())
            .await;

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::SubscribeEventsStream
        ))
    }

    async fn publish_message(
        &self,
        request: Request<PublishMessageRequest>,
    ) -> Result<Response<PublishMessageResponse>, Status> {
        let PublishMessageRequest { signature, message } = request.into_inner();

        let signature = signature
            .iter()
            .map(|s| Felt::from_bytes_be_slice(s))
            .collect::<Vec<_>>();
        let typed_data = serde_json::from_str(&message)
            .map_err(|_| Status::invalid_argument("Invalid message"))?;
        let entity_id = self
            .messaging
            .validate_and_set_entity(
                self.storage.clone(),
                &typed_data,
                &signature,
                &self.provider,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let message = Message { signature, message };
        if let Some(tx) = &self.cross_messaging_tx {
            tx.send(message)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        Ok(Response::new(PublishMessageResponse {
            entity_id: entity_id.to_bytes_be().to_vec(),
        }))
    }

    async fn publish_message_batch(
        &self,
        request: Request<PublishMessageBatchRequest>,
    ) -> Result<Response<PublishMessageBatchResponse>, Status> {
        let PublishMessageBatchRequest { messages } = request.into_inner();
        let mut responses = Vec::with_capacity(messages.len());
        for message in messages {
            let signature = message
                .signature
                .iter()
                .map(|s| Felt::from_bytes_be_slice(s))
                .collect::<Vec<_>>();
            let message = message.message;
            let typed_data = serde_json::from_str(&message)
                .map_err(|_| Status::invalid_argument("Invalid message"))?;

            let entity_id = self
                .messaging
                .validate_and_set_entity(
                    self.storage.clone(),
                    &typed_data,
                    &signature,
                    &self.provider,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            responses.push(PublishMessageResponse {
                entity_id: entity_id.to_bytes_be().to_vec(),
            });
        }

        Ok(Response::new(PublishMessageBatchResponse {
            responses: responses.into_iter().collect(),
        }))
    }
}

const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_EXPOSED_HEADERS: [&str; 4] = [
    "grpc-status",
    "grpc-message",
    "grpc-status-details-bin",
    "grpc-encoding",
];
const DEFAULT_ALLOW_HEADERS: [&str; 6] = [
    "x-grpc-web",
    "content-type",
    "x-user-agent",
    "grpc-timeout",
    "grpc-accept-encoding",
    "grpc-encoding",
];

#[derive(Clone, Debug)]
pub struct GrpcConfig {
    pub subscription_buffer_size: usize,
    pub optimistic: bool,
    pub tcp_keepalive_interval: Duration,
    pub http2_keepalive_interval: Duration,
    pub http2_keepalive_timeout: Duration,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            subscription_buffer_size: 1000,
            optimistic: false,
            tcp_keepalive_interval: Duration::from_secs(60),
            http2_keepalive_interval: Duration::from_secs(30),
            http2_keepalive_timeout: Duration::from_secs(10),
        }
    }
}

pub async fn new<P: Provider + Sync + Send + 'static>(
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    storage: Arc<dyn Storage>,
    provider: P,
    messaging: Arc<Messaging>,
    world_address: Felt,
    cross_messaging_tx: UnboundedSender<Message>,
    config: GrpcConfig,
) -> Result<
    (
        SocketAddr,
        impl Future<Output = Result<(), tonic::transport::Error>> + 'static,
    ),
    std::io::Error,
> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::world::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    // Extract keepalive settings before moving config
    let tcp_keepalive = config.tcp_keepalive_interval;
    let http2_keepalive_interval = config.http2_keepalive_interval;
    let http2_keepalive_timeout = config.http2_keepalive_timeout;

    let world = DojoWorld::new(
        storage,
        provider,
        messaging,
        world_address,
        Some(cross_messaging_tx),
        config,
    );
    let server = WorldServer::new(world)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

    let server_future = Server::builder()
        // GrpcWeb is over http1 so we must enable it.
        .accept_http1(true)
        // Configure keepalive for long-lived streaming connections
        .tcp_keepalive(Some(tcp_keepalive))
        .http2_keepalive_interval(Some(http2_keepalive_interval))
        .http2_keepalive_timeout(Some(http2_keepalive_timeout))
        .initial_stream_window_size(Some(1024 * 1024))
        .initial_connection_window_size(Some(1024 * 1024 * 10))
        // Should be enabled by default.
        .tcp_nodelay(true)
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::mirror_request())
                .allow_credentials(true)
                .max_age(DEFAULT_MAX_AGE)
                .expose_headers(
                    DEFAULT_EXPOSED_HEADERS
                        .iter()
                        .cloned()
                        .map(HeaderName::from_static)
                        .collect::<Vec<HeaderName>>(),
                )
                .allow_headers(
                    DEFAULT_ALLOW_HEADERS
                        .iter()
                        .cloned()
                        .map(HeaderName::from_static)
                        .collect::<Vec<HeaderName>>(),
                ),
        )
        .layer(GrpcWebLayer::new())
        .add_service(reflection)
        .add_service(server)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown_rx.recv().await.map_or((), |_| ())
        });

    Ok((addr, server_future))
}
