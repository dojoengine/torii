//! Client implementation for the gRPC service.
#[cfg(target_arch = "wasm32")]
extern crate wasm_prost as prost;
#[cfg(target_arch = "wasm32")]
extern crate wasm_tonic as tonic;

use std::num::ParseIntError;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

use crypto_bigint::{Encoding, U256};
use futures_util::stream::MapOk;
use futures_util::{Stream, StreamExt, TryStreamExt};
use starknet::core::types::{Felt, FromStrError};
use tonic::codec::CompressionEncoding;
#[cfg(not(target_arch = "wasm32"))]
use tonic::transport::Endpoint;

use torii_proto::error::ProtoError;
use torii_proto::proto::world::{
    world_client, PublishMessageBatchRequest, PublishMessageRequest, RetrieveAchievementsRequest,
    RetrieveAchievementsResponse, RetrieveActivitiesRequest, RetrieveActivitiesResponse,
    RetrieveAggregationsRequest, RetrieveAggregationsResponse, RetrieveContractsRequest,
    RetrieveContractsResponse, RetrieveControllersRequest, RetrieveControllersResponse,
    RetrieveEntitiesRequest, RetrieveEntitiesResponse, RetrieveEventsRequest,
    RetrieveEventsResponse, RetrievePlayerAchievementsRequest, RetrievePlayerAchievementsResponse,
    RetrieveTokenBalancesRequest, RetrieveTokenBalancesResponse, RetrieveTokenContractsRequest,
    RetrieveTokenContractsResponse, RetrieveTokenTransfersRequest, RetrieveTokenTransfersResponse,
    RetrieveTokensRequest, RetrieveTokensResponse, RetrieveTransactionsRequest,
    RetrieveTransactionsResponse, SearchRequest, SubscribeAchievementProgressionsRequest,
    SubscribeAchievementProgressionsResponse, SubscribeActivitiesRequest,
    SubscribeActivitiesResponse, SubscribeAggregationsRequest, SubscribeAggregationsResponse,
    SubscribeContractsRequest, SubscribeContractsResponse, SubscribeEntitiesRequest,
    SubscribeEntityResponse, SubscribeEventsRequest, SubscribeEventsResponse,
    SubscribeTokenBalancesRequest, SubscribeTokenBalancesResponse, SubscribeTokenTransfersRequest,
    SubscribeTokenTransfersResponse, SubscribeTokensRequest, SubscribeTokensResponse,
    SubscribeTransactionsRequest, SubscribeTransactionsResponse,
    UpdateAchievementProgressionsSubscriptionRequest, UpdateActivitiesSubscriptionRequest,
    UpdateAggregationsSubscriptionRequest, UpdateEntitiesSubscriptionRequest,
    UpdateTokenBalancesSubscriptionRequest, UpdateTokenSubscriptionRequest,
    UpdateTokenTransfersSubscriptionRequest, WorldsRequest,
};
use torii_proto::schema::Entity;
use torii_proto::{
    AchievementQuery, ActivityQuery, AggregationQuery, Clause, Contract, ContractQuery,
    ControllerQuery, Event, EventQuery, KeysClause, Message, PlayerAchievementQuery, Query,
    SearchQuery, SqlRow, Token, TokenBalance, TokenBalanceQuery, TokenContractQuery, TokenQuery,
    TokenTransfer, TokenTransferQuery, Transaction, TransactionFilter, TransactionQuery,
};

pub use torii_proto as types;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[cfg(not(target_arch = "wasm32"))]
    #[error("Endpoint error: {0}")]
    Endpoint(String),
    #[error(transparent)]
    Grpc(tonic::Status),
    #[error(transparent)]
    ParseStr(FromStrError),
    #[error(transparent)]
    ParseInt(ParseIntError),
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    Transport(tonic::transport::Error),
    #[error(transparent)]
    Proto(#[from] ProtoError),
}

// Default max message size for the client
const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
/// A lightweight wrapper around the grpc client.
pub struct WorldClient {
    #[cfg(not(target_arch = "wasm32"))]
    inner: world_client::WorldClient<tonic::transport::Channel>,
    #[cfg(target_arch = "wasm32")]
    inner: world_client::WorldClient<tonic_web_wasm_client::Client>,
}

impl WorldClient {
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn new(dst: String) -> Result<Self, Error> {
        Self::new_with_config(dst, DEFAULT_MAX_MESSAGE_SIZE).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn new_with_config(dst: String, max_message_size: usize) -> Result<Self, Error> {
        const KEEPALIVE_TIME: u64 = 60;

        let endpoint = Endpoint::from_shared(dst.clone())
            .map_err(|e| Error::Endpoint(e.to_string()))?
            .tcp_keepalive(Some(Duration::from_secs(KEEPALIVE_TIME)));
        let channel = endpoint.connect().await.map_err(Error::Transport)?;
        Ok(Self {
            inner: world_client::WorldClient::with_origin(channel, endpoint.uri().clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size),
        })
    }

    // we make this function async so that we can keep the function signature similar
    #[cfg(target_arch = "wasm32")]
    pub async fn new(endpoint: String) -> Result<Self, Error> {
        Self::new_with_config(endpoint, DEFAULT_MAX_MESSAGE_SIZE).await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn new_with_config(endpoint: String, max_message_size: usize) -> Result<Self, Error> {
        Ok(Self {
            inner: world_client::WorldClient::new(tonic_web_wasm_client::Client::new(endpoint))
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size),
        })
    }

    /// Retrieve the metadata of the World.
    pub async fn worlds(
        &mut self,
        world_addresses: Vec<Felt>,
    ) -> Result<Vec<torii_proto::World>, Error> {
        self.inner
            .worlds(WorldsRequest {
                world_addresses: world_addresses
                    .into_iter()
                    .map(|a| a.to_bytes_be().to_vec())
                    .collect(),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| {
                res.into_inner()
                    .worlds
                    .into_iter()
                    .map(|w| w.try_into().map_err(Error::Proto))
                    .collect::<Result<Vec<torii_proto::World>, Error>>()
            })?
    }

    pub async fn retrieve_controllers(
        &mut self,
        query: ControllerQuery,
    ) -> Result<RetrieveControllersResponse, Error> {
        self.inner
            .retrieve_controllers(RetrieveControllersRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_contracts(
        &mut self,
        query: ContractQuery,
    ) -> Result<RetrieveContractsResponse, Error> {
        self.inner
            .retrieve_contracts(RetrieveContractsRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_transactions(
        &mut self,
        query: TransactionQuery,
    ) -> Result<RetrieveTransactionsResponse, Error> {
        self.inner
            .retrieve_transactions(RetrieveTransactionsRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_aggregations(
        &mut self,
        query: AggregationQuery,
    ) -> Result<RetrieveAggregationsResponse, Error> {
        self.inner
            .retrieve_aggregations(RetrieveAggregationsRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_activities(
        &mut self,
        query: ActivityQuery,
    ) -> Result<RetrieveActivitiesResponse, Error> {
        self.inner
            .retrieve_activities(RetrieveActivitiesRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn subscribe_activities(
        &mut self,
        world_addresses: Vec<Felt>,
        namespaces: Vec<String>,
        caller_addresses: Vec<Felt>,
    ) -> Result<ActivityUpdateStreaming, Error> {
        let request = SubscribeActivitiesRequest {
            world_addresses: world_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            namespaces,
            caller_addresses: caller_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
        };
        let stream = self
            .inner
            .subscribe_activities(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;
        Ok(ActivityUpdateStreaming(stream.map_ok(Box::new(|res| {
            (
                res.subscription_id,
                res.activity
                    .map_or_else(torii_proto::Activity::default, |a| {
                        a.try_into().expect("must able to serialize")
                    }),
            )
        }))))
    }

    pub async fn update_activities_subscription(
        &mut self,
        subscription_id: u64,
        world_addresses: Vec<Felt>,
        namespaces: Vec<String>,
        caller_addresses: Vec<Felt>,
    ) -> Result<(), Error> {
        let request = UpdateActivitiesSubscriptionRequest {
            subscription_id,
            world_addresses: world_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            namespaces,
            caller_addresses: caller_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
        };
        self.inner
            .update_activities_subscription(request)
            .await
            .map_err(Error::Grpc)?;
        Ok(())
    }

    pub async fn retrieve_achievements(
        &mut self,
        query: AchievementQuery,
    ) -> Result<RetrieveAchievementsResponse, Error> {
        self.inner
            .retrieve_achievements(RetrieveAchievementsRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_player_achievements(
        &mut self,
        query: PlayerAchievementQuery,
    ) -> Result<RetrievePlayerAchievementsResponse, Error> {
        self.inner
            .retrieve_player_achievements(RetrievePlayerAchievementsRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn subscribe_achievement_progressions(
        &mut self,
        world_addresses: Vec<Felt>,
        namespaces: Vec<String>,
        player_addresses: Vec<Felt>,
        achievement_ids: Vec<String>,
    ) -> Result<AchievementProgressionUpdateStreaming, Error> {
        let request = SubscribeAchievementProgressionsRequest {
            world_addresses: world_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            namespaces,
            player_addresses: player_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            achievement_ids,
        };
        let stream = self
            .inner
            .subscribe_achievement_progressions(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;
        Ok(AchievementProgressionUpdateStreaming(stream.map_ok(
            Box::new(|res| {
                (
                    res.subscription_id,
                    res.progression
                        .map_or_else(torii_proto::AchievementProgression::default, |p| {
                            p.try_into().expect("must able to serialize")
                        }),
                )
            }),
        )))
    }

    pub async fn update_achievement_progressions_subscription(
        &mut self,
        subscription_id: u64,
        world_addresses: Vec<Felt>,
        namespaces: Vec<String>,
        player_addresses: Vec<Felt>,
        achievement_ids: Vec<String>,
    ) -> Result<(), Error> {
        let request = UpdateAchievementProgressionsSubscriptionRequest {
            subscription_id,
            world_addresses: world_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            namespaces,
            player_addresses: player_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            achievement_ids,
        };
        self.inner
            .update_achievement_progressions_subscription(request)
            .await
            .map_err(Error::Grpc)?;
        Ok(())
    }

    pub async fn subscribe_aggregations(
        &mut self,
        aggregator_ids: Vec<String>,
        entity_ids: Vec<String>,
    ) -> Result<AggregationUpdateStreaming, Error> {
        let request = SubscribeAggregationsRequest {
            aggregator_ids,
            entity_ids,
        };
        let stream = self
            .inner
            .subscribe_aggregations(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;
        Ok(AggregationUpdateStreaming(stream.map_ok(Box::new(|res| {
            (
                res.subscription_id,
                res.entry
                    .map_or_else(torii_proto::AggregationEntry::default, |e| {
                        e.try_into().expect("must able to serialize")
                    }),
            )
        }))))
    }

    pub async fn update_aggregations_subscription(
        &mut self,
        subscription_id: u64,
        aggregator_ids: Vec<String>,
        entity_ids: Vec<String>,
    ) -> Result<(), Error> {
        let request = UpdateAggregationsSubscriptionRequest {
            subscription_id,
            aggregator_ids,
            entity_ids,
        };
        self.inner
            .update_aggregations_subscription(request)
            .await
            .map_err(Error::Grpc)?;
        Ok(())
    }

    pub async fn subscribe_transactions(
        &mut self,
        filter: Option<TransactionFilter>,
    ) -> Result<TransactionUpdateStreaming, Error> {
        let request = SubscribeTransactionsRequest {
            filter: filter.map(|f| f.into()),
        };
        let stream = self
            .inner
            .subscribe_transactions(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;
        Ok(TransactionUpdateStreaming(stream.map_ok(Box::new(|res| {
            res.transaction.map_or(Transaction::default(), |t| {
                t.try_into().expect("must able to serialize")
            })
        }))))
    }

    pub async fn retrieve_tokens(
        &mut self,
        query: TokenQuery,
    ) -> Result<RetrieveTokensResponse, Error> {
        self.inner
            .retrieve_tokens(RetrieveTokensRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn subscribe_tokens(
        &mut self,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<TokenUpdateStreaming, Error> {
        let request = SubscribeTokensRequest {
            contract_addresses: contract_addresses
                .into_iter()
                .map(|c| c.to_bytes_be().to_vec())
                .collect(),
            token_ids: token_ids
                .into_iter()
                .map(|id| id.to_be_bytes().to_vec())
                .collect(),
        };
        let stream = self
            .inner
            .subscribe_tokens(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;
        Ok(TokenUpdateStreaming(stream.map_ok(Box::new(|res| {
            (
                res.subscription_id,
                res.token.map_or(Token::default(), |t| {
                    t.try_into().expect("must able to serialize")
                }),
            )
        }))))
    }

    pub async fn update_tokens_subscription(
        &mut self,
        subscription_id: u64,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<(), Error> {
        let contract_addresses = contract_addresses
            .into_iter()
            .map(|c| c.to_bytes_be().to_vec())
            .collect();
        let request = UpdateTokenSubscriptionRequest {
            subscription_id,
            contract_addresses,
            token_ids: token_ids
                .into_iter()
                .map(|id| id.to_be_bytes().to_vec())
                .collect(),
        };
        self.inner
            .update_tokens_subscription(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_token_balances(
        &mut self,
        query: TokenBalanceQuery,
    ) -> Result<RetrieveTokenBalancesResponse, Error> {
        self.inner
            .retrieve_token_balances(RetrieveTokenBalancesRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_token_contracts(
        &mut self,
        query: TokenContractQuery,
    ) -> Result<RetrieveTokenContractsResponse, Error> {
        self.inner
            .retrieve_token_contracts(RetrieveTokenContractsRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_token_transfers(
        &mut self,
        query: TokenTransferQuery,
    ) -> Result<RetrieveTokenTransfersResponse, Error> {
        self.inner
            .retrieve_token_transfers(RetrieveTokenTransfersRequest {
                query: Some(query.into()),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn subscribe_token_transfers(
        &mut self,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<TokenTransferUpdateStreaming, Error> {
        let request = SubscribeTokenTransfersRequest {
            contract_addresses: contract_addresses
                .into_iter()
                .map(|c| c.to_bytes_be().to_vec())
                .collect(),
            account_addresses: account_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            token_ids: token_ids
                .into_iter()
                .map(|id| id.to_be_bytes().to_vec())
                .collect(),
        };
        let stream = self
            .inner
            .subscribe_token_transfers(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;
        Ok(TokenTransferUpdateStreaming(stream.map_ok(Box::new(
            |res| {
                res.transfer.map_or(TokenTransfer::default(), |t| {
                    t.try_into().expect("must able to serialize")
                })
            },
        ))))
    }

    pub async fn update_token_transfers_subscription(
        &mut self,
        subscription_id: u64,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<(), Error> {
        let request = UpdateTokenTransfersSubscriptionRequest {
            subscription_id,
            contract_addresses: contract_addresses
                .into_iter()
                .map(|c| c.to_bytes_be().to_vec())
                .collect(),
            account_addresses: account_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            token_ids: token_ids
                .into_iter()
                .map(|id| id.to_be_bytes().to_vec())
                .collect(),
        };

        self.inner
            .update_token_transfers_subscription(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_entities(
        &mut self,
        query: Query,
    ) -> Result<RetrieveEntitiesResponse, Error> {
        let request = RetrieveEntitiesRequest {
            query: Some(query.into()),
        };
        self.inner
            .retrieve_entities(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_event_messages(
        &mut self,
        query: Query,
    ) -> Result<RetrieveEntitiesResponse, Error> {
        let request = RetrieveEntitiesRequest {
            query: Some(query.into()),
        };
        self.inner
            .retrieve_event_messages(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_events(
        &mut self,
        query: EventQuery,
    ) -> Result<RetrieveEventsResponse, Error> {
        let request = RetrieveEventsRequest {
            query: Some(query.into()),
        };
        self.inner
            .retrieve_events(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    /// Subscribe to contracts updates.
    pub async fn subscribe_contracts(
        &mut self,
        query: ContractQuery,
    ) -> Result<ContractUpdateStreaming, Error> {
        let request = SubscribeContractsRequest {
            query: Some(query.into()),
        };
        let stream = self
            .inner
            .subscribe_contracts(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;
        Ok(ContractUpdateStreaming(stream.map_ok(Box::new(|res| {
            res.contract
                .map(|c| c.try_into().expect("must able to serialize"))
                .expect("must able to serialize")
        }))))
    }

    /// Subscribe to entities updates of a World.
    pub async fn subscribe_entities(
        &mut self,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) -> Result<EntityUpdateStreaming, Error> {
        let stream = self
            .inner
            .subscribe_entities(SubscribeEntitiesRequest {
                clause: clause.map(|c| c.into()),
                world_addresses: world_addresses
                    .into_iter()
                    .map(|w| w.to_bytes_be().to_vec())
                    .collect(),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;

        Ok(EntityUpdateStreaming(stream.map_ok(Box::new(|res| {
            res.entity
                .map_or((res.subscription_id, Entity::default()), |entity| {
                    (
                        res.subscription_id,
                        entity.try_into().expect("must able to serialize"),
                    )
                })
        }))))
    }

    /// Update an entities subscription.
    pub async fn update_entities_subscription(
        &mut self,
        subscription_id: u64,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) -> Result<(), Error> {
        self.inner
            .update_entities_subscription(UpdateEntitiesSubscriptionRequest {
                subscription_id,
                clause: clause.map(|c| c.into()),
                world_addresses: world_addresses
                    .into_iter()
                    .map(|w| w.to_bytes_be().to_vec())
                    .collect(),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    /// Subscribe to event messages of a World.
    pub async fn subscribe_event_messages(
        &mut self,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) -> Result<EntityUpdateStreaming, Error> {
        let stream = self
            .inner
            .subscribe_event_messages(SubscribeEntitiesRequest {
                clause: clause.map(|c| c.into()),
                world_addresses: world_addresses
                    .into_iter()
                    .map(|w| w.to_bytes_be().to_vec())
                    .collect(),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;

        Ok(EntityUpdateStreaming(stream.map_ok(Box::new(|res| {
            res.entity
                .map_or((res.subscription_id, Entity::default()), |entity| {
                    (
                        res.subscription_id,
                        entity.try_into().expect("must able to serialize"),
                    )
                })
        }))))
    }

    /// Update an event messages subscription.
    pub async fn update_event_messages_subscription(
        &mut self,
        subscription_id: u64,
        clause: Option<Clause>,
        world_addresses: Vec<Felt>,
    ) -> Result<(), Error> {
        self.inner
            .update_event_messages_subscription(UpdateEntitiesSubscriptionRequest {
                subscription_id,
                clause: clause.map(|c| c.into()),
                world_addresses: world_addresses
                    .into_iter()
                    .map(|w| w.to_bytes_be().to_vec())
                    .collect(),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    /// Subscribe to the events of a World.
    pub async fn subscribe_events(
        &mut self,
        keys: Vec<KeysClause>,
    ) -> Result<EventUpdateStreaming, Error> {
        let keys = keys.into_iter().map(|c| c.into()).collect();

        let stream = self
            .inner
            .subscribe_events(SubscribeEventsRequest { keys })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;

        Ok(EventUpdateStreaming(stream.map_ok(Box::new(|res| {
            res.event.map_or(Event::default(), |e| e.into())
        }))))
    }

    /// Subscribe to token balances.
    pub async fn subscribe_token_balances(
        &mut self,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<TokenBalanceStreaming, Error> {
        let request = SubscribeTokenBalancesRequest {
            contract_addresses: contract_addresses
                .into_iter()
                .map(|c| c.to_bytes_be().to_vec())
                .collect(),
            account_addresses: account_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            token_ids: token_ids
                .into_iter()
                .map(|id| id.to_be_bytes().to_vec())
                .collect(),
        };
        let stream = self
            .inner
            .subscribe_token_balances(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;
        Ok(TokenBalanceStreaming(stream.map_ok(Box::new(|res| {
            (
                res.subscription_id,
                res.balance.map_or(TokenBalance::default(), |b| {
                    b.try_into().expect("must able to serialize")
                }),
            )
        }))))
    }

    /// Update a token balances subscription.
    pub async fn update_token_balances_subscription(
        &mut self,
        subscription_id: u64,
        contract_addresses: Vec<Felt>,
        account_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
    ) -> Result<(), Error> {
        let request = UpdateTokenBalancesSubscriptionRequest {
            subscription_id,
            contract_addresses: contract_addresses
                .into_iter()
                .map(|c| c.to_bytes_be().to_vec())
                .collect(),
            account_addresses: account_addresses
                .into_iter()
                .map(|a| a.to_bytes_be().to_vec())
                .collect(),
            token_ids: token_ids
                .into_iter()
                .map(|id| id.to_be_bytes().to_vec())
                .collect(),
        };
        self.inner
            .update_token_balances_subscription(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn publish_message(&mut self, message: Message) -> Result<String, Error> {
        self.inner
            .publish_message(PublishMessageRequest {
                message: message.message,
                signature: message
                    .signature
                    .into_iter()
                    .map(|s| s.to_bytes_be().to_vec())
                    .collect(),
                world_address: message.world_address.to_bytes_be().to_vec(),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner().id)
    }

    pub async fn publish_message_batch(
        &mut self,
        messages: Vec<Message>,
    ) -> Result<Vec<String>, Error> {
        self.inner
            .publish_message_batch(PublishMessageBatchRequest {
                messages: messages
                    .iter()
                    .map(|m| PublishMessageRequest {
                        signature: m
                            .signature
                            .iter()
                            .map(|s| s.to_bytes_be().to_vec())
                            .collect(),
                        message: m.message.clone(),
                        world_address: m.world_address.to_bytes_be().to_vec(),
                    })
                    .collect(),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| {
                res.into_inner()
                    .responses
                    .into_iter()
                    .map(|r| r.id)
                    .collect()
            })
    }

    /// Execute a SQL query against the Torii database.
    /// Returns the query results as rows.
    pub async fn execute_sql(&mut self, query: String) -> Result<Vec<SqlRow>, Error> {
        self.inner
            .execute_sql(torii_proto::proto::types::SqlQueryRequest { query })
            .await
            .map_err(Error::Grpc)
            .map(|res| {
                res.into_inner()
                    .rows
                    .into_iter()
                    .map(|r| r.into())
                    .collect()
            })
    }

    /// Perform a full-text search across indexed entities.
    /// Returns search results grouped by table with relevance scores.
    pub async fn search(
        &mut self,
        query: SearchQuery,
    ) -> Result<torii_proto::SearchResponse, Error> {
        let request = SearchRequest {
            query: Some(query.into()),
        };
        self.inner
            .search(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| {
                res.into_inner()
                    .response
                    .map(|r| r.into())
                    .unwrap_or_else(|| torii_proto::SearchResponse {
                        total: 0,
                        results: vec![],
                    })
            })
    }
}

type TokenMappedStream = MapOk<
    tonic::Streaming<SubscribeTokensResponse>,
    Box<dyn Fn(SubscribeTokensResponse) -> (SubscriptionId, Token) + Send>,
>;

#[derive(Debug)]
pub struct TokenUpdateStreaming(TokenMappedStream);

impl Stream for TokenUpdateStreaming {
    type Item = <TokenMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type TokenBalanceMappedStream = MapOk<
    tonic::Streaming<SubscribeTokenBalancesResponse>,
    Box<dyn Fn(SubscribeTokenBalancesResponse) -> (SubscriptionId, TokenBalance) + Send>,
>;

#[derive(Debug)]
pub struct TokenBalanceStreaming(TokenBalanceMappedStream);

impl Stream for TokenBalanceStreaming {
    type Item = <TokenBalanceMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type TokenTransferMappedStream = MapOk<
    tonic::Streaming<SubscribeTokenTransfersResponse>,
    Box<dyn Fn(SubscribeTokenTransfersResponse) -> TokenTransfer + Send>,
>;

#[derive(Debug)]
pub struct TokenTransferUpdateStreaming(TokenTransferMappedStream);

impl Stream for TokenTransferUpdateStreaming {
    type Item = <TokenTransferMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type SubscriptionId = u64;

type EntityMappedStream = MapOk<
    tonic::Streaming<SubscribeEntityResponse>,
    Box<dyn Fn(SubscribeEntityResponse) -> (SubscriptionId, Entity) + Send>,
>;

#[derive(Debug)]
pub struct EntityUpdateStreaming(EntityMappedStream);

impl Stream for EntityUpdateStreaming {
    type Item = <EntityMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type EventMappedStream = MapOk<
    tonic::Streaming<SubscribeEventsResponse>,
    Box<dyn Fn(SubscribeEventsResponse) -> Event + Send>,
>;

#[derive(Debug)]
pub struct EventUpdateStreaming(EventMappedStream);

impl Stream for EventUpdateStreaming {
    type Item = <EventMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type ContractMappedStream = MapOk<
    tonic::Streaming<SubscribeContractsResponse>,
    Box<dyn Fn(SubscribeContractsResponse) -> Contract + Send>,
>;

#[derive(Debug)]
pub struct ContractUpdateStreaming(ContractMappedStream);

impl Stream for ContractUpdateStreaming {
    type Item = <ContractMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type TransactionMappedStream = MapOk<
    tonic::Streaming<SubscribeTransactionsResponse>,
    Box<dyn Fn(SubscribeTransactionsResponse) -> Transaction + Send>,
>;

#[derive(Debug)]
pub struct TransactionUpdateStreaming(TransactionMappedStream);

impl Stream for TransactionUpdateStreaming {
    type Item = <TransactionMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type AggregationMappedStream = MapOk<
    tonic::Streaming<SubscribeAggregationsResponse>,
    Box<
        dyn Fn(SubscribeAggregationsResponse) -> (SubscriptionId, torii_proto::AggregationEntry)
            + Send,
    >,
>;

#[derive(Debug)]
pub struct AggregationUpdateStreaming(AggregationMappedStream);

impl Stream for AggregationUpdateStreaming {
    type Item = <AggregationMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type ActivityMappedStream = MapOk<
    tonic::Streaming<SubscribeActivitiesResponse>,
    Box<dyn Fn(SubscribeActivitiesResponse) -> (SubscriptionId, torii_proto::Activity) + Send>,
>;

#[derive(Debug)]
pub struct ActivityUpdateStreaming(ActivityMappedStream);

impl Stream for ActivityUpdateStreaming {
    type Item = <ActivityMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type AchievementProgressionMappedStream = MapOk<
    tonic::Streaming<SubscribeAchievementProgressionsResponse>,
    Box<
        dyn Fn(
                SubscribeAchievementProgressionsResponse,
            ) -> (SubscriptionId, torii_proto::AchievementProgression)
            + Send,
    >,
>;

#[derive(Debug)]
pub struct AchievementProgressionUpdateStreaming(AchievementProgressionMappedStream);

impl Stream for AchievementProgressionUpdateStreaming {
    type Item = <AchievementProgressionMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}
