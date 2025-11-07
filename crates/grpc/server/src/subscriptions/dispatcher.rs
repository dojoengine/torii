use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use futures_util::StreamExt;
use torii_broker::types::{
    AchievementProgressionUpdate, ActivityUpdate, AggregationUpdate, ContractUpdate,
    EntityUpdate, EventMessageUpdate, EventUpdate, TokenBalanceUpdate, TokenTransferUpdate,
    TokenUpdate, TransactionUpdate,
};
use torii_broker::MemoryBroker;
use tracing::trace;

use super::{
    achievement::AchievementProgressionManager, activity::ActivityManager,
    aggregation::AggregationManager, contract::ContractManager, entity::EntityManager,
    event::EventManager, event_message::EventMessageManager, token::TokenManager,
    token_balance::TokenBalanceManager, token_transfer::TokenTransferManager,
    transaction::TransactionManager,
};
use crate::GrpcConfig;

pub(crate) const LOG_TARGET: &str = "torii::grpc::server::subscriptions::dispatcher";

/// Unified broker dispatcher that handles all subscription types
/// This replaces the individual Service futures for each subscription type
#[must_use = "Dispatcher does nothing unless polled"]
pub struct BrokerDispatcher {
    // Broker streams for each update type
    entity_stream: Pin<Box<dyn Stream<Item = torii_proto::schema::EntityWithMetadata<false>> + Send>>,
    event_message_stream:
        Pin<Box<dyn Stream<Item = torii_proto::schema::EntityWithMetadata<true>> + Send>>,
    event_stream: Pin<Box<dyn Stream<Item = torii_proto::EventWithMetadata> + Send>>,
    contract_stream: Pin<Box<dyn Stream<Item = torii_proto::Contract> + Send>>,
    token_stream: Pin<Box<dyn Stream<Item = torii_proto::Token> + Send>>,
    token_balance_stream: Pin<Box<dyn Stream<Item = torii_proto::TokenBalance> + Send>>,
    token_transfer_stream: Pin<Box<dyn Stream<Item = torii_proto::TokenTransfer> + Send>>,
    transaction_stream: Pin<Box<dyn Stream<Item = torii_proto::Transaction> + Send>>,
    aggregation_stream: Pin<Box<dyn Stream<Item = torii_proto::AggregationEntry> + Send>>,
    activity_stream: Pin<Box<dyn Stream<Item = torii_proto::Activity> + Send>>,
    achievement_stream: Pin<Box<dyn Stream<Item = torii_proto::AchievementProgression> + Send>>,

    // Subscription managers
    entity_manager: Arc<EntityManager>,
    event_message_manager: Arc<EventMessageManager>,
    event_manager: Arc<EventManager>,
    contract_manager: Arc<ContractManager>,
    token_manager: Arc<TokenManager>,
    token_balance_manager: Arc<TokenBalanceManager>,
    token_transfer_manager: Arc<TokenTransferManager>,
    transaction_manager: Arc<TransactionManager>,
    aggregation_manager: Arc<AggregationManager>,
    activity_manager: Arc<ActivityManager>,
    achievement_manager: Arc<AchievementProgressionManager>,
}

impl BrokerDispatcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: GrpcConfig,
        entity_manager: Arc<EntityManager>,
        event_message_manager: Arc<EventMessageManager>,
        event_manager: Arc<EventManager>,
        contract_manager: Arc<ContractManager>,
        token_manager: Arc<TokenManager>,
        token_balance_manager: Arc<TokenBalanceManager>,
        token_transfer_manager: Arc<TokenTransferManager>,
        transaction_manager: Arc<TransactionManager>,
        aggregation_manager: Arc<AggregationManager>,
        activity_manager: Arc<ActivityManager>,
        achievement_manager: Arc<AchievementProgressionManager>,
    ) -> Self {
        let optimistic = config.optimistic;

        Self {
            // Subscribe to all broker streams based on optimistic setting
            entity_stream: if optimistic {
                Box::pin(MemoryBroker::<EntityUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<EntityUpdate>::subscribe())
            },
            event_message_stream: if optimistic {
                Box::pin(MemoryBroker::<EventMessageUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<EventMessageUpdate>::subscribe())
            },
            event_stream: if optimistic {
                Box::pin(MemoryBroker::<EventUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<EventUpdate>::subscribe())
            },
            contract_stream: if optimistic {
                Box::pin(MemoryBroker::<ContractUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<ContractUpdate>::subscribe())
            },
            token_stream: if optimistic {
                Box::pin(MemoryBroker::<TokenUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<TokenUpdate>::subscribe())
            },
            token_balance_stream: if optimistic {
                Box::pin(MemoryBroker::<TokenBalanceUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<TokenBalanceUpdate>::subscribe())
            },
            token_transfer_stream: if optimistic {
                Box::pin(MemoryBroker::<TokenTransferUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<TokenTransferUpdate>::subscribe())
            },
            transaction_stream: if optimistic {
                Box::pin(MemoryBroker::<TransactionUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<TransactionUpdate>::subscribe())
            },
            aggregation_stream: if optimistic {
                Box::pin(MemoryBroker::<AggregationUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<AggregationUpdate>::subscribe())
            },
            activity_stream: if optimistic {
                Box::pin(MemoryBroker::<ActivityUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<ActivityUpdate>::subscribe())
            },
            achievement_stream: if optimistic {
                Box::pin(MemoryBroker::<AchievementProgressionUpdate>::subscribe_optimistic())
            } else {
                Box::pin(MemoryBroker::<AchievementProgressionUpdate>::subscribe())
            },

            // Store manager references
            entity_manager,
            event_message_manager,
            event_manager,
            contract_manager,
            token_manager,
            token_balance_manager,
            token_transfer_manager,
            transaction_manager,
            aggregation_manager,
            activity_manager,
            achievement_manager,
        }
    }
}

impl Future for BrokerDispatcher {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut processed_any = false;

        // Poll all streams and dispatch to appropriate managers
        // Process inline for minimum latency

        // Entity updates
        while let Poll::Ready(Some(entity)) = this.entity_stream.poll_next_unpin(cx) {
            super::entity::Service::process_entity_update_sync(&this.entity_manager, &entity);
            processed_any = true;
        }

        // Event message updates
        while let Poll::Ready(Some(event)) = this.event_message_stream.poll_next_unpin(cx) {
            super::event_message::Service::process_event_update_sync(&this.event_message_manager, &event);
            processed_any = true;
        }

        // Event updates
        while let Poll::Ready(Some(event)) = this.event_stream.poll_next_unpin(cx) {
            super::event::Service::process_event_sync(&this.event_manager, &event);
            processed_any = true;
        }

        // Contract updates
        while let Poll::Ready(Some(contract)) = this.contract_stream.poll_next_unpin(cx) {
            super::contract::Service::process_contract_sync(&this.contract_manager, &contract);
            processed_any = true;
        }

        // Token updates
        while let Poll::Ready(Some(token)) = this.token_stream.poll_next_unpin(cx) {
            super::token::Service::process_token_sync(&this.token_manager, &token);
            processed_any = true;
        }

        // Token balance updates
        while let Poll::Ready(Some(balance)) = this.token_balance_stream.poll_next_unpin(cx) {
            super::token_balance::Service::process_balance_sync(&this.token_balance_manager, &balance);
            processed_any = true;
        }

        // Token transfer updates
        while let Poll::Ready(Some(transfer)) = this.token_transfer_stream.poll_next_unpin(cx) {
            super::token_transfer::Service::process_transfer_sync(&this.token_transfer_manager, &transfer);
            processed_any = true;
        }

        // Transaction updates
        while let Poll::Ready(Some(transaction)) = this.transaction_stream.poll_next_unpin(cx) {
            super::transaction::Service::process_transaction_sync(&this.transaction_manager, &transaction);
            processed_any = true;
        }

        // Aggregation updates
        while let Poll::Ready(Some(aggregation)) = this.aggregation_stream.poll_next_unpin(cx) {
            super::aggregation::Service::process_aggregation_sync(&this.aggregation_manager, &aggregation);
            processed_any = true;
        }

        // Activity updates
        while let Poll::Ready(Some(activity)) = this.activity_stream.poll_next_unpin(cx) {
            super::activity::Service::process_activity_sync(&this.activity_manager, &activity);
            processed_any = true;
        }

        // Achievement progression updates
        while let Poll::Ready(Some(progression)) = this.achievement_stream.poll_next_unpin(cx) {
            super::achievement::Service::process_progression_sync(&this.achievement_manager, &progression);
            processed_any = true;
        }

        if processed_any {
            trace!(target: LOG_TARGET, "Processed broker updates");
        }

        Poll::Pending
    }
}

