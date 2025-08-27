use std::pin::Pin;

use dojo_types::schema::Ty;
use futures::{Stream, stream};
use futures_util::StreamExt;
use rand::Rng;
use torii_broker::{types::EntityUpdate, MemoryBroker};
use torii_proto::schema::EntityWithMetadata;
use torii_proto::proto::world::SubscribeEntityResponse;
use torii_proto::Clause;

use crate::GrpcConfig;
use super::match_entity;

/// Creates a stream that subscribes to entity updates from the broker and applies filtering
pub fn subscribe_entities_stream(
    clause: Option<Clause>,
    config: GrpcConfig,
) -> Pin<Box<dyn Stream<Item = Result<SubscribeEntityResponse, tonic::Status>> + Send>> {
    let subscription_id = rand::thread_rng().gen::<u64>();
    
    let broker_stream: Pin<Box<dyn Stream<Item = EntityWithMetadata> + Send>> = if config.optimistic {
        Box::pin(MemoryBroker::<EntityUpdate>::subscribe_optimistic())
    } else {
        Box::pin(MemoryBroker::<EntityUpdate>::subscribe())
    };

    let filtered_stream = broker_stream
        .filter_map(move |entity: EntityWithMetadata| {
            let clause = clause.clone();
            async move {
                // Apply filter if clause exists
                if let Some(clause) = &clause {
                    if !match_entity(
                        entity.entity.hashed_keys,
                        &entity.keys,
                        &entity.entity.models.first().map(|m| Ty::Struct(m.clone())),
                        &clause,
                    ) {
                        return None;
                    }
                }

                Some(Ok(SubscribeEntityResponse {
                    entity: Some(entity.entity.clone().into()),
                    subscription_id,
                }))
            }
        })
        .chain(stream::once(async move {
            // Send initial empty response for firefox/safari unlock issue
            Ok(SubscribeEntityResponse {
                entity: None,
                subscription_id,
            })
        }));

    Box::pin(filtered_stream)
}
