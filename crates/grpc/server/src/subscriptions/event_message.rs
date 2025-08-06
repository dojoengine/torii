use std::pin::Pin;

use dojo_types::schema::Ty;
use futures::{Stream, stream};
use futures_util::StreamExt;
use rand::Rng;
use torii_broker::types::EventMessageUpdate;
use torii_broker::MemoryBroker;
use torii_proto::schema::EntityWithMetadata;
use torii_proto::Clause;

use torii_proto::proto::world::SubscribeEntityResponse;

use crate::GrpcConfig;
use super::match_entity;

/// Creates a stream that subscribes to event message updates from the broker and applies filtering
pub fn subscribe_event_messages_stream(
    clause: Option<Clause>,
    config: GrpcConfig,
) -> Pin<Box<dyn Stream<Item = Result<SubscribeEntityResponse, tonic::Status>> + Send>> {
    let subscription_id = rand::thread_rng().gen::<u64>();
    
    let broker_stream: Pin<Box<dyn Stream<Item = EntityWithMetadata<true>> + Send>> = if config.optimistic {
        Box::pin(MemoryBroker::<EventMessageUpdate>::subscribe_optimistic())
    } else {
        Box::pin(MemoryBroker::<EventMessageUpdate>::subscribe())
    };

    let filtered_stream = broker_stream
        .filter_map(move |event: EntityWithMetadata<true>| {
            let clause = clause.clone();
            async move {
                // Apply filter if clause exists
                if let Some(clause) = &clause {
                    if !match_entity(
                        event.entity.hashed_keys,
                        &event.keys,
                        &event.entity.models.first().map(|m| Ty::Struct(m.clone())),
                        &clause,
                    ) {
                        return None;
                    }
                }

                Some(Ok(SubscribeEntityResponse {
                    entity: Some(event.entity.clone().into()),
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
