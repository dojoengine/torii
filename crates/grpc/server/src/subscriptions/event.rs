use std::pin::Pin;

use futures::{Stream, stream};
use futures_util::StreamExt;
use torii_broker::types::EventUpdate;
use torii_broker::MemoryBroker;
use torii_proto::EventWithMetadata;
use torii_proto::KeysClause;

use crate::GrpcConfig;
use super::match_keys;
use torii_proto::proto::types::Event as ProtoEvent;
use torii_proto::proto::world::SubscribeEventsResponse;

/// Creates a stream that subscribes to event updates from the broker and applies filtering
pub fn subscribe_events_stream(
    keys: Vec<KeysClause>,
    config: GrpcConfig,
) -> Pin<Box<dyn Stream<Item = Result<SubscribeEventsResponse, tonic::Status>> + Send>> {
    let broker_stream: Pin<Box<dyn Stream<Item = EventWithMetadata> + Send>> = if config.optimistic {
        Box::pin(MemoryBroker::<EventUpdate>::subscribe_optimistic())
    } else {
        Box::pin(MemoryBroker::<EventUpdate>::subscribe())
    };

    let filtered_stream = broker_stream
        .filter_map(move |event: EventWithMetadata| {
            let keys_filter = keys.clone();
            async move {
                // Apply keys filter
                if !match_keys(&event.event.keys, &keys_filter) {
                    return None;
                }

                Some(Ok(SubscribeEventsResponse {
                    event: Some(ProtoEvent {
                        keys: event.event
                            .keys
                            .iter()
                            .map(|k| k.to_bytes_be().to_vec())
                            .collect(),
                        data: event.event
                            .data
                            .iter()
                            .map(|d| d.to_bytes_be().to_vec())
                            .collect(),
                        transaction_hash: event.event.transaction_hash.to_bytes_be().to_vec(),
                    }),
                }))
            }
        })
        .chain(stream::once(async move {
            // Send initial empty response for firefox/safari unlock issue
            Ok(SubscribeEventsResponse { event: None })
        }));

    Box::pin(filtered_stream)
}
