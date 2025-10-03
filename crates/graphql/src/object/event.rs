use std::str::FromStr;

use async_graphql::dynamic::{
    Field, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use starknet_crypto::Felt;
use tokio_stream::StreamExt;
use torii_broker::types::{EventUpdate, InnerType};
use torii_broker::MemoryBroker;

use super::inputs::keys_input::{keys_argument, parse_keys_argument};
use super::{BasicObject, ResolvableObject, TypeMapping};
use crate::constants::{DATETIME_FORMAT, EVENT_NAMES, EVENT_TYPE_NAME};
use crate::mapping::EVENT_TYPE_MAPPING;
use crate::types::ValueMapping;
use std::sync::Arc;
use torii_storage::{proto as storage_proto, ReadOnlyStorage};
use async_graphql::dynamic::FieldFuture;

#[derive(Debug)]
pub struct EventObject;

impl BasicObject for EventObject {
    fn name(&self) -> (&str, &str) {
        EVENT_NAMES
    }

    fn type_name(&self) -> &str {
        EVENT_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &EVENT_TYPE_MAPPING
    }
}

impl ResolvableObject for EventObject {
    fn resolvers(&self) -> Vec<Field> {
        // Events list via storage
        let mut field = Field::new(self.name().1, TypeRef::named_list(self.type_name()), |ctx| {
            FieldFuture::new(async move {
                let storage = ctx.data::<Arc<dyn ReadOnlyStorage>>()?.clone();
                // support optional keys filtering with KeysClause similar to subscription
                let keys = parse_keys_argument(&ctx)?;
                let pagination = storage_proto::Pagination { cursor: None, limit: None, direction: storage_proto::PaginationDirection::Forward, order_by: vec![] };
                let query = storage_proto::EventQuery { keys: keys.map(|k| storage_proto::KeysClause { keys: k.iter().map(|s| Some(Felt::from_str(s).unwrap())).collect(), pattern_matching: storage_proto::PatternMatching::VariableLen, models: vec![] }), pagination };
                let page = storage.events(query).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let list = page.items.into_iter().map(|e| {
                    let keys: Vec<String> = e.keys.into_iter().map(|k| format!("{:#x}", k)).collect();
                    let data: Vec<String> = e.data.into_iter().map(|k| format!("{:#x}", k)).collect();
                    Value::Object(ValueMapping::from([
                        (Name::new("id"), Value::from("")),
                        (Name::new("keys"), Value::from(keys)),
                        (Name::new("data"), Value::from(data)),
                        (Name::new("transactionHash"), Value::from("")),
                        (Name::new("executedAt"), Value::from("")),
                        (Name::new("createdAt"), Value::from("")),
                    ]))
                }).collect::<Vec<_>>();
                Ok(Some(Value::List(list)))
            })
        });
        field = keys_argument(field);
        vec![field]
    }

    fn subscriptions(&self) -> Option<Vec<SubscriptionField>> {
        Some(vec![SubscriptionField::new(
            "eventEmitted",
            TypeRef::named_nn(self.type_name()),
            |ctx| {
                SubscriptionFieldFuture::new(async move {
                    let input_keys = parse_keys_argument(&ctx)?;
                    Ok(
                        MemoryBroker::<EventUpdate>::subscribe().filter_map(move |event| {
                            EventObject::match_and_map_event(&input_keys, event)
                                .map(|value_mapping| Ok(Value::Object(value_mapping)))
                        }),
                    )
                })
            },
        )
        .argument(InputValue::new(
            "keys",
            TypeRef::named_list(TypeRef::STRING),
        ))])
    }
}

impl EventObject {
    fn value_mapping(event: <EventUpdate as InnerType>::Inner) -> ValueMapping {
        let keys: Vec<String> = event
            .event
            .keys
            .iter()
            .map(|k| format!("{:#x}", k))
            .collect();
        let data: Vec<String> = event
            .event
            .data
            .iter()
            .map(|k| format!("{:#x}", k))
            .collect();
        ValueMapping::from([
            (Name::new("id"), Value::from(event.id)),
            (Name::new("keys"), Value::from(keys)),
            (Name::new("data"), Value::from(data)),
            (
                Name::new("transactionHash"),
                Value::from(format!("{:#x}", event.event.transaction_hash)),
            ),
            (
                Name::new("executedAt"),
                Value::from(event.executed_at.format(DATETIME_FORMAT).to_string()),
            ),
            (
                Name::new("createdAt"),
                Value::from(event.created_at.format(DATETIME_FORMAT).to_string()),
            ),
        ])
    }

    fn match_and_map_event(
        input_keys: &Option<Vec<String>>,
        event: <EventUpdate as InnerType>::Inner,
    ) -> Option<ValueMapping> {
        if let Some(ref keys) = input_keys {
            if EventObject::match_keys(keys, &event) {
                return Some(EventObject::value_mapping(event));
            }

            // no match, keep listening
            None
        } else {
            // subscribed to all events
            Some(EventObject::value_mapping(event))
        }
    }

    // Checks if the provided keys match the event's keys, allowing '*' as a wildcard. Returns true
    // if all keys match or if a wildcard is present at the respective position.
    pub fn match_keys(input_keys: &[String], event: &<EventUpdate as InnerType>::Inner) -> bool {
        if input_keys.len() > event.event.keys.len() {
            return false;
        }

        for (input_key, event_key) in input_keys.iter().zip(event.event.keys.iter()) {
            if input_key != "*" && Felt::from_str(input_key).unwrap() != *event_key {
                return false;
            }
        }

        true
    }
}
