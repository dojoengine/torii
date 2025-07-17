use std::str::FromStr;

use async_graphql::dynamic::{
    Field, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use starknet_crypto::Felt;
use tokio_stream::StreamExt;
use torii_broker::types::{EventEmitted, InnerType};
use torii_broker::MemoryBroker;

use super::inputs::keys_input::{keys_argument, parse_keys_argument};
use super::{resolve_many, BasicObject, ResolvableObject, TypeMapping};
use crate::constants::{DATETIME_FORMAT, EVENT_NAMES, EVENT_TABLE, EVENT_TYPE_NAME, ID_COLUMN};
use crate::mapping::EVENT_TYPE_MAPPING;
use crate::types::ValueMapping;

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
        let mut resolve_many = resolve_many(
            EVENT_TABLE,
            ID_COLUMN,
            self.name().1,
            self.type_name(),
            self.type_mapping(),
        );
        resolve_many = keys_argument(resolve_many);

        vec![resolve_many]
    }

    fn subscriptions(&self) -> Option<Vec<SubscriptionField>> {
        Some(vec![SubscriptionField::new(
            "eventEmitted",
            TypeRef::named_nn(self.type_name()),
            |ctx| {
                SubscriptionFieldFuture::new(async move {
                    let input_keys = parse_keys_argument(&ctx)?;
                    Ok(MemoryBroker::<EventEmitted>::subscribe().filter_map(
                        move |event_update: EventEmitted| {
                            let event = event_update.into_inner();
                            EventObject::match_and_map_event(&input_keys, event)
                                .map(|value_mapping| Ok(Value::Object(value_mapping)))
                        },
                    ))
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
    fn value_mapping(event: <EventEmitted as InnerType>::Inner) -> ValueMapping {
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
        event: <EventEmitted as InnerType>::Inner,
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
    pub fn match_keys(input_keys: &[String], event: &<EventEmitted as InnerType>::Inner) -> bool {
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
