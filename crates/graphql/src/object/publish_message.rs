use async_graphql::dynamic::{Field, FieldFuture, InputValue, TypeRef};
use async_graphql::{Name, Value};
use starknet_crypto::Felt;
use std::str::FromStr;
use std::sync::Arc;
use torii_messaging::MessagingTrait;

use super::{BasicObject, TypeMapping, ValueMapping};
use crate::constants::{PUBLISH_MESSAGE_RESPONSE_TYPE_NAME, PUBLISH_MESSAGE_TYPE_NAME};
use crate::mapping::{PUBLISH_MESSAGE_INPUT_MAPPING, PUBLISH_MESSAGE_RESPONSE_MAPPING};
use crate::utils::extract;

#[derive(Debug)]
pub struct PublishMessageObject;

impl BasicObject for PublishMessageObject {
    fn name(&self) -> (&str, &str) {
        ("publishMessage", "")
    }

    fn type_name(&self) -> &str {
        PUBLISH_MESSAGE_RESPONSE_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &PUBLISH_MESSAGE_RESPONSE_MAPPING
    }
}

#[derive(Debug)]
pub struct PublishMessageInputObject;

impl BasicObject for PublishMessageInputObject {
    fn name(&self) -> (&str, &str) {
        ("publishMessageInput", "")
    }

    fn type_name(&self) -> &str {
        PUBLISH_MESSAGE_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &PUBLISH_MESSAGE_INPUT_MAPPING
    }
}

impl PublishMessageObject {
    pub fn mutation_field() -> Field {
        Field::new(
            "publishMessage",
            TypeRef::named_nn(PUBLISH_MESSAGE_RESPONSE_TYPE_NAME),
            move |ctx| {
                FieldFuture::new(async move {
                    let signature_strings =
                        extract::<Vec<String>>(ctx.args.as_index_map(), "signature")?;
                    let message = extract::<String>(ctx.args.as_index_map(), "message")?;

                    // Convert signature strings to Felt
                    let signature = signature_strings
                        .iter()
                        .map(|s| Felt::from_str(s))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| {
                            async_graphql::Error::new(format!("Invalid signature: {}", e))
                        })?;

                    // Parse the message as TypedData
                    let typed_data: starknet_core::types::TypedData =
                        serde_json::from_str(&message).map_err(|e| {
                            async_graphql::Error::new(format!("Invalid message JSON: {}", e))
                        })?;

                    // Get messaging from context
                    let messaging = ctx.data::<Arc<dyn MessagingTrait>>()?;

                    // Validate and set entity
                    let entity_id = messaging
                        .validate_and_set_entity(&typed_data, &signature)
                        .await
                        .map_err(|e| {
                            async_graphql::Error::new(format!("Failed to publish message: {}", e))
                        })?;

                    // Create response
                    let response = ValueMapping::from([(
                        Name::new("entityId"),
                        Value::from(format!("{:#x}", entity_id)),
                    )]);

                    Ok(Some(Value::Object(response)))
                })
            },
        )
        .argument(InputValue::new(
            "signature",
            TypeRef::named_nn_list(TypeRef::STRING),
        ))
        .argument(InputValue::new(
            "message",
            TypeRef::named_nn(TypeRef::STRING),
        ))
    }
}
