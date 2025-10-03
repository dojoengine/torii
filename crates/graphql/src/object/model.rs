use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
    Enum, Field, FieldFuture, InputObject, InputValue, SubscriptionField,
    SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use starknet::core::types::Felt;
use std::str::FromStr;
use std::sync::Arc;
use tokio_stream::StreamExt;
use torii_broker::types::ModelUpdate;
use torii_broker::MemoryBroker;
use torii_storage::{proto as storage_proto, ReadOnlyStorage};

use super::{BasicObject, ResolvableObject, TypeMapping, ValueMapping};
use crate::constants::{
    MODEL_NAMES, MODEL_ORDER_FIELD_TYPE_NAME, MODEL_ORDER_TYPE_NAME, MODEL_TYPE_NAME, ORDER_ASC,
    ORDER_DESC, ORDER_DIR_TYPE_NAME,
};
use crate::mapping::MODEL_TYPE_MAPPING;
use crate::utils;

const ORDER_BY_NAME: &str = "NAME";
const ORDER_BY_HASH: &str = "CLASS_HASH";

#[derive(Debug)]
pub struct ModelObject;

impl BasicObject for ModelObject {
    fn name(&self) -> (&str, &str) {
        MODEL_NAMES
    }

    fn type_name(&self) -> &str {
        MODEL_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &MODEL_TYPE_MAPPING
    }
}

impl ResolvableObject for ModelObject {
    fn input_objects(&self) -> Option<Vec<InputObject>> {
        let order_input = InputObject::new(MODEL_ORDER_TYPE_NAME)
            .field(InputValue::new(
                "direction",
                TypeRef::named_nn(ORDER_DIR_TYPE_NAME),
            ))
            .field(InputValue::new(
                "field",
                TypeRef::named_nn(MODEL_ORDER_FIELD_TYPE_NAME),
            ));

        Some(vec![order_input])
    }

    fn enum_objects(&self) -> Option<Vec<Enum>> {
        let direction = Enum::new(ORDER_DIR_TYPE_NAME)
            .item(ORDER_ASC)
            .item(ORDER_DESC);
        let field_order = Enum::new(MODEL_ORDER_FIELD_TYPE_NAME)
            .item(ORDER_BY_NAME)
            .item(ORDER_BY_HASH);

        Some(vec![direction, field_order])
    }

    fn resolvers(&self) -> Vec<Field> {
        // Single model by selector
        let get_one = Field::new(self.name().0, TypeRef::named_nn(self.type_name()), |ctx| {
            FieldFuture::new(async move {
                let storage = ctx.data::<Arc<dyn ReadOnlyStorage>>()?.clone();
                let id: String = utils::extract::<String>(ctx.args.as_index_map(), "id")?;
                let selector = Felt::from_str(&id).map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let model = storage
                    .model(selector)
                    .await
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let mapped = value_mapping_from_proto(model);
                Ok(Some(Value::Object(mapped)))
            })
        })
        .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID)));

        // Many models by optional selectors
        let get_many = Field::new(
            self.name().1,
            TypeRef::named_list(self.type_name()),
            |ctx| {
                FieldFuture::new(async move {
                    let storage = ctx.data::<Arc<dyn ReadOnlyStorage>>()?.clone();
                    let selectors: Vec<String> =
                        utils::extract::<Vec<String>>(ctx.args.as_index_map(), "selectors")
                            .unwrap_or_default();
                    let felt_selectors: Vec<Felt> =
                        selectors.into_iter().filter_map(|s| Felt::from_str(&s).ok()).collect();
                    let models = storage
                        .models(&felt_selectors)
                        .await
                        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                    let list = models
                        .into_iter()
                        .map(|m| Value::Object(value_mapping_from_proto(m)))
                        .collect::<Vec<Value>>();
                    Ok(Some(Value::List(list)))
                })
            },
        )
        .argument(InputValue::new(
            "selectors",
            TypeRef::named_list(TypeRef::ID),
        ));

        vec![get_one, get_many]
    }

    fn subscriptions(&self) -> Option<Vec<SubscriptionField>> {
        Some(vec![SubscriptionField::new(
            "modelRegistered",
            TypeRef::named_nn(self.type_name()),
            |ctx| {
                {
                    SubscriptionFieldFuture::new(async move {
                        let id = match ctx.args.get("id") {
                            Some(id) => Some(id.string()?.to_string()),
                            None => None,
                        };
                        // if id is None, then subscribe to all models
                        // if id is Some, then subscribe to only the model with that id
                        Ok(MemoryBroker::<ModelUpdate>::subscribe()
                            .then(move |model| {
                                let id = id.clone();
                                async move {
                                    let model = model;
                                    let model_id = format!("{:#x}", model.selector);
                                    if id.is_none() || id == Some(model_id.clone()) {
                                        let mapped = value_mapping_from_proto(model);
                                        Some(Ok(Value::Object(mapped)))
                                    } else {
                                        None
                                    }
                                }
                            })
                            .filter_map(|result| result))
                    })
                }
            },
        )
        .argument(InputValue::new("id", TypeRef::named(TypeRef::ID)))])
    }
}

impl ModelObject {
    pub fn value_mapping(model: storage_proto::Model) -> ValueMapping {
        value_mapping_from_proto(model)
    }
}

fn value_mapping_from_proto(model: storage_proto::Model) -> ValueMapping {
    IndexMap::from([
        (Name::new("name"), Value::from(model.name)),
        (Name::new("namespace"), Value::from(model.namespace)),
        (
            Name::new("selector"),
            Value::from(format!("{:#x}", model.selector)),
        ),
        (Name::new("classHash"), Value::from(format!("{:#x}", model.class_hash))),
        (
            Name::new("contractAddress"),
            Value::from(format!("{:#x}", model.contract_address)),
        ),
        (Name::new("packedSize"), Value::from(model.packed_size as i64)),
        (Name::new("unpackedSize"), Value::from(model.unpacked_size as i64)),
        (Name::new("useLegacyStore"), Value::from(model.use_legacy_store)),
        (
            Name::new("layout"),
            Value::from(serde_json::to_string(&model.layout).unwrap_or_default()),
        ),
        (
            Name::new("schema"),
            Value::from(serde_json::to_string(&model.schema).unwrap_or_default()),
        ),
    ])
}
