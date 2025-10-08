use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
    Enum, Field, InputObject, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use sqlx::{Pool, Sqlite};
use tokio_stream::StreamExt;
use torii_broker::types::ModelUpdate;
use torii_broker::MemoryBroker;
use torii_sqlite::types::Model;
use torii_sqlite::utils::felt_to_sql_string;
use torii_storage::utils::format_world_scoped_id;

use super::{resolve_many, BasicObject, ResolvableObject, TypeMapping, ValueMapping};
use crate::constants::{
    DATETIME_FORMAT, ID_COLUMN, MODEL_NAMES, MODEL_ORDER_FIELD_TYPE_NAME, MODEL_ORDER_TYPE_NAME,
    MODEL_TABLE, MODEL_TYPE_NAME, ORDER_ASC, ORDER_DESC, ORDER_DIR_TYPE_NAME,
};
use crate::mapping::MODEL_TYPE_MAPPING;
use crate::object::resolve_one;

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
        let resolve_one = resolve_one(
            MODEL_TABLE,
            ID_COLUMN,
            self.name().0,
            self.type_name(),
            self.type_mapping(),
        );

        let mut resolve_many = resolve_many(
            MODEL_TABLE,
            ID_COLUMN,
            self.name().1,
            self.type_name(),
            self.type_mapping(),
        );
        resolve_many = resolve_many.argument(InputValue::new(
            "order",
            TypeRef::named(MODEL_ORDER_TYPE_NAME),
        ));

        vec![resolve_one, resolve_many]
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
                        let pool = ctx.data::<Pool<Sqlite>>()?.clone();
                        // if id is None, then subscribe to all models
                        // if id is Some, then subscribe to only the model with that id
                        Ok(MemoryBroker::<ModelUpdate>::subscribe()
                            .then(move |model| {
                                let pool = pool.clone();
                                let id = id.clone();
                                async move {
                                    let model_id = felt_to_sql_string(&model.selector);
                                    let scoped_model_id = format_world_scoped_id(
                                        &model.world_address,
                                        &model.selector,
                                    );
                                    if id.is_none()
                                        || id == Some(model_id.clone())
                                        || id == Some(scoped_model_id.clone())
                                    {
                                        let mut conn = match pool.acquire().await {
                                            Ok(conn) => conn,
                                            Err(_) => return None,
                                        };

                                        let model = match sqlx::query_as::<_, Model>(
                                            "SELECT * FROM models WHERE id = ?",
                                        )
                                        .bind(&scoped_model_id)
                                        .fetch_one(&mut *conn)
                                        .await
                                        {
                                            Ok(model) => model,
                                            Err(_) => return None,
                                        };

                                        Some(Ok(Value::Object(ModelObject::value_mapping(model))))
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
    pub fn value_mapping(model: Model) -> ValueMapping {
        IndexMap::from([
            (Name::new("id"), Value::from(model.id)),
            (Name::new("name"), Value::from(model.name)),
            (Name::new("namespace"), Value::from(model.namespace)),
            (Name::new("classHash"), Value::from(model.class_hash)),
            (
                Name::new("contractAddress"),
                Value::from(model.contract_address),
            ),
            (
                Name::new("createdAt"),
                Value::from(model.created_at.format(DATETIME_FORMAT).to_string()),
            ),
            (
                Name::new("executedAt"),
                Value::from(model.executed_at.format(DATETIME_FORMAT).to_string()),
            ),
        ])
    }
}
