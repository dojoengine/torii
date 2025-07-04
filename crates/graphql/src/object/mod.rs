pub mod connection;
pub mod controller;
pub mod empty;
pub mod entity;
pub mod erc;
pub mod event;
pub mod event_message;
pub mod inputs;
pub mod metadata;
pub mod model;
pub mod model_data;
pub mod transaction;

use async_graphql::dynamic::{
    Enum, Field, FieldFuture, FieldValue, InputObject, InputValue, Object, SubscriptionField,
    TypeRef,
};
use async_graphql::Value;
use convert_case::{Case, Casing};
use erc::erc_token::ErcTokenType;
use erc::token_transfer::TokenTransferNode;
use erc::{Connection, ConnectionEdge};

use self::connection::edge::EdgeObject;
use self::connection::{
    connection_arguments, connection_output, parse_connection_arguments, ConnectionObject,
};
use self::inputs::keys_input::parse_keys_argument;
use self::inputs::order_input::parse_order_argument;
use crate::pagination::{build_query, page_to_connection};
use crate::query::data::{count_rows, fetch_single_row, fetch_single_row_with_joins, JoinConfig};
use crate::query::value_mapping_from_row;
use crate::types::{TypeMapping, ValueMapping};
use crate::utils::extract;
use torii_storage::Storage;

#[allow(missing_debug_implementations)]
pub enum ObjectVariant {
    Basic(Box<dyn BasicObject>),
    Resolvable(Box<dyn ResolvableObject>),
}

pub trait BasicObject: Send + Sync {
    // Name of the graphql object, singular and plural (eg "player" and "players")
    fn name(&self) -> (&str, &str);

    // Type name of the graphql object (eg "World__Player")
    fn type_name(&self) -> &str;

    // Type mapping defines the fields of the graphql object and their corresponding type
    fn type_mapping(&self) -> &TypeMapping;

    // Related field resolve to sibling graphql objects
    fn related_fields(&self) -> Option<Vec<Field>> {
        None
    }

    // Graphql objects that are created from the type mapping
    fn objects(&self) -> Vec<Object> {
        let mut object = Object::new(self.type_name());

        for (field_name, type_data) in self.type_mapping().clone() {
            let field = Field::new(field_name.to_string(), type_data.type_ref(), move |ctx| {
                let field_name = field_name.clone();

                FieldFuture::new(async move {
                    match ctx.parent_value.try_to_value() {
                        Ok(Value::Object(values)) => {
                            // safe unwrap
                            return Ok(Some(FieldValue::value(
                                values.get(&field_name).unwrap().clone(),
                            )));
                        }
                        // if the parent is `Value` then it must be a Object
                        Ok(_) => {
                            return Err(async_graphql::Error::new(
                                "incorrect value, requires Value::Object",
                            ))
                        }
                        _ => {}
                    };

                    // if its not we try to downcast to known types which is a special case for
                    // tokenBalances and tokenTransfers queries

                    if let Ok(values) = ctx
                        .parent_value
                        .try_downcast_ref::<Connection<ErcTokenType>>()
                    {
                        match field_name.as_str() {
                            "edges" => {
                                return Ok(Some(FieldValue::list(
                                    values
                                        .edges
                                        .iter()
                                        .map(FieldValue::borrowed_any)
                                        .collect::<Vec<FieldValue<'_>>>(),
                                )));
                            }
                            "pageInfo" => {
                                return Ok(Some(FieldValue::value(values.page_info.clone())));
                            }
                            "totalCount" => {
                                return Ok(Some(FieldValue::value(Value::from(
                                    values.total_count,
                                ))));
                            }
                            _ => {
                                return Err(async_graphql::Error::new(
                                    "incorrect value, requires Value::Object",
                                ))
                            }
                        }
                    }

                    if let Ok(values) = ctx
                        .parent_value
                        .try_downcast_ref::<ConnectionEdge<ErcTokenType>>()
                    {
                        match field_name.as_str() {
                            "node" => return Ok(Some(FieldValue::borrowed_any(&values.node))),
                            "cursor" => {
                                return Ok(Some(FieldValue::value(Value::String(
                                    values.cursor.clone(),
                                ))));
                            }
                            _ => {
                                return Err(async_graphql::Error::new(
                                    "incorrect value, requires Value::Object",
                                ))
                            }
                        }
                    }

                    if let Ok(values) = ctx
                        .parent_value
                        .try_downcast_ref::<Connection<TokenTransferNode>>()
                    {
                        match field_name.as_str() {
                            "edges" => {
                                return Ok(Some(FieldValue::list(
                                    values
                                        .edges
                                        .iter()
                                        .map(FieldValue::borrowed_any)
                                        .collect::<Vec<FieldValue<'_>>>(),
                                )));
                            }
                            "pageInfo" => {
                                return Ok(Some(FieldValue::value(values.page_info.clone())));
                            }
                            "totalCount" => {
                                return Ok(Some(FieldValue::value(Value::from(
                                    values.total_count,
                                ))));
                            }
                            _ => {
                                return Err(async_graphql::Error::new(
                                    "incorrect value, requires Value::Object",
                                ))
                            }
                        }
                    }

                    if let Ok(values) = ctx
                        .parent_value
                        .try_downcast_ref::<ConnectionEdge<TokenTransferNode>>()
                    {
                        match field_name.as_str() {
                            "node" => return Ok(Some(FieldValue::borrowed_any(&values.node))),
                            "cursor" => {
                                return Ok(Some(FieldValue::value(Value::String(
                                    values.cursor.clone(),
                                ))));
                            }
                            _ => {
                                return Err(async_graphql::Error::new(
                                    "incorrect value, requires Value::Object",
                                ))
                            }
                        }
                    }

                    if let Ok(values) = ctx.parent_value.try_downcast_ref::<TokenTransferNode>() {
                        match field_name.as_str() {
                            "from" => {
                                return Ok(Some(FieldValue::value(Value::String(
                                    values.from.clone(),
                                ))));
                            }
                            "to" => {
                                return Ok(Some(FieldValue::value(Value::String(
                                    values.to.clone(),
                                ))));
                            }
                            "executedAt" => {
                                return Ok(Some(FieldValue::value(Value::String(
                                    values.executed_at.clone(),
                                ))));
                            }
                            "tokenMetadata" => {
                                return Ok(Some(values.clone().token_metadata.to_field_value()));
                            }
                            "transactionHash" => {
                                return Ok(Some(FieldValue::value(Value::String(
                                    values.transaction_hash.clone(),
                                ))));
                            }
                            _ => {
                                return Err(async_graphql::Error::new(
                                    "incorrect value, requires Value::Object",
                                ))
                            }
                        }
                    }

                    if let Ok(values) = ctx.parent_value.try_downcast_ref::<ErcTokenType>() {
                        return Ok(Some(values.clone().to_field_value()));
                    }

                    Err(async_graphql::Error::new("unexpected parent value"))
                })
            });

            object = object.field(field);
        }

        // Add related graphql objects (eg event, system)
        if let Some(fields) = self.related_fields() {
            for field in fields {
                object = object.field(field);
            }
        }
        vec![object]
    }
}

pub trait ResolvableObject: BasicObject {
    // Resolvers that returns single and many objects
    fn resolvers(&self) -> Vec<Field>;

    // Resolves subscriptions, returns current object (eg "PlayerAdded")
    fn subscriptions(&self) -> Option<Vec<SubscriptionField>> {
        None
    }

    // Input objects consist of {type_name}WhereInput for filtering and {type_name}Order for
    // ordering
    fn input_objects(&self) -> Option<Vec<InputObject>> {
        None
    }

    // Enum objects
    fn enum_objects(&self) -> Option<Vec<Enum>> {
        None
    }

    // Connection type includes {type_name}Connection and {type_name}Edge according to relay spec https://relay.dev/graphql/connections.htm
    fn connection_objects(&self) -> Option<Vec<Object>> {
        let edge = EdgeObject::new(self.name().0.to_string(), self.type_name().to_string());
        let connection =
            ConnectionObject::new(self.name().0.to_string(), self.type_name().to_string());

        let mut objects = Vec::new();
        objects.extend(edge.objects());
        objects.extend(connection.objects());

        Some(objects)
    }
}

// Resolves single object queries, returns current object of type type_name (eg "Player")
pub fn resolve_one(
    table_name: &str,
    id_column: &str,
    field_name: &str,
    type_name: &str,
    type_mapping: &TypeMapping,
) -> Field {
    let type_mapping = type_mapping.clone();
    let table_name = table_name.to_owned();
    let id_column = id_column.to_owned();
    let argument = InputValue::new(
        id_column.to_case(Case::Camel),
        TypeRef::named_nn(TypeRef::ID),
    );

    Field::new(field_name, TypeRef::named_nn(type_name), move |ctx| {
        let type_mapping = type_mapping.clone();
        let table_name = table_name.to_owned();
        let id_column = id_column.to_owned();

        FieldFuture::new(async move {
            let storage = ctx.data::<Box<dyn Storage>>()?;
            let id: String =
                extract::<String>(ctx.args.as_index_map(), &id_column.to_case(Case::Camel))?;

            let query = build_query(&None, &None, &Default::default(), &None, None, false);
            let page = storage.entities(&query).await?;

            if let Some(entity) = page
                .items
                .into_iter()
                .find(|e| e.hashed_keys.to_hex() == id)
            {
                let mut model = ValueMapping::new();
                model.insert(
                    async_graphql::Name::new("id"),
                    Value::String(entity.hashed_keys.to_hex()),
                );
                Ok(Some(Value::Object(model)))
            } else {
                Ok(None)
            }
        })
    })
    .argument(argument)
}

// Resolves single object queries with joins, returns current object of type type_name with related
// data
pub fn resolve_one_with_joins(
    table_name: &str,
    id_column: &str,
    field_name: &str,
    type_name: &str,
    type_mapping: &TypeMapping,
    joins: Vec<JoinConfig>,
    select_columns: Option<Vec<String>>,
) -> Field {
    let type_mapping = type_mapping.clone();
    let table_name = table_name.to_owned();
    let id_column = id_column.to_owned();
    let joins = joins.to_owned();
    let select_columns = select_columns.to_owned();
    let argument = InputValue::new(
        id_column.to_case(Case::Camel),
        TypeRef::named_nn(TypeRef::ID),
    );

    Field::new(field_name, TypeRef::named_nn(type_name), move |ctx| {
        let type_mapping = type_mapping.clone();
        let table_name = table_name.to_owned();
        let id_column = id_column.to_owned();
        let joins = joins.to_owned();
        let select_columns = select_columns.to_owned();

        FieldFuture::new(async move {
            let storage = ctx.data::<Box<dyn Storage>>()?;
            let id: String =
                extract::<String>(ctx.args.as_index_map(), &id_column.to_case(Case::Camel))?;

            let query = build_query(&None, &None, &Default::default(), &None, None, false);
            let page = storage.entities(&query).await?;

            if let Some(entity) = page
                .items
                .into_iter()
                .find(|e| e.hashed_keys.to_hex() == id)
            {
                let mut model = ValueMapping::new();
                model.insert(
                    async_graphql::Name::new("id"),
                    Value::String(entity.hashed_keys.to_hex()),
                );
                Ok(Some(Value::Object(model)))
            } else {
                Ok(None)
            }
        })
    })
    .argument(argument)
}

// Resolves plural object queries, returns type of {type_name}Connection (eg "PlayerConnection")
pub fn resolve_many(
    table_name: &str,
    id_column: &str,
    field_name: &str,
    type_name: &str,
    type_mapping: &TypeMapping,
) -> Field {
    let type_mapping = type_mapping.clone();
    let table_name = table_name.to_owned();
    let id_column = id_column.to_owned();

    let mut field = Field::new(
        field_name,
        TypeRef::named(format!("{}Connection", type_name)),
        move |ctx| {
            let type_mapping = type_mapping.clone();
            let table_name = table_name.to_owned();
            let id_column = id_column.to_owned();

            FieldFuture::new(async move {
                let storage = ctx.data::<Box<dyn Storage>>()?;
                let connection = parse_connection_arguments(&ctx)?;
                let keys = parse_keys_argument(&ctx)?;
                let order = parse_order_argument(&ctx);

                let query = build_query(&keys, &None, &connection, &order, None, false);
                let page = storage.entities(&query).await?;
                let total_count = page.items.len() as i64;
                let (entities, page_info) = page_to_connection(page, &connection, total_count);

                let edges: Vec<Value> = entities
                    .into_iter()
                    .map(|entity| {
                        let cursor = entity.hashed_keys.to_hex();
                        let mut node = ValueMapping::new();
                        node.insert(
                            async_graphql::Name::new("id"),
                            Value::String(entity.hashed_keys.to_hex()),
                        );

                        let mut edge = ValueMapping::new();
                        edge.insert(async_graphql::Name::new("node"), Value::Object(node));
                        edge.insert(async_graphql::Name::new("cursor"), Value::String(cursor));
                        Value::Object(edge)
                    })
                    .collect();

                let connection_result = ValueMapping::from([
                    (
                        async_graphql::Name::new("totalCount"),
                        Value::from(total_count),
                    ),
                    (async_graphql::Name::new("edges"), Value::List(edges)),
                    (
                        async_graphql::Name::new("pageInfo"),
                        Value::Object(ValueMapping::from([
                            (
                                async_graphql::Name::new("hasNextPage"),
                                Value::from(page_info.has_next_page),
                            ),
                            (
                                async_graphql::Name::new("hasPreviousPage"),
                                Value::from(page_info.has_previous_page),
                            ),
                            (
                                async_graphql::Name::new("startCursor"),
                                Value::from(page_info.start_cursor.unwrap_or_default()),
                            ),
                            (
                                async_graphql::Name::new("endCursor"),
                                Value::from(page_info.end_cursor.unwrap_or_default()),
                            ),
                        ])),
                    ),
                ]);

                Ok(Some(Value::Object(connection_result)))
            })
        },
    );

    field = connection_arguments(field);

    field
}
