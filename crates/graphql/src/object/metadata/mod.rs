use async_graphql::connection::PageInfo;
use async_graphql::dynamic::{Field, FieldFuture, TypeRef};
use async_graphql::{Name, Value};
use convert_case::{Case, Casing};
use sqlx::sqlite::SqliteRow;
use sqlx::{Pool, Row, Sqlite};
use torii_storage::Storage;

use super::connection::page_info::PageInfoObject;
use super::connection::{connection_arguments, cursor, parse_connection_arguments};
use super::{BasicObject, ResolvableObject};
use crate::constants::{
    ID_COLUMN, JSON_COLUMN, METADATA_NAMES, METADATA_TABLE, METADATA_TYPE_NAME,
};
use crate::mapping::METADATA_TYPE_MAPPING;
use crate::pagination::{build_query, page_to_connection};
use crate::query::data::{count_rows, fetch_multiple_rows, fetch_world_address};
use crate::query::value_mapping_from_row;
use crate::types::{TypeMapping, ValueMapping};

pub mod content;
pub mod social;

#[derive(Debug)]
pub struct MetadataObject;

impl MetadataObject {
    fn row_types(&self) -> TypeMapping {
        let mut row_types = self.type_mapping().clone();
        row_types.swap_remove("worldAddress");
        row_types
    }
}

impl BasicObject for MetadataObject {
    fn name(&self) -> (&str, &str) {
        METADATA_NAMES
    }

    fn type_name(&self) -> &str {
        METADATA_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &METADATA_TYPE_MAPPING
    }
}

impl ResolvableObject for MetadataObject {
    fn resolvers(&self) -> Vec<Field> {
        let row_types = self.row_types();

        let mut field = Field::new(
            self.name().1,
            TypeRef::named(format!("{}Connection", self.type_name())),
            move |ctx| {
                let row_types = row_types.clone();

                FieldFuture::new(async move {
                    let storage = ctx.data::<Box<dyn Storage>>()?;
                    let connection = parse_connection_arguments(&ctx)?;

                    let query = build_query(&None, &None, &connection, &None, None, false);
                    let page = storage.entities(&query).await?;
                    let total_count = page.items.len() as i64;
                    let (entities, page_info) = page_to_connection(page, &connection, total_count);

                    let edges: Vec<Value> = entities
                        .into_iter()
                        .map(|entity| {
                            let cursor = entity.hashed_keys.to_hex();
                            let mut node = ValueMapping::new();
                            node.insert(
                                Name::new("id"),
                                Value::String(entity.hashed_keys.to_hex()),
                            );

                            let mut edge = ValueMapping::new();
                            edge.insert(Name::new("node"), Value::Object(node));
                            edge.insert(Name::new("cursor"), Value::String(cursor));
                            Value::Object(edge)
                        })
                        .collect();

                    let connection_result = ValueMapping::from([
                        (Name::new("totalCount"), Value::from(total_count)),
                        (Name::new("edges"), Value::List(edges)),
                        (
                            Name::new("pageInfo"),
                            Value::Object(ValueMapping::from([
                                (
                                    Name::new("hasNextPage"),
                                    Value::from(page_info.has_next_page),
                                ),
                                (
                                    Name::new("hasPreviousPage"),
                                    Value::from(page_info.has_previous_page),
                                ),
                                (
                                    Name::new("startCursor"),
                                    Value::from(page_info.start_cursor.unwrap_or_default()),
                                ),
                                (
                                    Name::new("endCursor"),
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

        vec![field]
    }
}

// NOTE: need to generalize `connection_output` or maybe preprocess to support both predefined
// objects AND dynamic model objects
fn metadata_connection_output(
    data: &[SqliteRow],
    row_types: &TypeMapping,
    total_count: i64,
    page_info: PageInfo,
    world_address: &String,
) -> sqlx::Result<ValueMapping> {
    let edges = data
        .iter()
        .map(|row| {
            let order = row.try_get::<String, &str>(ID_COLUMN)?;
            let cursor = cursor::encode(&order, &order);
            let mut value_mapping = value_mapping_from_row(row, row_types, false, true)?;
            value_mapping.insert(Name::new("worldAddress"), Value::from(world_address));

            let json_str = row.try_get::<String, &str>(JSON_COLUMN)?;
            let serde_value = serde_json::from_str(&json_str).unwrap_or_default();

            let content = ValueMapping::from([
                extract_str_mapping("name", &serde_value),
                extract_str_mapping("description", &serde_value),
                extract_str_mapping("website", &serde_value),
                extract_str_mapping("icon_uri", &serde_value),
                extract_str_mapping("cover_uri", &serde_value),
                extract_socials_mapping("socials", &serde_value),
            ]);

            value_mapping.insert(Name::new("content"), Value::Object(content));

            let edge = ValueMapping::from([
                (Name::new("node"), Value::Object(value_mapping)),
                (Name::new("cursor"), Value::String(cursor)),
            ]);

            Ok(Value::Object(edge))
        })
        .collect::<sqlx::Result<Vec<Value>>>();

    Ok(ValueMapping::from([
        (Name::new("totalCount"), Value::from(total_count)),
        (Name::new("edges"), Value::List(edges?)),
        (Name::new("pageInfo"), PageInfoObject::value(page_info)),
    ]))
}

fn extract_str_mapping(name: &str, serde_value: &serde_json::Value) -> (Name, Value) {
    let name_lower_camel = name.to_case(Case::Camel);
    if let Some(serde_json::Value::String(str)) = serde_value.get(name) {
        (Name::new(name_lower_camel), Value::String(str.to_owned()))
    } else {
        (Name::new(name_lower_camel), Value::Null)
    }
}

fn extract_socials_mapping(name: &str, serde_value: &serde_json::Value) -> (Name, Value) {
    if let Some(serde_json::Value::Object(obj)) = serde_value.get(name) {
        let list = obj
            .iter()
            .map(|(social_name, social_url)| {
                Value::Object(ValueMapping::from([
                    (Name::new("name"), Value::String(social_name.to_string())),
                    (
                        Name::new("url"),
                        Value::String(social_url.as_str().unwrap().to_string()),
                    ),
                ]))
            })
            .collect::<Vec<Value>>();

        return (Name::new(name), Value::List(list));
    }

    (Name::new(name), Value::List(vec![]))
}
