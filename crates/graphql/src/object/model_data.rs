use async_graphql::dynamic::{Enum, Field, FieldFuture, InputObject, Object, TypeRef};
use async_graphql::Value;
use dojo_types::naming::get_tag;
use dojo_types::schema::Ty;
use sqlx::{Pool, Sqlite};
use torii_storage::Storage;

use super::connection::{connection_arguments, connection_output, parse_connection_arguments};
use super::inputs::order_input::{order_argument, parse_order_argument, OrderInputObject};
use super::inputs::where_input::{parse_where_argument, where_argument, WhereInputObject};
use super::inputs::InputObjectTrait;
use super::{BasicObject, ResolvableObject, TypeMapping, ValueMapping};
use crate::constants::{
    ENTITY_ID_COLUMN, ENTITY_TABLE, ENTITY_TYPE_NAME, EVENT_MESSAGE_TABLE, EVENT_MESSAGE_TYPE_NAME,
    ID_COLUMN, INTERNAL_ENTITY_ID_KEY,
};
use crate::mapping::ENTITY_TYPE_MAPPING;
use crate::pagination::{build_query, page_to_connection};
use crate::query::data::{count_rows, fetch_multiple_rows, fetch_single_row};
use crate::query::value_mapping_from_row;
use crate::types::TypeData;
use crate::utils;

#[derive(Debug)]
pub struct ModelDataObject {
    pub name: String,
    pub plural_name: String,
    pub type_name: String,
    pub type_mapping: TypeMapping,
    pub schema: Ty,
    pub where_input: WhereInputObject,
    pub order_input: OrderInputObject,
}

impl ModelDataObject {
    pub fn new(name: String, type_name: String, type_mapping: TypeMapping, schema: Ty) -> Self {
        let where_input = WhereInputObject::new(type_name.as_str(), &type_mapping);
        let order_input = OrderInputObject::new(type_name.as_str(), &type_mapping);
        let plural_name = format!("{}Models", name);
        Self {
            name,
            plural_name,
            type_name,
            type_mapping,
            schema,
            where_input,
            order_input,
        }
    }
}

impl BasicObject for ModelDataObject {
    fn name(&self) -> (&str, &str) {
        (&self.name, &self.plural_name)
    }

    fn type_name(&self) -> &str {
        &self.type_name
    }

    fn type_mapping(&self) -> &TypeMapping {
        &self.type_mapping
    }

    fn objects(&self) -> Vec<Object> {
        let mut parts = self.type_name().split('_').collect::<Vec<&str>>();
        let model = parts.pop().unwrap();
        let namespace = parts.join("_");
        let type_name = get_tag(&namespace, model);
        let mut objects = data_objects_recursion(
            &TypeData::Nested((TypeRef::named(self.type_name()), self.type_mapping.clone())),
            &vec![type_name],
        );

        // root object requires entity_field association
        let mut root = objects.pop().unwrap();
        root = root.field(entity_field());
        root = root.field(event_message_field());

        objects.push(root);
        objects
    }
}

impl ResolvableObject for ModelDataObject {
    fn input_objects(&self) -> Option<Vec<InputObject>> {
        let mut objects = vec![];
        objects.extend(self.where_input.input_objects());
        objects.push(self.order_input.input_object());
        Some(objects)
    }

    fn enum_objects(&self) -> Option<Vec<Enum>> {
        self.order_input.enum_objects()
    }

    fn resolvers(&self) -> Vec<Field> {
        let type_name = self.type_name.clone();
        let type_mapping = self.type_mapping.clone();
        let where_mapping = self.where_input.type_mapping.clone();
        let field_type = format!("{}Connection", self.type_name());

        let mut field = Field::new(self.name().1, TypeRef::named(field_type), move |ctx| {
            let type_mapping = type_mapping.clone();
            let where_mapping = where_mapping.clone();
            let mut parts = type_name.split('_').collect::<Vec<&str>>();
            let model = parts.pop().unwrap();
            let namespace = parts.join("_");
            let table_name = get_tag(&namespace, model);

            FieldFuture::new(async move {
                let storage = ctx.data::<Box<dyn Storage>>()?;
                let order = parse_order_argument(&ctx);
                let filters = parse_where_argument(&ctx, &where_mapping)?;
                let connection = parse_connection_arguments(&ctx)?;

                let query = build_query(&None, &filters, &connection, &order, None, false);
                let page = storage.entities(&query).await?;
                let total_count = page.items.len() as i64;
                let (entities, page_info) = page_to_connection(page, &connection, total_count);

                let edges: Vec<Value> = entities
                    .into_iter()
                    .map(|entity| {
                        let cursor = entity.id.clone();
                        let mut node = ValueMapping::new();
                        node.insert("id".into(), Value::String(entity.id));

                        let mut edge = ValueMapping::new();
                        edge.insert("node".into(), Value::Object(node));
                        edge.insert("cursor".into(), Value::String(cursor));
                        Value::Object(edge)
                    })
                    .collect();

                let connection_result = ValueMapping::from([
                    ("totalCount".into(), Value::from(total_count)),
                    ("edges".into(), Value::List(edges)),
                    (
                        "pageInfo".into(),
                        Value::Object(ValueMapping::from([
                            ("hasNextPage".into(), Value::from(page_info.has_next_page)),
                            (
                                "hasPreviousPage".into(),
                                Value::from(page_info.has_previous_page),
                            ),
                            (
                                "startCursor".into(),
                                Value::from(page_info.start_cursor.unwrap_or_default()),
                            ),
                            (
                                "endCursor".into(),
                                Value::from(page_info.end_cursor.unwrap_or_default()),
                            ),
                        ])),
                    ),
                ]);

                Ok(Some(Value::Object(connection_result)))
            })
        });

        // Add relay connection fields (first, last, before, after, where)
        field = connection_arguments(field);
        field = where_argument(field, self.type_name());
        field = order_argument(field, self.type_name());

        vec![field]
    }
}

fn data_objects_recursion(type_data: &TypeData, path_array: &Vec<String>) -> Vec<Object> {
    let mut objects: Vec<Object> = vec![];

    match type_data {
        TypeData::Nested((nested_type, nested_mapping)) => {
            let nested_objects = nested_mapping.iter().flat_map(|(field_name, type_data)| {
                let mut nested_path = path_array.clone();
                nested_path.push(field_name.to_string());
                data_objects_recursion(type_data, &nested_path)
            });

            objects.extend(nested_objects);
            objects.push(object(
                &nested_type.to_string(),
                nested_mapping,
                path_array.clone(),
            ));
        }
        TypeData::List(inner) => {
            let nested_objects = data_objects_recursion(inner, path_array);

            objects.extend(nested_objects);
        }
        _ => {}
    }

    objects
}

pub fn object(type_name: &str, type_mapping: &TypeMapping, path_array: Vec<String>) -> Object {
    let mut object = Object::new(type_name);

    for (field_name, type_data) in type_mapping.clone() {
        let path_array = path_array.clone();

        let field = Field::new(field_name.to_string(), type_data.type_ref(), move |ctx| {
            let field_name = field_name.clone();
            let type_data = type_data.clone();
            let mut path_array = path_array.clone();

            // For nested types, we need to remove prefix in path array
            let namespace = format!("{}_", path_array[0]);
            path_array.push(field_name.to_string());
            let table_name = path_array.join("$").replace(&namespace, "");

            FieldFuture::new(async move {
                if let Some(value) = ctx.parent_value.as_value() {
                    // Nested types resolution
                    if let TypeData::Nested((_, nested_mapping)) = type_data {
                        return match ctx.parent_value.try_to_value()? {
                            Value::Object(indexmap) => {
                                let storage = ctx.data::<Box<dyn Storage>>()?;
                                let entity_id =
                                    utils::extract::<String>(indexmap, INTERNAL_ENTITY_ID_KEY)?;

                                // if we already fetched our model data, return it
                                if let Some(data) = indexmap.get(&field_name) {
                                    return Ok(Some(data.clone()));
                                }

                                let query = build_query(
                                    &None,
                                    &None,
                                    &Default::default(),
                                    &None,
                                    None,
                                    false,
                                );
                                let page = storage.entities(&query).await?;

                                if let Some(entity) =
                                    page.items.into_iter().find(|e| e.id == entity_id)
                                {
                                    let mut entity_data = ValueMapping::new();
                                    entity_data.insert("id".into(), Value::String(entity.id));
                                    Ok(Some(Value::Object(entity_data)))
                                } else {
                                    Ok(None)
                                }
                            }
                            _ => Err("incorrect value, requires Value::Object".into()),
                        };
                    }

                    // Simple types resolution
                    return match value {
                        Value::Object(value_mapping) => {
                            Ok(Some(value_mapping.get(&field_name).unwrap().clone()))
                        }
                        _ => Err("Incorrect value, requires Value::Object".into()),
                    };
                }

                // Catch model union resolutions, async-graphql sends union types as IndexMap<Name,
                // ConstValue>
                if let Some(value_mapping) = ctx.parent_value.downcast_ref::<ValueMapping>() {
                    return Ok(Some(value_mapping.get(&field_name).unwrap().clone()));
                }

                Err("Field resolver only accepts Value or IndexMap".into())
            })
        });

        object = object.field(field);
    }

    object
}

fn entity_field() -> Field {
    Field::new("entity", TypeRef::named(ENTITY_TYPE_NAME), |ctx| {
        FieldFuture::new(async move {
            match ctx.parent_value.try_to_value()? {
                Value::Object(indexmap) => {
                    let storage = ctx.data::<Box<dyn Storage>>()?;
                    let entity_id = utils::extract::<String>(indexmap, INTERNAL_ENTITY_ID_KEY)?;

                    let query = build_query(&None, &None, &Default::default(), &None, None, false);
                    let page = storage.entities(&query).await?;

                    if let Some(entity) = page.items.into_iter().find(|e| e.id == entity_id) {
                        let mut entity_data = ValueMapping::new();
                        entity_data.insert("id".into(), Value::String(entity.id));
                        Ok(Some(Value::Object(entity_data)))
                    } else {
                        Ok(None)
                    }
                }
                _ => Err("incorrect value, requires Value::Object".into()),
            }
        })
    })
}

fn event_message_field() -> Field {
    Field::new(
        "eventMessage",
        TypeRef::named(EVENT_MESSAGE_TYPE_NAME),
        |ctx| {
            FieldFuture::new(async move {
                match ctx.parent_value.try_to_value()? {
                    Value::Object(indexmap) => {
                        let storage = ctx.data::<Box<dyn Storage>>()?;
                        let entity_id = utils::extract::<String>(indexmap, INTERNAL_ENTITY_ID_KEY)?;

                        let query =
                            build_query(&None, &None, &Default::default(), &None, None, false);
                        let page = storage.event_messages(&query).await?;

                        if let Some(event_message_entity) =
                            page.items.into_iter().find(|e| e.id == entity_id)
                        {
                            let mut event_message = ValueMapping::new();
                            event_message
                                .insert("id".into(), Value::String(event_message_entity.id));
                            Ok(Some(Value::Object(event_message)))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Err("incorrect value, requires Value::Object".into()),
                }
            })
        },
    )
}
