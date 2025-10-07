use chrono::{DateTime, Utc};
use dojo_types::naming::try_compute_selector_from_tag;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::str::FromStr;
use torii_proto::schema::Entity;
use torii_proto::{
    Clause, ComparisonOperator, CompositeClause, LogicalOperator, MemberValue, OrderBy,
    OrderDirection, Page, Pagination,
};
use torii_storage::ReadOnlyStorage;

use async_trait::async_trait;
use crypto_bigint::U256;
use dojo_types::primitive::{Primitive, PrimitiveError};
use dojo_types::schema::Ty;
use dojo_world::contracts::abigen::model::Layout;
use dojo_world::contracts::model::ModelReader;
use serde_json::Value as JsonValue;
use sqlx::sqlite::SqliteRow;
use sqlx::{Pool, Row, Sqlite};
use starknet::core::types::Felt;

use super::error::{self, Error};
use crate::constants::SQL_MAX_JOINS;
use crate::error::{ParseError, QueryError};
use crate::utils::{build_keys_pattern, felt_to_sql_string};
use crate::Sql;

/// Helper function to parse array index from field name like "field[0]"
fn parse_array_index(field_name: &str) -> Option<(String, usize)> {
    if let Some(start) = field_name.find('[') {
        if let Some(end) = field_name.find(']') {
            if start < end {
                let field = field_name[..start].to_string();
                let index_str = &field_name[start + 1..end];
                if let Ok(index) = index_str.parse::<usize>() {
                    return Some((field, index));
                }
            }
        }
    }
    None
}

/// Helper function to build array-specific SQL queries
fn build_array_query(
    table: &str,
    model: &str,
    field: &str,
    operator: &ComparisonOperator,
    value: &str,
    historical: bool,
) -> Result<String, Error> {
    let column_access = if historical {
        format!("JSON_EXTRACT({table}.data, '$.{field}')")
    } else {
        format!("[{model}].[{field}]")
    };

    match operator {
        ComparisonOperator::Contains => Ok(format!(
            "EXISTS (SELECT 1 FROM json_each({column_access}) WHERE value = {value})"
        )),
        ComparisonOperator::ContainsAll => {
            // For CONTAINS_ALL, we need to check that every value in the input list exists in the array
            // We can't use COUNT(DISTINCT) because the input might have duplicates
            // Instead, we'll use NOT EXISTS to check if any input value is missing from the array
            //
            // Create a subquery that checks if all input values exist in the array
            // We use a VALUES clause to create a temporary table of the input values
            // and check that none of them are missing from the array
            Ok(format!(
                "NOT EXISTS (SELECT 1 FROM (VALUES {value}) AS input_vals(val) WHERE NOT EXISTS (SELECT 1 FROM json_each({column_access}) WHERE value = input_vals.val))"
            ))
        }
        ComparisonOperator::ContainsAny => Ok(format!(
            "EXISTS (SELECT 1 FROM json_each({column_access}) WHERE value IN {value})"
        )),
        ComparisonOperator::ArrayLengthEq => {
            Ok(format!("json_array_length({column_access}) = {value}"))
        }
        ComparisonOperator::ArrayLengthGt => {
            Ok(format!("json_array_length({column_access}) > {value}"))
        }
        ComparisonOperator::ArrayLengthLt => {
            Ok(format!("json_array_length({column_access}) < {value}"))
        }
        _ => {
            // For non-array operations, fallback to the original behavior
            if historical {
                Ok(format!(
                    "CAST(JSON_EXTRACT({table}.data, '$.{field}') AS TEXT) {operator} {value}"
                ))
            } else {
                Ok(format!("([{model}].[{field}] {operator} {value})"))
            }
        }
    }
}

#[derive(Debug)]
pub struct ModelSQLReader {
    /// Namespace of the model
    namespace: String,
    /// The name of the model
    name: String,
    /// The selector of the model
    selector: Felt,
    /// The class hash of the model
    class_hash: Felt,
    /// The contract address of the model
    contract_address: Felt,
    pool: Pool<Sqlite>,
    packed_size: u32,
    unpacked_size: u32,
    layout: Layout,
}

impl ModelSQLReader {
    pub async fn new(selector: Felt, pool: Pool<Sqlite>) -> Result<Self, Error> {
        let (namespace, name, class_hash, contract_address, packed_size, unpacked_size, layout): (
            String,
            String,
            String,
            String,
            u32,
            u32,
            String,
        ) = sqlx::query_as(
            "SELECT namespace, name, class_hash, contract_address, packed_size, unpacked_size, \
             layout FROM models WHERE id = ?",
        )
        .bind(felt_to_sql_string(&selector))
        .fetch_one(&pool)
        .await?;

        let class_hash = Felt::from_hex(&class_hash).map_err(error::ParseError::FromStr)?;
        let contract_address =
            Felt::from_hex(&contract_address).map_err(error::ParseError::FromStr)?;

        let layout = serde_json::from_str(&layout).map_err(error::ParseError::FromJsonStr)?;

        Ok(Self {
            namespace,
            name,
            selector,
            class_hash,
            contract_address,
            pool,
            packed_size,
            unpacked_size,
            layout,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ModelReader<Error> for ModelSQLReader {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn selector(&self) -> Felt {
        self.selector
    }

    fn class_hash(&self) -> Felt {
        self.class_hash
    }

    fn contract_address(&self) -> Felt {
        self.contract_address
    }

    async fn schema(&self) -> Result<Ty, Error> {
        let schema: String = sqlx::query_scalar("SELECT schema FROM models WHERE id = ?")
            .bind(felt_to_sql_string(&self.selector))
            .fetch_one(&self.pool)
            .await?;

        Ok(serde_json::from_str(&schema).map_err(error::ParseError::FromJsonStr)?)
    }

    async fn packed_size(&self) -> Result<u32, Error> {
        Ok(self.packed_size)
    }

    async fn unpacked_size(&self) -> Result<u32, Error> {
        Ok(self.unpacked_size)
    }

    async fn layout(&self) -> Result<Layout, Error> {
        Ok(self.layout.clone())
    }

    async fn use_legacy_storage(&self) -> Result<bool, Error> {
        todo!()
    }
}

/// Populate the values of a Ty (schema) from SQLite row.
pub fn map_row_to_ty(
    path: &str,
    name: &str,
    ty: &mut Ty,
    // the row that contains non dynamic data for Ty
    row: &SqliteRow,
) -> Result<(), Error> {
    let column_name = if path.is_empty() {
        name
    } else {
        &format!("{}.{}", path, name)
    };

    match ty {
        Ty::Primitive(primitive) => {
            match &primitive {
                Primitive::I8(_) => {
                    let value = row.try_get::<i8, &str>(column_name)?;
                    primitive.set_i8(Some(value))?;
                }
                Primitive::I16(_) => {
                    let value = row.try_get::<i16, &str>(column_name)?;
                    primitive.set_i16(Some(value))?;
                }
                Primitive::I32(_) => {
                    let value = row.try_get::<i32, &str>(column_name)?;
                    primitive.set_i32(Some(value))?;
                }
                Primitive::I64(_) => {
                    let value = row.try_get::<i64, &str>(column_name)?;
                    primitive.set_i64(Some(value))?;
                }
                Primitive::I128(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    let hex_str = value.trim_start_matches("0x");

                    primitive.set_i128(Some(
                        u128::from_str_radix(hex_str, 16).map_err(ParseError::ParseIntError)?
                            as i128,
                    ))?;
                }
                Primitive::U8(_) => {
                    let value = row.try_get::<u8, &str>(column_name)?;
                    primitive.set_u8(Some(value))?;
                }
                Primitive::U16(_) => {
                    let value = row.try_get::<u16, &str>(column_name)?;
                    primitive.set_u16(Some(value))?;
                }
                Primitive::U32(_) => {
                    let value = row.try_get::<u32, &str>(column_name)?;
                    primitive.set_u32(Some(value))?;
                }
                Primitive::U64(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    let hex_str = value.trim_start_matches("0x");

                    if !hex_str.is_empty() {
                        primitive.set_u64(Some(
                            u64::from_str_radix(hex_str, 16).map_err(ParseError::ParseIntError)?,
                        ))?;
                    }
                }
                Primitive::U128(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    let hex_str = value.trim_start_matches("0x");

                    if !hex_str.is_empty() {
                        primitive.set_u128(Some(
                            u128::from_str_radix(hex_str, 16).map_err(ParseError::ParseIntError)?,
                        ))?;
                    }
                }
                Primitive::U256(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    let hex_str = value.trim_start_matches("0x");

                    if !hex_str.is_empty() {
                        primitive.set_u256(Some(U256::from_be_hex(hex_str)))?;
                    }
                }
                Primitive::Bool(_) => {
                    let value = row.try_get::<bool, &str>(column_name)?;
                    primitive.set_bool(Some(value))?;
                }
                Primitive::Felt252(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    if !value.is_empty() {
                        primitive.set_felt252(Some(
                            Felt::from_str(&value).map_err(ParseError::FromStr)?,
                        ))?;
                    }
                }
                Primitive::ClassHash(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    if !value.is_empty() {
                        primitive.set_class_hash(Some(
                            Felt::from_str(&value).map_err(ParseError::FromStr)?,
                        ))?;
                    }
                }
                Primitive::ContractAddress(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    if !value.is_empty() {
                        primitive.set_contract_address(Some(
                            Felt::from_str(&value).map_err(ParseError::FromStr)?,
                        ))?;
                    }
                }
                Primitive::EthAddress(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    if !value.is_empty() {
                        primitive.set_eth_address(Some(
                            Felt::from_str(&value).map_err(ParseError::FromStr)?,
                        ))?;
                    }
                }
            };
        }
        Ty::Enum(enum_ty) => {
            let option_name = row.try_get::<String, &str>(column_name)?;
            if !option_name.is_empty() {
                enum_ty.set_option(&option_name)?;
            }

            for option in &mut enum_ty.options {
                if option.name != option_name {
                    continue;
                }

                map_row_to_ty(column_name, &option.name, &mut option.ty, row)?;
            }
        }
        Ty::Struct(struct_ty) => {
            for member in &mut struct_ty.children {
                map_row_to_ty(column_name, &member.name, &mut member.ty, row)?;
            }
        }
        Ty::Tuple(ty) => {
            for (i, member) in ty.iter_mut().enumerate() {
                map_row_to_ty(column_name, &i.to_string(), member, row)?;
            }
        }
        Ty::Array(ty) => {
            let schema = ty[0].clone();
            let serialized_array = row.try_get::<String, &str>(column_name)?;
            if serialized_array.is_empty() {
                *ty = vec![];
                return Ok(());
            }

            let values: Vec<JsonValue> =
                serde_json::from_str(&serialized_array).map_err(ParseError::FromJsonStr)?;
            *ty = values
                .iter()
                .map(|v| {
                    let mut ty = schema.clone();
                    ty.from_json_value(v.clone())?;
                    Result::<_, PrimitiveError>::Ok(ty)
                })
                .collect::<Result<Vec<Ty>, _>>()?;
        }
        Ty::FixedSizeArray((ty, array_size)) => {
            let schema = ty[0].clone();
            let serialized_array = row.try_get::<String, &str>(column_name)?;
            if serialized_array.is_empty() {
                *ty = vec![];
                return Ok(());
            }

            // Fixed size array is stored as json object: {"elements": Vec<JsonValue>, "size": u32}
            //
            // see Sql::set_entity_model
            let value: serde_json::Value =
                serde_json::from_str(&serialized_array).map_err(ParseError::FromJsonStr)?;

            let elems = value["elements"].as_array().ok_or_else(|| {
                ParseError::InvalidFixedSizeArray("Missing 'elements' field".to_string())
            })?;
            let serialized_size = value["size"].as_u64().ok_or_else(|| {
                ParseError::InvalidFixedSizeArray("Missing 'size' field".to_string())
            })? as u32;

            // sanity check
            debug_assert_eq!(*array_size, serialized_size);

            *ty = elems
                .iter()
                .map(|v| {
                    let mut ty = schema.clone();
                    ty.from_json_value(v.clone())?;
                    Result::<_, PrimitiveError>::Ok(ty)
                })
                .collect::<Result<Vec<Ty>, _>>()?;
        }
        Ty::ByteArray(bytearray) => {
            let value = row.try_get::<String, &str>(column_name)?;
            *bytearray = value;
        }
    };

    Ok(())
}

fn map_row_to_entity(
    row: &SqliteRow,
    schemas: &[Ty],
    dont_include_hashed_keys: bool,
) -> Result<Entity, Error> {
    let hashed_keys =
        Felt::from_str(&row.get::<String, _>("entity_id")).map_err(ParseError::FromStr)?;
    let created_at = row.get::<DateTime<Utc>, _>("created_at");
    let updated_at = row.get::<DateTime<Utc>, _>("updated_at");
    let executed_at = row.get::<DateTime<Utc>, _>("executed_at");
    let model_ids = row
        .get::<String, _>("model_ids")
        .split(',')
        .map(|id| Felt::from_str(id).map_err(ParseError::FromStr))
        .collect::<Result<Vec<_>, _>>()?;

    let models = schemas
        .iter()
        .try_fold(Vec::new(), |mut acc, schema| {
            let selector = try_compute_selector_from_tag(&schema.name())
                .map_err(|_| QueryError::InvalidNamespacedModel(schema.name().to_string()))?;

            if model_ids.contains(&selector) {
                acc.push(schema);
            }

            Ok::<Vec<&dojo_types::schema::Ty>, Error>(acc)
        })?
        .into_iter()
        .map(|schema| {
            let mut ty = schema.clone();
            map_row_to_ty("", &schema.name(), &mut ty, row)?;
            Ok(ty.as_struct().unwrap().clone())
        })
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(Entity {
        hashed_keys: if !dont_include_hashed_keys {
            hashed_keys
        } else {
            Felt::ZERO
        },
        models,
        created_at,
        updated_at,
        executed_at,
    })
}

// builds a composite clause for a query
fn build_composite_clause(
    table: &str,
    model_relation_table: &str,
    composite: &torii_proto::CompositeClause,
    historical: bool,
) -> Result<(String, Vec<String>), Error> {
    let is_or = composite.operator == LogicalOperator::Or;
    let mut where_clauses = Vec::new();
    let mut bind_values = Vec::new();

    for clause in &composite.clauses {
        match clause {
            Clause::HashedKeys(hashed_keys) => {
                let ids = hashed_keys
                    .iter()
                    .map(|id| {
                        bind_values.push(felt_to_sql_string(id));
                        "?".to_string()
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                where_clauses.push(format!("({table}.id IN ({}))", ids));
            }
            Clause::Keys(keys) => {
                let keys_pattern = build_keys_pattern(keys);
                bind_values.push(keys_pattern);
                let model_selectors: Vec<String> =
                    keys.models.iter().try_fold(Vec::new(), |mut acc, model| {
                        let selector = try_compute_selector_from_tag(model)
                            .map_err(|_| QueryError::InvalidNamespacedModel(model.to_string()))?;
                        acc.push(felt_to_sql_string(&selector));
                        Ok::<Vec<String>, Error>(acc)
                    })?;

                if model_selectors.is_empty() {
                    where_clauses.push(format!("({table}.keys REGEXP ?)"));
                } else {
                    // Add bind value placeholders for each model selector
                    let placeholders = vec!["?"; model_selectors.len()].join(", ");
                    where_clauses.push(format!(
                        "(({table}.keys REGEXP ? AND {model_relation_table}.model_id IN ({})) OR \
                         {model_relation_table}.model_id NOT IN ({}))",
                        placeholders, placeholders
                    ));
                    // Add each model selector twice (once for IN and once for NOT IN)
                    bind_values.extend(model_selectors.clone());
                    bind_values.extend(model_selectors);
                }
            }
            Clause::Member(member) => {
                fn prepare_comparison(
                    value: &MemberValue,
                    bind_values: &mut Vec<String>,
                    json_format: bool,
                ) -> Result<String, Error> {
                    match value {
                        MemberValue::String(value) => {
                            bind_values.push(value.to_string());
                            Ok("?".to_string())
                        }
                        MemberValue::Primitive(value) => {
                            let value = if json_format {
                                Ty::Primitive(*value)
                                    .to_json_value()?
                                    .to_string()
                                    .replace("\"", "")
                            } else {
                                value.to_sql_value()
                            };
                            bind_values.push(value);
                            Ok("?".to_string())
                        }
                        MemberValue::List(values) => Ok(format!(
                            "({})",
                            values
                                .iter()
                                .map(|v| prepare_comparison(v, bind_values, json_format))
                                .collect::<Result<Vec<String>, Error>>()?
                                .join(", ")
                        )),
                    }
                }

                let model = member.model.clone();
                let operator = member.operator.clone();

                // Determine if we need JSON formatting for values
                let array_index = parse_array_index(&member.member);
                let is_array_operation = matches!(
                    operator,
                    ComparisonOperator::Contains
                        | ComparisonOperator::ContainsAll
                        | ComparisonOperator::ContainsAny
                        | ComparisonOperator::ArrayLengthEq
                        | ComparisonOperator::ArrayLengthGt
                        | ComparisonOperator::ArrayLengthLt
                ) || array_index.is_some(); // Array indexing also needs JSON formatting

                let value =
                    prepare_comparison(&member.value, &mut bind_values, is_array_operation)?;

                // Check if this field has array indexing syntax like "field[0]"
                if let Some((field_name, index)) = array_index {
                    // Handle array element access
                    let column_access = if historical {
                        format!("JSON_EXTRACT({table}.data, '$.{field_name}')")
                    } else {
                        format!("[{model}].[{field_name}]")
                    };

                    let query =
                        format!("json_extract({column_access}, '$[{index}]') {operator} {value}");
                    where_clauses.push(query);
                } else if matches!(
                    operator,
                    ComparisonOperator::Contains
                        | ComparisonOperator::ContainsAll
                        | ComparisonOperator::ContainsAny
                        | ComparisonOperator::ArrayLengthEq
                        | ComparisonOperator::ArrayLengthGt
                        | ComparisonOperator::ArrayLengthLt
                ) {
                    // Use array-specific query building
                    let query = build_array_query(
                        table,
                        &model,
                        &member.member,
                        &operator,
                        &value,
                        historical,
                    )?;
                    where_clauses.push(query);
                } else {
                    // Regular field handling
                    if historical {
                        where_clauses.push(format!(
                            "CAST(JSON_EXTRACT({table}.data, '$.{}') AS TEXT) {operator} {value}",
                            member.member
                        ));
                    } else {
                        where_clauses.push(format!(
                            "([{model}].[{}] {operator} {value})",
                            member.member
                        ));
                    }
                }
            }
            Clause::Composite(nested) => {
                // Handle nested composite by recursively building the clause
                let (nested_where, nested_values) =
                    build_composite_clause(table, model_relation_table, nested, historical)?;

                if !nested_where.is_empty() {
                    where_clauses.push(nested_where);
                }
                bind_values.extend(nested_values);
            }
        }
    }

    let where_clause = if !where_clauses.is_empty() {
        where_clauses.join(if is_or { " OR " } else { " AND " })
    } else {
        String::new()
    };

    Ok((where_clause, bind_values))
}

impl Sql {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn query_by_composite(
        &self,
        table: &str,
        model_relation_table: &str,
        entity_relation_column: &str,
        composite: &CompositeClause,
        pagination: Pagination,
        no_hashed_keys: bool,
        models: Vec<String>,
        historical: bool,
        world_address: Option<Felt>,
    ) -> Result<Page<Entity>, Error> {
        let models = models.iter().try_fold(Vec::new(), |mut acc, model| {
            let selector = try_compute_selector_from_tag(model)
                .map_err(|_| QueryError::InvalidNamespacedModel(model.to_string()))?;
            acc.push(selector);
            Ok::<Vec<Felt>, Error>(acc)
        })?;

        let schemas = self
            .models(world_address, &models)
            .await?
            .iter()
            .map(|m| m.schema.clone())
            .collect::<Vec<_>>();

        let (mut where_clause, mut bind_values) =
            build_composite_clause(table, model_relation_table, composite, historical)?;

        if let Some(world_address) = world_address {
            where_clause = format!("({} AND {}.world_address = ?)", where_clause, table);
            bind_values.push(felt_to_sql_string(&world_address));
        }

        // Convert model Felts to hex strings for SQL binding
        let model_selectors: Vec<String> = models.iter().map(felt_to_sql_string).collect();

        if !model_selectors.is_empty() {
            let placeholders = vec!["?"; model_selectors.len()].join(", ");
            where_clause = format!(
                "({} AND {}.model_id IN ({}))",
                where_clause, model_relation_table, placeholders
            );
            bind_values.extend(model_selectors);
        }

        let page = if historical {
            self.fetch_historical_entities(
                table,
                model_relation_table,
                &where_clause,
                bind_values,
                pagination,
            )
            .await?
        } else {
            let page = self
                .fetch_entities(
                    &schemas,
                    table,
                    model_relation_table,
                    entity_relation_column,
                    if where_clause.is_empty() {
                        None
                    } else {
                        Some(&where_clause)
                    },
                    None, // No HAVING needed - filtered by WHERE
                    pagination,
                    bind_values,
                )
                .await?;
            Page {
                items: page
                    .items
                    .par_iter()
                    .map(|row| map_row_to_entity(row, &schemas, no_hashed_keys))
                    .collect::<Result<Vec<_>, Error>>()?,
                next_cursor: page.next_cursor,
            }
        };

        Ok(page)
    }

    async fn fetch_historical_entities(
        &self,
        table: &str,
        model_relation_table: &str,
        where_clause: &str,
        bind_values: Vec<String>,
        pagination: Pagination,
    ) -> Result<Page<torii_proto::schema::Entity>, Error> {
        use crate::query::{PaginationExecutor, QueryBuilder};

        let mut query_builder = QueryBuilder::new(table)
            .select(&[
                format!("{}.world_address", table),
                format!("{}.id", table),
                format!("{}.entity_id", table),
                format!("{}.data", table),
                format!("{}.model_id", table),
                format!("{}.event_id", table),
                format!("{}.created_at", table),
                format!("{}.updated_at", table),
                format!("{}.executed_at", table),
                format!("group_concat({model_relation_table}.model_id) as model_ids"),
            ])
            .join(&format!(
                "JOIN {model_relation_table} ON {table}.id = {model_relation_table}.entity_id",
            ))
            .group_by(&format!("{table}.event_id"));

        // Add user where clause if provided (applies to already-filtered set)
        if !where_clause.is_empty() {
            query_builder = query_builder.where_clause(where_clause);
        }

        // Add user bind values
        for value in bind_values {
            query_builder = query_builder.bind_value(value);
        }

        // Execute paginated query
        let executor = PaginationExecutor::new(self.pool.clone());
        let page = executor
            .execute_paginated_query(
                query_builder,
                &pagination,
                &OrderBy {
                    field: "event_id".to_string(),
                    direction: OrderDirection::Asc,
                },
            )
            .await?;

        // Process the results to create Entity objects
        let entities = page
            .items
            .iter()
            .map(|row| async {
                let entity_id: String = row.get("entity_id");
                let data: String = row.get("data");
                let model_id: Felt = Felt::from_str(&row.get::<String, _>("model_id"))
                    .map_err(ParseError::FromStr)?;
                let created_at: DateTime<Utc> = row.get("created_at");
                let updated_at: DateTime<Utc> = row.get("updated_at");
                let executed_at: DateTime<Utc> = row.get("executed_at");
                let world_address: Felt = Felt::from_str(&row.get::<String, _>("world_address"))
                    .map_err(ParseError::FromStr)?;

                let hashed_keys = Felt::from_str(&entity_id).map_err(ParseError::FromStr)?;
                let model = self
                    .cache
                    .as_ref()
                    .expect("Expected cache to be set")
                    .model(world_address, model_id)
                    .await?;
                let mut schema = model.schema;
                schema.from_json_value(
                    serde_json::from_str(&data).map_err(ParseError::FromJsonStr)?,
                )?;

                Ok::<_, Error>(torii_proto::schema::Entity {
                    hashed_keys,
                    models: vec![schema.as_struct().unwrap().clone()],
                    created_at,
                    updated_at,
                    executed_at,
                })
            })
            .collect::<Vec<_>>();

        // Execute all the async mapping operations concurrently
        let entities: Vec<torii_proto::schema::Entity> =
            futures::future::try_join_all(entities).await?;

        Ok(Page {
            items: entities,
            next_cursor: page.next_cursor,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_entities(
        &self,
        schemas: &[Ty],
        table_name: &str,
        model_relation_table: &str,
        entity_relation_column: &str,
        where_clause: Option<&str>,
        having_clause: Option<&str>,
        pagination: Pagination,
        bind_values: Vec<String>,
    ) -> Result<Page<SqliteRow>, Error> {
        use crate::query::{PaginationExecutor, QueryBuilder};

        // Helper function to collect columns
        fn collect_columns(table_prefix: &str, path: &str, ty: &Ty, selections: &mut Vec<String>) {
            match ty {
                Ty::Struct(s) => {
                    for child in &s.children {
                        let new_path = if path.is_empty() {
                            child.name.clone()
                        } else {
                            format!("{}.{}", path, child.name)
                        };
                        collect_columns(table_prefix, &new_path, &child.ty, selections);
                    }
                }
                Ty::Tuple(t) => {
                    for (i, child) in t.iter().enumerate() {
                        let new_path = if path.is_empty() {
                            format!("{}", i)
                        } else {
                            format!("{}.{}", path, i)
                        };
                        collect_columns(table_prefix, &new_path, child, selections);
                    }
                }
                Ty::Enum(e) => {
                    selections.push(format!(
                        "[{table_prefix}].[{path}] as \"{table_prefix}.{path}\"",
                    ));

                    for option in &e.options {
                        if let Ty::Tuple(t) = &option.ty {
                            if t.is_empty() {
                                continue;
                            }
                        }
                        let variant_path = format!("{}.{}", path, option.name);
                        collect_columns(table_prefix, &variant_path, &option.ty, selections);
                    }
                }
                // These are all simple fields. Which means their children, if present, are stored within the same column.
                // Like for primitive types, and types like Array where they are serialized into a JSON object.
                _ => {
                    selections.push(format!(
                        "[{table_prefix}].[{path}] as \"{table_prefix}.{path}\"",
                    ));
                }
            }
        }

        // Process schemas in chunks
        let mut all_rows = Vec::new();
        let mut next_cursor = None;
        let mut has_more_pages = false;
        let executor = PaginationExecutor::new(self.pool.clone());

        for chunk in schemas.chunks(SQL_MAX_JOINS) {
            // Strategy: Start from model_relation_table and use index hints for optimal performance
            // The composite index (model_id, entity_id) dramatically reduces the scan size
            // when filtering by model_id on a large entity set
            let mut query_builder = QueryBuilder::new(model_relation_table);

            // Build selections
            let mut selections = vec![
                format!("{table_name}.world_address"),
                format!("{table_name}.id"),
                format!("{table_name}.entity_id"),
                format!("{table_name}.keys"),
                format!("{table_name}.event_id"),
                format!("{table_name}.created_at"),
                format!("{table_name}.updated_at"),
                format!("{table_name}.executed_at"),
                format!("group_concat({model_relation_table}.model_id) as model_ids"),
            ];

            // Join to entities table - SQLite will use idx_entities_event_id_id for ordering
            query_builder = query_builder.join(&format!(
                "JOIN {table_name} ON {model_relation_table}.entity_id = {table_name}.id",
            ));

            // Add schema joins and collect columns
            for model in chunk {
                let model_table = model.name();
                query_builder = query_builder.join(&format!(
                    "LEFT JOIN [{model_table}] ON {table_name}.id = \
                     [{model_table}].{entity_relation_column}",
                ));
                collect_columns(&model_table, "", model, &mut selections);
            }

            query_builder = query_builder
                .select(&selections)
                .group_by(&format!("{}.id", table_name));

            // Add user where clause
            if let Some(where_clause) = where_clause {
                // Combine with existing where clause using AND
                query_builder = query_builder.where_clause(&format!("({})", where_clause));
            }

            // Add user bind values
            for value in bind_values.iter() {
                query_builder = query_builder.bind_value(value.clone());
            }

            // Add having clause
            if let Some(having_clause) = having_clause {
                query_builder = query_builder.having(having_clause);
            }

            // Execute paginated query
            let page = executor
                .execute_paginated_query(
                    query_builder,
                    &pagination,
                    &OrderBy {
                        field: "event_id".to_string(),
                        direction: OrderDirection::Desc,
                    },
                )
                .await?;

            let has_more = page.next_cursor.is_some();
            if has_more {
                has_more_pages = true;
                next_cursor = page.next_cursor;
            }

            all_rows.extend(page.items);

            // If we have more results than requested, stop processing chunks
            if has_more {
                break;
            }
        }

        Ok(Page {
            items: all_rows,
            next_cursor: if has_more_pages { next_cursor } else { None },
        })
    }
}

// Helper functions

#[cfg(test)]
mod tests {
    use super::*;
    use starknet::core::types::Felt;
    use torii_proto::{
        Clause, ComparisonOperator, CompositeClause, KeysClause, LogicalOperator, MemberClause,
        MemberValue,
    };

    #[test]
    fn test_build_composite_clause_hashed_keys() {
        let hashed_keys = vec![Felt::ONE, Felt::TWO];
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::HashedKeys(hashed_keys.clone())],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "(entities.id IN (?, ?))");
        assert_eq!(
            bind_values,
            hashed_keys
                .iter()
                .map(felt_to_sql_string)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_build_composite_clause_keys_no_models() {
        let keys = KeysClause {
            keys: vec![Some(Felt::ONE), Some(Felt::TWO)],
            pattern_matching: torii_proto::PatternMatching::FixedLen,
            models: vec![],
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Keys(keys)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "(entities.keys REGEXP ?)");
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_keys_with_models() {
        let keys = KeysClause {
            keys: vec![Some(Felt::ONE), Some(Felt::TWO)],
            pattern_matching: torii_proto::PatternMatching::FixedLen,
            models: vec!["ns-Player".to_string(), "ns-Position".to_string()],
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Keys(keys)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert!(where_clause.contains("entities.keys REGEXP ?"));
        assert!(where_clause.contains("entity_model.model_id IN"));
        assert!(where_clause.contains("entity_model.model_id NOT IN"));
        assert_eq!(bind_values.len(), 5); // keys pattern + 2 model selectors + 2 model selectors again
    }

    #[test]
    fn test_build_composite_clause_member_non_historical() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "score".to_string(),
            operator: ComparisonOperator::Eq,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "([Player].[score] = ?)");
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_member_historical() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "score".to_string(),
            operator: ComparisonOperator::Eq,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, true).unwrap();

        assert!(where_clause.contains("CAST(JSON_EXTRACT(entities.data, '$.score') AS TEXT) = ?"));
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_member_string_value() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "name".to_string(),
            operator: ComparisonOperator::Eq,
            value: MemberValue::String("Alice".to_string()),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "([Player].[name] = ?)");
        assert_eq!(bind_values, vec!["Alice".to_string()]);
    }

    #[test]
    fn test_build_composite_clause_member_list_value() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "level".to_string(),
            operator: ComparisonOperator::In,
            value: MemberValue::List(vec![
                MemberValue::Primitive(Primitive::U32(Some(1))),
                MemberValue::Primitive(Primitive::U32(Some(2))),
                MemberValue::Primitive(Primitive::U32(Some(3))),
            ]),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "([Player].[level] IN (?, ?, ?))");
        assert_eq!(bind_values.len(), 3);
    }

    #[test]
    fn test_build_composite_clause_or_operator() {
        let member1 = MemberClause {
            model: "Player".to_string(),
            member: "score".to_string(),
            operator: ComparisonOperator::Gt,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let member2 = MemberClause {
            model: "Player".to_string(),
            member: "level".to_string(),
            operator: ComparisonOperator::Lt,
            value: MemberValue::Primitive(Primitive::U32(Some(5))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::Or,
            clauses: vec![Clause::Member(member1), Clause::Member(member2)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert!(where_clause.contains("([Player].[score] > ?)"));
        assert!(where_clause.contains("([Player].[level] < ?)"));
        assert!(where_clause.contains(" OR "));
        assert_eq!(bind_values.len(), 2);
    }

    #[test]
    fn test_build_composite_clause_and_operator() {
        let member1 = MemberClause {
            model: "Player".to_string(),
            member: "score".to_string(),
            operator: ComparisonOperator::Gt,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let member2 = MemberClause {
            model: "Player".to_string(),
            member: "level".to_string(),
            operator: ComparisonOperator::Lt,
            value: MemberValue::Primitive(Primitive::U32(Some(5))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member1), Clause::Member(member2)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert!(where_clause.contains("([Player].[score] > ?)"));
        assert!(where_clause.contains("([Player].[level] < ?)"));
        assert!(where_clause.contains(" AND "));
        assert_eq!(bind_values.len(), 2);
    }

    #[test]
    fn test_build_composite_clause_nested() {
        let inner_composite = CompositeClause {
            operator: LogicalOperator::Or,
            clauses: vec![
                Clause::Member(MemberClause {
                    model: "Player".to_string(),
                    member: "score".to_string(),
                    operator: ComparisonOperator::Gt,
                    value: MemberValue::Primitive(Primitive::U32(Some(100))),
                }),
                Clause::Member(MemberClause {
                    model: "Player".to_string(),
                    member: "level".to_string(),
                    operator: ComparisonOperator::Gt,
                    value: MemberValue::Primitive(Primitive::U32(Some(10))),
                }),
            ],
        };

        let outer_composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![
                Clause::Composite(inner_composite),
                Clause::Member(MemberClause {
                    model: "Player".to_string(),
                    member: "active".to_string(),
                    operator: ComparisonOperator::Eq,
                    value: MemberValue::Primitive(Primitive::Bool(Some(true))),
                }),
            ],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &outer_composite, false).unwrap();

        assert!(where_clause.contains("([Player].[score] > ?)"));
        assert!(where_clause.contains("([Player].[level] > ?)"));
        assert!(where_clause.contains("([Player].[active] = ?)"));
        assert!(where_clause.contains(" OR "));
        assert!(where_clause.contains(" AND "));
        assert_eq!(bind_values.len(), 3);
    }

    #[test]
    fn test_build_composite_clause_empty() {
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "");
        assert_eq!(bind_values.len(), 0);
    }

    #[test]
    fn test_build_composite_clause_array_index() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "scores[0]".to_string(), // Array indexing syntax
            operator: ComparisonOperator::Eq,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "json_extract([Player].[scores], '$[0]') = ?");
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_array_index_historical() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "inventory[2]".to_string(), // Array indexing syntax
            operator: ComparisonOperator::Neq,
            value: MemberValue::String("sword".to_string()),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, true).unwrap();

        assert_eq!(
            where_clause,
            "json_extract(JSON_EXTRACT(entities.data, '$.inventory'), '$[2]') != ?"
        );
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_parse_array_index() {
        // Test valid cases
        assert_eq!(
            parse_array_index("field[0]"),
            Some(("field".to_string(), 0))
        );
        assert_eq!(
            parse_array_index("scores[42]"),
            Some(("scores".to_string(), 42))
        );
        assert_eq!(
            parse_array_index("inventory[999]"),
            Some(("inventory".to_string(), 999))
        );

        // Test invalid cases
        assert_eq!(parse_array_index("field"), None);
        assert_eq!(parse_array_index("field[]"), None);
        assert_eq!(parse_array_index("field[abc]"), None);
        assert_eq!(parse_array_index("field[0"), None);
        assert_eq!(parse_array_index("field0]"), None);
    }

    #[test]
    fn test_build_composite_clause_array_contains() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "scores".to_string(),
            operator: ComparisonOperator::Contains,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(
            where_clause,
            "EXISTS (SELECT 1 FROM json_each([Player].[scores]) WHERE value = ?)"
        );
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_array_contains_all() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "scores".to_string(),
            operator: ComparisonOperator::ContainsAll,
            value: MemberValue::List(vec![
                MemberValue::Primitive(Primitive::U32(Some(100))),
                MemberValue::Primitive(Primitive::U32(Some(200))),
                MemberValue::Primitive(Primitive::U32(Some(300))),
            ]),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "NOT EXISTS (SELECT 1 FROM (VALUES (?, ?, ?)) AS input_vals(val) WHERE NOT EXISTS (SELECT 1 FROM json_each([Player].[scores]) WHERE value = input_vals.val))");
        assert_eq!(bind_values.len(), 3); // The list values are used once
    }

    #[test]
    fn test_build_composite_clause_array_contains_all_with_duplicates() {
        // Test CONTAINS_ALL with duplicate values in the search list
        let member = MemberClause {
            model: "Player".to_string(),
            member: "scores".to_string(),
            operator: ComparisonOperator::ContainsAll,
            value: MemberValue::List(vec![
                MemberValue::Primitive(Primitive::U32(Some(100))),
                MemberValue::Primitive(Primitive::U32(Some(100))), // Duplicate
                MemberValue::Primitive(Primitive::U32(Some(200))),
            ]),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        // Should still work correctly even with duplicate values in the search criteria
        assert_eq!(where_clause, "NOT EXISTS (SELECT 1 FROM (VALUES (?, ?, ?)) AS input_vals(val) WHERE NOT EXISTS (SELECT 1 FROM json_each([Player].[scores]) WHERE value = input_vals.val))");
        assert_eq!(bind_values.len(), 3); // Still 3 bind values even with duplicates
    }

    #[test]
    fn test_build_composite_clause_array_contains_any() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "inventory".to_string(),
            operator: ComparisonOperator::ContainsAny,
            value: MemberValue::List(vec![
                MemberValue::String("sword".to_string()),
                MemberValue::String("shield".to_string()),
            ]),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(
            where_clause,
            "EXISTS (SELECT 1 FROM json_each([Player].[inventory]) WHERE value IN (?, ?))"
        );
        assert_eq!(bind_values.len(), 2);
    }

    #[test]
    fn test_build_composite_clause_array_length_eq() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "skills".to_string(),
            operator: ComparisonOperator::ArrayLengthEq,
            value: MemberValue::Primitive(Primitive::U32(Some(5))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "json_array_length([Player].[skills]) = ?");
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_array_length_gt() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "achievements".to_string(),
            operator: ComparisonOperator::ArrayLengthGt,
            value: MemberValue::Primitive(Primitive::U32(Some(10))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(
            where_clause,
            "json_array_length([Player].[achievements]) > ?"
        );
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_array_length_lt() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "buffs".to_string(),
            operator: ComparisonOperator::ArrayLengthLt,
            value: MemberValue::Primitive(Primitive::U32(Some(3))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "json_array_length([Player].[buffs]) < ?");
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_array_operators_historical() {
        let member = MemberClause {
            model: "Player".to_string(),
            member: "scores".to_string(),
            operator: ComparisonOperator::Contains,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, true).unwrap();

        assert_eq!(where_clause, "EXISTS (SELECT 1 FROM json_each(JSON_EXTRACT(entities.data, '$.scores')) WHERE value = ?)");
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_mixed_array_and_regular() {
        let array_member = MemberClause {
            model: "Player".to_string(),
            member: "scores".to_string(),
            operator: ComparisonOperator::Contains,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let regular_member = MemberClause {
            model: "Player".to_string(),
            member: "name".to_string(),
            operator: ComparisonOperator::Eq,
            value: MemberValue::String("Alice".to_string()),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(array_member), Clause::Member(regular_member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert!(where_clause
            .contains("EXISTS (SELECT 1 FROM json_each([Player].[scores]) WHERE value = ?)"));
        assert!(where_clause.contains("([Player].[name] = ?)"));
        assert!(where_clause.contains(" AND "));
        assert_eq!(bind_values.len(), 2);
    }

    #[test]
    fn test_build_composite_clause_array_index_out_of_bounds() {
        // Test what happens with array indexing when index might be out of bounds
        let member = MemberClause {
            model: "Player".to_string(),
            member: "scores[999]".to_string(), // Very high index
            operator: ComparisonOperator::Eq,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        // SQLite json_extract returns NULL for out-of-bounds access
        // This should still generate valid SQL, but will return NULL = 100 (which is false)
        assert_eq!(
            where_clause,
            "json_extract([Player].[scores], '$[999]') = ?"
        );
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_array_index_with_null_check() {
        // Test array indexing with IS NOT NULL to handle out-of-bounds
        let member = MemberClause {
            model: "Player".to_string(),
            member: "scores[0]".to_string(),
            operator: ComparisonOperator::Neq,
            value: MemberValue::String("null".to_string()),
        };
        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![Clause::Member(member)],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert_eq!(where_clause, "json_extract([Player].[scores], '$[0]') != ?");
        assert_eq!(bind_values.len(), 1);
    }

    #[test]
    fn test_build_composite_clause_mixed_types() {
        let hashed_keys = vec![Felt::ONE];
        let member = MemberClause {
            model: "Player".to_string(),
            member: "score".to_string(),
            operator: ComparisonOperator::Eq,
            value: MemberValue::Primitive(Primitive::U32(Some(100))),
        };
        let keys = KeysClause {
            keys: vec![Some(Felt::ONE)],
            pattern_matching: torii_proto::PatternMatching::FixedLen,
            models: vec![],
        };

        let composite = CompositeClause {
            operator: LogicalOperator::And,
            clauses: vec![
                Clause::HashedKeys(hashed_keys),
                Clause::Member(member),
                Clause::Keys(keys),
            ],
        };

        let (where_clause, bind_values) =
            build_composite_clause("entities", "entity_model", &composite, false).unwrap();

        assert!(where_clause.contains("(entities.id IN (?))"));
        assert!(where_clause.contains("([Player].[score] = ?)"));
        assert!(where_clause.contains("(entities.keys REGEXP ?)"));
        assert!(where_clause.contains(" AND "));
        assert_eq!(bind_values.len(), 3);
    }
}
