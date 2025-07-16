use dojo_types::naming::compute_selector_from_tag;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::str::FromStr;
use torii_proto::schema::Entity;
use torii_proto::{
    Clause, CompositeClause, LogicalOperator, MemberValue, OrderBy, OrderDirection, Page,
    Pagination,
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
use crate::utils::build_keys_pattern;
use crate::Sql;

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
        .bind(format!("{:#x}", selector))
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
            .bind(format!("{:#x}", self.selector))
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
    let hashed_keys = Felt::from_str(&row.get::<String, _>("id")).map_err(ParseError::FromStr)?;
    let model_ids = row
        .get::<String, _>("model_ids")
        .split(',')
        .map(|id| Felt::from_str(id).map_err(ParseError::FromStr))
        .collect::<Result<Vec<_>, _>>()?;

    let models = schemas
        .iter()
        .filter(|schema| model_ids.contains(&compute_selector_from_tag(&schema.name())))
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
                        bind_values.push(format!("{:#x}", id));
                        "?".to_string()
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                where_clauses.push(format!("({table}.id IN ({}))", ids));
            }
            Clause::Keys(keys) => {
                let keys_pattern = build_keys_pattern(keys);
                bind_values.push(keys_pattern);
                let model_selectors: Vec<String> = keys
                    .models
                    .iter()
                    .map(|model| format!("{:#x}", compute_selector_from_tag(model)))
                    .collect();

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
                    historical: bool,
                ) -> Result<String, Error> {
                    match value {
                        MemberValue::String(value) => {
                            bind_values.push(value.to_string());
                            Ok("?".to_string())
                        }
                        MemberValue::Primitive(value) => {
                            let value = if historical {
                                Ty::Primitive(*value).to_json_value()?.to_string()
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
                                .map(|v| prepare_comparison(v, bind_values, historical))
                                .collect::<Result<Vec<String>, Error>>()?
                                .join(", ")
                        )),
                    }
                }
                let value = prepare_comparison(&member.value, &mut bind_values, historical)?;

                let model = member.model.clone();
                let operator = member.operator.clone();

                if historical {
                    // For historical data, query the JSON data column
                    where_clauses.push(format!(
                        "CAST(JSON_EXTRACT({table}.data, '$.{}') AS TEXT) {operator} {value}",
                        member.member
                    ));
                } else {
                    // Use the column name directly since it's already flattened
                    where_clauses.push(format!(
                        "([{model}].[{}] {operator} {value})",
                        member.member
                    ));
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
    ) -> Result<Page<Entity>, Error> {
        let (where_clause, bind_values) =
            build_composite_clause(table, model_relation_table, composite, historical)?;

        let models = models
            .iter()
            .map(|model| compute_selector_from_tag(model))
            .collect::<Vec<_>>();
        let schemas = self
            .models(&models)
            .await?
            .iter()
            .map(|m| m.schema.clone())
            .collect::<Vec<_>>();

        let having_clause = models
            .iter()
            .map(|model| format!("INSTR(model_ids, '{:#x}') > 0", model))
            .collect::<Vec<_>>()
            .join(" OR ");

        let page = if historical {
            self.fetch_historical_entities(
                table,
                model_relation_table,
                &where_clause,
                &having_clause,
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
                    if having_clause.is_empty() {
                        None
                    } else {
                        Some(&having_clause)
                    },
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
        having_clause: &str,
        bind_values: Vec<String>,
        mut pagination: Pagination,
    ) -> Result<Page<torii_proto::schema::Entity>, Error> {
        use crate::query::{PaginationExecutor, QueryBuilder};

        if !pagination.order_by.is_empty() {
            return Err(QueryError::UnsupportedQuery(
                "Order by is not supported for historical entities".to_string(),
            )
            .into());
        }

        // Set default ordering for historical entities
        pagination.order_by.push(OrderBy {
            field: "event_id".to_string(),
            direction: OrderDirection::Asc,
        });

        let mut query_builder = QueryBuilder::new(table)
            .select(&[
                format!("{}.id", table),
                format!("{}.data", table),
                format!("{}.model_id", table),
                format!("{}.event_id", table),
                format!(
                    "group_concat({}.model_id) as model_ids",
                    model_relation_table
                ),
            ])
            .join(&format!(
                "JOIN {} ON {}.id = {}.entity_id",
                model_relation_table, table, model_relation_table
            ))
            .group_by(&format!("{}.event_id", table));

        // Add where clause if provided
        if !where_clause.is_empty() {
            query_builder = query_builder.where_clause(where_clause);
        }

        // Add bind values
        for value in bind_values {
            query_builder = query_builder.bind_value(value);
        }

        // Add having clause if provided
        if !having_clause.is_empty() {
            query_builder = query_builder.having(having_clause);
        }

        // Execute paginated query
        let executor = PaginationExecutor::new(self.pool.clone());
        let page = executor
            .execute_paginated_query(query_builder, &pagination)
            .await?;

        // Process the results to create Entity objects
        let entities = page
            .items
            .iter()
            .map(|row| async {
                let id: String = row.get("id");
                let data: String = row.get("data");
                let model_id: String = row.get("model_id");

                let hashed_keys = Felt::from_str(&id).map_err(ParseError::FromStr)?;
                let model = self
                    .cache
                    .as_ref()
                    .expect("Expected cache to be set")
                    .model(Felt::from_str(&model_id).map_err(ParseError::FromStr)?)
                    .await?;
                let mut schema = model.schema;
                schema.from_json_value(
                    serde_json::from_str(&data).map_err(ParseError::FromJsonStr)?,
                )?;

                Ok::<_, Error>(torii_proto::schema::Entity {
                    hashed_keys,
                    models: vec![schema.as_struct().unwrap().clone()],
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
        mut pagination: Pagination,
        bind_values: Vec<String>,
    ) -> Result<Page<SqliteRow>, Error> {
        use crate::query::{PaginationExecutor, QueryBuilder};

        pagination.order_by.push(OrderBy {
            field: "event_id".to_string(),
            direction: OrderDirection::Desc,
        });

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
                Ty::Array(_) | Ty::Primitive(_) | Ty::ByteArray(_) => {
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
            let mut query_builder = QueryBuilder::new(table_name);

            // Build selections
            let mut selections = vec![
                format!("{}.id", table_name),
                format!("{}.keys", table_name),
                format!("{}.event_id", table_name),
                format!(
                    "group_concat({}.model_id) as model_ids",
                    model_relation_table
                ),
            ];

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
                .join(&format!(
                    "JOIN {model_relation_table} ON {table_name}.id = {model_relation_table}.entity_id",
                ))
                .select(&selections)
                .group_by(&format!("{}.id", table_name));

            // Add where clause
            if let Some(where_clause) = where_clause {
                query_builder = query_builder.where_clause(where_clause);
            }

            // Add bind values
            for value in bind_values.iter() {
                query_builder = query_builder.bind_value(value.clone());
            }

            // Add having clause
            if let Some(having_clause) = having_clause {
                query_builder = query_builder.having(having_clause);
            }

            // Execute paginated query
            let page = executor
                .execute_paginated_query(query_builder, &pagination)
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
                .map(|k| format!("{:#x}", k))
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
