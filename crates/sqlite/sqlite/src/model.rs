use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use std::collections::HashSet;
use std::io::prelude::*;
use std::str::FromStr;

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
use crate::constants::{SQL_DEFAULT_LIMIT, SQL_MAX_JOINS};
use crate::error::{ParseError, QueryError};
use crate::types::{OrderDirection, Page, Pagination, PaginationDirection};

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

#[allow(clippy::too_many_arguments)]
pub async fn fetch_entities(
    pool: &Pool<sqlx::Sqlite>,
    schemas: &[Ty],
    table_name: &str,
    model_relation_table: &str,
    entity_relation_column: &str,
    where_clause: Option<&str>,
    having_clause: Option<&str>,
    pagination: Pagination,
    bind_values: Vec<String>,
) -> Result<Page<SqliteRow>, Error> {
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

    let original_limit = pagination.limit.unwrap_or(SQL_DEFAULT_LIMIT as u32);
    let fetch_limit = original_limit + 1;
    let mut has_more_pages = false;

    // Build order by clause with proper model joining
    let order_by_models: HashSet<String> = pagination
        .order_by
        .iter()
        .map(|ob| ob.model.clone())
        .collect();

    let order_clause = if pagination.order_by.is_empty() {
        format!("{table_name}.event_id DESC")
    } else {
        pagination
            .order_by
            .iter()
            .map(|ob| {
                let direction = match (&ob.direction, &pagination.direction) {
                    (OrderDirection::Asc, PaginationDirection::Forward) => "ASC",
                    (OrderDirection::Asc, PaginationDirection::Backward) => "DESC",
                    (OrderDirection::Desc, PaginationDirection::Forward) => "DESC",
                    (OrderDirection::Desc, PaginationDirection::Backward) => "ASC",
                };
                format!("[{}].[{}] {direction}", ob.model, ob.member)
            })
            .chain(std::iter::once(format!("{table_name}.event_id DESC")))
            .collect::<Vec<_>>()
            .join(", ")
    };

    // Parse cursor
    let cursor_values: Option<Vec<String>> = pagination
        .cursor
        .as_ref()
        .map(|cursor_str| {
            let decompressed_str = decode_cursor(cursor_str)?;
            Ok(decompressed_str.split('/').map(|s| s.to_string()).collect())
        })
        .transpose()
        .map_err(|e: Error| Error::Query(QueryError::InvalidCursor(e.to_string())))?;

    // Build cursor conditions
    let (cursor_conditions, cursor_binds) =
        build_cursor_conditions(&pagination, cursor_values.as_deref(), table_name)?;

    // Combine WHERE clauses
    let combined_where = combine_where_clauses(where_clause, &cursor_conditions);

    // Process schemas in chunks
    let mut all_rows = Vec::new();
    let mut next_cursor = None;

    for chunk in schemas.chunks(SQL_MAX_JOINS) {
        let mut selections = vec![
            format!("{}.id", table_name),
            format!("{}.keys", table_name),
            format!("{}.event_id", table_name),
            format!(
                "group_concat({}.model_id) as model_ids",
                model_relation_table
            ),
        ];
        let mut joins = Vec::new();

        // Add schema joins
        for model in chunk {
            let model_table = model.name();
            let join_type = if order_by_models.contains(&model_table) {
                "INNER"
            } else {
                "LEFT"
            };
            joins.push(format!(
                "{join_type} JOIN [{model_table}] ON {table_name}.id = \
                 [{model_table}].{entity_relation_column}",
            ));
            collect_columns(&model_table, "", model, &mut selections);
        }

        joins.push(format!(
            "JOIN {model_relation_table} ON {table_name}.id = {model_relation_table}.entity_id",
        ));

        // Build and execute query
        let query = build_query(
            &selections,
            table_name,
            &joins,
            &combined_where,
            having_clause,
            &order_clause,
        );

        let mut stmt = sqlx::query(&query);
        for value in bind_values.iter().chain(cursor_binds.iter()) {
            stmt = stmt.bind(value);
        }

        stmt = stmt.bind(fetch_limit);

        let mut rows = stmt.fetch_all(pool).await?;
        let has_more = rows.len() >= fetch_limit as usize;

        if pagination.direction == PaginationDirection::Backward {
            rows.reverse();
        }
        if has_more {
            // mark that there are more pages beyond the limit
            has_more_pages = true;
            rows.truncate(original_limit as usize);
        }

        all_rows.extend(rows);
        if has_more {
            break;
        }
    }

    // Helper functions
    // Replace generation of next cursor to only when there are more pages
    if has_more_pages {
        if let Some(last_row) = all_rows.last() {
            let cursor_values_str = build_cursor_values(&pagination, last_row)?.join("/");
            next_cursor = Some(encode_cursor(&cursor_values_str)?);
        }
    }

    Ok(Page {
        items: all_rows,
        next_cursor,
    })
}

// Helper functions
fn build_cursor_conditions(
    pagination: &Pagination,
    cursor_values: Option<&[String]>,
    table_name: &str,
) -> Result<(Vec<String>, Vec<String>), Error> {
    let mut conditions = Vec::new();
    let mut binds = Vec::new();

    if let Some(values) = cursor_values {
        let expected_len = if pagination.order_by.is_empty() {
            1
        } else {
            pagination.order_by.len() + 1
        };
        if values.len() != expected_len {
            return Err(Error::Query(QueryError::InvalidCursor(
                "Invalid cursor values length".to_string(),
            )));
        }

        if pagination.order_by.is_empty() {
            let operator = if pagination.direction == PaginationDirection::Forward {
                "<"
            } else {
                ">"
            };
            conditions.push(format!("{}.event_id {} ?", table_name, operator));
            binds.push(values[0].clone());
        } else {
            for (i, (ob, val)) in pagination.order_by.iter().zip(values).enumerate() {
                let operator = match (&ob.direction, &pagination.direction) {
                    (OrderDirection::Asc, PaginationDirection::Forward) => ">",
                    (OrderDirection::Asc, PaginationDirection::Backward) => "<",
                    (OrderDirection::Desc, PaginationDirection::Forward) => "<",
                    (OrderDirection::Desc, PaginationDirection::Backward) => ">",
                };

                let condition = if i == 0 {
                    format!("[{}.{}] {} ?", ob.model, ob.member, operator)
                } else {
                    let prev = (0..i)
                        .map(|j| {
                            let prev_ob = &pagination.order_by[j];
                            format!("[{}.{}] = ?", prev_ob.model, prev_ob.member)
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    format!("({} AND [{}.{}] {} ?)", prev, ob.model, ob.member, operator)
                };
                conditions.push(condition);
                binds.push(val.clone());
            }
            let operator = if pagination.direction == PaginationDirection::Forward {
                "<"
            } else {
                ">"
            };
            conditions.push(format!("{}.event_id {} ?", table_name, operator));
            binds.push(values.last().unwrap().clone());
        }
    }
    Ok((conditions, binds))
}

fn combine_where_clauses(base: Option<&str>, cursor_conditions: &[String]) -> String {
    let mut parts = Vec::new();
    if let Some(base_where) = base {
        parts.push(base_where.to_string());
    }
    parts.extend(cursor_conditions.iter().cloned());
    parts.join(" AND ")
}

fn build_query(
    selections: &[String],
    table_name: &str,
    joins: &[String],
    where_clause: &str,
    having_clause: Option<&str>,
    order_clause: &str,
) -> String {
    let mut query = format!(
        "SELECT {} FROM [{}] {}",
        selections.join(", "),
        table_name,
        joins.join(" ")
    );
    if !where_clause.is_empty() {
        query.push_str(&format!(" WHERE {}", where_clause));
    }

    query.push_str(&format!(" GROUP BY {}.id", table_name));

    if let Some(having) = having_clause {
        query.push_str(&format!(" HAVING {}", having));
    }
    query.push_str(&format!(" ORDER BY {} LIMIT ?", order_clause));
    query
}

fn build_cursor_values(pagination: &Pagination, row: &SqliteRow) -> Result<Vec<String>, Error> {
    if pagination.order_by.is_empty() {
        Ok(vec![row.try_get("event_id")?])
    } else {
        let mut values = Vec::new();
        for ob in &pagination.order_by {
            let col = format!("{}.{}", ob.model, ob.member);
            // Try as String first
            match row.try_get::<String, &str>(&col) {
                Ok(val) => values.push(val),
                Err(_) => {
                    // Try as i64 (INTEGER)
                    match row.try_get::<i64, &str>(&col) {
                        Ok(val) => values.push(val.to_string()),
                        Err(e) => {
                            return Err(Error::Query(QueryError::InvalidCursor(format!(
                                "Could not extract cursor value for column {}: {}",
                                col, e
                            ))));
                        }
                    }
                }
            }
        }
        values.push(row.try_get("event_id")?);
        Ok(values)
    }
}

/// Compresses a string using Deflate and then encodes it using Base64 (no padding).
pub fn encode_cursor(value: &str) -> Result<String, Error> {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(value.as_bytes()).map_err(|e| {
        Error::Query(QueryError::InvalidCursor(format!(
            "Cursor compression error: {}",
            e
        )))
    })?;
    let compressed_bytes = encoder.finish().map_err(|e| {
        Error::Query(QueryError::InvalidCursor(format!(
            "Cursor compression finish error: {}",
            e
        )))
    })?;

    Ok(BASE64_URL_SAFE_NO_PAD.encode(&compressed_bytes))
}

/// Decodes a Base64 (no padding) string and then decompresses it using Deflate.
pub fn decode_cursor(encoded_cursor: &str) -> Result<String, Error> {
    let compressed_cursor_bytes = BASE64_URL_SAFE_NO_PAD.decode(encoded_cursor).map_err(|e| {
        Error::Query(QueryError::InvalidCursor(format!(
            "Base64 decode error: {}",
            e
        )))
    })?;

    let mut decoder = DeflateDecoder::new(&compressed_cursor_bytes[..]);
    let mut decompressed_str = String::new();
    decoder.read_to_string(&mut decompressed_str).map_err(|e| {
        Error::Query(QueryError::InvalidCursor(format!(
            "Decompression error: {}",
            e
        )))
    })?;

    Ok(decompressed_str)
}
