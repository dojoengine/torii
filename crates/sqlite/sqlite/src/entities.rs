use std::collections::HashSet;
use std::str::FromStr;

use dojo_types::schema::Ty;
use futures_util::future::try_join_all;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use starknet_crypto::Felt;
use torii_proto::schema::Entity;

use crate::cursor::{build_cursor_conditions, build_cursor_values, decode_cursor, encode_cursor};
use crate::utils::{build_query, combine_where_clauses, map_row_to_entity};
use crate::{error::Error, Sql};
use crate::constants::{SQL_DEFAULT_LIMIT, SQL_MAX_JOINS};
use crate::error::{ParseError, QueryError};
use torii_proto::{OrderDirection, Page, Pagination, PaginationDirection};

impl Sql {
    #[allow(clippy::too_many_arguments)]
    pub async fn entities(
        &self,
        schemas: &[Ty],
        table_name: &str,
        model_relation_table: &str,
        entity_relation_column: &str,
        where_clause: Option<&str>,
        having_clause: Option<&str>,
        pagination: Pagination,
        bind_values: Vec<String>,
    ) -> Result<Page<Entity>, Error> {
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
            .map_err(|e: Error| Error::QueryError(QueryError::InvalidCursor(e.to_string())))?;

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

            let mut rows = stmt.fetch_all(&self.pool).await?;
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

        let entities: Vec<Entity> = all_rows
            .par_iter()
            .map(|row| map_row_to_entity(row, schemas))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            items: entities,
            next_cursor,
        })
    }
    
    async fn fetch_historical_entities(
        &self,
        table: &str,
        model_relation_table: &str,
        where_clause: &str,
        having_clause: &str,
        mut bind_values: Vec<String>,
        pagination: Pagination,
    ) -> Result<Page<Entity>, Error> {
        if !pagination.order_by.is_empty() {
            return Err(QueryError::UnsupportedQuery(
                "Order by is not supported for historical entities".to_string(),
            )
            .into());
        }

        let mut conditions = Vec::new();
        if !where_clause.is_empty() {
            conditions.push(where_clause.to_string());
        }

        let order_direction = match pagination.direction {
            PaginationDirection::Forward => "ASC",
            PaginationDirection::Backward => "DESC",
        };

        // Add cursor condition if present
        if let Some(ref cursor) = pagination.cursor {
            let decoded_cursor = decode_cursor(cursor)?;

            let operator = match pagination.direction {
                PaginationDirection::Forward => ">=",
                PaginationDirection::Backward => "<=",
            };
            conditions.push(format!("{table}.event_id {operator} ?"));
            bind_values.push(decoded_cursor);
        }

        let where_clause = if !conditions.is_empty() {
            format!("WHERE {}", conditions.join(" AND "))
        } else {
            String::new()
        };

        let limit = pagination.limit.unwrap_or(SQL_DEFAULT_LIMIT as u32);
        let query_limit = limit + 1;

        let query_str = format!(
            "SELECT {table}.id, {table}.data, {table}.model_id, {table}.event_id, \
             group_concat({model_relation_table}.model_id) as model_ids
            FROM {table}
            JOIN {model_relation_table} ON {table}.id = {model_relation_table}.entity_id
            {where_clause}
            GROUP BY {table}.event_id
            {}
            ORDER BY {table}.event_id {order_direction}
            LIMIT ?
            ",
            if !having_clause.is_empty() {
                format!("HAVING {}", having_clause)
            } else {
                String::new()
            }
        );

        let mut query = sqlx::query_as(&query_str);
        for value in bind_values {
            query = query.bind(value);
        }
        query = query.bind(query_limit);

        let db_entities: Vec<(String, String, String, String, String)> =
            query.fetch_all(&self.pool).await?;

        let has_more = db_entities.len() == query_limit as usize;
        let results_to_take = if has_more {
            limit as usize
        } else {
            db_entities.len()
        };

        let entities = db_entities
            .iter()
            .take(results_to_take)
            .map(|(id, data, model_id, _, _)| async {
                let hashed_keys = Felt::from_str(id)
                    .map_err(ParseError::FromStr)?;
                let model = self
                    .model_cache
                    .model(&Felt::from_str(model_id).map_err(ParseError::FromStr)?)
                    .await?;
                let mut schema = model.schema;
                schema.from_json_value(
                    serde_json::from_str(data).map_err(ParseError::FromJsonStr)?,
                )?;

                Ok::<_, Error>(Entity {
                    hashed_keys,
                    models: vec![schema.as_struct().unwrap().clone().into()],
                })
            })
            // Collect the futures into a Vec
            .collect::<Vec<_>>();

        // Execute all the async mapping operations concurrently
        let entities: Vec<Entity> = try_join_all(entities).await?;

        let next_cursor = if has_more {
            db_entities
                .last()
                .map(|(_, _, _, event_id, _)| encode_cursor(event_id))
                .transpose()?
        } else {
            None
        };

        Ok(Page {
            items: entities,
            next_cursor,
        })
    }
}
