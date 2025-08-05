use sqlx::{sqlite::SqliteRow, Pool, Sqlite};
use torii_proto::{OrderBy, OrderDirection, Page, Pagination, PaginationDirection};

use crate::{
    constants::SQL_DEFAULT_LIMIT,
    cursor::{build_cursor_conditions, build_cursor_values, decode_cursor_values, encode_cursor_values},
    error::Error,
};

#[derive(Debug)]
pub struct QueryBuilder {
    table_name: String,
    table_alias: String,
    selections: Vec<String>,
    joins: Vec<String>,
    where_conditions: Vec<String>,
    bind_values: Vec<String>,
    order_fields: Vec<String>,
    group_by: Option<String>,
    having_clause: Option<String>,
    limit: Option<u32>,
}

impl QueryBuilder {
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            table_alias: "".to_string(),
            selections: Vec::new(),
            joins: Vec::new(),
            where_conditions: Vec::new(),
            bind_values: Vec::new(),
            order_fields: Vec::new(),
            group_by: None,
            having_clause: None,
            limit: None,
        }
    }

    pub fn alias(mut self, alias: &str) -> Self {
        self.table_alias = alias.to_string();
        self
    }

    pub fn select(mut self, columns: &[String]) -> Self {
        self.selections.extend(columns.iter().cloned());
        self
    }

    pub fn join(mut self, join_clause: &str) -> Self {
        self.joins.push(join_clause.to_string());
        self
    }

    pub fn where_clause(mut self, condition: &str) -> Self {
        self.where_conditions.push(condition.to_string());
        self
    }

    pub fn bind_value(mut self, value: String) -> Self {
        self.bind_values.push(value);
        self
    }

    pub fn order_by(mut self, field: &str, direction: OrderDirection) -> Self {
        let direction_str = match direction {
            OrderDirection::Asc => "ASC",
            OrderDirection::Desc => "DESC",
        };
        self.order_fields
            .push(format!("{} {}", field, direction_str));
        self
    }

    pub fn group_by(mut self, field: &str) -> Self {
        self.group_by = Some(field.to_string());
        self
    }

    pub fn having(mut self, clause: &str) -> Self {
        self.having_clause = Some(clause.to_string());
        self
    }

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn build(self) -> String {
        let mut query = format!(
            "SELECT {} FROM [{}] {}",
            self.selections.join(", "),
            self.table_name,
            self.table_alias
        );

        if !self.joins.is_empty() {
            query.push_str(&format!(" {}", self.joins.join(" ")));
        }

        if !self.where_conditions.is_empty() {
            query.push_str(&format!(" WHERE {}", self.where_conditions.join(" AND ")));
        }

        if let Some(group_by) = &self.group_by {
            query.push_str(&format!(" GROUP BY {}", group_by));
        }

        if let Some(having) = &self.having_clause {
            query.push_str(&format!(" HAVING {}", having));
        }

        if !self.order_fields.is_empty() {
            query.push_str(&format!(" ORDER BY {}", self.order_fields.join(", ")));
        }

        if let Some(limit) = self.limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }

        query
    }

    pub fn bind_values(&self) -> &[String] {
        &self.bind_values
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[derive(Debug)]
pub struct PaginationExecutor {
    pool: Pool<Sqlite>,
}

impl PaginationExecutor {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }

    pub async fn execute_paginated_query(
        &self,
        mut query_builder: QueryBuilder,
        pagination: &Pagination,
        default_order_by: &OrderBy,
    ) -> Result<Page<SqliteRow>, Error> {
        let mut pagination = pagination.clone();
        pagination.order_by.push(default_order_by.clone());

        let original_limit = pagination.limit.unwrap_or(SQL_DEFAULT_LIMIT as u32);
        let fetch_limit = original_limit + 1;

        let cursor_values: Option<Vec<String>> = pagination
            .cursor
            .as_ref()
            .map(|cursor_str| decode_cursor_values(cursor_str))
            .transpose()?;

        let (cursor_conditions, cursor_binds) =
            build_cursor_conditions(&pagination, cursor_values.as_deref())?;

        for condition in cursor_conditions {
            query_builder = query_builder.where_clause(&condition);
        }

        for bind in cursor_binds {
            query_builder = query_builder.bind_value(bind);
        }

        for order_by in &pagination.order_by {
            let field = format!("[{}]", order_by.field);
            let direction = match (&order_by.direction, &pagination.direction) {
                (OrderDirection::Asc, PaginationDirection::Forward) => OrderDirection::Asc,
                (OrderDirection::Asc, PaginationDirection::Backward) => OrderDirection::Desc,
                (OrderDirection::Desc, PaginationDirection::Forward) => OrderDirection::Desc,
                (OrderDirection::Desc, PaginationDirection::Backward) => OrderDirection::Asc,
            };
            query_builder = query_builder.order_by(&field, direction);
        }

        query_builder = query_builder.limit(fetch_limit);

        let bind_values = query_builder.bind_values().to_vec();
        let query = query_builder.build();

        let mut stmt = sqlx::query(&query);
        for value in bind_values {
            stmt = stmt.bind(value);
        }

        let mut rows = stmt.fetch_all(&self.pool).await?;
        let has_more = rows.len() >= fetch_limit as usize;

        if pagination.direction == PaginationDirection::Backward {
            rows.reverse();
        }

        let mut next_cursor = None;

        if has_more {
            rows.truncate(original_limit as usize);
            if let Some(last_row) = rows.last() {
                let cursor_values = build_cursor_values(&pagination, last_row)?;
                next_cursor = Some(encode_cursor_values(&cursor_values)?);
            }
        }

        Ok(Page {
            items: rows,
            next_cursor,
        })
    }
}
