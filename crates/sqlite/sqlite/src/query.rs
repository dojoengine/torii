use sqlx::{sqlite::SqliteRow, SqlitePool};
use torii_proto::{OrderDirection, Page, Pagination, PaginationDirection};

use crate::{
    cursor::{build_cursor_conditions, build_cursor_values, decode_cursor, encode_cursor},
    error::{Error, QueryError},
};

/// A builder for constructing SQL queries dynamically.
pub struct QueryBuilder {
    selections: Vec<String>,
    from_table: Option<String>,
    joins: Vec<String>,
    where_clauses: Vec<String>,
    bind_values: Vec<String>,
    group_by: Option<String>,
    having_clauses: Vec<String>,
    order_by: Vec<String>,
    limit: Option<u32>,
    offset: Option<u32>,
    cursor_values: Vec<String>,
    pagination: Option<Pagination>,
}

impl QueryBuilder {
    /// Creates a new, empty `QueryBuilder`.
    pub fn new() -> Self {
        Self {
            selections: Vec::new(),
            from_table: None,
            joins: Vec::new(),
            where_clauses: Vec::new(),
            bind_values: Vec::new(),
            group_by: None,
            having_clauses: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            cursor_values: Vec::new(),
            pagination: None,
        }
    }

    /// Adds a column or expression to the SELECT clause.
    pub fn select(&mut self, selection: &str) -> &mut Self {
        self.selections.push(selection.to_string());
        self
    }

    /// Sets the table for the FROM clause.
    pub fn from(&mut self, table: &str) -> &mut Self {
        self.from_table = Some(table.to_string());
        self
    }

    /// Adds a JOIN clause.
    pub fn join(&mut self, join_clause: &str) -> &mut Self {
        self.joins.push(join_clause.to_string());
        self
    }

    /// Adds a WHERE condition with associated bind values.
    pub fn add_where(&mut self, condition: &str, bind_values: Vec<String>) -> &mut Self {
        self.where_clauses.push(condition.to_string());
        self.bind_values.extend(bind_values);
        self
    }

    /// Sets the GROUP BY clause.
    pub fn group_by(&mut self, group_by: &str) -> &mut Self {
        self.group_by = Some(group_by.to_string());
        self
    }

    /// Adds a HAVING condition.
    pub fn having(&mut self, condition: &str) -> &mut Self {
        self.having_clauses.push(condition.to_string());
        self
    }

    /// Sets the LIMIT for the query.
    pub fn limit(&mut self, limit: u32) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the OFFSET for the query.
    pub fn offset(&mut self, offset: u32) -> &mut Self {
        self.offset = Some(offset);
        self
    }

    /// Sets the ORDER BY clause.
    pub fn order_by(&mut self, order: &str) -> &mut Self {
        self.order_by.push(order.to_string());
        self
    }

    /// Builds the SQL query string up to the LIMIT clause.
    pub fn build(&self) -> String {
        let mut query = format!(
            "SELECT {} FROM [{}]",
            if self.selections.is_empty() {
                "*".to_string()
            } else {
                self.selections.join(", ")
            },
            self.from_table.as_ref().expect("FROM table must be set")
        );
        if !self.joins.is_empty() {
            query.push_str(&format!(" {}", self.joins.join(" ")));
        }
        if !self.where_clauses.is_empty() {
            query.push_str(&format!(" WHERE {}", self.where_clauses.join(" AND ")));
        }
        if let Some(group_by) = &self.group_by {
            query.push_str(&format!(" GROUP BY {}", group_by));
        }
        if !self.having_clauses.is_empty() {
            query.push_str(&format!(" HAVING {}", self.having_clauses.join(" AND ")));
        }
        if !self.order_by.is_empty() {
            query.push_str(&format!(" ORDER BY {}", self.order_by.join(", ")));
        }

        if let Some(limit) = self.limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = self.offset {
            query.push_str(&format!(" OFFSET {}", offset));
        }

        query
    }

    pub fn with_pagination(
        &mut self,
        pagination: &Pagination,
        table_name: &str,
    ) -> Result<&mut Self, Error> {
        for order in &pagination.order_by {
            let dir = match (&order.direction, &pagination.direction) {
                (OrderDirection::Asc, PaginationDirection::Forward) => "ASC",
                (OrderDirection::Asc, PaginationDirection::Backward) => "DESC",
                (OrderDirection::Desc, PaginationDirection::Forward) => "DESC",
                (OrderDirection::Desc, PaginationDirection::Backward) => "ASC",
            };
            self.order_by(&format!("[{}] {}", order.field, dir));
        }

        if let Some(cursor) = &pagination.cursor {
            let decoded = decode_cursor(cursor).expect("Invalid cursor");
            let cursor_values: Vec<String> = decoded.split('/').map(|s| s.to_string()).collect();
            let (cursor_where, cursor_binds) =
                build_cursor_conditions(pagination, Some(&cursor_values), table_name)?;
            for (condition, bind) in cursor_where.iter().zip(cursor_binds.iter()) {
                self.add_where(condition, vec![bind.clone()]);
            }
            self.cursor_values = cursor_values;
        }

        if let Some(limit) = pagination.limit {
            self.limit(limit + 1);
        }

        if let Some(offset) = pagination.offset {
            self.offset(offset);
        }

        self.pagination = Some(pagination.clone());

        Ok(self)
    }

    pub async fn fetch_page(&self, pool: &SqlitePool) -> Result<Page<SqliteRow>, Error> {
        let pagination = if let Some(pagination) = &self.pagination {
            pagination
        } else {
            return Err(Error::Query(QueryError::MissingParam("pagination".to_string())));
        };

        let query = self.build();
        let mut query = sqlx::query(&query);
        for bind in &self.bind_values {
            query = query.bind(bind);
        }
        let mut rows = query.fetch_all(pool).await?;
        let next_cursor = if pagination.limit.is_some() && rows.len() > pagination.limit.unwrap() as usize {
            let last_row = rows.pop().unwrap();
            let cursor_values_str = build_cursor_values(pagination, &last_row)?.join("/");
            Some(encode_cursor(&cursor_values_str)?)
        } else {
            None
        };
        Ok(Page {
            items: rows,
            next_cursor,
        })
    }
}
