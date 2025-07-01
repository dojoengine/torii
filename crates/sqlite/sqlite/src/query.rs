use torii_proto::Pagination;

use crate::{constants::SQL_DEFAULT_LIMIT, cursor::build_cursor_conditions};

/// Builder for constructing SQL queries with pagination, joins, and cursors
#[derive(Debug)]
pub struct QueryBuilder {
    pub selections: Vec<String>,
    pub joins: Vec<String>,
    pub where_conditions: Vec<String>,
    pub having_clause: Option<String>,
    pub order_clause: String,
    pub bind_values: Vec<String>,
    pub cursor_binds: Vec<String>,
    pub limit: u32,
    pub pagination: Option<Pagination>,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            selections: Vec::new(),
            joins: Vec::new(),
            where_conditions: Vec::new(),
            having_clause: None,
            order_clause: String::new(),
            bind_values: Vec::new(),
            cursor_binds: Vec::new(),
            limit: SQL_DEFAULT_LIMIT as u32,
            pagination: None,
        }
    }

    pub fn with_selections(mut self, selections: Vec<String>) -> Self {
        self.selections = selections;
        self
    }

    pub fn with_joins(mut self, joins: Vec<String>) -> Self {
        self.joins = joins;
        self
    }

    pub fn with_where_conditions(mut self, conditions: Vec<String>) -> Self {
        self.where_conditions = conditions;
        self
    }

    pub fn with_having_clause(mut self, having: Option<String>) -> Self {
        self.having_clause = having;
        self
    }

    pub fn with_order_clause(mut self, order: String) -> Self {
        self.order_clause = order;
        self
    }

    pub fn with_bind_values(mut self, values: Vec<String>) -> Self {
        self.bind_values = values;
        self
    }

    pub fn with_cursor_binds(mut self, cursor_binds: Vec<String>) -> Self {
        self.cursor_binds = cursor_binds;
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_pagination(mut self, pagination: Pagination, table_name: &str) -> Result<Self, Error> {
        // Parse cursor values
        let cursor_values = SchemaQueryHelper::parse_cursor(&pagination)?;
        
        // Build cursor conditions
        let (cursor_conditions, cursor_binds) = 
            build_cursor_conditions(&pagination, cursor_values.as_deref(), table_name)?;
        
        // Set up order clause
        let order_clause = SchemaQueryHelper::build_order_clause(&pagination, table_name);
        
        // Set limit from pagination
        let limit = pagination.limit.unwrap_or(SQL_DEFAULT_LIMIT as u32) + 1; // +1 to check for more pages
        
        self.pagination = Some(pagination);
        self.cursor_binds = cursor_binds;
        self.order_clause = order_clause;
        self.limit = limit;
        
        // Add cursor conditions to where conditions
        self.where_conditions.extend(cursor_conditions);
        
        Ok(self)
    }

    pub fn build_query(&self, table_name: &str) -> String {
        let mut query = format!(
            "SELECT {} FROM [{}]",
            self.selections.join(", "),
            table_name
        );

        if !self.joins.is_empty() {
            query.push_str(&format!(" {}", self.joins.join(" ")));
        }

        if !self.where_conditions.is_empty() {
            query.push_str(&format!(" WHERE {}", self.where_conditions.join(" AND ")));
        }

        query.push_str(&format!(" GROUP BY {}.id", table_name));

        if let Some(having) = &self.having_clause {
            query.push_str(&format!(" HAVING {}", having));
        }

        if !self.order_clause.is_empty() {
            query.push_str(&format!(" ORDER BY {}", self.order_clause));
        }

        query.push_str(" LIMIT ?");
        query
    }

    /// Build a simple query without GROUP BY (for non-entity queries)
    pub fn build_simple_query(&self, table_name: &str) -> String {
        let mut query = format!(
            "SELECT {} FROM [{}]",
            self.selections.join(", "),
            table_name
        );

        if !self.joins.is_empty() {
            query.push_str(&format!(" {}", self.joins.join(" ")));
        }

        if !self.where_conditions.is_empty() {
            query.push_str(&format!(" WHERE {}", self.where_conditions.join(" AND ")));
        }

        if !self.order_clause.is_empty() {
            query.push_str(&format!(" ORDER BY {}", self.order_clause));
        }

        query.push_str(" LIMIT ?");
        query
    }

    pub fn all_bind_values(&self) -> Vec<&String> {
        self.bind_values.iter().chain(self.cursor_binds.iter()).collect()
    }

    /// Process query results and handle pagination
    pub fn process_results<T>(&self, mut results: Vec<T>) -> Result<Page<T>, Error> 
    where 
        T: Clone,
    {
        let pagination = self.pagination.as_ref().ok_or_else(|| {
            Error::Query(QueryError::UnsupportedQuery("No pagination set".to_string()))
        })?;
        
        let original_limit = pagination.limit.unwrap_or(SQL_DEFAULT_LIMIT as u32);
        let has_more = results.len() > original_limit as usize;
        
        // Handle backward pagination
        if pagination.direction == PaginationDirection::Backward {
            results.reverse();
        }
        
        // Truncate to original limit if we have more results
        if has_more {
            results.truncate(original_limit as usize);
        }
        
        // Generate next cursor if there are more pages
        let next_cursor = if has_more && !results.is_empty() {
            // For now, we'll need to implement cursor generation per entity type
            // This is a placeholder - actual implementation would depend on the entity type
            None
        } else {
            None
        };
        
        Ok(Page {
            items: results,
            next_cursor,
        })
    }

    /// Process SQLite row results with cursor generation
    pub fn process_sqlite_results(&self, mut results: Vec<SqliteRow>) -> Result<Page<SqliteRow>, Error> {
        let pagination = self.pagination.as_ref().ok_or_else(|| {
            Error::Query(QueryError::UnsupportedQuery("No pagination set".to_string()))
        })?;
        
        let original_limit = pagination.limit.unwrap_or(SQL_DEFAULT_LIMIT as u32);
        let has_more = results.len() > original_limit as usize;
        
        // Handle backward pagination
        if pagination.direction == PaginationDirection::Backward {
            results.reverse();
        }
        
        // Truncate to original limit if we have more results
        if has_more {
            results.truncate(original_limit as usize);
        }
        
        // Generate next cursor if there are more pages
        let next_cursor = if has_more {
            if let Some(last_row) = results.last() {
                let cursor_values_str = build_cursor_values(pagination, last_row)?.join("/");
                Some(encode_cursor(&cursor_values_str)?)
            } else {
                None
            }
        } else {
            None
        };
        
        Ok(Page {
            items: results,
            next_cursor,
        })
    }
}