use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use sqlx::{Column, Row, TypeInfo};

/// A serializable representation of a SQLite row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedRow {
    /// Column metadata.
    pub columns: Vec<CachedColumn>,
    /// Row values.
    pub values: Vec<CachedValue>,
}

/// Column metadata for cached rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedColumn {
    /// Column name.
    pub name: String,
    /// SQLite type name.
    pub type_name: String,
}

/// Cached value types matching SQLite's type affinity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CachedValue {
    /// NULL value.
    Null,
    /// INTEGER value (i64).
    Integer(i64),
    /// REAL value (f64).
    Real(f64),
    /// TEXT value.
    Text(String),
    /// BLOB value.
    Blob(Vec<u8>),
}

impl CachedRow {
    /// Create a CachedRow from a SQLite row.
    pub fn from_sqlite_row(row: &SqliteRow) -> Self {
        let columns: Vec<CachedColumn> = row
            .columns()
            .iter()
            .map(|c| CachedColumn {
                name: c.name().to_string(),
                type_name: c.type_info().name().to_string(),
            })
            .collect();

        let values: Vec<CachedValue> = (0..columns.len())
            .map(|i| {
                // Try each type in order based on SQLite type affinity
                if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
                    match v {
                        Some(n) => CachedValue::Integer(n),
                        None => CachedValue::Null,
                    }
                } else if let Ok(v) = row.try_get::<Option<f64>, _>(i) {
                    match v {
                        Some(n) => CachedValue::Real(n),
                        None => CachedValue::Null,
                    }
                } else if let Ok(v) = row.try_get::<Option<String>, _>(i) {
                    match v {
                        Some(s) => CachedValue::Text(s),
                        None => CachedValue::Null,
                    }
                } else if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(i) {
                    match v {
                        Some(b) => CachedValue::Blob(b),
                        None => CachedValue::Null,
                    }
                } else {
                    CachedValue::Null
                }
            })
            .collect();

        Self { columns, values }
    }

    /// Get a value by column name.
    pub fn get(&self, column_name: &str) -> Option<&CachedValue> {
        let idx = self.columns.iter().position(|c| c.name == column_name)?;
        self.values.get(idx)
    }

    /// Get an integer value by column name.
    pub fn get_i64(&self, column_name: &str) -> Option<i64> {
        match self.get(column_name)? {
            CachedValue::Integer(v) => Some(*v),
            _ => None,
        }
    }

    /// Get a string value by column name.
    pub fn get_string(&self, column_name: &str) -> Option<&str> {
        match self.get(column_name)? {
            CachedValue::Text(v) => Some(v),
            _ => None,
        }
    }

    /// Get a blob value by column name.
    pub fn get_blob(&self, column_name: &str) -> Option<&[u8]> {
        match self.get(column_name)? {
            CachedValue::Blob(v) => Some(v),
            _ => None,
        }
    }
}

impl CachedValue {
    /// Check if the value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, CachedValue::Null)
    }

    /// Convert to i64 if possible.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            CachedValue::Integer(v) => Some(*v),
            _ => None,
        }
    }

    /// Convert to f64 if possible.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            CachedValue::Real(v) => Some(*v),
            CachedValue::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Convert to string if possible.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            CachedValue::Text(v) => Some(v),
            _ => None,
        }
    }

    /// Convert to bytes if possible.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            CachedValue::Blob(v) => Some(v),
            _ => None,
        }
    }
}

/// A cached page of query results with optional cursor for pagination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedPage {
    /// The rows in this page.
    pub rows: Vec<CachedRow>,
    /// Optional cursor for the next page.
    pub next_cursor: Option<String>,
}

impl CachedPage {
    /// Create a new cached page.
    pub fn new(rows: Vec<CachedRow>, next_cursor: Option<String>) -> Self {
        Self { rows, next_cursor }
    }

    /// Check if this page is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get the number of rows in this page.
    pub fn len(&self) -> usize {
        self.rows.len()
    }
}
