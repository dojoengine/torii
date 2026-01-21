//! CachingPool - A wrapper around Pool<Sqlite> that provides caching for SQL query results.
//!
//! This module provides a transparent cache layer for SQLite queries that:
//! - Caches SELECT query results using configurable backends (in-memory, Redis)
//! - Generates cache keys from SQL + bind parameters
//! - Supports pattern-based invalidation for write operations
//! - Falls back to direct pool access when caching is disabled

use sqlx::sqlite::SqliteRow;
use sqlx::{Pool, Row, Sqlite};
use std::sync::Arc;
use torii_cache::query_cache::{
    generate_cache_key, CacheResult, CachedRow, QueryCache, QueryCacheConfig,
};
use tracing::{debug, trace, warn};

use crate::error::Error;

/// Wrapper around Pool<Sqlite> that provides transparent query caching.
#[derive(Debug, Clone)]
pub struct CachingPool {
    inner: Pool<Sqlite>,
    cache: Option<Arc<dyn QueryCache>>,
}

impl CachingPool {
    /// Create a new CachingPool without caching (pass-through mode).
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { inner: pool, cache: None }
    }

    /// Add a cache backend to the pool.
    pub fn with_cache(mut self, cache: Arc<dyn QueryCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Get the inner pool for direct access (writes, schema operations, etc.).
    pub fn inner(&self) -> &Pool<Sqlite> {
        &self.inner
    }

    /// Check if caching is enabled and available.
    pub fn is_cache_enabled(&self) -> bool {
        self.cache.as_ref().map(|c| c.config().enabled && c.is_available()).unwrap_or(false)
    }

    /// Get cache configuration, if available.
    pub fn cache_config(&self) -> Option<&QueryCacheConfig> {
        self.cache.as_ref().map(|c| c.config())
    }

    /// Execute a SELECT query with caching.
    ///
    /// This method:
    /// 1. Generates a cache key from the SQL and bind parameters
    /// 2. Checks the cache for a hit
    /// 3. On miss, executes the query against the database
    /// 4. Caches the result for future requests
    ///
    /// # Arguments
    /// * `sql` - The SQL query string
    /// * `binds` - The bind parameter values as strings
    ///
    /// # Returns
    /// Cached rows that can be converted to the desired type.
    pub async fn fetch_all_cached(
        &self,
        sql: &str,
        binds: &[String],
    ) -> Result<CachedQueryResult, Error> {
        let cache_key = generate_cache_key(sql, binds);

        // Try cache first
        if let Some(cache) = &self.cache {
            match cache.get(&cache_key).await {
                CacheResult::Hit(data) => {
                    trace!(target: "torii::sqlite::caching_pool", key = %cache_key, "Cache hit");
                    if let Ok(cached_rows) = bincode::deserialize::<Vec<CachedRow>>(&data) {
                        return Ok(CachedQueryResult::Cached(cached_rows));
                    }
                    // Deserialization failed, fall through to database
                    warn!(
                        target: "torii::sqlite::caching_pool",
                        key = %cache_key,
                        "Cache deserialization failed, executing query"
                    );
                }
                CacheResult::Miss => {
                    trace!(target: "torii::sqlite::caching_pool", key = %cache_key, "Cache miss");
                }
                CacheResult::Disabled => {
                    // Caching disabled, just execute the query
                }
            }
        }

        // Execute query against database
        let rows = self.execute_query(sql, binds).await?;

        // Cache the result
        if let Some(cache) = &self.cache {
            let cached_rows: Vec<CachedRow> =
                rows.iter().map(CachedRow::from_sqlite_row).collect();
            if let Ok(data) = bincode::serialize(&cached_rows) {
                if let Err(e) = cache.set(&cache_key, &data).await {
                    debug!(
                        target: "torii::sqlite::caching_pool",
                        key = %cache_key,
                        error = %e,
                        "Failed to cache query result"
                    );
                }
            }
        }

        Ok(CachedQueryResult::Fresh(rows))
    }

    /// Execute a query directly against the database.
    async fn execute_query(&self, sql: &str, binds: &[String]) -> Result<Vec<SqliteRow>, Error> {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = query.bind(bind);
        }
        Ok(query.fetch_all(&self.inner).await?)
    }

    /// Invalidate all cached queries for a specific table.
    ///
    /// This should be called after write operations that modify a table.
    pub async fn invalidate_table(&self, table: &str) {
        if let Some(cache) = &self.cache {
            let pattern = format!("torii:query:{}:*", table);
            if let Err(e) = cache.invalidate_pattern(&pattern).await {
                warn!(
                    target: "torii::sqlite::caching_pool",
                    table = %table,
                    error = %e,
                    "Failed to invalidate cache pattern"
                );
            }
        }
    }

    /// Invalidate multiple tables at once.
    pub async fn invalidate_tables(&self, tables: &[&str]) {
        for table in tables {
            self.invalidate_table(table).await;
        }
    }

    /// Invalidate a specific cache key.
    pub async fn invalidate_key(&self, key: &str) {
        if let Some(cache) = &self.cache {
            if let Err(e) = cache.invalidate(key).await {
                debug!(
                    target: "torii::sqlite::caching_pool",
                    key = %key,
                    error = %e,
                    "Failed to invalidate cache key"
                );
            }
        }
    }
}

/// Result of a cached query execution.
pub enum CachedQueryResult {
    /// Result came from cache.
    Cached(Vec<CachedRow>),
    /// Result came from database (fresh).
    Fresh(Vec<SqliteRow>),
}

impl std::fmt::Debug for CachedQueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cached(rows) => f.debug_tuple("Cached").field(&rows.len()).finish(),
            Self::Fresh(rows) => f.debug_tuple("Fresh").field(&rows.len()).finish(),
        }
    }
}

impl CachedQueryResult {
    /// Get the number of rows.
    pub fn len(&self) -> usize {
        match self {
            Self::Cached(rows) => rows.len(),
            Self::Fresh(rows) => rows.len(),
        }
    }

    /// Check if result is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if result came from cache.
    pub fn is_cached(&self) -> bool {
        matches!(self, Self::Cached(_))
    }

    /// Convert to CachedRows (either from cache or by converting fresh rows).
    pub fn into_cached_rows(self) -> Vec<CachedRow> {
        match self {
            Self::Cached(rows) => rows,
            Self::Fresh(rows) => rows.iter().map(CachedRow::from_sqlite_row).collect(),
        }
    }

    /// Get a value by column index from the first row.
    pub fn get_first_i64(&self, idx: usize) -> Option<i64> {
        match self {
            Self::Cached(rows) => {
                rows.first().and_then(|row| row.values.get(idx)).and_then(|v| v.as_i64())
            }
            Self::Fresh(rows) => rows.first().and_then(|row| row.try_get::<i64, _>(idx).ok()),
        }
    }

    /// Get a string value by column index from the first row.
    pub fn get_first_string(&self, idx: usize) -> Option<String> {
        match self {
            Self::Cached(rows) => rows
                .first()
                .and_then(|row| row.values.get(idx))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            Self::Fresh(rows) => rows.first().and_then(|row| row.try_get::<String, _>(idx).ok()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;
    use torii_cache::query_cache::InMemoryQueryCache;

    async fn create_test_pool() -> Pool<Sqlite> {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_caching_pool_without_cache() {
        let pool = create_test_pool().await;

        // Create table
        sqlx::query("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO test (id, name) VALUES (1, 'test')")
            .execute(&pool)
            .await
            .unwrap();

        let caching_pool = CachingPool::new(pool);

        // Should work without cache
        let result = caching_pool
            .fetch_all_cached("SELECT * FROM test WHERE id = ?", &["1".to_string()])
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result.is_cached());
    }

    #[tokio::test]
    async fn test_caching_pool_with_cache() {
        let pool = create_test_pool().await;

        // Create table
        sqlx::query("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO test (id, name) VALUES (1, 'test')")
            .execute(&pool)
            .await
            .unwrap();

        let config = QueryCacheConfig { enabled: true, ttl_seconds: 60 };
        let cache = Arc::new(InMemoryQueryCache::new(config));
        let caching_pool = CachingPool::new(pool).with_cache(cache);

        // First query - should miss cache
        let result1 = caching_pool
            .fetch_all_cached("SELECT * FROM test WHERE id = ?", &["1".to_string()])
            .await
            .unwrap();

        assert_eq!(result1.len(), 1);
        assert!(!result1.is_cached());

        // Second query - should hit cache
        let result2 = caching_pool
            .fetch_all_cached("SELECT * FROM test WHERE id = ?", &["1".to_string()])
            .await
            .unwrap();

        assert_eq!(result2.len(), 1);
        assert!(result2.is_cached());
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let pool = create_test_pool().await;

        // Create table
        sqlx::query("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO test (id, name) VALUES (1, 'test')")
            .execute(&pool)
            .await
            .unwrap();

        let config = QueryCacheConfig { enabled: true, ttl_seconds: 60 };
        let cache = Arc::new(InMemoryQueryCache::new(config));
        let caching_pool = CachingPool::new(pool).with_cache(cache);

        // First query - populate cache
        let _ = caching_pool
            .fetch_all_cached("SELECT * FROM test WHERE id = ?", &["1".to_string()])
            .await
            .unwrap();

        // Second query - hits cache
        let result = caching_pool
            .fetch_all_cached("SELECT * FROM test WHERE id = ?", &["1".to_string()])
            .await
            .unwrap();
        assert!(result.is_cached());

        // Invalidate the table
        caching_pool.invalidate_table("test").await;

        // Third query - should miss cache
        let result = caching_pool
            .fetch_all_cached("SELECT * FROM test WHERE id = ?", &["1".to_string()])
            .await
            .unwrap();
        assert!(!result.is_cached());
    }
}
