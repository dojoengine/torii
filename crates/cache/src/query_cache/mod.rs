use async_trait::async_trait;

pub mod cached_row;
pub mod in_memory;
pub mod key;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "redis")]
pub mod tiered;

pub use cached_row::{CachedColumn, CachedPage, CachedRow, CachedValue};
pub use in_memory::InMemoryQueryCache;
pub use key::generate_cache_key;

/// Configuration for query caching.
#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    /// Whether caching is enabled.
    pub enabled: bool,
    /// Time-to-live for cache entries in seconds.
    pub ttl_seconds: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self { enabled: false, ttl_seconds: 60 }
    }
}

/// Result type for cache lookups.
#[derive(Debug)]
pub enum CacheResult<T> {
    /// Cache hit - returns the cached data.
    Hit(T),
    /// Cache miss - data not found.
    Miss,
    /// Cache is disabled.
    Disabled,
}

/// Cache error type.
#[derive(Debug, thiserror::Error)]
pub enum QueryCacheError {
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Cache backend error: {0}")]
    Backend(String),
    #[cfg(feature = "redis")]
    #[error("Redis error: {0}")]
    Redis(#[from] ::redis::RedisError),
}

/// Trait defining the query cache interface.
///
/// Implementors must be thread-safe (Send + Sync) and debuggable.
#[async_trait]
pub trait QueryCache: Send + Sync + std::fmt::Debug {
    /// Get a cached value by key.
    async fn get(&self, key: &str) -> CacheResult<Vec<u8>>;

    /// Set a cached value.
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), QueryCacheError>;

    /// Invalidate a specific cache key.
    async fn invalidate(&self, key: &str) -> Result<(), QueryCacheError>;

    /// Invalidate all cache keys matching a pattern (e.g., "torii:query:entities:*").
    async fn invalidate_pattern(&self, pattern: &str) -> Result<(), QueryCacheError>;

    /// Check if the cache backend is currently available.
    fn is_available(&self) -> bool;

    /// Get the cache configuration.
    fn config(&self) -> &QueryCacheConfig;
}
