use async_trait::async_trait;
use dashmap::DashMap;
use std::time::{Duration, Instant};

use super::{CacheResult, QueryCache, QueryCacheConfig, QueryCacheError};

/// A cached entry with expiration time.
#[derive(Debug)]
struct CacheEntry {
    data: Vec<u8>,
    expires_at: Instant,
}

/// In-memory query cache implementation using DashMap for thread-safety.
#[derive(Debug)]
pub struct InMemoryQueryCache {
    config: QueryCacheConfig,
    entries: DashMap<String, CacheEntry>,
}

impl InMemoryQueryCache {
    /// Create a new in-memory cache with the given configuration.
    pub fn new(config: QueryCacheConfig) -> Self {
        Self { config, entries: DashMap::new() }
    }

    /// Get the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        self.entries.clear();
    }

    /// Remove expired entries from the cache.
    pub fn evict_expired(&self) {
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.expires_at > now);
    }
}

#[async_trait]
impl QueryCache for InMemoryQueryCache {
    async fn get(&self, key: &str) -> CacheResult<Vec<u8>> {
        if !self.config.enabled {
            return CacheResult::Disabled;
        }

        if let Some(entry) = self.entries.get(key) {
            if entry.expires_at > Instant::now() {
                return CacheResult::Hit(entry.data.clone());
            }
            // Entry expired, remove it
            drop(entry);
            self.entries.remove(key);
        }
        CacheResult::Miss
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), QueryCacheError> {
        if !self.config.enabled {
            return Ok(());
        }

        self.entries.insert(
            key.to_string(),
            CacheEntry {
                data: value.to_vec(),
                expires_at: Instant::now() + Duration::from_secs(self.config.ttl_seconds),
            },
        );
        Ok(())
    }

    async fn invalidate(&self, key: &str) -> Result<(), QueryCacheError> {
        self.entries.remove(key);
        Ok(())
    }

    async fn invalidate_pattern(&self, pattern: &str) -> Result<(), QueryCacheError> {
        // Pattern is expected to end with "*" for prefix matching
        let prefix = pattern.trim_end_matches('*');
        self.entries.retain(|k, _| !k.starts_with(prefix));
        Ok(())
    }

    fn is_available(&self) -> bool {
        true
    }

    fn config(&self) -> &QueryCacheConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_disabled() {
        let config = QueryCacheConfig { enabled: false, ttl_seconds: 60 };
        let cache = InMemoryQueryCache::new(config);

        // Set should succeed but not actually store
        cache.set("key", b"value").await.unwrap();
        assert!(cache.is_empty());

        // Get should return Disabled
        let result = cache.get("key").await;
        assert!(matches!(result, CacheResult::Disabled));
    }

    #[tokio::test]
    async fn test_cache_hit_miss() {
        let config = QueryCacheConfig { enabled: true, ttl_seconds: 60 };
        let cache = InMemoryQueryCache::new(config);

        // Miss on empty cache
        let result = cache.get("key").await;
        assert!(matches!(result, CacheResult::Miss));

        // Set value
        cache.set("key", b"value").await.unwrap();

        // Hit
        let result = cache.get("key").await;
        match result {
            CacheResult::Hit(data) => assert_eq!(data, b"value"),
            _ => panic!("Expected hit"),
        }
    }

    #[tokio::test]
    async fn test_cache_invalidate() {
        let config = QueryCacheConfig { enabled: true, ttl_seconds: 60 };
        let cache = InMemoryQueryCache::new(config);

        cache.set("key1", b"value1").await.unwrap();
        cache.set("key2", b"value2").await.unwrap();

        // Invalidate single key
        cache.invalidate("key1").await.unwrap();

        let result = cache.get("key1").await;
        assert!(matches!(result, CacheResult::Miss));

        let result = cache.get("key2").await;
        assert!(matches!(result, CacheResult::Hit(_)));
    }

    #[tokio::test]
    async fn test_cache_invalidate_pattern() {
        let config = QueryCacheConfig { enabled: true, ttl_seconds: 60 };
        let cache = InMemoryQueryCache::new(config);

        cache.set("torii:query:entities:abc", b"value1").await.unwrap();
        cache.set("torii:query:entities:def", b"value2").await.unwrap();
        cache.set("torii:query:tokens:xyz", b"value3").await.unwrap();

        // Invalidate entities pattern
        cache.invalidate_pattern("torii:query:entities:*").await.unwrap();

        let result = cache.get("torii:query:entities:abc").await;
        assert!(matches!(result, CacheResult::Miss));

        let result = cache.get("torii:query:entities:def").await;
        assert!(matches!(result, CacheResult::Miss));

        let result = cache.get("torii:query:tokens:xyz").await;
        assert!(matches!(result, CacheResult::Hit(_)));
    }
}
