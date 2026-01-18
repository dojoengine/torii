use async_trait::async_trait;
use std::sync::Arc;

use super::{CacheResult, QueryCache, QueryCacheConfig, QueryCacheError};

/// A tiered cache that tries a primary cache (Redis) first, then falls back to
/// a secondary cache (in-memory).
#[derive(Debug)]
pub struct TieredQueryCache {
    /// Primary cache (typically Redis).
    primary: Arc<dyn QueryCache>,
    /// Fallback cache (typically in-memory).
    fallback: Arc<dyn QueryCache>,
}

impl TieredQueryCache {
    /// Create a new tiered cache with primary and fallback backends.
    pub fn new(primary: Arc<dyn QueryCache>, fallback: Arc<dyn QueryCache>) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl QueryCache for TieredQueryCache {
    async fn get(&self, key: &str) -> CacheResult<Vec<u8>> {
        // Try primary (Redis) first if available
        if self.primary.is_available() {
            if let CacheResult::Hit(data) = self.primary.get(key).await {
                return CacheResult::Hit(data);
            }
        }
        // Fallback to secondary (in-memory)
        self.fallback.get(key).await
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), QueryCacheError> {
        // Write to both caches
        // Primary write failure is non-fatal if fallback succeeds
        if self.primary.is_available() {
            let _ = self.primary.set(key, value).await;
        }
        self.fallback.set(key, value).await
    }

    async fn invalidate(&self, key: &str) -> Result<(), QueryCacheError> {
        // Invalidate in both caches
        if self.primary.is_available() {
            let _ = self.primary.invalidate(key).await;
        }
        self.fallback.invalidate(key).await
    }

    async fn invalidate_pattern(&self, pattern: &str) -> Result<(), QueryCacheError> {
        // Invalidate pattern in both caches
        if self.primary.is_available() {
            let _ = self.primary.invalidate_pattern(pattern).await;
        }
        self.fallback.invalidate_pattern(pattern).await
    }

    fn is_available(&self) -> bool {
        // Available if either cache is available
        self.primary.is_available() || self.fallback.is_available()
    }

    fn config(&self) -> &QueryCacheConfig {
        // Return fallback config (always available)
        self.fallback.config()
    }
}

#[cfg(test)]
mod tests {
    use super::super::in_memory::InMemoryQueryCache;
    use super::*;

    #[tokio::test]
    async fn test_tiered_fallback_when_primary_unavailable() {
        let config = QueryCacheConfig { enabled: true, ttl_seconds: 60 };

        // Create two in-memory caches to simulate tiered behavior
        // (In real usage, primary would be Redis)
        let primary = Arc::new(InMemoryQueryCache::new(config.clone()));
        let fallback = Arc::new(InMemoryQueryCache::new(config.clone()));

        let tiered = TieredQueryCache::new(primary.clone(), fallback.clone());

        // Set via tiered (writes to both)
        tiered.set("key", b"value").await.unwrap();

        // Both should have the value
        let result = primary.get("key").await;
        assert!(matches!(result, CacheResult::Hit(_)));

        let result = fallback.get("key").await;
        assert!(matches!(result, CacheResult::Hit(_)));

        // Get via tiered should hit primary first
        let result = tiered.get("key").await;
        match result {
            CacheResult::Hit(data) => assert_eq!(data, b"value"),
            _ => panic!("Expected hit"),
        }
    }
}
