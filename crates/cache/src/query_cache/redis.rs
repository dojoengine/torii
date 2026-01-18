use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;

use super::{CacheResult, QueryCache, QueryCacheConfig, QueryCacheError};

/// Redis-backed query cache implementation.
#[derive(Debug)]
pub struct RedisQueryCache {
    config: QueryCacheConfig,
    client: Client,
    connection: RwLock<Option<MultiplexedConnection>>,
    available: AtomicBool,
}

impl RedisQueryCache {
    /// Create a new Redis cache and verify connectivity.
    pub async fn new(url: &str, config: QueryCacheConfig) -> Result<Self, QueryCacheError> {
        let client = Client::open(url)?;

        // Test connection
        let mut conn = client.get_multiplexed_async_connection().await?;
        let _: () = redis::cmd("PING").query_async(&mut conn).await?;

        Ok(Self {
            config,
            client,
            connection: RwLock::new(Some(conn)),
            available: AtomicBool::new(true),
        })
    }

    /// Get or create a connection, handling reconnection on failure.
    async fn get_connection(&self) -> Option<MultiplexedConnection> {
        // Try to reuse existing connection
        {
            let conn = self.connection.read().await;
            if conn.is_some() {
                return conn.clone();
            }
        }

        // Try to reconnect
        match self.client.get_multiplexed_async_connection().await {
            Ok(conn) => {
                let mut lock = self.connection.write().await;
                *lock = Some(conn.clone());
                self.available.store(true, Ordering::Relaxed);
                Some(conn)
            }
            Err(_) => {
                self.available.store(false, Ordering::Relaxed);
                None
            }
        }
    }

    /// Mark connection as failed and clear it.
    async fn mark_connection_failed(&self) {
        let mut lock = self.connection.write().await;
        *lock = None;
        self.available.store(false, Ordering::Relaxed);
    }
}

#[async_trait]
impl QueryCache for RedisQueryCache {
    async fn get(&self, key: &str) -> CacheResult<Vec<u8>> {
        if !self.config.enabled {
            return CacheResult::Disabled;
        }

        let mut conn = match self.get_connection().await {
            Some(c) => c,
            None => return CacheResult::Miss,
        };

        match conn.get::<_, Option<Vec<u8>>>(key).await {
            Ok(Some(data)) => {
                self.available.store(true, Ordering::Relaxed);
                CacheResult::Hit(data)
            }
            Ok(None) => CacheResult::Miss,
            Err(_) => {
                self.mark_connection_failed().await;
                CacheResult::Miss
            }
        }
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), QueryCacheError> {
        if !self.config.enabled {
            return Ok(());
        }

        let mut conn = match self.get_connection().await {
            Some(c) => c,
            None => return Err(QueryCacheError::Backend("Redis unavailable".to_string())),
        };

        match conn.set_ex::<_, _, ()>(key, value, self.config.ttl_seconds).await {
            Ok(_) => {
                self.available.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.mark_connection_failed().await;
                Err(QueryCacheError::Redis(e))
            }
        }
    }

    async fn invalidate(&self, key: &str) -> Result<(), QueryCacheError> {
        let mut conn = match self.get_connection().await {
            Some(c) => c,
            None => return Err(QueryCacheError::Backend("Redis unavailable".to_string())),
        };

        match conn.del::<_, ()>(key).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.mark_connection_failed().await;
                Err(QueryCacheError::Redis(e))
            }
        }
    }

    async fn invalidate_pattern(&self, pattern: &str) -> Result<(), QueryCacheError> {
        let mut conn = match self.get_connection().await {
            Some(c) => c,
            None => return Err(QueryCacheError::Backend("Redis unavailable".to_string())),
        };

        // Use SCAN for production (KEYS can block), but KEYS is simpler for now
        // In production, consider using SCAN with iteration
        let keys: Vec<String> = match redis::cmd("KEYS").arg(pattern).query_async(&mut conn).await {
            Ok(k) => k,
            Err(e) => {
                self.mark_connection_failed().await;
                return Err(QueryCacheError::Redis(e));
            }
        };

        if !keys.is_empty() {
            if let Err(e) = conn.del::<_, ()>(keys).await {
                self.mark_connection_failed().await;
                return Err(QueryCacheError::Redis(e));
            }
        }

        Ok(())
    }

    fn is_available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }

    fn config(&self) -> &QueryCacheConfig {
        &self.config
    }
}
