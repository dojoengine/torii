//! Redis-backed broker for distributed Torii deployments.
//!
//! This module provides a distributed message broker using Redis Pub/Sub,
//! allowing multiple Torii replicas to share subscription updates. When
//! an indexer on any replica publishes an update, all subscribed clients
//! across all replicas will receive it.
//!
//! ## How it works
//!
//! When Redis is configured:
//! 1. `MemoryBroker::publish()` sends to local subscribers AND to Redis
//! 2. Redis listeners on other replicas receive the message
//! 3. Listeners call `MemoryBroker::publish_local()` to dispatch to their local subscribers
//!
//! This ensures updates from any replica reach all subscribers across all replicas.

use std::sync::{Arc, OnceLock};

use futures_util::StreamExt;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, trace, warn};

use crate::memory::MemoryBroker;
use crate::types::Update;

const LOG_TARGET: &str = "torii::broker::redis";

/// Stores both the client (for creating new connections) and the connection manager (for pub)
struct RedisState {
    client: Client,
    connection: ConnectionManager,
}

/// Global Redis state, initialized once.
static REDIS_STATE: OnceLock<Arc<RwLock<Option<RedisState>>>> = OnceLock::new();

/// Initializes the Redis connection for the broker.
///
/// This should be called once at startup with the Redis URL.
/// If not called, the broker will fall back to local-only mode.
pub async fn init_redis(redis_url: &str) -> Result<(), redis::RedisError> {
    let client = Client::open(redis_url)?;
    let connection = ConnectionManager::new(client.clone()).await?;

    let storage = REDIS_STATE.get_or_init(|| Arc::new(RwLock::new(None)));
    *storage.write().await = Some(RedisState { client, connection });

    debug!(target: LOG_TARGET, "Redis broker initialized with URL: {}", redis_url);
    Ok(())
}

/// Returns true if Redis is configured and available.
pub async fn is_redis_available() -> bool {
    if let Some(storage) = REDIS_STATE.get() {
        storage.read().await.is_some()
    } else {
        false
    }
}

/// Returns the channel name for a specific update type.
fn channel_name<T: 'static>() -> String {
    // Use a stable channel name based on the type name
    format!("torii:broker:{}", std::any::type_name::<T>())
}

/// Publishes a message to Redis Pub/Sub asynchronously.
///
/// This is called by `MemoryBroker::publish` when Redis is configured.
/// It spawns a background task to avoid blocking the caller.
pub fn publish_to_redis<T>(msg: Update<T>)
where
    T: std::fmt::Debug + Clone + Send + Sync + Serialize + 'static,
{
    tokio::spawn(async move {
        if let Some(storage) = REDIS_STATE.get() {
            if let Some(state) = storage.read().await.as_ref() {
                let channel = channel_name::<Update<T>>();
                match serde_json::to_string(&msg) {
                    Ok(payload) => {
                        let mut conn = state.connection.clone();
                        if let Err(e) = conn.publish::<_, _, ()>(&channel, &payload).await {
                            warn!(target: LOG_TARGET, channel = %channel, error = ?e, "Failed to publish to Redis");
                        } else {
                            trace!(target: LOG_TARGET, channel = %channel, "Published message to Redis");
                        }
                    }
                    Err(e) => {
                        error!(target: LOG_TARGET, error = ?e, "Failed to serialize message for Redis");
                    }
                }
            }
        }
    });
}

/// Start the Redis subscription listener for a specific update type.
///
/// This spawns a background task that listens to Redis Pub/Sub messages
/// and dispatches them to local subscribers via `MemoryBroker::publish_local`.
/// Call this once per update type at startup.
async fn start_listener<T>()
where
    T: std::fmt::Debug + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    let Some(storage) = REDIS_STATE.get() else {
        debug!(target: LOG_TARGET, "Redis not configured, skipping listener");
        return;
    };

    let client = match storage.read().await.as_ref() {
        Some(state) => state.client.clone(),
        None => {
            debug!(target: LOG_TARGET, "Redis connection not available, skipping listener");
            return;
        }
    };

    let channel = channel_name::<Update<T>>();
    debug!(target: LOG_TARGET, channel = %channel, "Starting Redis subscription listener");

    tokio::spawn(async move {
        // Get a pubsub connection using the stored client
        let mut pubsub = match client.get_async_pubsub().await {
            Ok(pubsub) => pubsub,
            Err(e) => {
                error!(target: LOG_TARGET, error = ?e, "Failed to get pubsub connection");
                return;
            }
        };

        if let Err(e) = pubsub.subscribe(&channel).await {
            error!(target: LOG_TARGET, channel = %channel, error = ?e, "Failed to subscribe to Redis channel");
            return;
        }

        debug!(target: LOG_TARGET, channel = %channel, "Subscribed to Redis channel");

        let mut stream = pubsub.on_message();
        while let Some(msg) = stream.next().await {
            let payload: String = match msg.get_payload() {
                Ok(p) => p,
                Err(e) => {
                    warn!(target: LOG_TARGET, error = ?e, "Failed to get message payload");
                    continue;
                }
            };

            match serde_json::from_str::<Update<T>>(&payload) {
                Ok(update) => {
                    trace!(target: LOG_TARGET, channel = %channel, "Received message from Redis, dispatching to local subscribers");
                    // Use publish_local to avoid re-publishing back to Redis
                    MemoryBroker::<Update<T>>::publish_local(update);
                }
                Err(e) => {
                    warn!(target: LOG_TARGET, error = ?e, "Failed to deserialize Redis message");
                }
            }
        }

        warn!(target: LOG_TARGET, channel = %channel, "Redis subscription stream ended");
    });
}

/// Starts Redis listeners for all standard Torii update types.
///
/// Call this at startup after `init_redis()` to enable cross-replica updates.
pub async fn start_all_listeners() {
    if !is_redis_available().await {
        debug!(target: LOG_TARGET, "Redis not available, skipping listener startup");
        return;
    }

    debug!(target: LOG_TARGET, "Starting Redis listeners for all update types");

    // Start listeners for each update type
    // Note: The inner type T must implement Serialize + DeserializeOwned
    start_listener::<torii_proto::schema::EntityWithMetadata<false>>().await;
    start_listener::<torii_proto::schema::EntityWithMetadata<true>>().await;
    start_listener::<torii_proto::Contract>().await;
    start_listener::<torii_proto::Model>().await;
    start_listener::<torii_proto::Token>().await;
    start_listener::<torii_proto::TokenBalance>().await;
    start_listener::<torii_proto::TokenTransfer>().await;
    start_listener::<torii_proto::EventWithMetadata>().await;
    start_listener::<torii_proto::Transaction>().await;
    start_listener::<torii_proto::AggregationEntry>().await;
    start_listener::<torii_proto::Activity>().await;
    start_listener::<torii_proto::AchievementProgression>().await;

    debug!(target: LOG_TARGET, "All Redis listeners started");
}
