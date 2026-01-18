//! Torii Broker - Message brokering system for Torii
//!
//! This crate provides different message broker implementations for
//! real-time communication and subscription management.
//!
//! ## Deployment Modes
//!
//! ### Single Replica (Default)
//! Uses the in-memory `MemoryBroker` for fastest possible latency. All
//! subscribers receive updates from the local indexer only.
//!
//! ### Multi-Replica (Redis)
//! When the `redis` feature is enabled and configured, `MemoryBroker::publish()`
//! automatically broadcasts messages to Redis Pub/Sub in addition to local
//! subscribers. Background listeners receive these messages on other replicas
//! and dispatch them to local subscribers.
//!
//! ## Usage
//!
//! ```rust,ignore
//! // Single replica (default) - no changes needed
//! use torii_broker::MemoryBroker;
//! MemoryBroker::<EntityUpdate>::publish(update);
//! let stream = MemoryBroker::<EntityUpdate>::subscribe();
//!
//! // Multi-replica (with redis feature)
//! // Just initialize Redis at startup - MemoryBroker works transparently!
//! #[cfg(feature = "redis")]
//! {
//!     use torii_broker::{init_redis, start_all_listeners};
//!
//!     // Initialize at startup
//!     init_redis("redis://localhost:6379").await?;
//!     start_all_listeners().await;
//!
//!     // Same API - publish() now also sends to Redis automatically
//!     MemoryBroker::<EntityUpdate>::publish(update);
//!     let stream = MemoryBroker::<EntityUpdate>::subscribe();
//! }
//! ```

pub mod memory;
#[cfg(feature = "redis")]
pub mod redis;
pub mod types;

// Re-export commonly used types from memory module
pub use memory::{MemoryBroker, Senders};

// Re-export Redis functions when feature is enabled
#[cfg(feature = "redis")]
pub use redis::{init_redis, is_redis_available, start_all_listeners};

#[cfg(test)]
mod test;
