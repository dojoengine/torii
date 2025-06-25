use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use starknet::core::types::{Felt, U256};
use tokio::sync::Mutex;
use torii_math::I256;
use torii_proto::Model;

pub mod inmemory;

pub struct CacheError(Box<dyn std::error::Error + Send + Sync>);

#[async_trait]
pub trait ReadOnlyCache: Send + Sync + std::fmt::Debug {
    /// Get models by selectors. If selectors is empty, returns all models.
    async fn models(&self, selectors: &[Felt]) -> Result<Vec<Model>, CacheError>;

    /// Get a specific model by selector.
    async fn model(&self, selector: Felt) -> Result<Model, CacheError>;

    /// Check if a token is registered.
    async fn is_token_registered(&self, token_id: &str) -> bool;

    /// Get a token registration lock for coordination.
    async fn get_token_registration_lock(&self, token_id: &str) -> Option<Arc<Mutex<()>>>;

    /// Get the balances diff.
    async fn balances_diff(&self) -> HashMap<String, I256>;
}

#[async_trait]
pub trait Cache: ReadOnlyCache + Send + Sync + std::fmt::Debug {
    /// Register a model in the cache.
    async fn register_model(&self, selector: Felt, model: Model);

    /// Clear all models from the cache.
    async fn clear_models(&self);

    /// Mark a token as registered.
    async fn mark_token_registered(&self, token_id: &str);

    /// Clear the balances diff.
    async fn clear_balances_diff(&self);

    /// Update the balances diff.
    async fn update_balance_diff(&self, token_id: &str, from: Felt, to: Felt, value: U256);
}