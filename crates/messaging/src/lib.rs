pub mod entity;
pub mod error;
pub mod parsing;
pub mod validation;

use std::sync::Arc;

use async_trait::async_trait;
pub use entity::{get_identity_from_ty, get_timestamp_from_ty, set_entity, ty_keys, ty_model_id};
pub use error::MessagingError;
pub use parsing::parse_value_to_ty;
use sqlx::types::chrono::Utc;
use starknet::providers::Provider;
use starknet_core::types::{typed_data::TypeReference, TypedData};
use starknet_crypto::{poseidon_hash_many, Felt};
use torii_storage::Storage;
use tracing::{debug, info, warn};
pub use validation::{validate_message, validate_signature};

pub const LOG_TARGET: &str = "torii::messaging";

#[derive(Debug, Clone)]
pub struct MessagingConfig {
    pub max_age: u64,
    pub future_tolerance: u64,
    pub require_timestamp: bool,
}

impl Default for MessagingConfig {
    fn default() -> Self {
        Self {
            max_age: 300_000,         // 5 minutes
            future_tolerance: 60_000, // 1 minute
            require_timestamp: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Messaging<P: Provider + Sync> {
    config: MessagingConfig,
    storage: Arc<dyn Storage>,
    provider: P,
}

#[async_trait]
pub trait MessagingTrait: Send + Sync {
    async fn validate_and_set_entity(
        &self,
        message: &TypedData,
        signature: &[Felt],
    ) -> Result<Felt, MessagingError>;
}

#[async_trait]
impl<P: Provider + Sync + Send> MessagingTrait for Messaging<P> {
    async fn validate_and_set_entity(
        &self,
        message: &TypedData,
        signature: &[Felt],
    ) -> Result<Felt, MessagingError> {
        self.validate_and_set_entity(message, signature).await
    }
}

impl<P: Provider + Sync> Messaging<P> {
    pub fn new(config: MessagingConfig, storage: Arc<dyn Storage>, provider: P) -> Self {
        Self {
            config,
            storage,
            provider,
        }
    }

    pub async fn validate_and_set_entity(
        &self,
        message: &TypedData,
        signature: &[Felt],
    ) -> Result<Felt, MessagingError> {
        let ty = match validate_message(self.storage.clone(), message).await {
            Ok(parsed_message) => parsed_message,
            Err(e) => {
                warn!(
                    target: LOG_TARGET,
                    error = ?e,
                    "Validating message."
                );
                return Err(e);
            }
        };

        debug!(
            target: LOG_TARGET,
            message = ?message,
            "Received message."
        );

        info!(
            target: LOG_TARGET,
            message = ?message.primary_type().signature_ref_repr(),
            "Received message."
        );

        let keys = match ty_keys(&ty) {
            Ok(keys) => keys,
            Err(e) => {
                warn!(
                    target: LOG_TARGET,
                    error = ?e,
                    "Retrieving message model keys."
                );
                return Err(MessagingError::ModelNotFound(e.to_string()));
            }
        };
        let entity_id = poseidon_hash_many(&keys);
        let model_id = ty_model_id(&ty).unwrap();

        // Get the provided timestamp from the message, and validate it
        let message_timestamp = get_timestamp_from_ty(&ty);
        if let Some(timestamp) = message_timestamp {
            let now = Utc::now().timestamp_millis() as u64;

            if timestamp > now + self.config.future_tolerance {
                return Err(MessagingError::TimestampTooFuture);
            }

            if now.saturating_sub(timestamp) > self.config.max_age {
                return Err(MessagingError::TimestampTooOld);
            }
        }

        // Check if timestamps are required
        if self.config.require_timestamp && message_timestamp.is_none() {
            return Err(MessagingError::TimestampNotFound);
        }

        let entity_model = self.storage.entity_model(entity_id, model_id).await?;
        let entity_identity = match &entity_model {
            Some(entity_model) => match get_identity_from_ty(entity_model) {
                Ok(identity) => identity,
                Err(e) => {
                    warn!(
                        target: LOG_TARGET,
                        error = ?e,
                        "Getting identity from entity model."
                    );
                    return Err(e);
                }
            },
            _ => match get_identity_from_ty(&ty) {
                Ok(identity) => identity,
                Err(e) => {
                    warn!(
                        target: LOG_TARGET,
                        error = ?e,
                        "Getting identity from message."
                    );
                    return Err(e);
                }
            },
        };

        let entity_timestamp = match &entity_model {
            Some(entity_model) => get_timestamp_from_ty(entity_model),
            None => None,
        };

        if let Some(timestamp) = entity_timestamp {
            // We need to assert that the message has a timestamp & that it is greater than the entity timestamp
            if message_timestamp.ok_or(MessagingError::TimestampNotFound)? <= timestamp {
                return Err(MessagingError::InvalidTimestamp);
            }
        }

        // Verify the signature
        if !match validate_signature(&self.provider, entity_identity, message, signature).await {
            Ok(res) => res,
            Err(e) => {
                warn!(
                    target: LOG_TARGET,
                    error = ?e,
                    "Verifying signature."
                );
                return Err(e);
            }
        } {
            warn!(
                target: LOG_TARGET,
                message = ?message,
                signature = ?signature,
                "Invalid signature."
            );
            return Err(MessagingError::InvalidSignature);
        }

        if let Err(e) = set_entity(
            self.storage.clone(),
            ty.clone(),
            message_timestamp.unwrap_or_else(|| Utc::now().timestamp() as u64), // Use client timestamp if available, otherwise server timestamp
            entity_id,
            model_id,
            keys,
        )
        .await
        {
            warn!(
                target: LOG_TARGET,
                error = ?e,
                "Setting message."
            );
            return Err(e);
        }

        info!(
            target: LOG_TARGET,
            entity_id = %entity_id,
            model_id = %model_id,
            "Message verified and set."
        );

        Ok(entity_id)
    }
}
