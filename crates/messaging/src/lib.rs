pub mod entity;
pub mod error;
pub mod parsing;
pub mod validation;

use std::sync::Arc;

pub use entity::{get_identity_from_ty, set_entity, ty_keys, ty_model_id};
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

pub async fn validate_and_set_entity<P: Provider + Sync>(
    storage: Arc<dyn Storage>,
    message: &TypedData,
    signature: &[Felt],
    provider: &P,
) -> Result<Felt, MessagingError> {
    let ty = match validate_message(storage.clone(), message).await {
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

    let entity_model = storage.entity_model(entity_id, model_id).await?;

    let entity_identity = match entity_model {
        Some(entity_model) => match get_identity_from_ty(&entity_model) {
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

    // TODO: have a nonce in model to check
    // against entity nonce and message nonce
    // to prevent replay attacks.

    // Verify the signature
    if !match validate_signature(provider, entity_identity, message, signature).await {
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
        storage.clone(),
        ty.clone(),
        Utc::now().timestamp() as u64,
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
