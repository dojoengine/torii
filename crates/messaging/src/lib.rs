pub mod entity;
pub mod error;
pub mod parsing;
pub mod validation;

use std::str::FromStr;

pub use entity::{get_identity_from_ty, set_entity, ty_keys, ty_model_id};
pub use error::MessagingError;
pub use parsing::parse_value_to_ty;
use sqlx::types::chrono::Utc;
use starknet::providers::Provider;
use starknet_core::types::{typed_data::TypeReference, TypedData};
use starknet_crypto::{poseidon_hash_many, Felt};
use torii_sqlite::{utils::felts_to_sql_string, Sql};
use tracing::{debug, info, warn};
pub use validation::{validate_message, validate_signature};

pub const LOG_TARGET: &str = "torii::messaging";

pub async fn validate_and_set_entity<P: Provider + Sync>(
    db: &mut Sql,
    message: &TypedData,
    signature: &[Felt],
    provider: &P,
) -> Result<(), MessagingError> {
    let ty = match validate_message(db, message).await {
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

    // retrieve entity identity from db
    let mut pool = match db.pool.acquire().await {
        Ok(pool) => pool,
        Err(e) => {
            warn!(
                target: LOG_TARGET,
                error = ?e,
                "Acquiring pool."
            );
            return Err(MessagingError::SqliteError(e.into()));
        }
    };

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
    let keys_str = felts_to_sql_string(&keys);
    let entity_id = poseidon_hash_many(&keys);
    let model_id = ty_model_id(&ty).unwrap();

    // select only identity field, if doesn't exist, empty string
    let query = format!("SELECT identity FROM [{}] WHERE internal_id = ?", ty.name());
    let entity_identity: Option<String> = match sqlx::query_scalar(&query)
        .bind(format!("{:#x}", entity_id))
        .fetch_optional(&mut *pool)
        .await
    {
        Ok(entity_identity) => entity_identity,
        Err(e) => {
            warn!(
                target: LOG_TARGET,
                error = ?e,
                "Fetching entity."
            );
            return Err(MessagingError::SqliteError(e.into()));
        }
    };

    let entity_identity = match entity_identity {
        Some(identity) => match Felt::from_str(&identity) {
            Ok(identity) => identity,
            Err(e) => {
                warn!(
                    target: LOG_TARGET,
                    error = ?e,
                    "Parsing identity."
                );
                return Err(e.into());
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
    if !match validate_signature(
        provider,
        entity_identity,
        &message,
        signature,
    )
    .await
    {
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
        db,
        ty.clone(),
        Utc::now().timestamp() as u64,
        entity_id,
        model_id,
        &keys_str,
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

    Ok(())
}
