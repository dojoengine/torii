use dojo_types::naming::is_valid_tag;
use dojo_types::schema::Ty;
use dojo_world::contracts::naming::compute_selector_from_tag;
use starknet::core::types::Felt;
use torii_sqlite::Sql;

use crate::error::MessagingError;

pub fn ty_keys(ty: &Ty) -> Result<Vec<Felt>, MessagingError> {
    if let Ty::Struct(s) = &ty {
        let mut keys = Vec::new();
        for m in s.keys() {
            keys.extend(
                m.serialize()
                    .map_err(MessagingError::SerializeModelKeyError)?,
            );
        }
        Ok(keys)
    } else {
        Err(MessagingError::InvalidType(
            "Message should be a struct".to_string(),
        ))
    }
}

pub fn ty_model_id(ty: &Ty) -> Result<Felt, MessagingError> {
    let namespaced_name = ty.name();

    if !is_valid_tag(&namespaced_name) {
        return Err(MessagingError::InvalidModelTag(namespaced_name));
    }

    let selector = compute_selector_from_tag(&namespaced_name);
    Ok(selector)
}

pub fn get_identity_from_ty(ty: &Ty) -> Result<Felt, MessagingError> {
    let identity = ty
        .as_struct()
        .ok_or_else(|| MessagingError::MessageNotStruct)?
        .get("identity")
        .ok_or_else(|| MessagingError::FieldNotFound("identity".to_string()))?
        .as_primitive()
        .ok_or_else(|| MessagingError::InvalidType("Identity should be a primitive".to_string()))?
        .as_contract_address()
        .ok_or_else(|| {
            MessagingError::InvalidType("Identity should be a contract address".to_string())
        })?;
    Ok(identity)
}

#[allow(clippy::too_many_arguments)]
pub async fn set_entity(
    db: &Sql,
    ty: Ty,
    block_timestamp: u64,
    entity_id: Felt,
    model_id: Felt,
    keys: &str,
) -> Result<(), MessagingError> {
    let event_id = format!("{:#064x}", block_timestamp);

    db.set_entity(
        ty,
        &event_id,
        block_timestamp,
        entity_id,
        model_id,
        Some(keys),
    )
    .await
    .map_err(MessagingError::SqliteError)?;
    db.execute().await?;
    Ok(())
}
