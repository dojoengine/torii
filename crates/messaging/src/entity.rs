use dojo_types::naming::is_valid_tag;
use dojo_types::schema::Ty;
use dojo_world::contracts::naming::compute_selector_from_tag;
use starknet::core::types::Felt;
use torii_sqlite::executor::QueryMessage;
use torii_sqlite::Sql;

use crate::error::MessageError;

#[derive(thiserror::Error, Debug)]
pub enum EntityError {
    #[error(transparent)]
    MessageError(#[from] MessageError),

    #[error(transparent)]
    SqliteError(#[from] torii_sqlite::error::Error),

    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),

    #[error(transparent)]
    SendError(#[from] tokio::sync::mpsc::error::SendError<QueryMessage>),
}

pub fn ty_keys(ty: &Ty) -> Result<Vec<Felt>, EntityError> {
    if let Ty::Struct(s) = &ty {
        let mut keys = Vec::new();
        for m in s.keys() {
            keys.extend(
                m.serialize().map_err(|e| {
                    EntityError::MessageError(MessageError::SerializeModelKeyError(e))
                })?,
            );
        }
        Ok(keys)
    } else {
        Err(EntityError::MessageError(MessageError::InvalidType(
            "Message should be a struct".to_string(),
        )))
    }
}

pub fn ty_model_id(ty: &Ty) -> Result<Felt, EntityError> {
    let namespaced_name = ty.name();

    if !is_valid_tag(&namespaced_name) {
        return Err(EntityError::MessageError(MessageError::InvalidModelTag(
            namespaced_name,
        )));
    }

    let selector = compute_selector_from_tag(&namespaced_name);
    Ok(selector)
}

pub fn get_identity_from_ty(ty: &Ty) -> Result<Felt, EntityError> {
    let identity = ty
        .as_struct()
        .ok_or_else(|| EntityError::MessageError(MessageError::MessageNotStruct))?
        .get("identity")
        .ok_or_else(|| {
            EntityError::MessageError(MessageError::FieldNotFound("identity".to_string()))
        })?
        .as_primitive()
        .ok_or_else(|| {
            EntityError::MessageError(MessageError::InvalidType(
                "Identity should be a primitive".to_string(),
            ))
        })?
        .as_contract_address()
        .ok_or_else(|| {
            EntityError::MessageError(MessageError::InvalidType(
                "Identity should be a contract address".to_string(),
            ))
        })?;
    Ok(identity)
}

#[allow(clippy::too_many_arguments)]
pub async fn set_entity(
    db: &mut Sql,
    ty: Ty,
    message_id: &str,
    block_timestamp: u64,
    entity_id: Felt,
    model_id: Felt,
    keys: &str,
) -> Result<(), EntityError> {
    db.set_entity(
        ty,
        message_id,
        block_timestamp,
        entity_id,
        model_id,
        Some(keys),
    )
    .await?;
    db.executor.send(QueryMessage::execute())?;
    Ok(())
}
