use dojo_types::naming::is_valid_tag;
use dojo_types::schema::Ty;
use dojo_world::contracts::naming::compute_selector_from_tag;
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall};
use starknet::core::utils::get_selector_from_name;
use starknet::providers::Provider;
use starknet_core::types::typed_data::TypeReference;
use starknet_core::types::TypedData;
use torii_sqlite::Sql;

use crate::error::MessagingError;
use crate::parsing::parse_value_to_ty;

pub async fn validate_signature<P: Provider + Sync>(
    provider: &P,
    entity_identity: Felt,
    message: &TypedData,
    signature: &[Felt],
) -> Result<bool, MessagingError> {
    let message_hash = message.message_hash(entity_identity)?;

    let mut calldata = vec![message_hash, Felt::from(signature.len())];
    calldata.extend(signature);
    provider
        .call(
            FunctionCall {
                contract_address: entity_identity,
                entry_point_selector: get_selector_from_name("is_valid_signature").unwrap(),
                calldata,
            },
            BlockId::Tag(BlockTag::Pending),
        )
        .await
        .map_err(MessagingError::ProviderError)
        .map(|res| res[0] != Felt::ZERO)
}

pub async fn validate_message(db: &Sql, message: &TypedData) -> Result<Ty, MessagingError> {
    let tag = message.primary_type().signature_ref_repr();
    if !is_valid_tag(&tag) {
        return Err(MessagingError::InvalidModelTag(tag));
    }

    let selector = compute_selector_from_tag(&tag);

    let mut ty = db
        .model(selector)
        .await
        .map_err(|e| MessagingError::ModelNotFound(e.to_string()))?
        .schema;

    parse_value_to_ty(message.message(), &mut ty)?;

    Ok(ty)
}