use std::sync::Arc;

use dojo_types::naming::try_compute_selector_from_tag;
use dojo_types::schema::Ty;
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall};
use starknet::macros::selector;
use starknet::providers::Provider;
use starknet_core::types::typed_data::TypeReference;
use starknet_core::types::TypedData;
use torii_storage::Storage;

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
                entry_point_selector: selector!("is_valid_signature"),
                calldata,
            },
            BlockId::Tag(BlockTag::PreConfirmed),
        )
        .await
        .map_err(MessagingError::ProviderError)
        .map(|res| res[0] != Felt::ZERO)
}

pub async fn validate_message(
    world_address: Felt,
    storage: Arc<dyn Storage>,
    message: &TypedData,
) -> Result<Ty, MessagingError> {
    let tag = message.primary_type().signature_ref_repr();

    let selector =
        try_compute_selector_from_tag(&tag).map_err(|_| MessagingError::InvalidModelTag(tag))?;

    let mut ty = storage
        .model(Some(world_address), selector)
        .await
        .map_err(|e| MessagingError::ModelNotFound(e.to_string()))?
        .schema;

    parse_value_to_ty(message.message(), &mut ty)?;

    Ok(ty)
}
