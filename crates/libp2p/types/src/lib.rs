use serde::{Deserialize, Serialize};
use starknet::core::types::Felt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub signature: Vec<Felt>,
    // The raw TypedData. Should be deserializable to a TypedData struct.
    pub message: String,
}
