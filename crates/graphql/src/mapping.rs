use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::TypeRef;
use async_graphql::Name;
use dojo_types::primitive::Primitive;
use std::sync::LazyLock;

use crate::constants::{CONTENT_TYPE_NAME, SOCIAL_TYPE_NAME, TOKEN_UNION_TYPE_NAME};
use crate::types::{GraphqlType, TypeData, TypeMapping};

pub static ENTITY_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("id"),
            TypeData::Simple(TypeRef::named(TypeRef::ID)),
        ),
        (
            Name::new("keys"),
            TypeData::Simple(TypeRef::named_list(TypeRef::STRING)),
        ),
        (
            Name::new("eventId"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("executedAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
        (
            Name::new("createdAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
        (
            Name::new("updatedAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
    ])
});

pub static EVENT_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("id"),
            TypeData::Simple(TypeRef::named(TypeRef::ID)),
        ),
        (
            Name::new("keys"),
            TypeData::Simple(TypeRef::named_list(TypeRef::STRING)),
        ),
        (
            Name::new("data"),
            TypeData::Simple(TypeRef::named_list(TypeRef::STRING)),
        ),
        (
            Name::new("executedAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
        (
            Name::new("createdAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
        (
            Name::new("transactionHash"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
    ])
});

pub static MODEL_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("id"),
            TypeData::Simple(TypeRef::named(TypeRef::ID)),
        ),
        (
            Name::new("name"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("namespace"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("classHash"),
            TypeData::Simple(TypeRef::named(Primitive::Felt252(None).to_string())),
        ),
        (
            Name::new("contractAddress"),
            TypeData::Simple(TypeRef::named(Primitive::Felt252(None).to_string())),
        ),
        (
            Name::new("transactionHash"),
            TypeData::Simple(TypeRef::named(Primitive::Felt252(None).to_string())),
        ),
        (
            Name::new("executedAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
        (
            Name::new("createdAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
    ])
});

pub static CALL_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("transactionHash"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("contractAddress"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("entrypoint"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("calldata"),
            TypeData::Simple(TypeRef::named_list(TypeRef::STRING)),
        ),
        (
            Name::new("callType"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("callerAddress"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
    ])
});

pub static TRANSACTION_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("id"),
            TypeData::Simple(TypeRef::named(TypeRef::ID)),
        ),
        (
            Name::new("transactionHash"),
            TypeData::Simple(TypeRef::named(Primitive::Felt252(None).to_string())),
        ),
        (
            Name::new("senderAddress"),
            TypeData::Simple(TypeRef::named(Primitive::Felt252(None).to_string())),
        ),
        (
            Name::new("calldata"),
            TypeData::Simple(TypeRef::named_list(Primitive::Felt252(None).to_string())),
        ),
        (
            Name::new("maxFee"),
            TypeData::Simple(TypeRef::named(Primitive::Felt252(None).to_string())),
        ),
        (
            Name::new("signature"),
            TypeData::Simple(TypeRef::named_list(Primitive::Felt252(None).to_string())),
        ),
        (
            Name::new("nonce"),
            TypeData::Simple(TypeRef::named(Primitive::Felt252(None).to_string())),
        ),
        (
            Name::new("executedAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
        (
            Name::new("createdAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
        (
            Name::new("blockNumber"),
            TypeData::Simple(TypeRef::named(TypeRef::INT)),
        ),
    ])
});

pub static PAGE_INFO_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    TypeMapping::from([
        (
            Name::new("hasPreviousPage"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::BOOLEAN)),
        ),
        (
            Name::new("hasNextPage"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::BOOLEAN)),
        ),
        (
            Name::new("startCursor"),
            TypeData::Simple(TypeRef::named(GraphqlType::Cursor.to_string())),
        ),
        (
            Name::new("endCursor"),
            TypeData::Simple(TypeRef::named(GraphqlType::Cursor.to_string())),
        ),
    ])
});

pub static SOCIAL_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("name"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("url"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
    ])
});

pub static CONTENT_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("name"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("description"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("website"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("iconUri"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("coverUri"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("socials"),
            TypeData::Simple(TypeRef::named_list(SOCIAL_TYPE_NAME)),
        ),
    ])
});

// Todo: refactor this to use the same type as the one in dojo-world
pub static METADATA_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("id"),
            TypeData::Simple(TypeRef::named(TypeRef::ID)),
        ),
        (
            Name::new("uri"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("worldAddress"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("content"),
            TypeData::Nested((TypeRef::named(CONTENT_TYPE_NAME), IndexMap::new())),
        ),
        (
            Name::new("iconImg"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("coverImg"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("executedAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
        (
            Name::new("createdAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
        (
            Name::new("updatedAt"),
            TypeData::Simple(TypeRef::named(GraphqlType::DateTime.to_string())),
        ),
    ])
});

pub static TOKEN_BALANCE_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([(
        Name::new("tokenMetadata"),
        TypeData::Nested((TypeRef::named_nn(TOKEN_UNION_TYPE_NAME), IndexMap::new())),
    )])
});

pub static TOKEN_TRANSFER_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("from"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("to"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("executedAt"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("tokenMetadata"),
            TypeData::Nested((TypeRef::named_nn(TOKEN_UNION_TYPE_NAME), IndexMap::new())),
        ),
        (
            Name::new("transactionHash"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
    ])
});

pub static ERC20_TOKEN_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("name"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("symbol"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("decimals"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::INT)),
        ),
        (
            Name::new("contractAddress"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("amount"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
    ])
});

pub static ERC721_TOKEN_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("name"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("symbol"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("tokenId"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("contractAddress"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("metadata"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("metadataName"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("metadataDescription"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("metadataAttributes"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("imagePath"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
    ])
});

pub static ERC1155_TOKEN_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("name"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("symbol"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("tokenId"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("contractAddress"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("amount"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("metadata"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("metadataName"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("metadataDescription"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("metadataAttributes"),
            TypeData::Simple(TypeRef::named(TypeRef::STRING)),
        ),
        (
            Name::new("imagePath"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
    ])
});

pub static EMPTY_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([(
        Name::new("id"),
        TypeData::Simple(TypeRef::named(TypeRef::ID)),
    )])
});

pub static CONTROLLER_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("id"),
            TypeData::Simple(TypeRef::named(TypeRef::ID)),
        ),
        (
            Name::new("username"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("address"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
        (
            Name::new("deployedAt"),
            TypeData::Simple(TypeRef::named_nn(GraphqlType::DateTime.to_string())),
        ),
    ])
});

pub static TOKEN_TYPE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([(
        Name::new("tokenMetadata"),
        TypeData::Nested((TypeRef::named_nn(TOKEN_UNION_TYPE_NAME), IndexMap::new())),
    )])
});

pub static PUBLISH_MESSAGE_INPUT_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([
        (
            Name::new("signature"),
            TypeData::Simple(TypeRef::named_nn_list(TypeRef::STRING)),
        ),
        (
            Name::new("message"),
            TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
        ),
    ])
});

pub static PUBLISH_MESSAGE_RESPONSE_MAPPING: LazyLock<TypeMapping> = LazyLock::new(|| {
    IndexMap::from([(
        Name::new("entityId"),
        TypeData::Simple(TypeRef::named_nn(TypeRef::STRING)),
    )])
});
