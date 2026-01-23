use chrono::{DateTime, Utc};
use crypto_bigint::{Encoding, U256};
use dojo_types::primitive::Primitive;
use dojo_types::schema::{Enum, EnumOption, Member, Struct, Ty};
use serde::{Deserialize, Serialize};
use starknet::core::types::Felt;

use crate::error::ProtoError;
use crate::proto;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct EntityWithMetadata<const EVENT_MESSAGE: bool = false> {
    pub entity: Entity<EVENT_MESSAGE>,
    pub event_id: String,
    pub keys: Vec<Felt>,
    pub deleted: bool,
    pub match_model: Option<Ty>,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Hash, Eq, Clone)]
pub struct Entity<const EVENT_MESSAGE: bool = false> {
    pub world_address: Felt,
    pub hashed_keys: Felt,
    pub models: Vec<Struct>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub executed_at: DateTime<Utc>,
}

impl<const EVENT_MESSAGE: bool> From<Entity<EVENT_MESSAGE>> for proto::types::Entity {
    fn from(entity: Entity<EVENT_MESSAGE>) -> Self {
        proto::types::Entity {
            world_address: entity.world_address.to_bytes_be().to_vec(),
            hashed_keys: entity.hashed_keys.to_bytes_be().to_vec(),
            models: entity
                .models
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>(),
            created_at: entity.created_at.timestamp() as u64,
            updated_at: entity.updated_at.timestamp() as u64,
            executed_at: entity.executed_at.timestamp() as u64,
        }
    }
}

impl<const EVENT_MESSAGE: bool> TryFrom<proto::types::Entity> for Entity<EVENT_MESSAGE> {
    type Error = ProtoError;
    fn try_from(entity: proto::types::Entity) -> Result<Self, Self::Error> {
        Ok(Self {
            world_address: Felt::from_bytes_be_slice(&entity.world_address),
            hashed_keys: Felt::from_bytes_be_slice(&entity.hashed_keys),
            models: entity
                .models
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            created_at: DateTime::from_timestamp(entity.created_at as i64, 0).unwrap(),
            updated_at: DateTime::from_timestamp(entity.updated_at as i64, 0).unwrap(),
            executed_at: DateTime::from_timestamp(entity.executed_at as i64, 0).unwrap(),
        })
    }
}

impl From<Ty> for proto::types::Ty {
    fn from(ty: Ty) -> Self {
        let ty_type = match ty {
            Ty::Primitive(primitive) => Some(proto::types::ty::TyType::Primitive(primitive.into())),
            Ty::Enum(r#enum) => Some(proto::types::ty::TyType::Enum(r#enum.into())),
            Ty::Struct(r#struct) => Some(proto::types::ty::TyType::Struct(r#struct.into())),
            Ty::Tuple(tuple) => Some(proto::types::ty::TyType::Tuple(proto::types::Array {
                children: tuple.into_iter().map(Into::into).collect::<Vec<_>>(),
            })),
            Ty::Array(array) => Some(proto::types::ty::TyType::Array(proto::types::Array {
                children: array.into_iter().map(Into::into).collect::<Vec<_>>(),
            })),
            Ty::ByteArray(string) => Some(proto::types::ty::TyType::Bytearray(string)),
            Ty::FixedSizeArray(_) => todo!(),
        };

        proto::types::Ty { ty_type }
    }
}

impl TryFrom<proto::types::Member> for Member {
    type Error = ProtoError;
    fn try_from(member: proto::types::Member) -> Result<Self, Self::Error> {
        Ok(Member {
            name: member.name,
            ty: member
                .ty
                .ok_or(ProtoError::MissingExpectedData("ty".to_string()))?
                .try_into()?,
            key: member.key,
        })
    }
}

impl From<Member> for proto::types::Member {
    fn from(member: Member) -> Self {
        proto::types::Member {
            name: member.name,
            ty: Some(member.ty.into()),
            key: member.key,
        }
    }
}

impl TryFrom<proto::types::EnumOption> for EnumOption {
    type Error = ProtoError;
    fn try_from(option: proto::types::EnumOption) -> Result<Self, Self::Error> {
        Ok(EnumOption {
            name: option.name,
            ty: option
                .ty
                .ok_or(ProtoError::MissingExpectedData("ty".to_string()))?
                .try_into()?,
        })
    }
}

impl From<EnumOption> for proto::types::EnumOption {
    fn from(option: EnumOption) -> Self {
        proto::types::EnumOption {
            name: option.name,
            ty: Some(option.ty.into()),
        }
    }
}

impl TryFrom<proto::types::Enum> for Enum {
    type Error = ProtoError;
    fn try_from(r#enum: proto::types::Enum) -> Result<Self, Self::Error> {
        Ok(Enum {
            name: r#enum.name.clone(),
            option: Some(r#enum.option as u8),
            options: r#enum
                .options
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<Enum> for proto::types::Enum {
    fn from(r#enum: Enum) -> Self {
        proto::types::Enum {
            name: r#enum.name,
            option: r#enum.option.unwrap_or_default() as u32,
            options: r#enum
                .options
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>(),
        }
    }
}

impl TryFrom<proto::types::Struct> for Struct {
    type Error = ProtoError;
    fn try_from(r#struct: proto::types::Struct) -> Result<Self, Self::Error> {
        Ok(Struct {
            name: r#struct.name,
            children: r#struct
                .children
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<Struct> for proto::types::Struct {
    fn from(r#struct: Struct) -> Self {
        proto::types::Struct {
            name: r#struct.name,
            children: r#struct
                .children
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>(),
        }
    }
}

impl TryFrom<proto::types::Primitive> for Primitive {
    type Error = ProtoError;
    fn try_from(primitive: proto::types::Primitive) -> Result<Self, Self::Error> {
        let value = primitive
            .primitive_type
            .ok_or(ProtoError::MissingExpectedData(
                "primitive_type".to_string(),
            ))?;

        let primitive = match &value {
            proto::types::primitive::PrimitiveType::Bool(bool) => Primitive::Bool(Some(*bool)),
            proto::types::primitive::PrimitiveType::I8(int) => Primitive::I8(Some(*int as i8)),
            proto::types::primitive::PrimitiveType::I16(int) => Primitive::I16(Some(*int as i16)),
            proto::types::primitive::PrimitiveType::I32(int) => Primitive::I32(Some(*int)),
            proto::types::primitive::PrimitiveType::I64(int) => Primitive::I64(Some(*int)),
            proto::types::primitive::PrimitiveType::I128(bytes) => Primitive::I128(Some(
                i128::from_be_bytes(bytes.as_slice().try_into().map_err(ProtoError::FromSlice)?),
            )),
            proto::types::primitive::PrimitiveType::U8(int) => Primitive::U8(Some(*int as u8)),
            proto::types::primitive::PrimitiveType::U16(int) => Primitive::U16(Some(*int as u16)),
            proto::types::primitive::PrimitiveType::U32(int) => Primitive::U32(Some(*int)),
            proto::types::primitive::PrimitiveType::U64(int) => Primitive::U64(Some(*int)),
            proto::types::primitive::PrimitiveType::U128(bytes) => Primitive::U128(Some(
                u128::from_be_bytes(bytes.as_slice().try_into().map_err(ProtoError::FromSlice)?),
            )),
            proto::types::primitive::PrimitiveType::Felt252(felt) => {
                Primitive::Felt252(Some(Felt::from_bytes_be_slice(felt.as_slice())))
            }
            proto::types::primitive::PrimitiveType::ClassHash(felt) => {
                Primitive::ClassHash(Some(Felt::from_bytes_be_slice(felt.as_slice())))
            }
            proto::types::primitive::PrimitiveType::ContractAddress(felt) => {
                Primitive::ContractAddress(Some(Felt::from_bytes_be_slice(felt.as_slice())))
            }
            proto::types::primitive::PrimitiveType::EthAddress(felt) => {
                Primitive::EthAddress(Some(Felt::from_bytes_be_slice(felt.as_slice())))
            }
            proto::types::primitive::PrimitiveType::U256(bytes) => Primitive::U256(Some(
                U256::from_be_bytes(bytes.as_slice().try_into().map_err(ProtoError::FromSlice)?),
            )),
        };

        Ok(primitive)
    }
}

impl From<Primitive> for proto::types::Primitive {
    fn from(primitive: Primitive) -> Self {
        let value = match primitive {
            Primitive::Bool(bool) => {
                proto::types::primitive::PrimitiveType::Bool(bool.unwrap_or_default())
            }
            Primitive::I8(i8) => {
                proto::types::primitive::PrimitiveType::I8(i8.unwrap_or_default() as i32)
            }
            Primitive::I16(i16) => {
                proto::types::primitive::PrimitiveType::I16(i16.unwrap_or_default() as i32)
            }
            Primitive::I32(i32) => {
                proto::types::primitive::PrimitiveType::I32(i32.unwrap_or_default())
            }
            Primitive::I64(i64) => {
                proto::types::primitive::PrimitiveType::I64(i64.unwrap_or_default())
            }
            Primitive::I128(i128) => proto::types::primitive::PrimitiveType::I128(
                i128.unwrap_or_default().to_be_bytes().to_vec(),
            ),
            Primitive::U8(u8) => {
                proto::types::primitive::PrimitiveType::U8(u8.unwrap_or_default() as u32)
            }
            Primitive::U16(u16) => {
                proto::types::primitive::PrimitiveType::U16(u16.unwrap_or_default() as u32)
            }
            Primitive::U32(u32) => {
                proto::types::primitive::PrimitiveType::U32(u32.unwrap_or_default())
            }
            Primitive::U64(u64) => {
                proto::types::primitive::PrimitiveType::U64(u64.unwrap_or_default())
            }
            Primitive::U128(u128) => proto::types::primitive::PrimitiveType::U128(
                u128.unwrap_or_default().to_be_bytes().to_vec(),
            ),
            Primitive::Felt252(felt) => proto::types::primitive::PrimitiveType::Felt252(
                felt.unwrap_or_default().to_bytes_be().to_vec(),
            ),
            Primitive::ClassHash(felt) => proto::types::primitive::PrimitiveType::ClassHash(
                felt.unwrap_or_default().to_bytes_be().to_vec(),
            ),
            Primitive::ContractAddress(felt) => {
                proto::types::primitive::PrimitiveType::ContractAddress(
                    felt.unwrap_or_default().to_bytes_be().to_vec(),
                )
            }
            Primitive::EthAddress(felt) => proto::types::primitive::PrimitiveType::EthAddress(
                felt.unwrap_or_default().to_bytes_be().to_vec(),
            ),
            Primitive::U256(u256) => proto::types::primitive::PrimitiveType::U256(
                u256.unwrap_or_default().to_be_bytes().to_vec(),
            ),
        };

        proto::types::Primitive {
            primitive_type: Some(value),
        }
    }
}

impl TryFrom<proto::types::Ty> for Ty {
    type Error = ProtoError;
    fn try_from(ty: proto::types::Ty) -> Result<Self, Self::Error> {
        match ty
            .ty_type
            .ok_or(ProtoError::MissingExpectedData("ty_type".to_string()))?
        {
            proto::types::ty::TyType::Primitive(primitive) => {
                Ok(Ty::Primitive(primitive.try_into()?))
            }
            proto::types::ty::TyType::Struct(r#struct) => Ok(Ty::Struct(r#struct.try_into()?)),
            proto::types::ty::TyType::Enum(r#enum) => Ok(Ty::Enum(r#enum.try_into()?)),
            proto::types::ty::TyType::Tuple(array) => Ok(Ty::Tuple(
                array
                    .children
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            proto::types::ty::TyType::FixedSizeArray(array) => {
                let proto::types::FixedSizeArray { children, size } = array;

                let elems = children
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Ty::FixedSizeArray((elems, size)))
            }
            proto::types::ty::TyType::Array(array) => Ok(Ty::Array(
                array
                    .children
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            proto::types::ty::TyType::Bytearray(string) => Ok(Ty::ByteArray(string)),
        }
    }
}
