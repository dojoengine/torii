use std::str::FromStr;

use crypto_bigint::U256;
use dojo_types::{primitive::Primitive, schema::Ty};
use starknet_core::types::typed_data::Value;
use starknet_crypto::Felt;

use crate::error::MessageError as Error;

macro_rules! from_str {
    ($string:expr, $type:ty) => {
        if $string.starts_with("0x") || $string.starts_with("0X") {
            <$type>::from_str_radix(&$string[2..], 16)
        } else {
            <$type>::from_str($string)
        }
        .map_err(|e| Error::ParseIntError(e))
    };
}

pub fn parse_value_to_ty(value: &Value, ty: &mut Ty) -> Result<(), Error> {
    match value {
        Value::Object(object) => match ty {
            Ty::Struct(struct_) => {
                for (key, value) in &object.fields {
                    let member = struct_
                        .children
                        .iter_mut()
                        .find(|member| member.name == *key)
                        .ok_or_else(|| Error::FieldNotFound(key.clone()))?;

                    parse_value_to_ty(value, &mut member.ty)?;
                }
            }
            Ty::Primitive(Primitive::U256(u256)) => {
                let mut low = Ty::Primitive(Primitive::U128(None));
                let mut high = Ty::Primitive(Primitive::U128(None));

                parse_value_to_ty(&object.fields["low"], &mut low)?;
                parse_value_to_ty(&object.fields["high"], &mut high)?;

                let low = low.as_primitive().unwrap().as_u128().unwrap();
                let high = high.as_primitive().unwrap().as_u128().unwrap();

                let mut bytes = [0u8; 32];
                bytes[..16].copy_from_slice(&high.to_be_bytes());
                bytes[16..].copy_from_slice(&low.to_be_bytes());

                *u256 = Some(U256::from_be_slice(&bytes));
            }
            Ty::Enum(enum_) => {
                let (option_name, value) = object
                    .fields
                    .first()
                    .ok_or_else(|| Error::FieldNotFound("enum variant not found".to_string()))?;

                enum_
                    .options
                    .iter_mut()
                    .map(|option| {
                        if option.name == *option_name {
                            parse_value_to_ty(value, &mut option.ty)
                        } else {
                            Ok(())
                        }
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                enum_.set_option(option_name).map_err(Error::EnumError)?;
            }
            _ => {
                return Err(Error::InvalidType(format!(
                    "Invalid object type for {}",
                    ty.name()
                )));
            }
        },
        Value::Array(values) => match ty {
            Ty::Array(array) => {
                let inner_type = array[0].clone();

                array.clear();

                for value in &values.elements {
                    let mut ty = inner_type.clone();
                    parse_value_to_ty(value, &mut ty)?;
                    array.push(ty);
                }
            }
            Ty::Tuple(tuple) => {
                if tuple.len() != values.elements.len() {
                    return Err(Error::InvalidTupleLength);
                }

                for (i, value) in tuple.iter_mut().enumerate() {
                    parse_value_to_ty(&values.elements[i], value)?;
                }
            }
            _ => {
                return Err(Error::InvalidType(format!(
                    "Invalid array type for {}",
                    ty.name()
                )));
            }
        },
        Value::UnsignedInteger(number) => match ty {
            Ty::Primitive(primitive) => match *primitive {
                Primitive::U8(ref mut u8) => {
                    *u8 = Some(*number as u8);
                }
                Primitive::U16(ref mut u16) => {
                    *u16 = Some(*number as u16);
                }
                Primitive::U32(ref mut u32) => {
                    *u32 = Some(*number as u32);
                }
                Primitive::U64(ref mut u64) => {
                    *u64 = Some(*number as u64);
                }
                Primitive::U128(ref mut u128) => {
                    *u128 = Some(*number);
                }
                _ => {
                    return Err(Error::InvalidType(format!(
                        "Invalid number type for {}",
                        ty.name()
                    )));
                }
            },
            _ => {
                return Err(Error::InvalidType(format!(
                    "Invalid number type for {}",
                    ty.name()
                )));
            }
        },
        Value::SignedInteger(number) => match ty {
            Ty::Primitive(primitive) => match *primitive {
                Primitive::I8(ref mut i8) => {
                    *i8 = Some(*number as i8);
                }
                Primitive::I16(ref mut i16) => {
                    *i16 = Some(*number as i16);
                }
                Primitive::I32(ref mut i32) => {
                    *i32 = Some(*number as i32);
                }
                Primitive::I64(ref mut i64) => {
                    *i64 = Some(*number as i64);
                }
                Primitive::I128(ref mut i128) => {
                    *i128 = Some(*number);
                }
                _ => {
                    return Err(Error::InvalidType(format!(
                        "Invalid number type for {}",
                        ty.name()
                    )));
                }
            },
            _ => {
                return Err(Error::InvalidType(format!(
                    "Invalid number type for {}",
                    ty.name()
                )));
            }
        },
        Value::Boolean(boolean) => {
            *ty = Ty::Primitive(Primitive::Bool(Some(*boolean)));
        }
        Value::String(string) => match ty {
            Ty::Primitive(primitive) => match primitive {
                Primitive::I64(v) => {
                    *v = Some(from_str!(string, i64)?);
                }
                Primitive::I128(v) => {
                    *v = Some(from_str!(string, i128)?);
                }
                Primitive::U64(v) => {
                    *v = Some(from_str!(string, u64)?);
                }
                Primitive::U128(v) => {
                    *v = Some(from_str!(string, u128)?);
                }
                Primitive::Felt252(v) => {
                    *v = Some(Felt::from_str(string).map_err(Error::ParseFeltError)?);
                }
                Primitive::ClassHash(v) => {
                    *v = Some(Felt::from_str(string).map_err(Error::ParseFeltError)?);
                }
                Primitive::ContractAddress(v) => {
                    *v = Some(Felt::from_str(string).map_err(Error::ParseFeltError)?);
                }
                Primitive::EthAddress(v) => {
                    *v = Some(Felt::from_str(string).map_err(Error::ParseFeltError)?);
                }
                _ => {
                    return Err(Error::InvalidType("Invalid primitive type".to_string()));
                }
            },
            Ty::ByteArray(s) => {
                s.clone_from(string);
            }
            _ => {
                return Err(Error::InvalidType(format!(
                    "Invalid string type for {}",
                    ty.name()
                )));
            }
        },
    }

    Ok(())
}
