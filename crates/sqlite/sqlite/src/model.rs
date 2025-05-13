
use std::str::FromStr;

use async_trait::async_trait;
use crypto_bigint::U256;
use dojo_types::primitive::{Primitive, PrimitiveError};
use dojo_types::schema::Ty;
use dojo_world::contracts::abigen::model::Layout;
use dojo_world::contracts::model::ModelReader;
use serde_json::Value as JsonValue;
use sqlx::sqlite::SqliteRow;
use sqlx::{Pool, Row, Sqlite};
use starknet::core::types::Felt;

use crate::error::ParseError;

use super::error::{self, Error};

#[derive(Debug)]
pub struct ModelSQLReader {
    /// Namespace of the model
    namespace: String,
    /// The name of the model
    name: String,
    /// The selector of the model
    selector: Felt,
    /// The class hash of the model
    class_hash: Felt,
    /// The contract address of the model
    contract_address: Felt,
    pool: Pool<Sqlite>,
    packed_size: u32,
    unpacked_size: u32,
    layout: Layout,
}

impl ModelSQLReader {
    pub async fn new(selector: Felt, pool: Pool<Sqlite>) -> Result<Self, Error> {
        let (namespace, name, class_hash, contract_address, packed_size, unpacked_size, layout): (
            String,
            String,
            String,
            String,
            u32,
            u32,
            String,
        ) = sqlx::query_as(
            "SELECT namespace, name, class_hash, contract_address, packed_size, unpacked_size, \
             layout FROM models WHERE id = ?",
        )
        .bind(format!("{:#x}", selector))
        .fetch_one(&pool)
        .await?;

        let class_hash = Felt::from_hex(&class_hash).map_err(error::ParseError::FromStr)?;
        let contract_address =
            Felt::from_hex(&contract_address).map_err(error::ParseError::FromStr)?;

        let layout = serde_json::from_str(&layout).map_err(error::ParseError::FromJsonStr)?;

        Ok(Self {
            namespace,
            name,
            selector,
            class_hash,
            contract_address,
            pool,
            packed_size,
            unpacked_size,
            layout,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ModelReader<Error> for ModelSQLReader {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn selector(&self) -> Felt {
        self.selector
    }

    fn class_hash(&self) -> Felt {
        self.class_hash
    }

    fn contract_address(&self) -> Felt {
        self.contract_address
    }

    async fn schema(&self) -> Result<Ty, Error> {
        let schema: String = sqlx::query_scalar("SELECT schema FROM models WHERE id = ?")
            .bind(format!("{:#x}", self.selector))
            .fetch_one(&self.pool)
            .await?;

        Ok(serde_json::from_str(&schema).map_err(error::ParseError::FromJsonStr)?)
    }

    async fn packed_size(&self) -> Result<u32, Error> {
        Ok(self.packed_size)
    }

    async fn unpacked_size(&self) -> Result<u32, Error> {
        Ok(self.unpacked_size)
    }

    async fn layout(&self) -> Result<Layout, Error> {
        Ok(self.layout.clone())
    }
}

/// Populate the values of a Ty (schema) from SQLite row.
pub fn map_row_to_ty(
    path: &str,
    name: &str,
    ty: &mut Ty,
    // the row that contains non dynamic data for Ty
    row: &SqliteRow,
) -> Result<(), Error> {
    let column_name = if path.is_empty() {
        name
    } else {
        &format!("{}.{}", path, name)
    };

    match ty {
        Ty::Primitive(primitive) => {
            match &primitive {
                Primitive::I8(_) => {
                    let value = row.try_get::<i8, &str>(column_name)?;
                    primitive.set_i8(Some(value))?;
                }
                Primitive::I16(_) => {
                    let value = row.try_get::<i16, &str>(column_name)?;
                    primitive.set_i16(Some(value))?;
                }
                Primitive::I32(_) => {
                    let value = row.try_get::<i32, &str>(column_name)?;
                    primitive.set_i32(Some(value))?;
                }
                Primitive::I64(_) => {
                    let value = row.try_get::<i64, &str>(column_name)?;
                    primitive.set_i64(Some(value))?;
                }
                Primitive::I128(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    let hex_str = value.trim_start_matches("0x");

                    primitive.set_i128(Some(
                        u128::from_str_radix(hex_str, 16).map_err(ParseError::ParseIntError)?
                            as i128,
                    ))?;
                }
                Primitive::U8(_) => {
                    let value = row.try_get::<u8, &str>(column_name)?;
                    primitive.set_u8(Some(value))?;
                }
                Primitive::U16(_) => {
                    let value = row.try_get::<u16, &str>(column_name)?;
                    primitive.set_u16(Some(value))?;
                }
                Primitive::U32(_) => {
                    let value = row.try_get::<u32, &str>(column_name)?;
                    primitive.set_u32(Some(value))?;
                }
                Primitive::U64(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    let hex_str = value.trim_start_matches("0x");

                    if !hex_str.is_empty() {
                        primitive.set_u64(Some(
                            u64::from_str_radix(hex_str, 16).map_err(ParseError::ParseIntError)?,
                        ))?;
                    }
                }
                Primitive::U128(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    let hex_str = value.trim_start_matches("0x");

                    if !hex_str.is_empty() {
                        primitive.set_u128(Some(
                            u128::from_str_radix(hex_str, 16).map_err(ParseError::ParseIntError)?,
                        ))?;
                    }
                }
                Primitive::U256(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    let hex_str = value.trim_start_matches("0x");

                    if !hex_str.is_empty() {
                        primitive.set_u256(Some(U256::from_be_hex(hex_str)))?;
                    }
                }
                Primitive::Bool(_) => {
                    let value = row.try_get::<bool, &str>(column_name)?;
                    primitive.set_bool(Some(value))?;
                }
                Primitive::Felt252(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    if !value.is_empty() {
                        primitive.set_felt252(Some(
                            Felt::from_str(&value).map_err(ParseError::FromStr)?,
                        ))?;
                    }
                }
                Primitive::ClassHash(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    if !value.is_empty() {
                        primitive.set_class_hash(Some(
                            Felt::from_str(&value).map_err(ParseError::FromStr)?,
                        ))?;
                    }
                }
                Primitive::ContractAddress(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    if !value.is_empty() {
                        primitive.set_contract_address(Some(
                            Felt::from_str(&value).map_err(ParseError::FromStr)?,
                        ))?;
                    }
                }
                Primitive::EthAddress(_) => {
                    let value = row.try_get::<String, &str>(column_name)?;
                    if !value.is_empty() {
                        primitive.set_eth_address(Some(
                            Felt::from_str(&value).map_err(ParseError::FromStr)?,
                        ))?;
                    }
                }
            };
        }
        Ty::Enum(enum_ty) => {
            let option_name = row.try_get::<String, &str>(column_name)?;
            if !option_name.is_empty() {
                enum_ty.set_option(&option_name)?;
            }

            for option in &mut enum_ty.options {
                if option.name != option_name {
                    continue;
                }

                map_row_to_ty(column_name, &option.name, &mut option.ty, row)?;
            }
        }
        Ty::Struct(struct_ty) => {
            for member in &mut struct_ty.children {
                map_row_to_ty(column_name, &member.name, &mut member.ty, row)?;
            }
        }
        Ty::Tuple(ty) => {
            for (i, member) in ty.iter_mut().enumerate() {
                map_row_to_ty(column_name, &i.to_string(), member, row)?;
            }
        }
        Ty::Array(ty) => {
            let schema = ty[0].clone();
            let serialized_array = row.try_get::<String, &str>(column_name)?;
            if serialized_array.is_empty() {
                *ty = vec![];
                return Ok(());
            }

            let values: Vec<JsonValue> =
                serde_json::from_str(&serialized_array).map_err(ParseError::FromJsonStr)?;
            *ty = values
                .iter()
                .map(|v| {
                    let mut ty = schema.clone();
                    ty.from_json_value(v.clone())?;
                    Result::<_, PrimitiveError>::Ok(ty)
                })
                .collect::<Result<Vec<Ty>, _>>()?;
        }
        Ty::ByteArray(bytearray) => {
            let value = row.try_get::<String, &str>(column_name)?;
            *bytearray = value;
        }
    };

    Ok(())
}