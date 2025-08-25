use std::str::FromStr;

use async_graphql::dynamic::{
    Field, InputObject, InputValue, ResolverContext, TypeRef, ValueAccessor,
};
use async_graphql::{Error as GqlError, Name, Result};
use dojo_types::primitive::{Primitive, SqlType};
use strum::IntoEnumIterator;

use super::InputObjectTrait;
use crate::object::TypeMapping;
use crate::query::filter::{parse_filter, Comparator, Filter, FilterValue};
use crate::types::TypeData;

/// Format a value using the same formatting as Primitive::to_sql_value().
///
/// This function automatically stays in sync with any changes to the Primitive enum
/// by leveraging the existing `from_json_value` and `to_sql_value` methods.
///
/// # How it works
/// 1. Parse the input value into the appropriate primitive type using `from_json_value`
/// 2. Use `to_sql_value` to get the correctly formatted string
///
/// This approach eliminates the need to manually maintain padding mappings.
fn format_value_with_primitive_formatting(
    input_value: &str,
    mut primitive: Primitive,
) -> Result<String, String> {
    use serde_json::Value as JsonValue;

    // Based on the Primitive::from_json_value implementation, we need to provide the right JSON type:
    // - Small integers (I8-I32, U8-U32): Numbers
    // - Large integers (I64, I128, U64, U128): Strings
    // - Addresses and U256: Strings
    // - Hex strings: Always strings
    let json_value = match primitive {
        // Small integers can accept numbers
        Primitive::I8(_)
        | Primitive::I16(_)
        | Primitive::I32(_)
        | Primitive::U8(_)
        | Primitive::U16(_)
        | Primitive::U32(_)
        | Primitive::Bool(_) => {
            if let Ok(num) = input_value.parse::<i64>() {
                JsonValue::Number(serde_json::Number::from(num))
            } else {
                JsonValue::String(input_value.to_string())
            }
        }

        // I64 can accept both numbers and strings
        Primitive::I64(_) => {
            if let Ok(num) = input_value.parse::<i64>() {
                JsonValue::Number(serde_json::Number::from(num))
            } else {
                JsonValue::String(input_value.to_string())
            }
        }

        // Large integers and addresses must be strings
        Primitive::U64(_)
        | Primitive::I128(_)
        | Primitive::U128(_)
        | Primitive::U256(_)
        | Primitive::ContractAddress(_)
        | Primitive::ClassHash(_)
        | Primitive::Felt252(_)
        | Primitive::EthAddress(_) => JsonValue::String(input_value.to_string()),
    };

    // Use the primitive's own parsing logic
    primitive
        .from_json_value(json_value)
        .map_err(|e| format!("from_json_value error: {:?}", e))?;

    // Get the correctly formatted SQL value
    Ok(primitive.to_sql_value())
}

#[derive(Debug)]
pub struct WhereInputObject {
    pub type_name: String,
    pub type_mapping: TypeMapping,
    pub nested_inputs: Vec<WhereInputObject>,
}

impl WhereInputObject {
    fn build_field_mapping(type_name: &str, type_data: &TypeData) -> Vec<(Name, TypeData)> {
        if type_data.type_ref() == TypeRef::named("Enum")
            || type_data.type_ref() == TypeRef::named("bool")
        {
            return vec![(Name::new(type_name), type_data.clone())];
        }

        Comparator::iter().fold(
            vec![(Name::new(type_name), type_data.clone())],
            |mut acc, comparator| {
                let name = format!("{}{}", type_name, comparator.as_ref());
                match comparator {
                    Comparator::In | Comparator::NotIn => {
                        acc.push((Name::new(name), TypeData::List(Box::new(type_data.clone()))))
                    }
                    _ => {
                        acc.push((Name::new(name), type_data.clone()));
                    }
                }
                acc
            },
        )
    }

    pub fn new(type_name: &str, object_types: &TypeMapping) -> Self {
        let mut nested_inputs = Vec::new();
        let mut where_mapping = TypeMapping::new();

        for (field_name, type_data) in object_types {
            if !type_data.is_list() {
                match type_data {
                    TypeData::Nested((_, nested_types)) => {
                        // Create nested input object
                        let nested_input = WhereInputObject::new(
                            &format!("{}_{}", type_name, field_name),
                            nested_types,
                        );

                        // Add field for the nested input using TypeData::Nested
                        where_mapping.insert(
                            Name::new(field_name),
                            TypeData::Nested((
                                TypeRef::named(&nested_input.type_name),
                                nested_types.clone(),
                            )),
                        );
                        nested_inputs.push(nested_input);
                    }
                    _ => {
                        // Add regular field with comparators
                        for (name, mapped_type) in Self::build_field_mapping(field_name, type_data)
                        {
                            where_mapping.insert(name, mapped_type);
                        }
                    }
                }
            }
        }

        Self {
            type_name: format!("{}WhereInput", type_name),
            type_mapping: where_mapping,
            nested_inputs,
        }
    }
}

impl WhereInputObject {
    pub fn input_objects(&self) -> Vec<InputObject> {
        let mut objects = vec![self.input_object()];
        for nested in &self.nested_inputs {
            objects.extend(nested.input_objects());
        }
        objects
    }
}

impl InputObjectTrait for WhereInputObject {
    fn type_name(&self) -> &str {
        &self.type_name
    }

    fn type_mapping(&self) -> &TypeMapping {
        &self.type_mapping
    }

    fn input_object(&self) -> InputObject {
        self.type_mapping
            .iter()
            .fold(InputObject::new(self.type_name()), |acc, (ty_name, ty)| {
                acc.field(InputValue::new(ty_name.to_string(), ty.type_ref()))
            })
    }
}

pub fn where_argument(field: Field, type_name: &str) -> Field {
    field.argument(InputValue::new(
        "where",
        TypeRef::named(format!("{}WhereInput", type_name)),
    ))
}

fn parse_nested_where(
    input_object: &ValueAccessor<'_>,
    type_name: &str,
    type_data: &TypeData,
) -> Result<Vec<Filter>> {
    match type_data {
        TypeData::Nested((_, nested_mapping)) => {
            let nested_input = input_object.object()?;
            nested_mapping
                .iter()
                .filter_map(|(field_name, field_type)| {
                    nested_input.get(field_name).map(|input| {
                        let nested_filters = parse_where_value(
                            input,
                            &format!("{}.{}", type_name, field_name),
                            field_type,
                        )?;
                        Ok(nested_filters)
                    })
                })
                .collect::<Result<Vec<_>>>()
                .map(|filters| filters.into_iter().flatten().collect())
        }
        _ => Ok(vec![]),
    }
}

fn parse_where_value(
    input: ValueAccessor<'_>,
    field_path: &str,
    type_data: &TypeData,
) -> Result<Vec<Filter>> {
    match type_data {
        TypeData::Simple(_) => {
            if type_data.type_ref() == TypeRef::named("Enum") {
                let value = input.string()?;
                let mut filter = parse_filter(
                    &Name::new(field_path),
                    FilterValue::String(value.to_string()),
                );
                // complex enums have a nested option field for their variant name.
                // we trim the .option suffix to get the actual db field name
                filter.field = filter.field.trim_end_matches(".option").to_string();
                return Ok(vec![filter]);
            }

            let primitive = Primitive::from_str(&type_data.type_ref().to_string())?;
            let filter_value = match primitive.to_sql_type() {
                SqlType::Integer => parse_integer(input, field_path, primitive)?,
                SqlType::Text => parse_string(input, field_path, primitive)?,
            };

            Ok(vec![parse_filter(&Name::new(field_path), filter_value)])
        }
        TypeData::List(inner) => {
            let list = input.list()?;
            let values = list
                .iter()
                .map(|value| {
                    let primitive = Primitive::from_str(&inner.type_ref().to_string())?;
                    match primitive.to_sql_type() {
                        SqlType::Integer => parse_integer(value, field_path, primitive),
                        SqlType::Text => parse_string(value, field_path, primitive),
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(vec![parse_filter(
                &Name::new(field_path),
                FilterValue::List(values),
            )])
        }
        TypeData::Nested(_) => parse_nested_where(&input, field_path, type_data),
    }
}

pub fn parse_where_argument(
    ctx: &ResolverContext<'_>,
    where_mapping: &TypeMapping,
) -> Result<Option<Vec<Filter>>> {
    ctx.args.get("where").map_or(Ok(None), |where_input| {
        let input_object = where_input.object()?;
        where_mapping
            .iter()
            .filter_map(|(field_name, type_data)| {
                input_object
                    .get(field_name)
                    .map(|input| parse_where_value(input, field_name, type_data))
            })
            .collect::<Result<Vec<_>>>()
            .map(|filters| Some(filters.into_iter().flatten().collect()))
    })
}

fn parse_integer(
    input: ValueAccessor<'_>,
    type_name: &str,
    primitive: Primitive,
) -> Result<FilterValue> {
    match primitive {
        Primitive::Bool(_) => input
            .boolean()
            .map(|b| FilterValue::Int(b as i64)) // treat bool as int per sqlite
            .map_err(|_| GqlError::new(format!("Expected boolean on field {}", type_name))),
        _ => input
            .i64()
            .map(FilterValue::Int)
            .map_err(|_| GqlError::new(format!("Expected integer on field {}", type_name))),
    }
}

fn parse_string(
    input: ValueAccessor<'_>,
    type_name: &str,
    primitive: Primitive,
) -> Result<FilterValue> {
    match input.string() {
        Ok(i) => {
            // Use the primitive's own formatting logic for consistency
            match format_value_with_primitive_formatting(i, primitive) {
                Ok(formatted_value) => Ok(FilterValue::String(formatted_value)),
                Err(err) => {
                    // If parsing fails, fallback to original string for backward compatibility
                    eprintln!("Warning: primitive formatting failed for '{}': {}", i, err);
                    Ok(FilterValue::String(i.to_string()))
                }
            }
        }
        Err(_) => Err(GqlError::new(format!(
            "Expected string on field {}",
            type_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dojo_types::primitive::Primitive;

    #[test]
    fn test_automatic_primitive_formatting() {
        // Test that our automatic formatting matches Primitive::to_sql_value()

        // Test U64 with decimal input
        let result = format_value_with_primitive_formatting("12345", Primitive::U64(None));
        if let Err(e) = &result {
            println!("U64 decimal formatting failed: {}", e);
        }
        assert!(result.is_ok(), "U64 formatting should work: {:?}", result);
        assert_eq!(result.unwrap(), "0x0000000000003039");

        // Test U64 with hex input (like in the failing test)
        let result = format_value_with_primitive_formatting("0x5", Primitive::U64(None));
        if let Err(e) = &result {
            println!("U64 hex formatting failed: {}", e);
        }
        assert!(
            result.is_ok(),
            "U64 hex formatting should work: {:?}",
            result
        );
        assert_eq!(result.unwrap(), "0x0000000000000005");

        // Test U128 with decimal input
        let result = format_value_with_primitive_formatting("12345", Primitive::U128(None));
        if let Err(e) = &result {
            println!("U128 formatting failed: {}", e);
        }
        assert!(result.is_ok(), "U128 formatting should work: {:?}", result);
        assert_eq!(result.unwrap(), "0x00000000000000000000000000003039");

        // Test small integer (should work with numbers)
        let result = format_value_with_primitive_formatting("255", Primitive::U32(None));
        if let Err(e) = &result {
            println!("U32 formatting failed: {}", e);
        }
        assert!(result.is_ok(), "U32 formatting should work: {:?}", result);
        assert_eq!(result.unwrap(), "255"); // U32 is stored as integer, not hex
    }

    #[test]
    fn test_hex_input_formatting() {
        // Test hex inputs are properly formatted
        let primitive = Primitive::U256(None);
        let result = format_value_with_primitive_formatting("0x123abc", primitive).unwrap();
        // Should be padded to 64 hex characters
        assert!(result.starts_with("0x"));
        assert_eq!(result.len(), 66); // "0x" + 64 chars
    }

    #[test]
    fn test_decimal_input_formatting() {
        // Test decimal inputs are converted to proper hex format
        let primitive = Primitive::U64(None);
        let result = format_value_with_primitive_formatting("255", primitive);
        assert!(result.is_ok(), "U64 decimal formatting should work");
        assert_eq!(result.unwrap(), "0x00000000000000ff");
    }

    #[test]
    fn test_exact_failing_case() {
        // Test the exact case that's failing in the integration test
        let primitive = Primitive::U64(None);
        let result = format_value_with_primitive_formatting("0x5", primitive);
        if let Err(e) = &result {
            println!("Failed to format 0x5: {}", e);
        }
        assert!(result.is_ok(), "Should format 0x5 correctly: {:?}", result);
        let formatted = result.unwrap();
        println!("Formatted 0x5 as: {}", formatted);
        assert_eq!(formatted, "0x0000000000000005");

        // Also test that this matches what Primitive::to_sql_value produces directly
        let direct_primitive = Primitive::U64(Some(5));
        let direct_sql = direct_primitive.to_sql_value();
        println!("Direct Primitive::to_sql_value for U64(5): {}", direct_sql);
        assert_eq!(formatted, direct_sql);
    }
}
