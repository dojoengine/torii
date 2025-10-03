use std::str::FromStr;

use async_graphql::dynamic::{Field, InputValue, ResolverContext, TypeRef};
use async_graphql::Error;
use starknet_crypto::Felt;
use torii_sqlite::utils::felt_to_sql_string;

use crate::utils::extract;

pub fn keys_argument(field: Field) -> Field {
    field.argument(InputValue::new(
        "keys",
        TypeRef::named_list(TypeRef::STRING),
    ))
}

pub fn parse_keys_argument(ctx: &ResolverContext<'_>) -> Result<Option<Vec<String>>, Error> {
    let keys = extract::<Vec<String>>(ctx.args.as_index_map(), "keys");

    if let Ok(keys) = keys {
        if !keys.iter().all(|s| is_hex_or_star(s)) {
            return Err("Key parts can only be hex string or wild card `*`".into());
        }

        let keys = keys
            .iter()
            .map(|s| {
                if s == "*" {
                    Ok("*".to_string())
                } else {
                    let felt = Felt::from_str(s)
                        .map_err(|e| Error::new(format!("Failed to parse key as felt: {}", e)))?;
                    Ok(felt_to_sql_string(&felt))
                }
            })
            .collect::<Result<Vec<_>, Error>>()?;
        return Ok(Some(keys));
    }

    Ok(None)
}

fn is_hex_or_star(s: &str) -> bool {
    if s == "*" {
        return true;
    }
    let s = if let Some(stripped) = s.strip_prefix("0x") {
        stripped
    } else {
        s
    };

    s.chars().all(|c| c.is_ascii_hexdigit())
}
