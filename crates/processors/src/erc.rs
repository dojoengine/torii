use std::{str::FromStr, sync::Arc};

use cainome_cairo_serde::{ByteArray, CairoSerde};
use data_url::{mime::Mime, DataUrl};
use starknet::{
    core::{
        types::{requests::CallRequest, BlockId, BlockTag, FunctionCall, U256},
        utils::{get_selector_from_name, parse_cairo_short_string},
    },
    macros::selector,
    providers::{Provider, ProviderRequestData, ProviderResponseData},
};
use starknet_crypto::Felt;
use tokio::sync::Semaphore;
use torii_cache::Cache;
use torii_storage::Storage;
use tracing::{debug, warn};

use crate::{
    error::{Error, ParseError, TokenMetadataError},
    fetch::{fetch_content_from_http, fetch_content_from_ipfs},
};

#[allow(dead_code)]
const SQL_FELT_DELIMITER: &str = "/";

#[allow(dead_code)]
pub fn felts_to_sql_string(felts: &[Felt]) -> String {
    felts
        .iter()
        .map(|k| format!("{:#x}", k))
        .collect::<Vec<String>>()
        .join(SQL_FELT_DELIMITER)
        + SQL_FELT_DELIMITER
}

pub fn felt_to_sql_string(felt: &Felt) -> String {
    format!("{:#x}", felt)
}

pub fn felt_and_u256_to_sql_string(felt: &Felt, u256: &U256) -> String {
    format!("{}:{}", felt_to_sql_string(felt), u256_to_sql_string(u256))
}

pub fn u256_to_sql_string(u256: &U256) -> String {
    format!("{:#064x}", u256)
}

pub async fn try_register_nft_token_metadata<P: Provider + Sync>(
    id: &str,
    contract_address: Felt,
    actual_token_id: U256,
    provider: &P,
    cache: Arc<dyn Cache + Send + Sync>,
    storage: Arc<dyn Storage>,
    nft_metadata_semaphore: Arc<Semaphore>,
) -> Result<(), Error> {
    let _lock = match cache.get_token_registration_lock(id).await {
        Some(lock) => lock,
        None => return Ok(()), // Already registered by another thread
    };
    let _guard = _lock.lock().await;
    if cache.is_token_registered(id).await {
        return Ok(());
    }

    let _permit = nft_metadata_semaphore
        .acquire()
        .await
        .map_err(|e| Error::TokenMetadataError(TokenMetadataError::AcquireError(e)))?;
    let metadata = fetch_token_metadata(contract_address, actual_token_id, provider).await?;

    storage
        .register_nft_token(contract_address, actual_token_id, metadata)
        .await?;

    cache.mark_token_registered(id).await;

    Ok(())
}

pub(crate) async fn try_register_erc20_token<P: Provider + Sync>(
    contract_address: Felt,
    provider: &P,
    storage: Arc<dyn Storage>,
    cache: Arc<dyn Cache + Send + Sync>,
) -> Result<(), Error> {
    let token_id = format!("{:#x}", contract_address);
    let _lock = match cache.get_token_registration_lock(&token_id).await {
        Some(lock) => lock,
        None => return Ok(()), // Already registered by another thread
    };
    let _guard = _lock.lock().await;
    if cache.is_token_registered(&token_id).await {
        return Ok(());
    }

    let (name, symbol, decimals) = fetch_erc20_token_metadata(provider, contract_address).await?;
    storage
        .register_erc20_token(contract_address, name, symbol, decimals)
        .await?;

    cache.mark_token_registered(&token_id).await;

    Ok(())
}

pub async fn fetch_erc20_token_metadata<P: Provider + Sync>(
    provider: &P,
    contract_address: Felt,
) -> Result<(String, String, u8), TokenMetadataError> {
    let block_id = BlockId::Tag(BlockTag::Pending);
    let requests = vec![
        ProviderRequestData::Call(CallRequest {
            request: FunctionCall {
                contract_address,
                entry_point_selector: selector!("name"),
                calldata: vec![],
            },
            block_id,
        }),
        ProviderRequestData::Call(CallRequest {
            request: FunctionCall {
                contract_address,
                entry_point_selector: selector!("symbol"),
                calldata: vec![],
            },
            block_id,
        }),
        ProviderRequestData::Call(CallRequest {
            request: FunctionCall {
                contract_address,
                entry_point_selector: selector!("decimals"),
                calldata: vec![],
            },
            block_id,
        }),
    ];

    let results = provider.batch_requests(requests).await?;

    // Parse name
    let name = match &results[0] {
        ProviderResponseData::Call(name) if name.len() == 1 => {
            parse_cairo_short_string(&name[0])
                .map_err(|e| TokenMetadataError::Parse(ParseError::ParseCairoShortString(e)))?
        }
        ProviderResponseData::Call(name) => ByteArray::cairo_deserialize(name, 0)
            .map_err(|e| TokenMetadataError::Parse(ParseError::CairoSerdeError(e)))?
            .to_string()
            .map_err(|e| TokenMetadataError::Parse(ParseError::FromUtf8(e)))?,
        _ => return Err(TokenMetadataError::InvalidTokenName),
    };

    // Parse symbol
    let symbol = match &results[1] {
        ProviderResponseData::Call(symbol) if symbol.len() == 1 => {
            parse_cairo_short_string(&symbol[0])
                .map_err(|e| TokenMetadataError::Parse(ParseError::ParseCairoShortString(e)))?
        }
        ProviderResponseData::Call(symbol) => ByteArray::cairo_deserialize(symbol, 0)
            .map_err(|e| TokenMetadataError::Parse(ParseError::CairoSerdeError(e)))?
            .to_string()
            .map_err(|e| TokenMetadataError::Parse(ParseError::FromUtf8(e)))?,
        _ => return Err(TokenMetadataError::InvalidTokenSymbol),
    };

    // Parse decimals
    let decimals = match &results[2] {
        ProviderResponseData::Call(decimals) => u8::cairo_deserialize(decimals, 0)
            .map_err(|e| TokenMetadataError::Parse(ParseError::CairoSerdeError(e)))?,
        _ => return Err(TokenMetadataError::InvalidTokenDecimals),
    };

    Ok((name, symbol, decimals))
}

pub async fn fetch_token_uri<P: Provider + Sync>(
    provider: &P,
    contract_address: Felt,
    token_id: U256,
) -> Result<String, TokenMetadataError> {
    let token_uri = if let Ok(token_uri) = provider
        .call(
            FunctionCall {
                contract_address,
                entry_point_selector: get_selector_from_name("token_uri").unwrap(),
                calldata: vec![token_id.low().into(), token_id.high().into()],
            },
            BlockId::Tag(BlockTag::Pending),
        )
        .await
    {
        token_uri
    } else if let Ok(token_uri) = provider
        .call(
            FunctionCall {
                contract_address,
                entry_point_selector: get_selector_from_name("tokenURI").unwrap(),
                calldata: vec![token_id.low().into(), token_id.high().into()],
            },
            BlockId::Tag(BlockTag::Pending),
        )
        .await
    {
        token_uri
    } else if let Ok(token_uri) = provider
        .call(
            FunctionCall {
                contract_address,
                entry_point_selector: get_selector_from_name("uri").unwrap(),
                calldata: vec![token_id.low().into(), token_id.high().into()],
            },
            BlockId::Tag(BlockTag::Pending),
        )
        .await
    {
        token_uri
    } else {
        warn!(
            contract_address = format!("{:#x}", contract_address),
            token_id = %token_id,
            "Error fetching token URI, empty metadata will be used instead.",
        );
        return Ok("".to_string());
    };

    let mut token_uri = if let Ok(byte_array) = ByteArray::cairo_deserialize(&token_uri, 0) {
        byte_array
            .to_string()
            .map_err(|e| TokenMetadataError::Parse(ParseError::FromUtf8(e)))?
    } else if let Ok(felt_array) = Vec::<Felt>::cairo_deserialize(&token_uri, 0) {
        felt_array
            .iter()
            .map(parse_cairo_short_string)
            .collect::<Result<Vec<String>, _>>()
            .map(|strings| strings.join(""))
            .map_err(|e| TokenMetadataError::Parse(ParseError::ParseCairoShortString(e)))?
    } else {
        debug!(
            contract_address = format!("{:#x}", contract_address),
            token_id = %token_id,
            token_uri = %token_uri.iter().map(|f| format!("{:#x}", f)).collect::<Vec<String>>().join(", "),
            "token_uri is neither ByteArray nor Array<Felt>"
        );
        "".to_string()
    };

    // Handle ERC1155 {id} replacement
    let token_id_hex = format!("{:064x}", token_id);
    token_uri = token_uri.replace("{id}", &token_id_hex);

    Ok(token_uri)
}

pub async fn fetch_token_metadata<P: Provider + Sync>(
    contract_address: Felt,
    token_id: U256,
    provider: &P,
) -> Result<String, TokenMetadataError> {
    let token_uri = fetch_token_uri(provider, contract_address, token_id).await?;

    if token_uri.is_empty() {
        return Ok("".to_string());
    }

    let metadata = fetch_metadata(&token_uri).await;
    match metadata {
        Ok(metadata) => serde_json::to_string(&metadata)
            .map_err(|e| TokenMetadataError::Parse(ParseError::FromJsonStr(e))),
        Err(_) => {
            warn!(
                contract_address = format!("{:#x}", contract_address),
                token_id = %token_id,
                token_uri = %token_uri,
                "Error fetching metadata, empty metadata will be used instead.",
            );
            Ok("".to_string())
        }
    }
}

// given a uri which can be either http/https url or data uri, fetch the metadata erc721
// metadata json schema
pub async fn fetch_metadata(token_uri: &str) -> Result<serde_json::Value, TokenMetadataError> {
    // Parse the token_uri

    match token_uri {
        uri if uri.starts_with("http") || uri.starts_with("https") => {
            // Fetch metadata from HTTP/HTTPS URL
            debug!(token_uri = %token_uri, "Fetching metadata from http/https URL");
            let response = fetch_content_from_http(token_uri)
                .await
                .map_err(TokenMetadataError::Http)?;

            let json: serde_json::Value = serde_json::from_slice(&response)
                .map_err(|e| TokenMetadataError::Parse(ParseError::FromJsonStr(e)))?;

            Ok(json)
        }
        uri if uri.starts_with("ipfs") => {
            let cid = uri.strip_prefix("ipfs://").unwrap();
            debug!(cid = %cid, "Fetching metadata from IPFS");
            let response = fetch_content_from_ipfs(cid)
                .await
                .map_err(TokenMetadataError::Ipfs)?;

            let json: serde_json::Value = serde_json::from_slice(&response)
                .map_err(|e| TokenMetadataError::Parse(ParseError::FromJsonStr(e)))?;

            Ok(json)
        }
        uri if uri.starts_with("data") => {
            // Parse and decode data URI
            debug!(data_uri = %token_uri, "Parsing metadata from data URI");

            // HACK: https://github.com/servo/rust-url/issues/908
            let uri = token_uri.replace("#", "%23");

            let data_url = DataUrl::process(&uri).map_err(TokenMetadataError::DataUrl)?;

            // Ensure the MIME type is JSON
            if data_url.mime_type() != &Mime::from_str("application/json").unwrap() {
                return Err(TokenMetadataError::InvalidMimeType(
                    data_url.mime_type().to_string(),
                ));
            }

            let decoded = data_url
                .decode_to_vec()
                .map_err(TokenMetadataError::InvalidBase64)?;
            // HACK: Loot Survior NFT metadata contains control characters which makes the json
            // DATA invalid so filter them out
            let decoded_str = String::from_utf8_lossy(&decoded.0)
                .chars()
                .filter(|c| !c.is_ascii_control())
                .collect::<String>();
            let sanitized_json = sanitize_json_string(&decoded_str);

            let json: serde_json::Value = serde_json::from_str(&sanitized_json)
                .map_err(|e| TokenMetadataError::Parse(ParseError::FromJsonStr(e)))?;

            Ok(json)
        }
        uri => Err(TokenMetadataError::UnsupportedUriScheme(uri.to_string())),
    }
}

/// Sanitizes a JSON string by escaping unescaped double quotes within string values.
pub fn sanitize_json_string(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();
    let mut in_string = false;
    let mut backslash_count = 0;

    while let Some(c) = chars.next() {
        if !in_string {
            if c == '"' {
                in_string = true;
                backslash_count = 0;
                result.push('"');
            } else {
                result.push(c);
            }
        } else if c == '\\' {
            backslash_count += 1;
            result.push('\\');
        } else if c == '"' {
            if backslash_count % 2 == 0 {
                // Unescaped double quote
                let mut temp_chars = chars.clone();
                // Skip whitespace
                while let Some(&next_c) = temp_chars.peek() {
                    if next_c.is_whitespace() {
                        temp_chars.next();
                    } else {
                        break;
                    }
                }
                // Check next non-whitespace character
                if let Some(&next_c) = temp_chars.peek() {
                    if next_c == ':' || next_c == ',' || next_c == '}' {
                        // End of string
                        result.push('"');
                        in_string = false;
                    } else {
                        // Internal unescaped quote, escape it
                        result.push_str("\\\"");
                    }
                } else {
                    // End of input, treat as end of string
                    result.push('"');
                    in_string = false;
                }
            } else {
                // Escaped double quote, part of string
                result.push('"');
            }
            backslash_count = 0;
        } else {
            result.push(c);
            backslash_count = 0;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_json_string() {
        let input = r#"{"name":""Rage Shout" DireWolf"}"#;
        let expected = r#"{"name":"\"Rage Shout\" DireWolf"}"#;
        let sanitized = sanitize_json_string(input);
        assert_eq!(sanitized, expected);

        let input_escaped = r#"{"name":"\"Properly Escaped\" Wolf"}"#;
        let expected_escaped = r#"{"name":"\"Properly Escaped\" Wolf"}"#;
        let sanitized_escaped = sanitize_json_string(input_escaped);
        assert_eq!(sanitized_escaped, expected_escaped);
    }
}
