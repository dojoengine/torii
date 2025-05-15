use std::collections::HashMap;
use std::mem;
use std::str::FromStr;

use anyhow::{Context, Result};
use cainome::cairo_serde::{ByteArray, CairoSerde};
use data_url::mime::Mime;
use data_url::DataUrl;
use reqwest::Client;
use starknet::core::types::requests::CallRequest;
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall, U256};
use starknet::core::utils::{get_selector_from_name, parse_cairo_short_string};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use super::utils::{u256_to_sql_string, I256};
use super::{Sql, SQL_FELT_DELIMITER};
use crate::constants::TOKEN_TRANSFER_TABLE;
use crate::executor::erc::{RegisterNftTokenQuery, UpdateNftMetadataQuery};
use crate::executor::{
    ApplyBalanceDiffQuery, Argument, QueryMessage, QueryType, RegisterErc20TokenQuery,
};
use crate::types::ContractType;
use crate::utils::{
    felt_and_u256_to_sql_string, felt_to_sql_string, felts_to_sql_string, fetch_content_from_ipfs,
    sanitize_json_string, utc_dt_string_from_timestamp,
};

impl Sql {
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_erc20_transfer<P: Provider + Sync>(
        &mut self,
        contract_address: Felt,
        from_address: Felt,
        to_address: Felt,
        amount: U256,
        provider: &P,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<()> {
        // contract_address
        let token_id = felt_to_sql_string(&contract_address);

        // optimistically add the token_id to cache
        // this cache is used while applying the cache diff
        // so we need to make sure that all RegisterErc*Token queries
        // are applied before the cache diff is applied
        let token_exists: bool = !self
            .local_cache
            .try_register_token_id(token_id.to_string())
            .await;
        if !token_exists {
            self.register_erc20_token_metadata(contract_address, &token_id, provider)
                .await?;
        }

        self.store_erc_transfer_event(
            contract_address,
            from_address,
            to_address,
            amount,
            &token_id,
            block_timestamp,
            event_id,
        )?;

        {
            let mut erc_cache = self.local_cache.erc_cache.write().await;
            if from_address != Felt::ZERO {
                // from_address/contract_address/
                let from_balance_id = felts_to_sql_string(&[from_address, contract_address]);
                let from_balance = erc_cache
                    .entry((ContractType::ERC20, from_balance_id))
                    .or_default();
                *from_balance -= I256::from(amount);
            }

            if to_address != Felt::ZERO {
                let to_balance_id = felts_to_sql_string(&[to_address, contract_address]);
                let to_balance = erc_cache
                    .entry((ContractType::ERC20, to_balance_id))
                    .or_default();
                *to_balance += I256::from(amount);
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn handle_nft_transfer<P: Provider + Sync>(
        &mut self,
        provider: &P,
        contract_address: Felt,
        from_address: Felt,
        to_address: Felt,
        token_id: U256,
        amount: U256,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<()> {
        // contract_address:id
        let id = felt_and_u256_to_sql_string(&contract_address, &token_id);
        // optimistically add the token_id to cache
        // this cache is used while applying the cache diff
        // so we need to make sure that all RegisterErc*Token queries
        // are applied before the cache diff is applied
        let token_exists: bool = !self.local_cache.try_register_token_id(id.clone()).await;
        if !token_exists {
            let metadata = fetch_token_metadata(
                contract_address,
                token_id,
                provider,
                &self.nft_metadata_semaphore,
            )
            .await?;
            self.register_nft_token_metadata(&id, contract_address, token_id, metadata)
                .await?;
        }

        self.store_erc_transfer_event(
            contract_address,
            from_address,
            to_address,
            amount,
            &id,
            block_timestamp,
            event_id,
        )?;

        // from_address/contract_address:id
        {
            let mut erc_cache = self.local_cache.erc_cache.write().await;
            if from_address != Felt::ZERO {
                let from_balance_id = format!(
                    "{}{SQL_FELT_DELIMITER}{}",
                    felt_to_sql_string(&from_address),
                    &id
                );
                let from_balance = erc_cache
                    .entry((ContractType::ERC721, from_balance_id))
                    .or_default();
                *from_balance -= I256::from(amount);
            }

            if to_address != Felt::ZERO {
                let to_balance_id = format!(
                    "{}{SQL_FELT_DELIMITER}{}",
                    felt_to_sql_string(&to_address),
                    &id
                );
                let to_balance = erc_cache
                    .entry((ContractType::ERC721, to_balance_id))
                    .or_default();
                *to_balance += I256::from(amount);
            }
        }

        Ok(())
    }

    pub async fn update_nft_metadata<P: Provider + Sync>(
        &mut self,
        provider: &P,
        contract_address: Felt,
        token_id: U256,
    ) -> Result<()> {
        let id = felt_and_u256_to_sql_string(&contract_address, &token_id);
        if !self.local_cache.contains_token_id(&id).await {
            return Ok(());
        }

        let metadata = fetch_token_metadata(
            contract_address,
            token_id,
            provider,
            &self.nft_metadata_semaphore,
        )
        .await?;

        self.executor.send(QueryMessage::new(
            "".to_string(),
            vec![],
            QueryType::UpdateNftMetadata(UpdateNftMetadataQuery {
                id,
                contract_address,
                token_id,
                metadata,
            }),
        ))?;

        Ok(())
    }

    async fn register_erc20_token_metadata<P: Provider + Sync>(
        &mut self,
        contract_address: Felt,
        token_id: &str,
        provider: &P,
    ) -> Result<()> {
        // Prepare batch requests for name, symbol, and decimals
        let block_id = BlockId::Tag(BlockTag::Pending);
        let requests = vec![
            ProviderRequestData::Call(CallRequest {
                request: FunctionCall {
                    contract_address,
                    entry_point_selector: get_selector_from_name("name").unwrap(),
                    calldata: vec![],
                },
                block_id,
            }),
            ProviderRequestData::Call(CallRequest {
                request: FunctionCall {
                    contract_address,
                    entry_point_selector: get_selector_from_name("symbol").unwrap(),
                    calldata: vec![],
                },
                block_id,
            }),
            ProviderRequestData::Call(CallRequest {
                request: FunctionCall {
                    contract_address,
                    entry_point_selector: get_selector_from_name("decimals").unwrap(),
                    calldata: vec![],
                },
                block_id,
            }),
        ];

        let results = provider.batch_requests(requests).await?;

        // Parse name
        let name = match &results[0] {
            ProviderResponseData::Call(name) if name.len() == 1 => {
                parse_cairo_short_string(&name[0]).unwrap()
            }
            ProviderResponseData::Call(name) => ByteArray::cairo_deserialize(name, 0)
                .expect("Return value not ByteArray")
                .to_string()
                .expect("Return value not String"),
            _ => return Err(anyhow::anyhow!("Invalid response for name")),
        };

        // Parse symbol
        let symbol = match &results[1] {
            ProviderResponseData::Call(symbol) if symbol.len() == 1 => {
                parse_cairo_short_string(&symbol[0]).unwrap()
            }
            ProviderResponseData::Call(symbol) => ByteArray::cairo_deserialize(symbol, 0)
                .expect("Return value not ByteArray")
                .to_string()
                .expect("Return value not String"),
            _ => return Err(anyhow::anyhow!("Invalid response for symbol")),
        };

        // Parse decimals
        let decimals = match &results[2] {
            ProviderResponseData::Call(decimals) => {
                u8::cairo_deserialize(decimals, 0).expect("Return value not u8")
            }
            _ => return Err(anyhow::anyhow!("Invalid response for decimals")),
        };

        self.executor.send(QueryMessage::new(
            "".to_string(),
            vec![],
            QueryType::RegisterErc20Token(RegisterErc20TokenQuery {
                token_id: token_id.to_string(),
                contract_address,
                name,
                symbol,
                decimals,
            }),
        ))?;

        Ok(())
    }

    async fn register_nft_token_metadata(
        &mut self,
        id: &str,
        contract_address: Felt,
        actual_token_id: U256,
        metadata: String,
    ) -> Result<()> {
        self.executor.send(QueryMessage::new(
            "".to_string(),
            vec![],
            QueryType::RegisterNftToken(RegisterNftTokenQuery {
                id: id.to_string(),
                contract_address,
                token_id: actual_token_id,
                metadata,
            }),
        ))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn store_erc_transfer_event(
        &mut self,
        contract_address: Felt,
        from: Felt,
        to: Felt,
        amount: U256,
        token_id: &str,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<()> {
        let id = format!("{}:{}", event_id, token_id);
        let insert_query = format!(
            "INSERT INTO {TOKEN_TRANSFER_TABLE} (id, contract_address, from_address, to_address, \
             amount, token_id, event_id, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        );

        self.executor.send(QueryMessage::new(
            insert_query.to_string(),
            vec![
                Argument::String(id),
                Argument::FieldElement(contract_address),
                Argument::FieldElement(from),
                Argument::FieldElement(to),
                Argument::String(u256_to_sql_string(&amount)),
                Argument::String(token_id.to_string()),
                Argument::String(event_id.to_string()),
                Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
            ],
            QueryType::Other,
        ))?;

        Ok(())
    }

    pub async fn apply_cache_diff(&mut self) -> Result<()> {
        if !self.local_cache.erc_cache.read().await.is_empty() {
            let mut erc_cache = self.local_cache.erc_cache.write().await;
            self.executor.send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::ApplyBalanceDiff(ApplyBalanceDiffQuery {
                    erc_cache: mem::replace(&mut erc_cache, HashMap::with_capacity(64)),
                }),
            ))?;
        }
        Ok(())
    }
}

pub async fn fetch_token_uri<P: Provider + Sync>(
    provider: &P,
    contract_address: Felt,
    token_id: U256,
) -> Result<String> {
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
        byte_array.to_string().expect("Return value not String")
    } else if let Ok(felt_array) = Vec::<Felt>::cairo_deserialize(&token_uri, 0) {
        felt_array
            .iter()
            .map(parse_cairo_short_string)
            .collect::<Result<Vec<String>, _>>()
            .map(|strings| strings.join(""))
            .map_err(|_| anyhow::anyhow!("Failed parsing Array<Felt> to String"))?
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
    nft_metadata_semaphore: &Semaphore,
) -> Result<String> {
    let token_uri = fetch_token_uri(provider, contract_address, token_id).await?;

    if token_uri.is_empty() {
        return Ok("".to_string());
    }

    let permit = nft_metadata_semaphore.acquire().await?;
    let metadata = fetch_metadata(&token_uri).await;
    drop(permit);
    match metadata {
        Ok(metadata) => serde_json::to_string(&metadata).context("Failed to serialize metadata"),
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
pub async fn fetch_metadata(token_uri: &str) -> Result<serde_json::Value> {
    // Parse the token_uri

    match token_uri {
        uri if uri.starts_with("http") || uri.starts_with("https") => {
            // Fetch metadata from HTTP/HTTPS URL
            debug!(token_uri = %token_uri, "Fetching metadata from http/https URL");
            let client = Client::new();
            let response = client
                .get(token_uri)
                .send()
                .await
                .context("Failed to fetch metadata from URL")?;

            let bytes = response
                .bytes()
                .await
                .context("Failed to read response bytes")?;
            let json: serde_json::Value = serde_json::from_slice(&bytes).context(format!(
                "Failed to parse metadata JSON from response: {:?}",
                bytes
            ))?;

            Ok(json)
        }
        uri if uri.starts_with("ipfs") => {
            let cid = uri.strip_prefix("ipfs://").unwrap();
            debug!(cid = %cid, "Fetching metadata from IPFS");
            let response = fetch_content_from_ipfs(cid)
                .await
                .context("Failed to fetch metadata from IPFS")?;

            let json: serde_json::Value = serde_json::from_slice(&response).context(format!(
                "Failed to parse metadata JSON from IPFS: {:?}, data: {:?}",
                cid, &response
            ))?;

            Ok(json)
        }
        uri if uri.starts_with("data") => {
            // Parse and decode data URI
            debug!(data_uri = %token_uri, "Parsing metadata from data URI");

            // HACK: https://github.com/servo/rust-url/issues/908
            let uri = token_uri.replace("#", "%23");

            let data_url = DataUrl::process(&uri).context("Failed to parse data URI")?;

            // Ensure the MIME type is JSON
            if data_url.mime_type() != &Mime::from_str("application/json").unwrap() {
                return Err(anyhow::anyhow!("Data URI is not of JSON type"));
            }

            let decoded = data_url
                .decode_to_vec()
                .context("Failed to decode data URI")?;
            // HACK: Loot Survior NFT metadata contains control characters which makes the json
            // DATA invalid so filter them out
            let decoded_str = String::from_utf8_lossy(&decoded.0)
                .chars()
                .filter(|c| !c.is_ascii_control())
                .collect::<String>();
            let sanitized_json = sanitize_json_string(&decoded_str);

            let json: serde_json::Value =
                serde_json::from_str(&sanitized_json).with_context(|| {
                    format!("Failed to parse metadata JSON from data URI: {}", &uri)
                })?;

            Ok(json)
        }
        uri => Err(anyhow::anyhow!(
            "Unsupported URI scheme found in token URI: {}",
            uri
        )),
    }
}
