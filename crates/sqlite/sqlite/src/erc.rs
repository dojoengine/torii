use std::str::FromStr;

use cainome::cairo_serde::{ByteArray, CairoSerde};
use data_url::mime::Mime;
use data_url::DataUrl;
use starknet::core::types::requests::CallRequest;
use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall, U256};
use starknet::core::utils::{get_selector_from_name, parse_cairo_short_string};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use tracing::{debug, warn};

use super::utils::u256_to_sql_string;
use super::Sql;
use crate::constants::TOKEN_TRANSFER_TABLE;
use crate::error::{Error, ParseError, TokenMetadataError};
use crate::executor::erc::RegisterNftTokenQuery;
use crate::executor::error::ExecutorQueryError;
use crate::executor::{
     Argument, QueryMessage, QueryType, RegisterErc20TokenQuery,
};
use crate::utils::{
    fetch_content_from_http,
    fetch_content_from_ipfs, sanitize_json_string, utc_dt_string_from_timestamp,
};

impl Sql {
    pub(crate) async fn try_register_nft_token_metadata<P: Provider + Sync>(
        &self,
        id: &str,
        contract_address: Felt,
        actual_token_id: U256,
        provider: &P,
    ) -> Result<(), Error> {
        let _lock = match self.cache.erc_cache.get_token_registration_lock(id).await {
            Some(lock) => lock,
            None => return Ok(()), // Already registered by another thread
        };
        let _guard = _lock.lock().await;
        if self.cache.erc_cache.is_token_registered(id).await {
            return Ok(());
        }

        let _permit = self
            .nft_metadata_semaphore
            .acquire()
            .await
            .map_err(|e| Error::TokenMetadata(TokenMetadataError::AcquireError(e)))?;
        let metadata = fetch_token_metadata(contract_address, actual_token_id, provider).await?;

        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::RegisterNftToken(RegisterNftTokenQuery {
                    id: id.to_string(),
                    contract_address,
                    token_id: actual_token_id,
                    metadata,
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        self.cache.erc_cache.mark_token_registered(id).await;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn store_erc_transfer_event(
        &self,
        contract_address: Felt,
        from: Felt,
        to: Felt,
        amount: U256,
        token_id: &str,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<(), Error> {
        let id = format!("{}:{}", event_id, token_id);
        let insert_query = format!(
            "INSERT INTO {TOKEN_TRANSFER_TABLE} (id, contract_address, from_address, to_address, \
             amount, token_id, event_id, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"
        );

        self.executor
            .send(QueryMessage::new(
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
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }
}
