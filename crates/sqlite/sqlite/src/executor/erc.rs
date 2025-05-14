use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use cainome::cairo_serde::{ByteArray, CairoSerde};
use data_url::mime::Mime;
use data_url::DataUrl;
use reqwest::Client;
use starknet::core::types::{BlockId, BlockTag, FunctionCall, U256};
use starknet::core::utils::{get_selector_from_name, parse_cairo_short_string};
use starknet::providers::Provider;
use starknet_crypto::Felt;
use tracing::{debug, info, warn};

use super::{ApplyBalanceDiffQuery, BrokerMessage, Executor};
use crate::constants::{SQL_FELT_DELIMITER, TOKEN_BALANCE_TABLE};
use crate::executor::LOG_TARGET;
use crate::simple_broker::SimpleBroker;
use crate::types::{ContractType, OptimisticToken, OptimisticTokenBalance, Token, TokenBalance};
use crate::utils::{
    felt_to_sql_string, fetch_content_from_ipfs, sanitize_json_string, sql_string_to_u256,
    u256_to_sql_string, I256,
};

#[derive(Debug, Clone)]
pub struct RegisterNftTokenQuery {
    pub id: String,
    pub contract_address: Felt,
    pub token_id: U256,
    pub metadata: String,
}

#[derive(Debug, Clone)]
pub struct UpdateNftMetadataQuery {
    pub contract_address: Felt,
    pub token_id: U256,
    pub metadata: String,
}

#[derive(Debug, Clone)]
pub struct RegisterErc20TokenQuery {
    pub token_id: String,
    pub contract_address: Felt,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

impl<P: Provider + Sync + Send + 'static> Executor<'_, P> {
    pub async fn apply_balance_diff(
        &mut self,
        apply_balance_diff: ApplyBalanceDiffQuery,
        provider: Arc<P>,
    ) -> Result<()> {
        let erc_cache = apply_balance_diff.erc_cache;
        for ((contract_type, id_str), balance) in erc_cache.iter() {
            let id = id_str.split(SQL_FELT_DELIMITER).collect::<Vec<&str>>();
            match contract_type {
                ContractType::WORLD => unreachable!(),
                ContractType::UDC => unreachable!(),
                ContractType::ERC721 => {
                    // account_address/contract_address:id => ERC721
                    assert!(id.len() == 2);
                    let account_address = id[0];
                    let token_id = id[1];
                    let mid = token_id.split(":").collect::<Vec<&str>>();
                    let contract_address = mid[0];

                    self.apply_balance_diff_helper(
                        id_str,
                        account_address,
                        contract_address,
                        token_id,
                        balance,
                        Arc::clone(&provider),
                    )
                    .await?;
                }
                ContractType::ERC20 => {
                    // account_address/contract_address/ => ERC20
                    assert!(id.len() == 3);
                    let account_address = id[0];
                    let contract_address = id[1];
                    let token_id = id[1];

                    self.apply_balance_diff_helper(
                        id_str,
                        account_address,
                        contract_address,
                        token_id,
                        balance,
                        Arc::clone(&provider),
                    )
                    .await?;
                }
                ContractType::ERC1155 => {
                    // account_address/contract_address:id => ERC1155
                    assert!(id.len() == 2);
                    let account_address = id[0];
                    let token_id = id[1];
                    let mid = token_id.split(":").collect::<Vec<&str>>();
                    let contract_address = mid[0];

                    self.apply_balance_diff_helper(
                        id_str,
                        account_address,
                        contract_address,
                        token_id,
                        balance,
                        Arc::clone(&provider),
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn apply_balance_diff_helper(
        &mut self,
        id: &str,
        account_address: &str,
        contract_address: &str,
        token_id: &str,
        balance_diff: &I256,
        provider: Arc<P>,
    ) -> Result<()> {
        let tx = &mut self.transaction;
        let balance: Option<(String,)> = sqlx::query_as(&format!(
            "SELECT balance FROM {TOKEN_BALANCE_TABLE} WHERE id = ?"
        ))
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?;

        let mut balance = if let Some(balance) = balance {
            sql_string_to_u256(&balance.0)
        } else {
            U256::from(0u8)
        };

        if balance_diff.is_negative {
            if balance < balance_diff.value {
                // HACK: ideally we should never hit this case. But ETH on starknet mainnet didn't
                // emit transfer events properly so they are broken. For those cases
                // we manually fetch the balance of the address using RPC

                let current_balance = provider
                    .call(
                        FunctionCall {
                            contract_address: Felt::from_str(contract_address).unwrap(),
                            entry_point_selector: get_selector_from_name("balanceOf").unwrap(),
                            calldata: vec![Felt::from_str(account_address).unwrap()],
                        },
                        BlockId::Tag(BlockTag::Pending),
                    )
                    .await
                    .with_context(|| format!("Failed to fetch balance for id: {}", id))?;

                let current_balance =
                    cainome::cairo_serde::U256::cairo_deserialize(&current_balance, 0).unwrap();

                warn!(
                    target: LOG_TARGET,
                    id = id,
                    "Invalid transfer event detected, overriding balance by querying RPC directly"
                );
                // override the balance from onchain data
                balance = U256::from_words(current_balance.low, current_balance.high);
            } else {
                balance -= balance_diff.value;
            }
        } else {
            balance += balance_diff.value;
        }

        // write the new balance to the database
        let token_balance: TokenBalance = sqlx::query_as(&format!(
            "INSERT OR REPLACE INTO {TOKEN_BALANCE_TABLE} (id, contract_address, account_address, \
             token_id, balance) VALUES (?, ?, ?, ?, ?) RETURNING *",
        ))
        .bind(id)
        .bind(contract_address)
        .bind(account_address)
        .bind(token_id)
        .bind(u256_to_sql_string(&balance))
        .fetch_one(&mut **tx)
        .await?;

        debug!(target: LOG_TARGET, token_balance = ?token_balance, "Applied balance diff");
        SimpleBroker::publish(unsafe {
            std::mem::transmute::<TokenBalance, OptimisticTokenBalance>(token_balance.clone())
        });
        self.publish_queue
            .push(BrokerMessage::TokenBalanceUpdated(token_balance));

        Ok(())
    }
}
