use std::collections::HashSet;
use std::str::FromStr;

use cainome::cairo_serde::CairoSerde;
use starknet::core::types::{BlockId, BlockTag, FunctionCall, U256};
use starknet::macros::selector;
use starknet::providers::Provider;
use starknet_crypto::Felt;
use tracing::{debug, warn};

use super::{ApplyBalanceDiffQuery, BrokerMessage, Executor};
use crate::constants::{SQL_FELT_DELIMITER, TOKEN_BALANCE_TABLE};
use crate::error::Error;
use crate::executor::LOG_TARGET;
use crate::types::TokenBalance;
use crate::utils::{sql_string_to_u256, u256_to_sql_string};
use torii_math::I256;

#[derive(Debug, Clone)]
pub struct RegisterNftTokenQuery {
    pub contract_address: Felt,
    pub token_id: U256,
    pub metadata: String,
}

#[derive(Debug, Clone)]
pub struct UpdateTokenMetadataQuery {
    pub contract_address: Felt,
    pub token_id: Option<U256>,
    pub metadata: String,
}

#[derive(Debug, Clone)]
pub struct RegisterTokenContractQuery {
    pub contract_address: Felt,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub metadata: Option<String>,
}

impl<P: Provider + Sync + Send + Clone + 'static> Executor<'_, P> {
    pub async fn apply_balance_diff(
        &mut self,
        apply_balance_diff: ApplyBalanceDiffQuery,
        provider: P,
    ) -> Result<(), Error> {
        // Update total supply for all token types
        let tx = self.transaction.as_mut().unwrap();
        
        // Track which ERC-1155 contracts need their supply recalculated
        let mut erc1155_contracts = HashSet::new();
        
        for (token_id, supply_diff) in apply_balance_diff.total_supply_diff.iter() {
            // Determine if this is a contract-level or token-level supply update
            // Contract-level: ERC-20 (no colon) and ERC-721 contract totals
            // Token-level: ERC-721/ERC-1155 specific token IDs (has colon)
            
            // Check if this is an ERC-1155 token (has colon and value != 1)
            if token_id.contains(':') && supply_diff.value != U256::from(1u8) {
                // This is likely an ERC-1155 token, track its contract
                let parts: Vec<&str> = token_id.split(':').collect();
                if parts.len() == 2 {
                    erc1155_contracts.insert(parts[0].to_string());
                }
            }

            // Skip contract-level updates for ERC-1155 (we'll compute them separately)
            if !token_id.contains(':') && erc1155_contracts.contains(token_id) {
                continue;
            }

            // Get current total supply
            let current_supply: Option<String> =
                sqlx::query_scalar("SELECT total_supply FROM tokens WHERE id = ?")
                    .bind(token_id)
                    .fetch_one(&mut **tx)
                    .await?;

            if let Some(supply_str) = current_supply {
                // Token/contract exists in database
                // After migration, total_supply should never be NULL or empty
                let mut total_supply = if supply_str.is_empty() {
                    // Handle edge case of empty string (shouldn't happen but be safe)
                    U256::from(0u8)
                } else {
                    sql_string_to_u256(&supply_str)
                };

                // Apply the supply diff
                if supply_diff.is_negative {
                    if total_supply >= supply_diff.value {
                        total_supply -= supply_diff.value;
                    } else {
                        // Handle underflow - set to 0
                        total_supply = U256::from(0u8);
                    }
                } else {
                    total_supply += supply_diff.value;
                }

                // Update the total supply in the database
                sqlx::query("UPDATE tokens SET total_supply = ? WHERE id = ?")
                    .bind(u256_to_sql_string(&total_supply))
                    .bind(token_id)
                    .execute(&mut **tx)
                    .await?;

                debug!(target: LOG_TARGET, token_id = ?token_id, total_supply = ?total_supply, "Updated total supply");
            }
        }
        
        // Update ERC-1155 contract-level supplies by counting unique token IDs
        for contract_address in erc1155_contracts {
            // Count unique token IDs with non-zero supply for this contract
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT token_id) FROM tokens WHERE contract_address = ? AND token_id IS NOT NULL AND total_supply != '0x0'"
            )
            .bind(&contract_address)
            .fetch_one(&mut **tx)
            .await?;
            
            let total_supply = U256::from(count as u64);
            
            // Update the contract-level total supply
            sqlx::query("UPDATE tokens SET total_supply = ? WHERE id = ? AND token_id IS NULL")
                .bind(u256_to_sql_string(&total_supply))
                .bind(&contract_address)
                .execute(&mut **tx)
                .await?;
                
            debug!(target: LOG_TARGET, contract_address = ?contract_address, unique_tokens = ?count, "Updated ERC-1155 contract total supply");
        }

        // Then, update individual balances
        let balances_diff = apply_balance_diff.balances_diff;
        for (id_str, balance) in balances_diff.iter() {
            let id = id_str.split(SQL_FELT_DELIMITER).collect::<Vec<&str>>();
            match id.len() {
                2 => {
                    // account_address/contract_address:id => ERC721
                    let account_address = id[0];
                    let token_id = id[1];
                    let mid = token_id.split(":").collect::<Vec<&str>>();
                    let contract_address = mid[0];

                    let cursor = apply_balance_diff
                        .cursors
                        .get(&Felt::from_str(contract_address).unwrap())
                        .unwrap();
                    let block_id = if cursor.last_pending_block_tx.is_some() {
                        BlockId::Tag(BlockTag::PreConfirmed)
                    } else {
                        BlockId::Number(cursor.head.unwrap())
                    };

                    self.apply_balance_diff_helper(
                        id_str,
                        account_address,
                        contract_address,
                        token_id,
                        balance,
                        block_id,
                        provider.clone(),
                    )
                    .await?;
                }
                3 => {
                    // account_address/contract_address/ => ERC20
                    let account_address = id[0];
                    let contract_address = id[1];
                    let token_id = id[1];

                    let cursor = apply_balance_diff
                        .cursors
                        .get(&Felt::from_str(contract_address).unwrap())
                        .unwrap();
                    let block_id = if cursor.last_pending_block_tx.is_some() {
                        BlockId::Tag(BlockTag::PreConfirmed)
                    } else {
                        BlockId::Number(cursor.head.unwrap())
                    };

                    self.apply_balance_diff_helper(
                        id_str,
                        account_address,
                        contract_address,
                        token_id,
                        balance,
                        block_id,
                        provider.clone(),
                    )
                    .await?;
                }
                _ => unreachable!(),
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
        block_id: BlockId,
        provider: P,
    ) -> Result<(), Error> {
        let tx = self.transaction.as_mut().unwrap();
        let balance: Option<String> = sqlx::query_scalar(&format!(
            "SELECT balance FROM {TOKEN_BALANCE_TABLE} WHERE id = ?"
        ))
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?;

        let mut balance = if let Some(balance) = balance {
            sql_string_to_u256(&balance)
        } else {
            U256::from(0u8)
        };

        if balance_diff.is_negative {
            if balance < balance_diff.value {
                // HACK: ideally we should never hit this case. But ETH on starknet mainnet didn't
                // emit transfer events properly so they are broken. For those cases
                // we manually fetch the balance of the address using RPC

                let current_balance = if let Ok(current_balance) = provider
                    .call(
                        FunctionCall {
                            contract_address: Felt::from_str(contract_address).unwrap(),
                            entry_point_selector: selector!("balance_of"),
                            calldata: vec![Felt::from_str(account_address).unwrap()],
                        },
                        block_id,
                    )
                    .await
                {
                    current_balance
                } else {
                    provider
                        .call(
                            FunctionCall {
                                contract_address: Felt::from_str(contract_address).unwrap(),
                                entry_point_selector: selector!("balanceOf"),
                                calldata: vec![Felt::from_str(account_address).unwrap()],
                            },
                            block_id,
                        )
                        .await?
                };

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
            "INSERT INTO {TOKEN_BALANCE_TABLE} (id, contract_address, account_address, \
             token_id, balance) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET balance = EXCLUDED.balance RETURNING *",
        ))
        .bind(id)
        .bind(contract_address)
        .bind(account_address)
        .bind(token_id)
        .bind(u256_to_sql_string(&balance))
        .fetch_one(&mut **tx)
        .await?;

        debug!(target: LOG_TARGET, token_balance = ?token_balance, "Applied balance diff");
        self.publish_optimistic_and_queue(BrokerMessage::TokenBalanceUpdated(token_balance.into()));

        Ok(())
    }
}
