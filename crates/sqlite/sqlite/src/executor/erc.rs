use std::str::FromStr;

use cainome::cairo_serde::CairoSerde;
use serde_json;
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
use crate::utils::{felt_to_sql_string, sql_string_to_u256, u256_to_sql_string};
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

/// Extract traits from NFT metadata JSON and merge them with existing traits
pub fn extract_traits_from_metadata(
    metadata: &str,
    existing_traits: &str,
) -> Result<String, serde_json::Error> {
    let mut current_traits: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(existing_traits).unwrap_or_default();

    let metadata_json: serde_json::Value = serde_json::from_str(metadata)?;

    if let Some(attributes) = metadata_json.get("attributes") {
        if let Ok(attributes_array) =
            serde_json::from_value::<Vec<serde_json::Value>>(attributes.clone())
        {
            // Extract traits from this token's attributes
            for attr in attributes_array {
                // Handle both "trait_type" and "trait" field names
                let trait_type = attr.get("trait_type").or_else(|| attr.get("trait"));

                if let (Some(trait_type), Some(trait_value)) = (trait_type, attr.get("value")) {
                    if let (Some(trait_type_str), Some(trait_value_str)) =
                        (trait_type.as_str(), trait_value.as_str())
                    {
                        // Get or create the trait type array
                        let trait_values = current_traits
                            .entry(trait_type_str.to_string())
                            .or_insert_with(|| serde_json::Value::Array(Vec::new()));

                        if let Some(trait_values_array) = trait_values.as_array_mut() {
                            // Add the value if it's not already present
                            if !trait_values_array
                                .iter()
                                .any(|v| v.as_str() == Some(trait_value_str))
                            {
                                trait_values_array
                                    .push(serde_json::Value::String(trait_value_str.to_string()));
                            }
                        }
                    }
                }
            }
        }
    }

    serde_json::to_string(&current_traits)
}

/// Extract and store individual token attributes in the normalized table
pub async fn store_token_attributes(
    metadata: &str,
    token_id: &str,
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    let metadata_json: serde_json::Value = match serde_json::from_str(metadata) {
        Ok(json) => json,
        Err(_) => return Ok(()), // Skip invalid JSON
    };

    // Clear existing attributes for this token
    sqlx::query("DELETE FROM token_attributes WHERE token_id = ?")
        .bind(token_id)
        .execute(&mut **tx)
        .await?;

    if let Some(attributes) = metadata_json.get("attributes") {
        if let Ok(attributes_array) =
            serde_json::from_value::<Vec<serde_json::Value>>(attributes.clone())
        {
            for attr in attributes_array {
                // Handle both "trait_type" and "trait" field names
                let trait_type = attr.get("trait_type").or_else(|| attr.get("trait"));

                if let (Some(trait_type), Some(trait_value)) = (trait_type, attr.get("value")) {
                    if let (Some(trait_type_str), Some(trait_value_str)) =
                        (trait_type.as_str(), trait_value.as_str())
                    {
                        // Generate a predictable ID using token_id + trait_name + trait_value
                        let id = format!("{}:{}:{}", token_id, trait_type_str, trait_value_str);

                        // Store the attribute
                        sqlx::query(
                            "INSERT INTO token_attributes (id, token_id, trait_name, trait_value) 
                             VALUES (?, ?, ?, ?)",
                        )
                        .bind(id)
                        .bind(token_id)
                        .bind(trait_type_str)
                        .bind(trait_value_str)
                        .execute(&mut **tx)
                        .await?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Helper function to extract traits from NFT metadata and update the token contract's traits
pub async fn update_contract_traits_from_metadata(
    metadata: &str,
    contract_address: &Felt,
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    // Get current traits for the contract
    let contract_id = felt_to_sql_string(contract_address);
    let current_traits_result = sqlx::query_as::<_, (String,)>(
        "SELECT traits FROM tokens WHERE contract_address = ? AND (token_id = '' OR token_id IS NULL) LIMIT 1"
    )
    .bind(&contract_id)
    .fetch_one(&mut **tx)
    .await;

    if let Ok((current_traits_str,)) = current_traits_result {
        match extract_traits_from_metadata(metadata, &current_traits_str) {
            Ok(updated_traits) => {
                // Update the contract's traits
                sqlx::query("UPDATE tokens SET traits = ? WHERE contract_address = ? AND (token_id = '' OR token_id IS NULL)")
                    .bind(&updated_traits)
                    .bind(&contract_id)
                    .execute(&mut **tx)
                    .await?;

                debug!(target: LOG_TARGET, contract_address = %contract_id, traits = %updated_traits, "Updated token contract traits");
            }
            Err(e) => {
                warn!(target: LOG_TARGET, contract_address = %contract_id, error = %e, "Failed to extract traits from metadata");
            }
        }
    }
    Ok(())
}

impl<P: Provider + Sync + Send + Clone + 'static> Executor<'_, P> {
    pub async fn apply_balance_diff(
        &mut self,
        apply_balance_diff: ApplyBalanceDiffQuery,
        provider: P,
    ) -> Result<(), Error> {
        // Update total supply for all token types
        let tx = self.transaction.as_mut().unwrap();
        for (token_id, supply_diff) in apply_balance_diff.total_supply_diff.iter() {
            // Determine if this is a contract-level or token-level supply update
            // Contract-level: ERC-20 (no colon) and contract totals (handled in registration)
            // Token-level: ERC-721/ERC-1155 specific token IDs (has colon)

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_traits_from_metadata_empty_existing() {
        let metadata = r#"{
            "name": "Test NFT",
            "description": "A test NFT",
            "attributes": [
                {"trait_type": "Resource", "value": "Steel"},
                {"trait_type": "Rarity", "value": "Common"},
                {"trait_type": "Resource", "value": "Wood"}
            ]
        }"#;

        let existing_traits = "{}";
        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        let expected = json!({
            "Resource": ["Steel", "Wood"],
            "Rarity": ["Common"]
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_extract_traits_from_metadata_with_existing() {
        let metadata = r#"{
            "name": "Test NFT",
            "description": "A test NFT",
            "attributes": [
                {"trait_type": "Resource", "value": "Gold"},
                {"trait_type": "Rarity", "value": "Rare"}
            ]
        }"#;

        let existing_traits = r#"{
            "Resource": ["Steel", "Wood"],
            "Rarity": ["Common"]
        }"#;

        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        let expected = json!({
            "Resource": ["Steel", "Wood", "Gold"],
            "Rarity": ["Common", "Rare"]
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_extract_traits_from_metadata_no_duplicates() {
        let metadata = r#"{
            "name": "Test NFT",
            "description": "A test NFT",
            "attributes": [
                {"trait_type": "Resource", "value": "Steel"},
                {"trait_type": "Rarity", "value": "Common"}
            ]
        }"#;

        let existing_traits = r#"{
            "Resource": ["Steel", "Wood"],
            "Rarity": ["Common"]
        }"#;

        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        let expected = json!({
            "Resource": ["Steel", "Wood"],
            "Rarity": ["Common"]
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_extract_traits_from_metadata_no_attributes() {
        let metadata = r#"{
            "name": "Test NFT",
            "description": "A test NFT"
        }"#;

        let existing_traits = r#"{
            "Resource": ["Steel", "Wood"],
            "Rarity": ["Common"]
        }"#;

        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        // Should return existing traits unchanged (but compacted)
        let expected = json!({
            "Resource": ["Steel", "Wood"],
            "Rarity": ["Common"]
        });
        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_extract_traits_from_metadata_empty_attributes() {
        let metadata = r#"{
            "name": "Test NFT",
            "description": "A test NFT",
            "attributes": []
        }"#;

        let existing_traits = r#"{
            "Resource": ["Steel", "Wood"],
            "Rarity": ["Common"]
        }"#;

        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        // Should return existing traits unchanged (but compacted)
        let expected = json!({
            "Resource": ["Steel", "Wood"],
            "Rarity": ["Common"]
        });
        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_extract_traits_from_metadata_invalid_json() {
        let metadata = "invalid json";
        let existing_traits = "{}";

        let result = extract_traits_from_metadata(metadata, existing_traits);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_traits_from_metadata_malformed_attributes() {
        let metadata = r#"{
            "name": "Test NFT",
            "description": "A test NFT",
            "attributes": [
                {"trait_type": "Resource"},
                {"value": "Steel"},
                {"trait_type": "Rarity", "value": "Common"}
            ]
        }"#;

        let existing_traits = "{}";
        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        // Should only extract valid attributes
        let expected = json!({
            "Rarity": ["Common"]
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_extract_traits_from_metadata_non_string_values() {
        let metadata = r#"{
            "name": "Test NFT",
            "description": "A test NFT",
            "attributes": [
                {"trait_type": "Level", "value": 5},
                {"trait_type": "Active", "value": true},
                {"trait_type": "Resource", "value": "Steel"}
            ]
        }"#;

        let existing_traits = "{}";
        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        // Should only extract string values
        let expected = json!({
            "Resource": ["Steel"]
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_extract_traits_from_metadata_complex_example() {
        let metadata = r#"{
            "name": "Epic Sword",
            "description": "A legendary weapon",
            "image": "https://example.com/sword.png",
            "attributes": [
                {"trait_type": "Weapon Type", "value": "Sword"},
                {"trait_type": "Damage", "value": "High"},
                {"trait_type": "Rarity", "value": "Legendary"},
                {"trait_type": "Element", "value": "Fire"},
                {"trait_type": "Durability", "value": "100"}
            ]
        }"#;

        let existing_traits = r#"{
            "Weapon Type": ["Bow", "Staff"],
            "Damage": ["Low", "Medium"],
            "Rarity": ["Common", "Rare"],
            "Element": ["Water", "Earth"]
        }"#;

        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        let expected = json!({
            "Weapon Type": ["Bow", "Staff", "Sword"],
            "Damage": ["Low", "Medium", "High"],
            "Rarity": ["Common", "Rare", "Legendary"],
            "Element": ["Water", "Earth", "Fire"],
            "Durability": ["100"]
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_extract_traits_from_metadata_invalid_existing_traits() {
        let metadata = r#"{
            "name": "Test NFT",
            "attributes": [
                {"trait_type": "Resource", "value": "Steel"}
            ]
        }"#;

        let existing_traits = "invalid json";
        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        // Should treat invalid existing traits as empty and still work
        let expected = json!({
            "Resource": ["Steel"]
        });

        assert_eq!(result, expected.to_string());
    }
}
