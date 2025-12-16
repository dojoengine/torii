use cainome::cairo_serde::CairoSerde;
use serde_json;
use starknet::core::types::{BlockId, BlockTag, FunctionCall, U256};
use starknet::macros::selector;
use starknet::providers::Provider;
use starknet_crypto::Felt;
use torii_proto::{BalanceId, TokenId};
use tracing::{debug, warn};

use super::{ApplyBalanceDiffQuery, BrokerMessage, Executor};
use crate::constants::TOKEN_BALANCE_TABLE;
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
    pub token_id: TokenId,
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

/// Represents a trait extracted from NFT metadata
#[derive(Debug, Clone)]
pub struct TokenTrait {
    pub trait_type: String,
    pub trait_value: String,
}

/// Extract the name from NFT metadata JSON if present
pub fn extract_name_from_metadata(metadata: &str) -> Option<String> {
    if metadata.is_empty() {
        return None;
    }

    let metadata_json: serde_json::Value = serde_json::from_str(metadata).ok()?;
    metadata_json
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

/// Extract traits from NFT metadata JSON
pub fn extract_traits_from_json(metadata: &str) -> Result<Vec<TokenTrait>, serde_json::Error> {
    let metadata_json: serde_json::Value = serde_json::from_str(metadata)?;
    let mut traits = Vec::new();

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
                        traits.push(TokenTrait {
                            trait_type: trait_type_str.to_string(),
                            trait_value: trait_value_str.to_string(),
                        });
                    }
                }
            }
        }
    }

    Ok(traits)
}

/// Apply trait operations to existing trait counts
pub fn apply_trait_operations(
    existing_traits: &str,
    add_traits: &[TokenTrait],
    subtract_traits: &[TokenTrait],
) -> Result<String, serde_json::Error> {
    let mut current_traits: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(existing_traits).unwrap_or_default();

    // First, subtract traits
    for trait_item in subtract_traits {
        if let Some(trait_values) = current_traits.get_mut(&trait_item.trait_type) {
            if let Some(trait_values_obj) = trait_values.as_object_mut() {
                if let Some(existing_count) = trait_values_obj.get(&trait_item.trait_value) {
                    if let Some(count_num) = existing_count.as_u64() {
                        if count_num > 1 {
                            // Decrement the count
                            trait_values_obj.insert(
                                trait_item.trait_value.clone(),
                                serde_json::Value::Number(serde_json::Number::from(count_num - 1)),
                            );
                        } else {
                            // Remove the trait value if count becomes 0
                            trait_values_obj.remove(&trait_item.trait_value);
                        }
                    }
                }

                // Remove the trait type if no values remain
                if trait_values_obj.is_empty() {
                    current_traits.remove(&trait_item.trait_type);
                }
            }
        }
    }

    // Then, add traits
    for trait_item in add_traits {
        // Get or create the trait type object
        let trait_values = current_traits
            .entry(trait_item.trait_type.clone())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));

        if let Some(trait_values_obj) = trait_values.as_object_mut() {
            // Increment count if trait value exists, otherwise set to 1
            if let Some(existing_count) = trait_values_obj.get(&trait_item.trait_value) {
                if let Some(count_num) = existing_count.as_u64() {
                    trait_values_obj.insert(
                        trait_item.trait_value.clone(),
                        serde_json::Value::Number(serde_json::Number::from(count_num + 1)),
                    );
                }
            } else {
                trait_values_obj.insert(
                    trait_item.trait_value.clone(),
                    serde_json::Value::Number(serde_json::Number::from(1)),
                );
            }
        }
    }

    serde_json::to_string(&current_traits)
}

/// Extract traits from NFT metadata JSON and merge them with existing traits
pub fn extract_traits_from_metadata(
    metadata: &str,
    existing_traits: &str,
) -> Result<String, serde_json::Error> {
    let traits = extract_traits_from_json(metadata)?;
    apply_trait_operations(existing_traits, &traits, &[])
}

/// Subtract traits from NFT metadata JSON from existing traits
pub fn subtract_traits_from_metadata(
    metadata: &str,
    existing_traits: &str,
) -> Result<String, serde_json::Error> {
    let traits = extract_traits_from_json(metadata)?;
    apply_trait_operations(existing_traits, &[], &traits)
}

/// Extract and store individual token attributes in the normalized table
pub async fn store_token_attributes(
    metadata: &str,
    token_id: &str,
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    if metadata.is_empty() {
        return Ok(());
    }

    // Extract traits using the shared function
    let traits = match extract_traits_from_json(metadata) {
        Ok(traits) => traits,
        Err(_) => return Ok(()), // Skip invalid JSON
    };

    // Clear existing attributes for this token
    sqlx::query("DELETE FROM token_attributes WHERE token_id = ?")
        .bind(token_id)
        .execute(&mut **tx)
        .await?;

    // Store each trait as an attribute
    for trait_item in traits {
        // Generate a predictable ID using token_id + trait_name + trait_value
        let id = format!(
            "{}:{}:{}",
            token_id, trait_item.trait_type, trait_item.trait_value
        );

        // Store the attribute
        sqlx::query(
            "INSERT INTO token_attributes (id, token_id, trait_name, trait_value) 
             VALUES (?, ?, ?, ?)",
        )
        .bind(id)
        .bind(token_id)
        .bind(&trait_item.trait_type)
        .bind(&trait_item.trait_value)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

/// Helper function to extract traits from NFT metadata and update the token contract's traits
pub async fn update_contract_traits_from_metadata(
    metadata: &str,
    contract_address: &Felt,
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    if metadata.is_empty() {
        return Ok(());
    }

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
                // Update the contract's traits with counts
                sqlx::query("UPDATE tokens SET traits = ? WHERE contract_address = ? AND (token_id = '' OR token_id IS NULL)")
                    .bind(&updated_traits)
                    .bind(&contract_id)
                    .execute(&mut **tx)
                    .await?;

                debug!(target: LOG_TARGET, contract_address = %contract_id, traits = %updated_traits, "Updated token contract traits with counts");
            }
            Err(e) => {
                warn!(target: LOG_TARGET, contract_address = %contract_id, error = %e, "Failed to extract traits from metadata");
            }
        }
    }
    Ok(())
}

/// Helper function to update contract traits when token metadata changes
/// This properly handles subtracting old traits and adding new traits
pub async fn update_contract_traits_on_metadata_change(
    old_metadata: &str,
    new_metadata: &str,
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
        // Extract traits from both old and new metadata
        let old_traits = if old_metadata.is_empty() {
            Vec::new()
        } else {
            match extract_traits_from_json(old_metadata) {
                Ok(traits) => traits,
                Err(e) => {
                    warn!(target: LOG_TARGET, contract_address = %contract_id, error = %e, "Failed to parse old metadata");
                    Vec::new()
                }
            }
        };

        let new_traits = if new_metadata.is_empty() {
            Vec::new()
        } else {
            match extract_traits_from_json(new_metadata) {
                Ok(traits) => traits,
                Err(e) => {
                    warn!(target: LOG_TARGET, contract_address = %contract_id, error = %e, "Failed to parse new metadata");
                    return Ok(()); // Don't update if we can't parse new metadata
                }
            }
        };

        // Apply both operations in a single call
        match apply_trait_operations(&current_traits_str, &new_traits, &old_traits) {
            Ok(updated_traits) => {
                // Update the contract's traits with the final counts
                sqlx::query("UPDATE tokens SET traits = ? WHERE contract_address = ? AND (token_id = '' OR token_id IS NULL)")
                    .bind(&updated_traits)
                    .bind(&contract_id)
                    .execute(&mut **tx)
                    .await?;

                debug!(target: LOG_TARGET, contract_address = %contract_id, traits = %updated_traits, "Updated token contract traits after metadata change");
            }
            Err(e) => {
                warn!(target: LOG_TARGET, contract_address = %contract_id, error = %e, "Failed to apply trait operations");
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
            let token_id_str = token_id.to_string();
            let current_supply: Option<String> =
                sqlx::query_scalar("SELECT total_supply FROM tokens WHERE id = ?")
                    .bind(&token_id_str)
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
                    .bind(&token_id_str)
                    .execute(&mut **tx)
                    .await?;

                debug!(target: LOG_TARGET, token_id = ?token_id, total_supply = ?total_supply, "Updated total supply");
            }
        }

        // Then, update individual balances
        let balances_diff = apply_balance_diff.balances_diff;
        for (balance_id, balance) in balances_diff.iter() {
            let cursor = apply_balance_diff
                .cursors
                .get(&balance_id.token_id.contract_address())
                .unwrap();
            let block_id = if cursor.last_pending_block_tx.is_some() {
                BlockId::Tag(BlockTag::PreConfirmed)
            } else {
                BlockId::Number(cursor.head.unwrap())
            };

            self.apply_balance_diff_helper(balance_id, balance, block_id, provider.clone())
                .await?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn apply_balance_diff_helper(
        &mut self,
        id: &BalanceId,
        balance_diff: &I256,
        block_id: BlockId,
        provider: P,
    ) -> Result<(), Error> {
        let tx = self.transaction.as_mut().unwrap();
        let balance: Option<String> = sqlx::query_scalar(&format!(
            "SELECT balance FROM {TOKEN_BALANCE_TABLE} WHERE id = ?"
        ))
        .bind(id.to_string())
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
                            contract_address: id.token_id.contract_address(),
                            entry_point_selector: selector!("balance_of"),
                            calldata: vec![id.account_address],
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
                                contract_address: id.token_id.contract_address(),
                                entry_point_selector: selector!("balanceOf"),
                                calldata: vec![id.account_address],
                            },
                            block_id,
                        )
                        .await?
                };

                let current_balance =
                    cainome::cairo_serde::U256::cairo_deserialize(&current_balance, 0).unwrap();

                warn!(
                    target: LOG_TARGET,
                    id = id.to_string(),
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
        .bind(id.to_string())
        .bind(felt_to_sql_string(&id.token_id.contract_address()))
        .bind(felt_to_sql_string(&id.account_address))
        .bind(id.token_id.to_string())
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
    fn test_extract_name_from_metadata() {
        // Test with valid name
        let metadata = r#"{"name": "Cool NFT #42", "description": "A cool NFT"}"#;
        assert_eq!(
            extract_name_from_metadata(metadata),
            Some("Cool NFT #42".to_string())
        );

        // Test with empty name
        let metadata = r#"{"name": "", "description": "A cool NFT"}"#;
        assert_eq!(extract_name_from_metadata(metadata), None);

        // Test with no name field
        let metadata = r#"{"description": "A cool NFT"}"#;
        assert_eq!(extract_name_from_metadata(metadata), None);

        // Test with empty metadata
        assert_eq!(extract_name_from_metadata(""), None);

        // Test with invalid JSON
        assert_eq!(extract_name_from_metadata("invalid json"), None);

        // Test with name as number (should return None)
        let metadata = r#"{"name": 123}"#;
        assert_eq!(extract_name_from_metadata(metadata), None);
    }

    #[test]
    fn test_extract_traits_from_json() {
        let metadata = r#"{
            "name": "Test NFT",
            "attributes": [
                {"trait_type": "Resource", "value": "Steel"},
                {"trait_type": "Rarity", "value": "Common"},
                {"trait_type": "Resource", "value": "Wood"}
            ]
        }"#;

        let result = extract_traits_from_json(metadata).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].trait_type, "Resource");
        assert_eq!(result[0].trait_value, "Steel");
        assert_eq!(result[1].trait_type, "Rarity");
        assert_eq!(result[1].trait_value, "Common");
        assert_eq!(result[2].trait_type, "Resource");
        assert_eq!(result[2].trait_value, "Wood");
    }

    #[test]
    fn test_extract_traits_from_json_edge_cases() {
        // Test empty attributes
        let metadata = r#"{"attributes": []}"#;
        let result = extract_traits_from_json(metadata).unwrap();
        assert_eq!(result.len(), 0);

        // Test no attributes field
        let metadata = r#"{"name": "Test NFT"}"#;
        let result = extract_traits_from_json(metadata).unwrap();
        assert_eq!(result.len(), 0);

        // Test invalid JSON
        let result = extract_traits_from_json("invalid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_trait_operations_add_and_subtract() {
        let existing_traits = r#"{
            "Resource": {"Steel": 5, "Wood": 3},
            "Rarity": {"Common": 8, "Rare": 2}
        }"#;

        let add_traits = vec![
            TokenTrait {
                trait_type: "Resource".to_string(),
                trait_value: "Gold".to_string(),
            },
            TokenTrait {
                trait_type: "Resource".to_string(),
                trait_value: "Steel".to_string(),
            },
        ];

        let subtract_traits = vec![
            TokenTrait {
                trait_type: "Resource".to_string(),
                trait_value: "Wood".to_string(),
            },
            TokenTrait {
                trait_type: "Rarity".to_string(),
                trait_value: "Common".to_string(),
            },
        ];

        let result =
            apply_trait_operations(existing_traits, &add_traits, &subtract_traits).unwrap();

        let expected = json!({
            "Resource": {"Steel": 6, "Wood": 2, "Gold": 1},
            "Rarity": {"Common": 7, "Rare": 2}
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_apply_trait_operations_remove_zero_counts() {
        let existing_traits = r#"{
            "Resource": {"Steel": 1, "Wood": 1},
            "Rarity": {"Common": 1}
        }"#;

        let add_traits = vec![TokenTrait {
            trait_type: "Resource".to_string(),
            trait_value: "Gold".to_string(),
        }];

        let subtract_traits = vec![
            TokenTrait {
                trait_type: "Resource".to_string(),
                trait_value: "Steel".to_string(),
            },
            TokenTrait {
                trait_type: "Resource".to_string(),
                trait_value: "Wood".to_string(),
            },
            TokenTrait {
                trait_type: "Rarity".to_string(),
                trait_value: "Common".to_string(),
            },
        ];

        let result =
            apply_trait_operations(existing_traits, &add_traits, &subtract_traits).unwrap();

        let expected = json!({
            "Resource": {"Gold": 1}
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_apply_trait_operations_metadata_update_scenario() {
        // Simulate updating a token's metadata from one set of traits to another
        let existing_traits = r#"{
            "Background": {"Blue": 10, "Red": 5},
            "Eyes": {"Normal": 8, "Laser": 2},
            "Hat": {"Cap": 3, "Crown": 1}
        }"#;

        // Old token had: Background=Blue, Eyes=Normal, Hat=Cap
        let old_token_traits = vec![
            TokenTrait {
                trait_type: "Background".to_string(),
                trait_value: "Blue".to_string(),
            },
            TokenTrait {
                trait_type: "Eyes".to_string(),
                trait_value: "Normal".to_string(),
            },
            TokenTrait {
                trait_type: "Hat".to_string(),
                trait_value: "Cap".to_string(),
            },
        ];

        // New token has: Background=Red, Eyes=Laser, Hat=Crown, Accessory=Ring
        let new_token_traits = vec![
            TokenTrait {
                trait_type: "Background".to_string(),
                trait_value: "Red".to_string(),
            },
            TokenTrait {
                trait_type: "Eyes".to_string(),
                trait_value: "Laser".to_string(),
            },
            TokenTrait {
                trait_type: "Hat".to_string(),
                trait_value: "Crown".to_string(),
            },
            TokenTrait {
                trait_type: "Accessory".to_string(),
                trait_value: "Ring".to_string(),
            },
        ];

        let result =
            apply_trait_operations(existing_traits, &new_token_traits, &old_token_traits).unwrap();

        let expected = json!({
            "Background": {"Blue": 9, "Red": 6},
            "Eyes": {"Normal": 7, "Laser": 3},
            "Hat": {"Cap": 2, "Crown": 2},
            "Accessory": {"Ring": 1}
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_extract_traits_from_metadata_with_existing() {
        let metadata = r#"{
            "attributes": [
                {"trait_type": "Resource", "value": "Gold"},
                {"trait_type": "Rarity", "value": "Rare"}
            ]
        }"#;

        let existing_traits = r#"{
            "Resource": {"Steel": 5, "Wood": 3},
            "Rarity": {"Common": 8}
        }"#;

        let result = extract_traits_from_metadata(metadata, existing_traits).unwrap();

        let expected = json!({
            "Resource": {"Steel": 5, "Wood": 3, "Gold": 1},
            "Rarity": {"Common": 8, "Rare": 1}
        });

        assert_eq!(result, expected.to_string());
    }

    #[test]
    fn test_subtract_traits_from_metadata() {
        let metadata = r#"{
            "attributes": [
                {"trait_type": "Resource", "value": "Steel"},
                {"trait_type": "Rarity", "value": "Common"}
            ]
        }"#;

        let existing_traits = r#"{
            "Resource": {"Steel": 3, "Wood": 2},
            "Rarity": {"Common": 5, "Rare": 1}
        }"#;

        let result = subtract_traits_from_metadata(metadata, existing_traits).unwrap();

        let expected = json!({
            "Resource": {"Steel": 2, "Wood": 2},
            "Rarity": {"Common": 4, "Rare": 1}
        });

        assert_eq!(result, expected.to_string());
    }
}
