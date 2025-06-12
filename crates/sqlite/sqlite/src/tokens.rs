use crypto_bigint::U256;
use starknet_crypto::Felt;
use torii_proto::{Page, Token, TokenBalance};

use crate::{constants::SQL_DEFAULT_LIMIT, cursor::{decode_cursor, encode_cursor}, error::Error, utils::u256_to_sql_string, Sql};

impl Sql {
    async fn tokens(
        &self,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
        limit: Option<u32>,
        cursor: Option<String>,
    ) -> Result<Page<Token>, Error> {
        let mut query = "SELECT * FROM tokens".to_string();
        let mut bind_values = Vec::new();
        let mut conditions = Vec::new();

        if !contract_addresses.is_empty() {
            let placeholders = vec!["?"; contract_addresses.len()].join(", ");
            conditions.push(format!("contract_address IN ({})", placeholders));
            bind_values.extend(contract_addresses.iter().map(|addr| format!("{:#x}", addr)));
        }
        if !token_ids.is_empty() {
            let placeholders = vec!["?"; token_ids.len()].join(", ");
            conditions.push(format!("token_id IN ({})", placeholders));
            bind_values.extend(token_ids.iter().map(|id| u256_to_sql_string(&(*id).into())));
        }

        if let Some(cursor) = cursor {
            bind_values.push(decode_cursor(&cursor)?);
            conditions.push("id >= ?".to_string());
        }

        if !conditions.is_empty() {
            query += &format!(" WHERE {}", conditions.join(" AND "));
        }

        query += " ORDER BY id LIMIT ?";
        bind_values.push((limit.unwrap_or(SQL_DEFAULT_LIMIT as u32) + 1).to_string());

        let mut query = sqlx::query_as(&query);
        for value in bind_values {
            query = query.bind(value);
        }

        let mut tokens: Vec<torii_sqlite_types::Token> = query.fetch_all(&self.pool).await?;
        let next_cursor = if tokens.len() > limit.unwrap_or(SQL_DEFAULT_LIMIT as u32) as usize {
            Some(encode_cursor(&tokens.pop().unwrap().id)?)
        } else {
            None
        };

        let tokens = tokens.iter().map(|token| token.clone().into()).collect();
        Ok(Page {
            items: tokens,
            next_cursor,
        })
    }

    
    async fn token_balances(
        &self,
        account_addresses: Vec<Felt>,
        contract_addresses: Vec<Felt>,
        token_ids: Vec<U256>,
        limit: Option<u32>,
        cursor: Option<String>,
    ) -> Result<Page<TokenBalance>, Error> {
        let mut query = "SELECT * FROM token_balances".to_string();
        let mut bind_values = Vec::new();
        let mut conditions = Vec::new();

        if !account_addresses.is_empty() {
            let placeholders = vec!["?"; account_addresses.len()].join(", ");
            conditions.push(format!("account_address IN ({})", placeholders));
            bind_values.extend(account_addresses.iter().map(|addr| format!("{:#x}", addr)));
        }

        if !contract_addresses.is_empty() {
            let placeholders = vec!["?"; contract_addresses.len()].join(", ");
            conditions.push(format!("contract_address IN ({})", placeholders));
            bind_values.extend(contract_addresses.iter().map(|addr| format!("{:#x}", addr)));
        }

        if !token_ids.is_empty() {
            let placeholders = vec!["?"; token_ids.len()].join(", ");
            conditions.push(format!(
                "SUBSTR(token_id, INSTR(token_id, ':') + 1) IN ({})",
                placeholders
            ));
            bind_values.extend(token_ids.iter().map(|id| u256_to_sql_string(&(*id).into())));
        }

        if let Some(cursor) = cursor {
            bind_values.push(decode_cursor(&cursor)?);
            conditions.push("id >= ?".to_string());
        }

        if !conditions.is_empty() {
            query += &format!(" WHERE {}", conditions.join(" AND "));
        }

        query += " ORDER BY id LIMIT ?";
        bind_values.push((limit.unwrap_or(SQL_DEFAULT_LIMIT as u32) + 1).to_string());

        let mut query = sqlx::query_as(&query);
        for value in bind_values {
            query = query.bind(value);
        }

        let mut balances: Vec<torii_sqlite_types::TokenBalance> = query.fetch_all(&self.pool).await?;
        let next_cursor = if balances.len() > limit.unwrap_or(SQL_DEFAULT_LIMIT as u32) as usize {
            Some(encode_cursor(&balances.pop().unwrap().id)?)
        } else {
            None
        };

        let balances = balances
            .iter()
            .map(|balance| balance.clone().into())
            .collect();
        Ok(Page {
            items: balances,
            next_cursor,
        })
    }
}