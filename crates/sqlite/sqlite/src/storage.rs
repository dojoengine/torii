use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dojo_types::{naming::compute_selector_from_names, schema::Ty};
use dojo_world::{config::WorldMetadata, contracts::abigen::model::Layout};
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use starknet::core::types::U256;
use starknet_crypto::{poseidon_hash_many, Felt};
use torii_math::I256;
use torii_proto::{
    schema::Entity, Activity, ActivityQuery, AggregationEntry, AggregationQuery, BalanceId,
    CallType, Clause, CompositeClause, Contract, ContractCursor, ContractQuery, Controller,
    ControllerQuery, Event, EventQuery, LogicalOperator, Model, OrderBy, OrderDirection, Page,
    Query, SearchMatch, SearchQuery, SearchResponse, TableSearchResults, Token, TokenBalance,
    TokenBalanceQuery, TokenContract, TokenContractQuery, TokenId, TokenQuery, TokenTransfer,
    TokenTransferQuery, Transaction, TransactionCall, TransactionQuery,
};
use torii_sqlite_types::{HookEvent, Model as SQLModel};
use torii_storage::{utils::format_world_scoped_id, ReadOnlyStorage, Storage, StorageError};
use tracing::warn;

use crate::{
    constants::{
        ENTITIES_ENTITY_RELATION_COLUMN, ENTITIES_HISTORICAL_TABLE, ENTITIES_MODEL_RELATION_TABLE,
        ENTITIES_TABLE, EVENT_MESSAGES_ENTITY_RELATION_COLUMN, EVENT_MESSAGES_HISTORICAL_TABLE,
        EVENT_MESSAGES_MODEL_RELATION_TABLE, EVENT_MESSAGES_TABLE, TOKEN_TRANSFER_TABLE,
    },
    executor::{erc::UpdateTokenMetadataQuery, RegisterNftTokenQuery, RegisterTokenContractQuery},
    model::map_row_to_ty,
    query::{PaginationExecutor, QueryBuilder},
    utils::{build_keys_pattern, u256_to_sql_string},
};
use crate::{
    error::{Error, ParseError},
    executor::{
        error::ExecutorQueryError, ApplyBalanceDiffQuery, Argument, DeleteEntityQuery, EntityQuery,
        EventMessageQuery, QueryMessage, QueryType, StoreTransactionQuery, UpdateCursorsQuery,
    },
    utils::{felt_to_sql_string, felts_to_sql_string, utc_dt_string_from_timestamp},
    Sql,
};

pub const LOG_TARGET: &str = "torii::sqlite::storage";

#[async_trait]
impl ReadOnlyStorage for Sql {
    fn as_read_only(&self) -> &dyn ReadOnlyStorage {
        self
    }

    /// Returns the model metadata for the storage.
    async fn model(&self, world_address: Felt, selector: Felt) -> Result<Model, StorageError> {
        if let Some(cache) = &self.cache {
            if let Ok(model) = cache.model(world_address, selector).await {
                return Ok(model);
            } else {
                warn!(
                    target: LOG_TARGET,
                    model_selector = %format!("{:#x}", selector),
                    "Failed to get model from cache, falling back to database."
                );
            }
        }

        let model = sqlx::query_as::<_, SQLModel>("SELECT * FROM models WHERE id = ?")
            .bind(format_world_scoped_id(&world_address, &selector))
            .fetch_one(&self.pool)
            .await?;
        let model: torii_proto::Model = model.into();

        // Update cache to prevent repeated cache misses
        if let Some(cache) = &self.cache {
            cache
                .register_model(world_address, selector, model.clone())
                .await;
        }

        Ok(model)
    }

    /// Returns the models for the storage.
    /// If world_addresses is empty, returns models from all worlds.
    /// If selectors is empty, returns all models from the specified worlds.
    async fn models(
        &self,
        world_addresses: &[Felt],
        selectors: &[Felt],
    ) -> Result<Vec<Model>, StorageError> {
        // Try cache first
        if let Some(cache) = &self.cache {
            if let Ok(models) = cache.models(world_addresses, selectors).await {
                return Ok(models);
            } else {
                warn!(
                    target: LOG_TARGET,
                    selectors = ?selectors.iter().map(|s| format!("{:#x}", s)).collect::<Vec<_>>(),
                    "Failed to get models from cache, falling back to database.",
                );
            }
        }

        // Build SQL query for multiple worlds
        let mut query = String::from("SELECT * FROM models");
        let mut bind_values = Vec::new();
        let mut conditions = Vec::new();

        // Add world address filter
        if !world_addresses.is_empty() {
            let placeholders = vec!["?"; world_addresses.len()].join(", ");
            conditions.push(format!("world_address IN ({})", placeholders));
            bind_values.extend(world_addresses.iter().map(felt_to_sql_string));
        }

        // Add selector filter if specified
        if !selectors.is_empty() {
            let placeholders = vec!["?"; selectors.len()].join(", ");
            conditions.push(format!("model_selector IN ({})", placeholders));
            bind_values.extend(selectors.iter().map(felt_to_sql_string));
        }

        // Add WHERE clause if we have any conditions
        if !conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&conditions.join(" AND "));
        }

        // Execute query
        let mut query = sqlx::query_as::<_, SQLModel>(&query);
        for value in bind_values {
            query = query.bind(value);
        }
        let models: Vec<torii_proto::Model> = query
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|m| m.into())
            .collect();

        // Update cache to prevent repeated cache misses
        if let Some(cache) = &self.cache {
            for model in &models {
                cache
                    .register_model(model.world_address, model.selector, model.clone())
                    .await;
            }
        }

        Ok(models)
    }

    async fn token_ids(&self) -> Result<HashSet<TokenId>, StorageError> {
        let token_ids = sqlx::query_scalar::<_, String>("SELECT id FROM tokens")
            .fetch_all(&self.pool)
            .await?;
        Ok(token_ids
            .into_iter()
            .map(|id| {
                let parts = id.split(':').collect::<Vec<&str>>();
                if parts.len() == 2 {
                    TokenId::Nft(
                        Felt::from_str(parts[0]).unwrap(),
                        crypto_bigint::U256::from_be_hex(parts[1].trim_start_matches("0x")).into(),
                    )
                } else {
                    TokenId::Contract(Felt::from_str(parts[0]).unwrap())
                }
            })
            .collect())
    }

    /// Returns the controllers for the storage.
    async fn controllers(&self, query: &ControllerQuery) -> Result<Page<Controller>, StorageError> {
        let executor = PaginationExecutor::new(self.pool.clone());
        let mut query_builder = QueryBuilder::new("controllers").select(&[
            "address".to_string(),
            "username".to_string(),
            "deployed_at".to_string(),
        ]);

        if !query.usernames.is_empty() {
            let placeholders = vec!["?"; query.usernames.len()].join(", ");
            query_builder = query_builder.where_clause(&format!("id IN ({})", placeholders));
            for username in &query.usernames {
                query_builder = query_builder.bind_value(username.clone());
            }
        }

        if !query.contract_addresses.is_empty() {
            let placeholders = vec!["?"; query.contract_addresses.len()].join(", ");
            query_builder = query_builder.where_clause(&format!("address IN ({})", placeholders));
            for addr in &query.contract_addresses {
                query_builder = query_builder.bind_value(format!("{:#064x}", addr));
            }
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "address".to_string(),
                    direction: OrderDirection::Desc,
                },
            )
            .await?;
        let items: Vec<Controller> = page
            .items
            .into_iter()
            .map(|row| {
                Result::<Controller, Error>::Ok(
                    torii_sqlite_types::Controller::from_row(&row)?.into(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    async fn contracts(&self, query: &ContractQuery) -> Result<Vec<Contract>, StorageError> {
        let mut query_builder = "SELECT * FROM contracts".to_string();
        let mut bind_values = vec![];
        let mut conditions = vec![];

        if !query.contract_addresses.is_empty() {
            let placeholders = vec!["?"; query.contract_addresses.len()].join(", ");
            conditions.push(format!("contract_address IN ({})", placeholders));
            bind_values.extend(query.contract_addresses.iter().map(felt_to_sql_string));
        }

        if !query.contract_types.is_empty() {
            let placeholders = vec!["?"; query.contract_types.len()].join(", ");
            conditions.push(format!("contract_type IN ({})", placeholders));
            bind_values.extend(query.contract_types.iter().map(|t| t.to_string()));
        }

        if !conditions.is_empty() {
            query_builder += &format!(" WHERE {}", conditions.join(" AND "));
        }

        query_builder += " ORDER BY created_at DESC";

        let mut query = sqlx::query_as::<_, torii_sqlite_types::Contract>(&query_builder);
        for value in bind_values {
            query = query.bind(value);
        }

        let contracts = query.fetch_all(&self.pool).await?;
        let items: Vec<Contract> = contracts
            .into_iter()
            .map(|contract| contract.into())
            .collect();

        Ok(items)
    }

    async fn tokens(&self, query: &TokenQuery) -> Result<Page<Token>, StorageError> {
        let executor = PaginationExecutor::new(self.pool.clone());
        let mut query_builder = QueryBuilder::new("tokens")
            .alias("t")
            .select(&["t.*".to_string()]);

        let mut join_conditions = Vec::new();
        let mut where_conditions = Vec::new();

        // Always filter for NFTs only
        where_conditions.push("t.token_id != '' AND t.token_id IS NOT NULL".to_string());

        if !query.contract_addresses.is_empty() {
            let placeholders = vec!["?"; query.contract_addresses.len()].join(", ");
            where_conditions.push(format!("t.contract_address IN ({})", placeholders));
            for addr in &query.contract_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if !query.token_ids.is_empty() {
            let placeholders = vec!["?"; query.token_ids.len()].join(", ");
            where_conditions.push(format!("t.token_id IN ({})", placeholders));
            for token_id in &query.token_ids {
                query_builder =
                    query_builder.bind_value(u256_to_sql_string(&U256::from(*token_id)));
            }
        }

        // Add attribute filters
        for (i, filter) in query.attribute_filters.iter().enumerate() {
            let alias = format!("ta{}", i);
            join_conditions.push(format!(
                "JOIN token_attributes {} ON t.id = {}.token_id",
                alias, alias
            ));

            // Simple equality matching for trait name and value
            let condition = format!("{}.trait_name = ? AND {}.trait_value = ?", alias, alias);
            where_conditions.push(condition);
            query_builder = query_builder.bind_value(filter.trait_name.clone());
            query_builder = query_builder.bind_value(filter.trait_value.clone());
        }

        // Add joins
        for join in join_conditions {
            query_builder = query_builder.join(&join);
        }

        // Add where conditions
        if !where_conditions.is_empty() {
            query_builder = query_builder.where_clause(&where_conditions.join(" AND ").to_string());
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "id".to_string(),
                    direction: OrderDirection::Desc,
                },
            )
            .await?;
        let items: Vec<Token> = page
            .items
            .into_iter()
            .map(|row| {
                Result::<Token, Error>::Ok(torii_sqlite_types::Token::from_row(&row)?.into())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    async fn token_balances(
        &self,
        query: &TokenBalanceQuery,
    ) -> Result<Page<TokenBalance>, StorageError> {
        let executor = PaginationExecutor::new(self.pool.clone());
        let mut query_builder = QueryBuilder::new("token_balances").select(&["*".to_string()]);

        if !query.account_addresses.is_empty() {
            let placeholders = vec!["?"; query.account_addresses.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("account_address IN ({})", placeholders));
            for addr in &query.account_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if !query.contract_addresses.is_empty() {
            let placeholders = vec!["?"; query.contract_addresses.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("contract_address IN ({})", placeholders));
            for addr in &query.contract_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if !query.token_ids.is_empty() {
            let placeholders = vec!["?"; query.token_ids.len()].join(", ");
            query_builder = query_builder.where_clause(&format!(
                "SUBSTR(token_id, INSTR(token_id, ':') + 1) IN ({})",
                placeholders
            ));
            for token_id in &query.token_ids {
                query_builder =
                    query_builder.bind_value(u256_to_sql_string(&U256::from(*token_id)));
            }
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "id".to_string(),
                    direction: OrderDirection::Desc,
                },
            )
            .await?;
        let items: Vec<TokenBalance> = page
            .items
            .into_iter()
            .map(|row| {
                Result::<TokenBalance, Error>::Ok(
                    torii_sqlite_types::TokenBalance::from_row(&row)?.into(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    async fn token_contracts(
        &self,
        query: &TokenContractQuery,
    ) -> Result<Page<TokenContract>, StorageError> {
        use crate::query::{PaginationExecutor, QueryBuilder};

        let executor = PaginationExecutor::new(self.pool.clone());
        let mut query_builder = QueryBuilder::new("tokens")
            .alias("t")
            .select(&[
                "t.contract_address as contract_address".to_string(),
                "c.contract_type as contract_type".to_string(),
                "t.name as name".to_string(),
                "t.symbol as symbol".to_string(),
                "t.decimals as decimals".to_string(),
                "t.metadata as metadata".to_string(),
                "t.total_supply as total_supply".to_string(),
                "t.traits as traits".to_string(),
                "COALESCE((
                    SELECT metadata 
                    FROM tokens tk 
                    WHERE tk.contract_address = t.contract_address 
                    AND tk.token_id != '' 
                    AND tk.token_id IS NOT NULL
                    ORDER BY tk.token_id 
                    LIMIT 1
                ), '') as token_metadata"
                    .to_string(),
                "t.contract_address as ordering".to_string(),
            ])
            .join("JOIN contracts c ON c.contract_address = t.contract_address")
            .where_clause("t.token_id = '' OR t.token_id IS NULL");

        if !query.contract_addresses.is_empty() {
            let placeholders = vec!["?"; query.contract_addresses.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("t.contract_address IN ({})", placeholders));
            for addr in &query.contract_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if !query.contract_types.is_empty() {
            let placeholders = vec!["?"; query.contract_types.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("c.contract_type IN ({})", placeholders));
            for contract_type in &query.contract_types {
                query_builder = query_builder.bind_value(contract_type.to_string());
            }
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "ordering".to_string(),
                    direction: OrderDirection::Desc,
                },
            )
            .await?;
        let items: Vec<TokenContract> = page
            .items
            .into_iter()
            .map(|row| {
                Result::<TokenContract, Error>::Ok(
                    torii_sqlite_types::TokenContract::from_row(&row)?.into(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    async fn transactions(
        &self,
        query: &TransactionQuery,
    ) -> Result<Page<Transaction>, StorageError> {
        use crate::query::{PaginationExecutor, QueryBuilder};
        use crate::utils::sql_string_to_felts;

        let executor = PaginationExecutor::new(self.pool.clone());
        let mut query_builder = QueryBuilder::new("transactions").alias("t").select(&[
            "t.id".to_string(),
            "t.transaction_hash".to_string(),
            "t.sender_address".to_string(),
            "t.calldata".to_string(),
            "t.max_fee".to_string(),
            "t.signature".to_string(),
            "t.nonce".to_string(),
            "t.block_number".to_string(),
            "t.transaction_type".to_string(),
            "t.executed_at".to_string(),
            "t.created_at".to_string(),
        ]);

        // Apply filters
        if let Some(filter) = &query.filter {
            if !filter.transaction_hashes.is_empty() {
                let placeholders = vec!["?"; filter.transaction_hashes.len()].join(", ");
                query_builder = query_builder
                    .where_clause(&format!("t.transaction_hash IN ({})", placeholders));
                for hash in &filter.transaction_hashes {
                    query_builder = query_builder.bind_value(felt_to_sql_string(hash));
                }
            }

            // Handle transaction calls filters
            if !filter.contract_addresses.is_empty()
                || !filter.entrypoints.is_empty()
                || !filter.caller_addresses.is_empty()
            {
                query_builder = query_builder
                    .join("JOIN transaction_calls tc ON tc.transaction_hash = t.transaction_hash");

                let mut call_conditions = Vec::new();

                if !filter.contract_addresses.is_empty() {
                    let placeholders = vec!["?"; filter.contract_addresses.len()].join(", ");
                    call_conditions.push(format!("tc.contract_address IN ({})", placeholders));
                    for addr in &filter.contract_addresses {
                        query_builder = query_builder.bind_value(felt_to_sql_string(addr));
                    }
                }

                if !filter.entrypoints.is_empty() {
                    let placeholders = vec!["?"; filter.entrypoints.len()].join(", ");
                    call_conditions.push(format!("tc.entrypoint IN ({})", placeholders));
                    for entrypoint in &filter.entrypoints {
                        query_builder = query_builder.bind_value(entrypoint.clone());
                    }
                }

                if !filter.caller_addresses.is_empty() {
                    let placeholders = vec!["?"; filter.caller_addresses.len()].join(", ");
                    call_conditions.push(format!("tc.caller_address IN ({})", placeholders));
                    for caller in &filter.caller_addresses {
                        query_builder = query_builder.bind_value(felt_to_sql_string(caller));
                    }
                }

                if !call_conditions.is_empty() {
                    query_builder =
                        query_builder.where_clause(&format!("({})", call_conditions.join(" AND ")));
                }
            }

            if !filter.model_selectors.is_empty() {
                let placeholders = vec!["?"; filter.model_selectors.len()].join(", ");
                query_builder = query_builder
                    .join("JOIN transaction_models tm ON tm.transaction_hash = t.transaction_hash");
                query_builder =
                    query_builder.where_clause(&format!("tm.model_id IN ({})", placeholders));
                for model in &filter.model_selectors {
                    query_builder = query_builder.bind_value(felt_to_sql_string(model));
                }
            }

            if let Some(from_block) = filter.from_block {
                query_builder = query_builder.where_clause("t.block_number >= ?");
                query_builder = query_builder.bind_value(from_block.to_string());
            }

            if let Some(to_block) = filter.to_block {
                query_builder = query_builder.where_clause("t.block_number <= ?");
                query_builder = query_builder.bind_value(to_block.to_string());
            }
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "id".to_string(),
                    direction: OrderDirection::Desc,
                },
            )
            .await?;

        let headers: Vec<torii_sqlite_types::Transaction> = page
            .items
            .into_iter()
            .map(|row| {
                Result::<torii_sqlite_types::Transaction, Error>::Ok(
                    torii_sqlite_types::Transaction::from_row(&row)?,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut transactions = Vec::with_capacity(headers.len());
        for header in headers {
            let calls = self
                .fetch_transaction_calls(&header.transaction_hash)
                .await?;
            let unique_models = self
                .fetch_transaction_models(&header.transaction_hash)
                .await?;
            let transaction = Transaction {
                transaction_hash: Felt::from_str(&header.transaction_hash)
                    .map_err(|e| Error::Parse(ParseError::FromStr(e)))?,
                sender_address: Felt::from_str(&header.sender_address)
                    .map_err(|e| Error::Parse(ParseError::FromStr(e)))?,
                calldata: sql_string_to_felts(&header.calldata),
                max_fee: Felt::from_str(&header.max_fee)
                    .map_err(|e| Error::Parse(ParseError::FromStr(e)))?,
                signature: sql_string_to_felts(&header.signature),
                nonce: Felt::from_str(&header.nonce)
                    .map_err(|e| Error::Parse(ParseError::FromStr(e)))?,
                block_number: header.block_number,
                transaction_type: header.transaction_type,
                block_timestamp: header.executed_at,
                calls,
                unique_models,
            };
            transactions.push(transaction);
        }

        Ok(Page {
            items: transactions,
            next_cursor: page.next_cursor,
        })
    }

    async fn events(&self, query: EventQuery) -> Result<Page<Event>, StorageError> {
        let executor = PaginationExecutor::new(self.pool.clone());
        let mut query_builder = QueryBuilder::new("events").select(&[
            "id".to_string(),
            "keys".to_string(),
            "data".to_string(),
            "transaction_hash".to_string(),
            "executed_at".to_string(),
            "created_at".to_string(),
        ]);

        if let Some(keys) = &query.keys {
            let keys_pattern = build_keys_pattern(keys);
            if !keys_pattern.is_empty() {
                query_builder = query_builder.where_clause("keys REGEXP ?");
                query_builder = query_builder.bind_value(keys_pattern);
            }
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "id".to_string(),
                    direction: OrderDirection::Desc,
                },
            )
            .await?;
        let items: Vec<Event> = page
            .items
            .into_iter()
            .map(|row| {
                Result::<Event, Error>::Ok(torii_sqlite_types::Event::from_row(&row)?.into())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    async fn token_transfers(
        &self,
        query: &TokenTransferQuery,
    ) -> Result<Page<TokenTransfer>, StorageError> {
        use crate::query::{PaginationExecutor, QueryBuilder};

        let executor = PaginationExecutor::new(self.pool.clone());
        let mut query_builder = QueryBuilder::new("token_transfers").select(&["*".to_string()]);

        if !query.account_addresses.is_empty() {
            let placeholders_from = vec!["?"; query.account_addresses.len()].join(", ");
            let placeholders_to = vec!["?"; query.account_addresses.len()].join(", ");
            query_builder = query_builder.where_clause(&format!(
                "((from_address IN ({})) OR (to_address IN ({})))",
                placeholders_from, placeholders_to
            ));
            for addr in &query.account_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
            for addr in &query.account_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if !query.contract_addresses.is_empty() {
            let placeholders = vec!["?"; query.contract_addresses.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("contract_address IN ({})", placeholders));
            for addr in &query.contract_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if !query.token_ids.is_empty() {
            let placeholders = vec!["?"; query.token_ids.len()].join(", ");
            // Match numeric token id when present (after ':')
            query_builder = query_builder.where_clause(&format!(
                "SUBSTR(token_id, INSTR(token_id, ':') + 1) IN ({})",
                placeholders
            ));
            for token_id in &query.token_ids {
                query_builder =
                    query_builder.bind_value(u256_to_sql_string(&U256::from(*token_id)));
            }
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "id".to_string(),
                    direction: OrderDirection::Desc,
                },
            )
            .await?;

        let items: Vec<TokenTransfer> = page
            .items
            .into_iter()
            .map(|row| {
                Result::<TokenTransfer, Error>::Ok(
                    torii_sqlite_types::TokenTransfer::from_row(&row)?.into(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    /// Queries the entities from the storage.
    async fn entities(&self, query: &Query) -> Result<Page<Entity>, StorageError> {
        // Map other clauses to a composite clause
        let composite = match &query.clause {
            Some(Clause::Composite(composite)) => composite.clone(),
            _ => {
                let mut composite = CompositeClause {
                    operator: LogicalOperator::And,
                    clauses: vec![],
                };
                if let Some(clause) = &query.clause {
                    composite.clauses.push(clause.clone());
                }
                composite
            }
        };

        let table = if query.historical {
            ENTITIES_HISTORICAL_TABLE
        } else {
            ENTITIES_TABLE
        };
        let model_relation_table = ENTITIES_MODEL_RELATION_TABLE;
        let entity_relation_column = ENTITIES_ENTITY_RELATION_COLUMN;

        let page = self
            .query_by_composite(
                table,
                model_relation_table,
                entity_relation_column,
                &composite,
                query.pagination.clone(),
                query.no_hashed_keys,
                query.models.clone(),
                query.historical,
                &query.world_addresses,
            )
            .await?;

        Ok(page)
    }

    /// Queries the event messages from the storage.
    async fn event_messages(&self, query: &Query) -> Result<Page<Entity>, StorageError> {
        // Map other clauses to a composite clause
        let composite = match &query.clause {
            Some(Clause::Composite(composite)) => composite.clone(),
            _ => {
                let mut composite = CompositeClause {
                    operator: LogicalOperator::And,
                    clauses: vec![],
                };
                if let Some(clause) = &query.clause {
                    composite.clauses.push(clause.clone());
                }
                composite
            }
        };

        let table = if query.historical {
            EVENT_MESSAGES_HISTORICAL_TABLE
        } else {
            EVENT_MESSAGES_TABLE
        };
        let model_relation_table = EVENT_MESSAGES_MODEL_RELATION_TABLE;
        let entity_relation_column = EVENT_MESSAGES_ENTITY_RELATION_COLUMN;

        let page = self
            .query_by_composite(
                table,
                model_relation_table,
                entity_relation_column,
                &composite,
                query.pagination.clone(),
                query.no_hashed_keys,
                query.models.clone(),
                query.historical,
                &query.world_addresses,
            )
            .await?;

        Ok(page)
    }

    /// Returns the model data of an entity.
    async fn entity_model(
        &self,
        world_address: Felt,
        entity_id: Felt,
        model_selector: Felt,
    ) -> Result<Option<Ty>, StorageError> {
        let mut schema = self.model(world_address, model_selector).await?.schema;
        let query = format!("SELECT * FROM [{}] WHERE internal_id = ?", schema.name());
        let mut query = sqlx::query(&query);
        query = query.bind(format_world_scoped_id(&world_address, &entity_id));
        let row: Option<SqliteRow> = query.fetch_optional(&self.pool).await?;
        match row {
            Some(row) => {
                map_row_to_ty("", "", &mut schema, &row)?;
                Ok(Some(schema))
            }
            None => Ok(None),
        }
    }

    /// Returns aggregations for the storage with calculated positions.
    async fn aggregations(
        &self,
        query: &AggregationQuery,
    ) -> Result<Page<AggregationEntry>, StorageError> {
        let executor = PaginationExecutor::new(self.pool.clone());

        // Use window function to calculate positions on-the-fly
        let mut query_builder = QueryBuilder::new("aggregations").alias("a").select(&[
            "a.id".to_string(),
            "a.aggregator_id".to_string(),
            "a.entity_id".to_string(),
            "a.value".to_string(),
            "a.display_value".to_string(),
            "a.model_id".to_string(),
            "a.created_at".to_string(),
            "a.updated_at".to_string(),
            // Calculate position using ROW_NUMBER() window function
            // Partitioned by aggregator_id and ordered by value DESC
            "ROW_NUMBER() OVER (PARTITION BY a.aggregator_id ORDER BY a.value DESC) as position"
                .to_string(),
        ]);

        if !query.aggregator_ids.is_empty() {
            let placeholders = vec!["?"; query.aggregator_ids.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("a.aggregator_id IN ({})", placeholders));
            for aggregator_id in &query.aggregator_ids {
                query_builder = query_builder.bind_value(aggregator_id.clone());
            }
        }

        if !query.entity_ids.is_empty() {
            let placeholders = vec!["?"; query.entity_ids.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("a.entity_id IN ({})", placeholders));
            for entity_id in &query.entity_ids {
                query_builder = query_builder.bind_value(entity_id.clone());
            }
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "position".to_string(),
                    direction: OrderDirection::Asc,
                },
            )
            .await?;

        let items: Vec<AggregationEntry> = page
            .items
            .into_iter()
            .map(|row| {
                let aggregation = torii_sqlite_types::AggregationEntryWithPosition::from_row(&row)?;
                Result::<AggregationEntry, Error>::Ok(aggregation.into())
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    /// Returns activities for the storage.
    async fn activities(&self, query: &ActivityQuery) -> Result<Page<Activity>, StorageError> {
        let executor = PaginationExecutor::new(self.pool.clone());
        let mut query_builder = QueryBuilder::new("activities").select(&[
            "id".to_string(),
            "world_address".to_string(),
            "namespace".to_string(),
            "caller_address".to_string(),
            "session_start".to_string(),
            "session_end".to_string(),
            "action_count".to_string(),
            "actions".to_string(),
            "updated_at".to_string(),
        ]);

        if !query.world_addresses.is_empty() {
            let placeholders = vec!["?"; query.world_addresses.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("world_address IN ({})", placeholders));
            for addr in &query.world_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if !query.namespaces.is_empty() {
            let placeholders = vec!["?"; query.namespaces.len()].join(", ");
            query_builder = query_builder.where_clause(&format!("namespace IN ({})", placeholders));
            for namespace in &query.namespaces {
                query_builder = query_builder.bind_value(namespace.clone());
            }
        }

        if !query.caller_addresses.is_empty() {
            let placeholders = vec!["?"; query.caller_addresses.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("caller_address IN ({})", placeholders));
            for addr in &query.caller_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if let Some(from_time) = &query.from_time {
            query_builder = query_builder.where_clause("session_end >= ?");
            query_builder = query_builder.bind_value(from_time.to_rfc3339());
        }

        if let Some(to_time) = &query.to_time {
            query_builder = query_builder.where_clause("session_end <= ?");
            query_builder = query_builder.bind_value(to_time.to_rfc3339());
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "session_end".to_string(),
                    direction: OrderDirection::Desc,
                },
            )
            .await?;
        let items: Vec<Activity> = page
            .items
            .into_iter()
            .map(|row| {
                Result::<Activity, Error>::Ok(torii_sqlite_types::Activity::from_row(&row)?.into())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    /// Returns achievements with optional filtering by world, namespace, and hidden status.
    async fn achievements(
        &self,
        query: &torii_proto::AchievementQuery,
    ) -> Result<Page<torii_proto::Achievement>, StorageError> {
        let executor = PaginationExecutor::new(self.pool.clone());
        let mut query_builder = QueryBuilder::new("achievements").select(&[
            "id".to_string(),
            "world_address".to_string(),
            "namespace".to_string(),
            "entity_id".to_string(),
            "hidden".to_string(),
            "index_num".to_string(),
            "points".to_string(),
            "start".to_string(),
            "end".to_string(),
            "group_name".to_string(),
            "icon".to_string(),
            "title".to_string(),
            "description".to_string(),
            "tasks".to_string(),
            "data".to_string(),
            "total_completions".to_string(),
            "completion_rate".to_string(),
            "created_at".to_string(),
            "updated_at".to_string(),
        ]);

        if !query.world_addresses.is_empty() {
            let placeholders = vec!["?"; query.world_addresses.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("world_address IN ({})", placeholders));
            for addr in &query.world_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if !query.namespaces.is_empty() {
            let placeholders = vec!["?"; query.namespaces.len()].join(", ");
            query_builder = query_builder.where_clause(&format!("namespace IN ({})", placeholders));
            for namespace in &query.namespaces {
                query_builder = query_builder.bind_value(namespace.clone());
            }
        }

        if let Some(hidden) = query.hidden {
            query_builder = query_builder.where_clause("hidden = ?");
            query_builder = query_builder.bind_value(if hidden {
                "1".to_string()
            } else {
                "0".to_string()
            });
        }

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "index_num".to_string(),
                    direction: OrderDirection::Asc,
                },
            )
            .await?;

        // For each achievement, fetch its tasks
        let mut achievements = Vec::new();
        for row in page.items {
            let achievement = torii_sqlite_types::Achievement::from_row(&row)?;
            let achievement_id = achievement.id.clone();

            // Fetch tasks for this achievement
            let tasks: Vec<torii_sqlite_types::AchievementTask> = sqlx::query_as(
                "SELECT id, achievement_id, task_id, world_address, namespace, description, total, 
                 total_completions, completion_rate, created_at 
                 FROM achievement_tasks 
                 WHERE achievement_id = ? 
                 ORDER BY created_at ASC",
            )
            .bind(&achievement_id)
            .fetch_all(&self.pool)
            .await?;

            // Convert to proto types (simplified, no redundant fields)
            let proto_tasks: Vec<torii_proto::AchievementTask> = tasks
                .into_iter()
                .map(|t| torii_proto::AchievementTask {
                    task_id: t.task_id,
                    description: t.description,
                    total: t.total as u32,
                    total_completions: t.total_completions as u32,
                    completion_rate: t.completion_rate,
                    created_at: t.created_at,
                })
                .collect();

            // Calculate total completions and completion rate from tasks
            let (total_completions, avg_completion_rate) = if !proto_tasks.is_empty() {
                let sum_completions: u32 = proto_tasks.iter().map(|t| t.total_completions).sum();
                let sum_rate: f64 = proto_tasks.iter().map(|t| t.completion_rate).sum();
                (
                    sum_completions / proto_tasks.len() as u32,
                    sum_rate / proto_tasks.len() as f64,
                )
            } else {
                (0, 0.0)
            };

            achievements.push(torii_proto::Achievement {
                id: achievement.id.clone(),
                world_address: Felt::from_str(&achievement.world_address)
                    .map_err(|e| Error::Parse(ParseError::FromStr(e)))?,
                namespace: achievement
                    .id
                    .split(':')
                    .nth(1)
                    .unwrap_or_default()
                    .to_string(),
                entity_id: achievement
                    .id
                    .split(':')
                    .next_back()
                    .unwrap_or_default()
                    .to_string(),
                hidden: achievement.hidden != 0,
                index: achievement.index_num as u32,
                points: achievement.points as u32,
                start: achievement.start,
                end: achievement.end,
                group: achievement.group_name,
                icon: achievement.icon,
                title: achievement.title,
                description: achievement.description,
                tasks: proto_tasks,
                data: achievement.data,
                total_completions,
                completion_rate: avg_completion_rate,
                created_at: achievement.created_at,
                updated_at: achievement.updated_at,
            });
        }

        Ok(Page {
            items: achievements,
            next_cursor: page.next_cursor,
        })
    }

    /// Returns player achievement data grouped by player globally (across all worlds/namespaces).
    /// Empty arrays mean no filter is applied for that dimension.
    /// Results are paginated based on unique players (aggregated from player_achievements table).
    async fn player_achievements(
        &self,
        query: &torii_proto::PlayerAchievementQuery,
    ) -> Result<Page<torii_proto::PlayerAchievementEntry>, StorageError> {
        use std::collections::HashMap;

        let executor = PaginationExecutor::new(self.pool.clone());

        // Step 1: Build paginated query for player stats grouped by player_id
        let mut query_builder = QueryBuilder::new("player_achievements").select(&[
            "player_id".to_string(),
            "SUM(total_points) as total_points".to_string(),
            "SUM(completed_achievements) as completed_achievements".to_string(),
            "SUM(total_achievements) as total_achievements".to_string(),
            "AVG(completion_percentage) as completion_percentage".to_string(),
            "MAX(last_achievement_at) as last_achievement_at".to_string(),
            "MIN(created_at) as created_at".to_string(),
            "MAX(updated_at) as updated_at".to_string(),
        ]);

        if !query.world_addresses.is_empty() {
            let placeholders = vec!["?"; query.world_addresses.len()].join(", ");
            query_builder =
                query_builder.where_clause(&format!("world_address IN ({})", placeholders));
            for addr in &query.world_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        if !query.namespaces.is_empty() {
            let placeholders = vec!["?"; query.namespaces.len()].join(", ");
            query_builder = query_builder.where_clause(&format!("namespace IN ({})", placeholders));
            for namespace in &query.namespaces {
                query_builder = query_builder.bind_value(namespace.clone());
            }
        }

        if !query.player_addresses.is_empty() {
            let placeholders = vec!["?"; query.player_addresses.len()].join(", ");
            query_builder = query_builder.where_clause(&format!("player_id IN ({})", placeholders));
            for addr in &query.player_addresses {
                query_builder = query_builder.bind_value(felt_to_sql_string(addr));
            }
        }

        query_builder = query_builder.group_by("player_id");

        let page = executor
            .execute_paginated_query(
                query_builder,
                &query.pagination,
                &OrderBy {
                    field: "total_points".to_string(),
                    direction: OrderDirection::Desc,
                },
            )
            .await?;

        struct AggregatedPlayerStats {
            player_id: String,
            total_points: i32,
            completed_achievements: i32,
            total_achievements: i32,
            completion_percentage: f64,
            last_achievement_at: Option<String>,
            created_at: String,
            updated_at: String,
        }

        let aggregated_stats: Vec<AggregatedPlayerStats> = page
            .items
            .into_iter()
            .map(|row| {
                Ok(AggregatedPlayerStats {
                    player_id: row.try_get("player_id")?,
                    total_points: row.try_get("total_points")?,
                    completed_achievements: row.try_get("completed_achievements")?,
                    total_achievements: row.try_get("total_achievements")?,
                    completion_percentage: row.try_get("completion_percentage")?,
                    last_achievement_at: row.try_get("last_achievement_at").ok(),
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;

        if aggregated_stats.is_empty() {
            return Ok(Page {
                items: vec![],
                next_cursor: page.next_cursor,
            });
        }

        // Step 2: Get all player_achievements rows for these players (to know which worlds/namespaces they have)
        let player_ids: Vec<String> = aggregated_stats
            .iter()
            .map(|s| s.player_id.clone())
            .collect();

        let mut world_namespace_query = "SELECT DISTINCT world_address, namespace FROM player_achievements WHERE player_id IN (".to_string();
        world_namespace_query.push_str(&vec!["?"; player_ids.len()].join(", "));
        world_namespace_query.push(')');

        // Apply world/namespace filters if specified
        if !query.world_addresses.is_empty() {
            world_namespace_query.push_str(" AND world_address IN (");
            world_namespace_query.push_str(&vec!["?"; query.world_addresses.len()].join(", "));
            world_namespace_query.push(')');
        }
        if !query.namespaces.is_empty() {
            world_namespace_query.push_str(" AND namespace IN (");
            world_namespace_query.push_str(&vec!["?"; query.namespaces.len()].join(", "));
            world_namespace_query.push(')');
        }

        let mut wn_query = sqlx::query_as::<_, (String, String)>(&world_namespace_query);
        for player_id in &player_ids {
            wn_query = wn_query.bind(player_id);
        }
        if !query.world_addresses.is_empty() {
            for addr in &query.world_addresses {
                wn_query = wn_query.bind(felt_to_sql_string(addr));
            }
        }
        if !query.namespaces.is_empty() {
            for namespace in &query.namespaces {
                wn_query = wn_query.bind(namespace);
            }
        }

        let world_namespace_pairs: Vec<(String, String)> = wn_query.fetch_all(&self.pool).await?;

        // Step 3: Fetch all achievements and tasks in ONE query using JOIN
        // This is more efficient than separate queries for achievements and tasks
        let mut achievements_with_tasks: HashMap<
            (String, String),
            Vec<(
                torii_sqlite_types::Achievement,
                Vec<torii_sqlite_types::AchievementTask>,
            )>,
        > = HashMap::new();

        for (world_address, namespace) in &world_namespace_pairs {
            // Get all achievements
            let achievements: Vec<torii_sqlite_types::Achievement> = sqlx::query_as(
                "SELECT id, world_address, hidden, index_num, points, start, end, group_name, 
                 icon, title, description, tasks, data, created_at, updated_at 
                 FROM achievements 
                 WHERE world_address = ? AND namespace = ? 
                 ORDER BY index_num ASC",
            )
            .bind(world_address)
            .bind(namespace)
            .fetch_all(&self.pool)
            .await?;

            let mut ach_with_tasks = Vec::new();

            for achievement in achievements {
                // Get tasks for this achievement
                let tasks: Vec<torii_sqlite_types::AchievementTask> = sqlx::query_as(
                    "SELECT id, achievement_id, task_id, world_address, namespace, description, total, 
                     total_completions, completion_rate, created_at 
                     FROM achievement_tasks 
                     WHERE achievement_id = ? 
                     ORDER BY created_at ASC",
                )
                .bind(&achievement.id)
                .fetch_all(&self.pool)
                .await?;

                ach_with_tasks.push((achievement, tasks));
            }

            achievements_with_tasks
                .insert((world_address.clone(), namespace.clone()), ach_with_tasks);
        }

        // Step 4: Fetch all progressions for all players in this page
        let mut progressions_map: HashMap<String, Vec<torii_sqlite_types::AchievementProgression>> =
            HashMap::new();

        if !aggregated_stats.is_empty() {
            let mut progressions_sql =
                "SELECT id, task_id, world_address, namespace, player_id, count, 
                 completed, completed_at, created_at, updated_at 
                 FROM achievement_progressions 
                 WHERE player_id IN ("
                    .to_string();
            progressions_sql.push_str(&vec!["?"; player_ids.len()].join(", "));
            progressions_sql.push(')');

            // Apply world/namespace filters if specified
            if !query.world_addresses.is_empty() {
                progressions_sql.push_str(" AND world_address IN (");
                progressions_sql.push_str(&vec!["?"; query.world_addresses.len()].join(", "));
                progressions_sql.push(')');
            }
            if !query.namespaces.is_empty() {
                progressions_sql.push_str(" AND namespace IN (");
                progressions_sql.push_str(&vec!["?"; query.namespaces.len()].join(", "));
                progressions_sql.push(')');
            }

            let mut prog_query =
                sqlx::query_as::<_, torii_sqlite_types::AchievementProgression>(&progressions_sql);
            for player_id in &player_ids {
                prog_query = prog_query.bind(player_id);
            }
            if !query.world_addresses.is_empty() {
                for addr in &query.world_addresses {
                    prog_query = prog_query.bind(felt_to_sql_string(addr));
                }
            }
            if !query.namespaces.is_empty() {
                for namespace in &query.namespaces {
                    prog_query = prog_query.bind(namespace);
                }
            }

            let all_progressions = prog_query.fetch_all(&self.pool).await?;
            for prog in all_progressions {
                // Key by player_id only since we're grouping globally
                progressions_map
                    .entry(prog.player_id.clone())
                    .or_default()
                    .push(prog);
            }
        }

        // Step 5: Build the response by combining all data (grouped by player globally)
        let mut player_entries = Vec::new();

        for stats in aggregated_stats {
            let player_address = Felt::from_str(&stats.player_id)
                .map_err(|e| Error::Parse(ParseError::FromStr(e)))?;

            // Parse dates
            let last_achievement_at = stats.last_achievement_at.as_deref().and_then(|s| {
                DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|dt| dt.with_timezone(&Utc))
            });
            let created_at = DateTime::parse_from_rfc3339(&stats.created_at)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);
            let updated_at = DateTime::parse_from_rfc3339(&stats.updated_at)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);

            let stats_proto = torii_proto::PlayerAchievementStats {
                total_points: stats.total_points as u32,
                completed_achievements: stats.completed_achievements as u32,
                total_achievements: stats.total_achievements as u32,
                completion_percentage: stats.completion_percentage,
                last_achievement_at,
                created_at,
                updated_at,
            };

            // Get all progressions for this player (across all worlds/namespaces)
            let player_progressions = progressions_map
                .get(&stats.player_id)
                .cloned()
                .unwrap_or_default();

            let mut achievement_progress = Vec::new();

            // Iterate through all world/namespace pairs and build achievement progress
            for (world_address_str, namespace) in &world_namespace_pairs {
                let achievements_with_tasks_list = achievements_with_tasks
                    .get(&(world_address_str.clone(), namespace.clone()))
                    .cloned()
                    .unwrap_or_default();

                for (achievement, tasks) in achievements_with_tasks_list {
                    // Build task progress (just references with count/completed)
                    let mut task_progress = Vec::new();
                    let mut completed_tasks = 0;

                    for task in &tasks {
                        let progression = player_progressions
                            .iter()
                            .find(|p| p.task_id == task.task_id);

                        let (count, completed) = if let Some(p) = progression {
                            if p.completed != 0 {
                                completed_tasks += 1;
                            }
                            (p.count as u32, p.completed != 0)
                        } else {
                            (0, false)
                        };

                        task_progress.push(torii_proto::TaskProgress {
                            task_id: task.task_id.clone(),
                            count,
                            completed,
                        });
                    }

                    let total_tasks = task_progress.len();
                    let achievement_completed = total_tasks > 0 && completed_tasks == total_tasks;
                    let progress_percentage = if total_tasks > 0 {
                        (completed_tasks as f64 / total_tasks as f64) * 100.0
                    } else {
                        0.0
                    };

                    // Build full task definitions for the achievement (simplified, no redundant fields)
                    let proto_tasks: Vec<torii_proto::AchievementTask> = tasks
                        .iter()
                        .map(|task| torii_proto::AchievementTask {
                            task_id: task.task_id.clone(),
                            description: task.description.clone(),
                            total: task.total as u32,
                            total_completions: task.total_completions as u32,
                            completion_rate: task.completion_rate,
                            created_at: task.created_at,
                        })
                        .collect();

                    // Calculate total completions and completion rate from tasks
                    let (total_completions, avg_completion_rate) = if !tasks.is_empty() {
                        let sum_completions: i32 = tasks.iter().map(|t| t.total_completions).sum();
                        let sum_rate: f64 = tasks.iter().map(|t| t.completion_rate).sum();
                        (
                            sum_completions / tasks.len() as i32,
                            sum_rate / tasks.len() as f64,
                        )
                    } else {
                        (0, 0.0)
                    };

                    let world_address = Felt::from_str(world_address_str)
                        .map_err(|e| Error::Parse(ParseError::FromStr(e)))?;

                    achievement_progress.push(torii_proto::PlayerAchievementProgress {
                        achievement: torii_proto::Achievement {
                            id: achievement.id.clone(),
                            world_address,
                            namespace: namespace.clone(),
                            entity_id: achievement
                                .id
                                .split(':')
                                .next_back()
                                .unwrap_or_default()
                                .to_string(),
                            hidden: achievement.hidden != 0,
                            index: achievement.index_num as u32,
                            points: achievement.points as u32,
                            start: achievement.start,
                            end: achievement.end,
                            group: achievement.group_name,
                            icon: achievement.icon,
                            title: achievement.title,
                            description: achievement.description,
                            tasks: proto_tasks,
                            data: achievement.data,
                            total_completions: total_completions as u32,
                            completion_rate: avg_completion_rate,
                            created_at: achievement.created_at,
                            updated_at: achievement.updated_at,
                        },
                        task_progress,
                        completed: achievement_completed,
                        progress_percentage,
                    });
                }
            }

            player_entries.push(torii_proto::PlayerAchievementEntry {
                player_address,
                stats: stats_proto,
                achievements: achievement_progress,
            });
        }

        Ok(Page {
            items: player_entries,
            next_cursor: page.next_cursor,
        })
    }

    /// Performs a global search across the unified FTS5 search index.
    ///
    /// Uses a single SQLite FTS5 virtual table for fast, ranked full-text search
    /// across all entity types:
    /// - Achievements: title, description, group_name
    /// - Controllers: username
    /// - Token Attributes: trait_name, trait_value (NFT traits)
    /// - Tokens: name, symbol (ERC20 only, token_id IS NULL)
    ///
    /// The unified index is automatically maintained via triggers.
    ///
    /// Query syntax supports FTS5 features:
    /// - Simple: "dragon" or "USDC"
    /// - Phrase: '"dragon slayer"'
    /// - Prefix: "dra*" (if prefix_matching enabled in config)
    /// - Boolean: "dragon OR knight"
    /// - Column-specific: "primary_text:dragon"
    ///
    /// Results are ranked by relevance using BM25 algorithm and grouped by entity type.
    async fn search(&self, query: &SearchQuery) -> Result<SearchResponse, StorageError> {
        use std::collections::HashMap;

        // Validate query length
        let search_term = query.query.trim();
        if search_term.is_empty() {
            return Ok(SearchResponse {
                total: 0,
                results: vec![],
            });
        }

        // Validate against min_query_length from config
        if search_term.len() < self.config.search.min_query_length {
            return Ok(SearchResponse {
                total: 0,
                results: vec![],
            });
        }

        // Apply prefix matching if enabled
        let fts_query = if self.config.search.prefix_matching && !search_term.ends_with('*') {
            format!("{}*", search_term)
        } else {
            search_term.to_string()
        };

        let limit = if query.limit > 0 && query.limit <= self.config.search.max_results as u32 {
            query.limit
        } else {
            self.config.search.max_results as u32
        };

        // Build unified search query
        let sql = format!(
            "SELECT entity_type, entity_id, primary_text, secondary_text, metadata, \
             bm25(search_index) as rank \
             FROM search_index \
             WHERE search_index MATCH ? \
             ORDER BY rank ASC \
             LIMIT {}",
            limit * 3 // Get more results for proper grouping by entity type
        );
        let bind_values: Vec<String> = vec![fts_query];

        // Execute query
        let mut sqlx_query = sqlx::query(&sql);
        for value in bind_values {
            sqlx_query = sqlx_query.bind(value);
        }

        let rows = match sqlx_query.fetch_all(&self.pool).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!("Unified FTS5 search failed: {}", e);
                return Ok(SearchResponse {
                    total: 0,
                    results: vec![],
                });
            }
        };

        // Group results by entity_type
        let mut grouped_results: HashMap<String, Vec<SearchMatch>> = HashMap::new();
        let mut entity_counts: HashMap<String, u32> = HashMap::new();

        for row in rows {
            let entity_type: String = row.try_get("entity_type").unwrap_or_default();
            let entity_id: String = row.try_get("entity_id").unwrap_or_default();
            let primary_text: String = row.try_get("primary_text").unwrap_or_default();
            let secondary_text: String = row.try_get("secondary_text").unwrap_or_default();
            let metadata: String = row.try_get("metadata").unwrap_or_else(|_| "{}".to_string());
            let score: Option<f64> = row.try_get("rank").ok();

            // Count per entity type
            *entity_counts.entry(entity_type.clone()).or_insert(0) += 1;

            // Apply per-type limit
            let type_count = entity_counts.get(&entity_type).copied().unwrap_or(0);
            if type_count > limit {
                continue;
            }

            // Parse metadata JSON
            let metadata_map: HashMap<String, String> =
                serde_json::from_str(&metadata).unwrap_or_default();

            let mut fields = metadata_map;
            fields.insert("entity_id".to_string(), entity_id.clone());
            fields.insert("primary_text".to_string(), primary_text.clone());
            if !secondary_text.is_empty() {
                fields.insert("secondary_text".to_string(), secondary_text.clone());
            }

            // Add snippet if enabled
            if self.config.search.return_snippets {
                let text = if !secondary_text.is_empty() {
                    &secondary_text
                } else {
                    &primary_text
                };
                let snippet = if text.len() > self.config.search.snippet_length {
                    format!("{}...", &text[..self.config.search.snippet_length])
                } else {
                    text.clone()
                };
                fields.insert("snippet".to_string(), snippet);
            }

            grouped_results
                .entry(entity_type)
                .or_default()
                .push(SearchMatch {
                    id: entity_id,
                    fields,
                    score,
                });
        }

        // Convert to response format
        let mut all_results = Vec::new();
        let mut total_count = 0u32;

        // Map entity_type to table names for compatibility
        let type_to_table = |entity_type: &str| -> String {
            match entity_type {
                "achievement" => "achievements".to_string(),
                "controller" => "controllers".to_string(),
                "token_attribute" => "token_attributes".to_string(),
                "token" => "tokens".to_string(),
                _ => entity_type.to_string(),
            }
        };

        for (entity_type, matches) in grouped_results {
            let count = matches.len() as u32;
            total_count += count;
            all_results.push(TableSearchResults {
                table: type_to_table(&entity_type),
                count,
                matches,
            });
        }

        Ok(SearchResponse {
            total: total_count,
            results: all_results,
        })
    }
}

#[async_trait]
impl Storage for Sql {
    /// Updates the contract cursors with the storage.
    async fn update_cursors(
        &self,
        cursors: HashMap<Felt, ContractCursor>,
        cursor_transactions: HashMap<Felt, HashSet<Felt>>,
    ) -> Result<(), StorageError> {
        let (query, recv) = QueryMessage::new_recv(
            "".to_string(),
            vec![],
            QueryType::UpdateCursors(UpdateCursorsQuery {
                cursors,
                cursor_transactions,
            }),
        );

        self.executor.send(query).map_err(|e| {
            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
        })?;

        recv.await
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::RecvError(e))))?
            .map_err(|e| Error::ExecutorQuery(Box::new(e)))?;

        Ok(())
    }

    /// Registers a model with the storage, along with its table.
    /// This is also used when a model is upgraded, which should
    /// update the model schema and its table.
    async fn register_model(
        &self,
        world_address: Felt,
        selector: Felt,
        model: &Ty,
        layout: &Layout,
        class_hash: Felt,
        contract_address: Felt,
        packed_size: u32,
        unpacked_size: u32,
        block_timestamp: u64,
        schema_diff: Option<&Ty>,
        upgrade_diff: Option<&Ty>,
        legacy_store: bool,
    ) -> Result<(), StorageError> {
        let namespaced_name = model.name();
        let (namespace, name) = namespaced_name.split_once('-').unwrap();

        // Create world-scoped model ID: "world_address:model_selector"
        let scoped_model_id =
            torii_storage::utils::format_world_scoped_id(&world_address, &selector);
        let selector_str = felt_to_sql_string(&selector);
        let world_address_str = felt_to_sql_string(&world_address);
        let insert_models =
            "INSERT INTO models (id, world_address, model_selector, namespace, name, class_hash, contract_address, layout, \
             legacy_store, schema, packed_size, unpacked_size, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
             ?, ?) ON CONFLICT(id) DO UPDATE SET world_address=EXCLUDED.world_address, model_selector=EXCLUDED.model_selector, contract_address=EXCLUDED.contract_address, \
             class_hash=EXCLUDED.class_hash, layout=EXCLUDED.layout, legacy_store=EXCLUDED.legacy_store, \
             schema=EXCLUDED.schema, packed_size=EXCLUDED.packed_size, unpacked_size=EXCLUDED.unpacked_size, \
             executed_at=EXCLUDED.executed_at RETURNING *";
        let arguments = vec![
            Argument::String(scoped_model_id),
            Argument::String(world_address_str),
            Argument::String(selector_str),
            Argument::String(namespace.to_string()),
            Argument::String(name.to_string()),
            Argument::FieldElement(class_hash),
            Argument::FieldElement(contract_address),
            Argument::String(
                serde_json::to_string(&layout)
                    .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?,
            ),
            Argument::Bool(legacy_store),
            Argument::String(
                serde_json::to_string(&model)
                    .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?,
            ),
            Argument::Int(packed_size as i64),
            Argument::Int(unpacked_size as i64),
            Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
        ];
        self.executor
            .send(QueryMessage::new(
                insert_models.to_string(),
                arguments,
                QueryType::RegisterModel,
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        self.build_model_query(
            vec![namespaced_name.clone()],
            model,
            schema_diff,
            upgrade_diff,
        )?;

        for hook in self.config.hooks.iter() {
            if let HookEvent::ModelRegistered { model_tag } = &hook.event {
                if namespaced_name == *model_tag {
                    // For hooks, pass the world-scoped model ID
                    let scoped_model_id =
                        torii_storage::utils::format_world_scoped_id(&world_address, &selector);
                    self.executor
                        .send(QueryMessage::other(
                            hook.statement.clone(),
                            vec![Argument::String(scoped_model_id)],
                        ))
                        .map_err(|e| {
                            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(
                                e,
                            ))))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Registers a contract with the storage.
    /// This is used when a new contract is registered in the world.
    async fn register_contract(
        &self,
        address: Felt,
        contract_type: torii_proto::ContractType,
        head: u64,
    ) -> Result<(), StorageError> {
        let insert_contract = "INSERT INTO contracts (id, contract_address, contract_type, head, updated_at, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT(id) DO UPDATE SET contract_type=EXCLUDED.contract_type, head=EXCLUDED.head, updated_at=CURRENT_TIMESTAMP RETURNING *";

        let arguments = vec![
            Argument::FieldElement(address),
            Argument::FieldElement(address),
            Argument::String(contract_type.to_string()),
            Argument::Int(head as i64),
        ];

        self.executor
            .send(QueryMessage::new(
                insert_contract.to_string(),
                arguments,
                QueryType::RegisterContract,
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        Ok(())
    }

    /// Sets an entity with the storage.
    /// It should insert or update the entity if it already exists.
    /// Along with its model state in the model table.
    async fn set_entity(
        &self,
        world_address: Felt,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
        entity_id: Felt,
        model_selector: Felt,
        keys: Option<Vec<Felt>>,
    ) -> Result<(), StorageError> {
        let namespaced_name = entity.name();

        // Format entity_id with world_address prefix for multi-world support
        let scoped_entity_id =
            torii_storage::utils::format_world_scoped_id(&world_address, &entity_id);
        let entity_id_str = felt_to_sql_string(&entity_id);
        let scoped_model_id =
            torii_storage::utils::format_world_scoped_id(&world_address, &model_selector);
        let world_address_str = felt_to_sql_string(&world_address);

        let keys_str = keys.map(|keys| felts_to_sql_string(&keys));
        let insert_entities = if keys_str.is_some() {
            "INSERT INTO entities (id, world_address, entity_id, event_id, executed_at, keys) VALUES (?, ?, ?, ?, ?, ?) ON \
             CONFLICT(id) DO UPDATE SET world_address=EXCLUDED.world_address, updated_at=CURRENT_TIMESTAMP, entity_id=EXCLUDED.entity_id, \
             executed_at=EXCLUDED.executed_at, event_id=EXCLUDED.event_id, keys=EXCLUDED.keys RETURNING *"
        } else {
            "INSERT INTO entities (id, world_address, entity_id, event_id, executed_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO \
             UPDATE SET world_address=EXCLUDED.world_address, updated_at=CURRENT_TIMESTAMP, entity_id=EXCLUDED.entity_id, executed_at=EXCLUDED.executed_at, \
             event_id=EXCLUDED.event_id RETURNING *"
        };

        let mut arguments = vec![
            Argument::String(scoped_entity_id.clone()),
            Argument::String(world_address_str.clone()),
            Argument::String(entity_id_str.clone()),
            Argument::String(event_id.to_string()),
            Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
        ];

        if let Some(keys) = keys_str.clone() {
            arguments.push(Argument::String(keys));
        }

        arguments.push(Argument::String(world_address_str.clone()));

        self.executor
            .send(QueryMessage::new(
                insert_entities.to_string(),
                arguments,
                QueryType::SetEntity(EntityQuery {
                    event_id: event_id.to_string(),
                    block_timestamp: utc_dt_string_from_timestamp(block_timestamp),
                    entity_id: scoped_entity_id.clone(),
                    model_id: scoped_model_id.clone(),
                    keys_str: keys_str.clone(),
                    ty: entity.clone(),
                    is_historical: self.config.is_historical(&model_selector),
                }),
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        self.executor.send(QueryMessage::other(
            "INSERT INTO entity_model (entity_id, model_id) VALUES (?, ?) ON CONFLICT(entity_id, \
             model_id) DO NOTHING"
                .to_string(),
            vec![
                Argument::String(scoped_entity_id.clone()),
                Argument::String(scoped_model_id.clone()),
            ],
        )).map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e)))))?;

        self.set_entity_model(
            &namespaced_name,
            event_id,
            &scoped_entity_id,
            &entity,
            block_timestamp,
        )?;

        for hook in self.config.hooks.iter() {
            if let HookEvent::ModelUpdated { model_tag } = &hook.event {
                if namespaced_name == *model_tag {
                    self.executor
                        .send(QueryMessage::other(
                            hook.statement.clone(),
                            vec![Argument::String(scoped_entity_id.clone())],
                        ))
                        .map_err(|e| {
                            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(
                                e,
                            ))))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Sets an event message with the storage.
    /// It should insert or update the event message if it already exists.
    /// Along with its model state in the model table.
    async fn set_event_message(
        &self,
        world_address: Felt,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
        keys: Vec<Felt>,
    ) -> Result<(), StorageError> {
        let namespaced_name = entity.name();
        let (model_namespace, model_name) = namespaced_name.split_once('-').unwrap();

        let entity_id = poseidon_hash_many(&keys);
        let scoped_entity_id =
            torii_storage::utils::format_world_scoped_id(&world_address, &entity_id);
        let entity_id_str = felt_to_sql_string(&entity_id);
        let model_selector = compute_selector_from_names(model_namespace, model_name);
        let scoped_model_id =
            torii_storage::utils::format_world_scoped_id(&world_address, &model_selector);
        let world_address_str = felt_to_sql_string(&world_address);

        let keys_str = felts_to_sql_string(&keys);
        let block_timestamp_str = utc_dt_string_from_timestamp(block_timestamp);

        let insert_entities = "INSERT INTO event_messages (id, world_address, entity_id, keys, event_id, executed_at) \
                               VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET \
                               updated_at=CURRENT_TIMESTAMP, executed_at=EXCLUDED.executed_at, \
                               event_id=EXCLUDED.event_id RETURNING *";
        self.executor
            .send(QueryMessage::new(
                insert_entities.to_string(),
                vec![
                    Argument::String(scoped_entity_id.clone()),
                    Argument::String(world_address_str.clone()),
                    Argument::String(entity_id_str.clone()),
                    Argument::String(keys_str.clone()),
                    Argument::String(event_id.to_string()),
                    Argument::String(block_timestamp_str.clone()),
                ],
                QueryType::EventMessage(EventMessageQuery {
                    world_address: world_address_str.clone(),
                    entity_id: scoped_entity_id.clone(),
                    model_id: scoped_model_id.clone(),
                    keys_str: keys_str.clone(),
                    event_id: event_id.to_string(),
                    block_timestamp: block_timestamp_str.clone(),
                    ty: entity.clone(),
                    is_historical: self.config.is_historical(&model_selector),
                }),
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        self.set_entity_model(
            &namespaced_name,
            event_id,
            &format!("event:{}", scoped_entity_id),
            &entity,
            block_timestamp,
        )?;

        for hook in self.config.hooks.iter() {
            if let HookEvent::ModelUpdated { model_tag } = &hook.event {
                if namespaced_name == *model_tag {
                    self.executor
                        .send(QueryMessage::other(
                            hook.statement.clone(),
                            vec![Argument::String(scoped_entity_id.clone())],
                        ))
                        .map_err(|e| {
                            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(
                                e,
                            ))))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Deletes an entity with the storage.
    /// It should delete the entity from the entity table.
    /// Along with its model state in the model table.
    async fn delete_entity(
        &self,
        world_address: Felt,
        entity_id: Felt,
        model_id: Felt,
        entity: Ty,
        event_id: &str,
        block_timestamp: u64,
    ) -> Result<(), StorageError> {
        let scoped_entity_id =
            torii_storage::utils::format_world_scoped_id(&world_address, &entity_id);
        let scoped_model_id =
            torii_storage::utils::format_world_scoped_id(&world_address, &model_id);
        let model_table = entity.name();

        self.executor
            .send(QueryMessage::new(
                format!("DELETE FROM [{model_table}] WHERE internal_id = ?").to_string(),
                vec![Argument::String(scoped_entity_id.clone())],
                QueryType::DeleteEntity(DeleteEntityQuery {
                    model_id: scoped_model_id.clone(),
                    entity_id: scoped_entity_id.clone(),
                    event_id: event_id.to_string(),
                    block_timestamp: utc_dt_string_from_timestamp(block_timestamp),
                    ty: entity.clone(),
                }),
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        for hook in self.config.hooks.iter() {
            if let HookEvent::ModelDeleted { model_tag } = &hook.event {
                if model_table == *model_tag {
                    self.executor
                        .send(QueryMessage::other(
                            hook.statement.clone(),
                            vec![Argument::String(scoped_entity_id.clone())],
                        ))
                        .map_err(|e| {
                            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(
                                e,
                            ))))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Sets the metadata for a resource with the storage.
    /// It should insert or update the metadata if it already exists.
    /// Along with its model state in the model table.
    async fn set_metadata(
        &self,
        resource: &Felt,
        uri: &str,
        block_timestamp: u64,
    ) -> Result<(), StorageError> {
        let resource = Argument::FieldElement(*resource);
        let uri = Argument::String(uri.to_string());
        let executed_at = Argument::String(utc_dt_string_from_timestamp(block_timestamp));

        self.executor
            .send(QueryMessage::other(
                "INSERT INTO metadata (id, uri, executed_at) VALUES (?, ?, ?) ON CONFLICT(id) DO \
             UPDATE SET id=excluded.id, executed_at=excluded.executed_at, \
             updated_at=CURRENT_TIMESTAMP"
                    .to_string(),
                vec![resource, uri, executed_at],
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        Ok(())
    }

    /// Updates the metadata for a resource with the storage.
    /// It should update the metadata if it already exists.
    /// Along with its model state in the model table.
    async fn update_metadata(
        &self,
        resource: &Felt,
        uri: &str,
        metadata: &WorldMetadata,
        icon_img: &Option<String>,
        cover_img: &Option<String>,
    ) -> Result<(), StorageError> {
        let json = serde_json::to_string(metadata).unwrap(); // safe unwrap

        let mut update = vec!["uri=?", "json=?", "updated_at=CURRENT_TIMESTAMP"];
        let mut arguments = vec![Argument::String(uri.to_string()), Argument::String(json)];

        if let Some(icon) = icon_img {
            update.push("icon_img=?");
            arguments.push(Argument::String(icon.clone()));
        }

        if let Some(cover) = cover_img {
            update.push("cover_img=?");
            arguments.push(Argument::String(cover.clone()));
        }

        let statement = format!("UPDATE metadata SET {} WHERE id = ?", update.join(","));
        arguments.push(Argument::FieldElement(*resource));

        self.executor
            .send(QueryMessage::other(statement, arguments))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        Ok(())
    }

    /// Stores a transaction with the storage.
    /// It should insert or ignore the transaction if it already exists.
    /// And store all the relevant calls made in the transaction.
    /// Along with the unique models if any used in the transaction.
    async fn store_transaction(
        &self,
        transaction_hash: Felt,
        sender_address: Felt,
        calldata: &[Felt],
        max_fee: Felt,
        signature: &[Felt],
        nonce: Felt,
        block_number: u64,
        contract_addresses: &HashSet<Felt>,
        transaction_type: &str,
        block_timestamp: u64,
        calls: &[TransactionCall],
        unique_models: &HashSet<Felt>,
    ) -> Result<(), StorageError> {
        // Store the transaction in the transactions table
        self.executor
            .send(QueryMessage::new(
                "INSERT INTO transactions (id, transaction_hash, sender_address, calldata, \
             max_fee, signature, nonce, transaction_type, executed_at, block_number) VALUES (?, \
             ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET transaction_hash=excluded.transaction_hash RETURNING *"
                    .to_string(),
                vec![
                    Argument::FieldElement(transaction_hash),
                    Argument::FieldElement(transaction_hash),
                    Argument::FieldElement(sender_address),
                    Argument::String(felts_to_sql_string(calldata)),
                    Argument::FieldElement(max_fee),
                    Argument::String(felts_to_sql_string(signature)),
                    Argument::FieldElement(nonce),
                    Argument::String(transaction_type.to_string()),
                    Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
                    Argument::String(block_number.to_string()),
                ],
                QueryType::StoreTransaction(StoreTransactionQuery {
                    contract_addresses: contract_addresses.clone(),
                    calls: calls.to_vec(),
                    unique_models: unique_models.clone(),
                }),
            ))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e)))))?;

        Ok(())
    }

    /// Stores an event with the storage.
    async fn store_event(
        &self,
        event_id: &str,
        event: &starknet::core::types::Event,
        transaction_hash: Felt,
        block_timestamp: u64,
    ) -> Result<(), StorageError> {
        let id = Argument::String(event_id.to_string());
        let keys = Argument::String(felts_to_sql_string(&event.keys));
        let data = Argument::String(felts_to_sql_string(&event.data));
        let hash = Argument::FieldElement(transaction_hash);
        let executed_at = Argument::String(utc_dt_string_from_timestamp(block_timestamp));

        self.executor
            .send(QueryMessage::new(
                "INSERT INTO events (id, keys, data, transaction_hash, executed_at) VALUES \
             (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING *"
                    .to_string(),
                vec![id, keys, data, hash, executed_at],
                QueryType::StoreEvent,
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        Ok(())
    }

    /// Adds a controller to the storage.
    async fn add_controller(
        &self,
        username: &str,
        address: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let insert_controller = "
            INSERT INTO controllers (id, username, address, deployed_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                username=EXCLUDED.username,
                address=EXCLUDED.address,
                deployed_at=EXCLUDED.deployed_at
            RETURNING *";

        let arguments = vec![
            Argument::String(username.to_string()),
            Argument::String(username.to_string()),
            Argument::String(address.to_string()),
            Argument::String(timestamp.to_rfc3339()),
        ];

        self.executor
            .send(QueryMessage::other(
                insert_controller.to_string(),
                arguments,
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        Ok(())
    }

    /// Registers a token contract with the storage.
    async fn register_token_contract(
        &self,
        contract_address: Felt,
        name: String,
        symbol: String,
        decimals: u8,
        metadata: Option<String>,
    ) -> Result<(), StorageError> {
        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::RegisterTokenContract(RegisterTokenContractQuery {
                    contract_address,
                    name,
                    symbol,
                    decimals,
                    metadata,
                }),
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;
        Ok(())
    }

    /// Registers an NFT token with the storage.
    async fn register_nft_token(
        &self,
        contract_address: Felt,
        token_id: U256,
        metadata: String,
    ) -> Result<(), StorageError> {
        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::RegisterNftToken(RegisterNftTokenQuery {
                    contract_address,
                    token_id,
                    metadata,
                }),
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        Ok(())
    }

    /// Updates metadata for a token.
    async fn update_token_metadata(
        &self,
        token_id: TokenId,
        metadata: String,
    ) -> Result<(), StorageError> {
        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::UpdateTokenMetadata(UpdateTokenMetadataQuery { token_id, metadata }),
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        Ok(())
    }

    /// Stores an ERC transfer event with the storage.
    #[allow(clippy::too_many_arguments)]
    async fn store_token_transfer(
        &self,
        token_id: TokenId,
        from: Felt,
        to: Felt,
        amount: U256,
        block_timestamp: u64,
        event_id: &str,
    ) -> Result<(), StorageError> {
        let id = format!("{}:{}", event_id, token_id);
        let token_id_str = token_id.to_string();
        let contract_address = token_id.contract_address();
        let event_id_str = event_id.to_string();
        let executed_at_str = utc_dt_string_from_timestamp(block_timestamp);

        let insert_query = format!(
            "INSERT INTO {TOKEN_TRANSFER_TABLE} (id, contract_address, from_address, to_address, \
             amount, token_id, event_id, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING *"
        );

        self.executor
            .send(QueryMessage::new(
                insert_query.to_string(),
                vec![
                    Argument::String(id.clone()),
                    Argument::FieldElement(contract_address),
                    Argument::FieldElement(from),
                    Argument::FieldElement(to),
                    Argument::String(u256_to_sql_string(&amount)),
                    Argument::String(token_id_str.clone()),
                    Argument::String(event_id_str.clone()),
                    Argument::String(executed_at_str.clone()),
                ],
                QueryType::StoreTokenTransfer,
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;

        Ok(())
    }

    /// Applies cached balance differences to the storage.
    async fn apply_balances_diff(
        &self,
        balances_diff: HashMap<BalanceId, I256>,
        total_supply_diff: HashMap<TokenId, I256>,
        cursors: HashMap<Felt, ContractCursor>,
    ) -> Result<(), StorageError> {
        self.executor
            .send(QueryMessage::new(
                "".to_string(),
                vec![],
                QueryType::ApplyBalanceDiff(ApplyBalanceDiffQuery {
                    balances_diff,
                    total_supply_diff,
                    cursors,
                }),
            ))
            .map_err(|e| {
                Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
            })?;
        Ok(())
    }

    /// Executes pending operations and commits the current transaction.
    async fn execute(&self) -> Result<(), StorageError> {
        let (execute, recv) = QueryMessage::execute_recv();
        self.executor.send(execute).map_err(|e| {
            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
        })?;
        let res = recv
            .await
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::RecvError(e))))?;
        res.map_err(|e| Error::ExecutorQuery(Box::new(e)))?;
        Ok(())
    }

    /// Rolls back the current transaction and starts a new one.
    async fn rollback(&self) -> Result<(), StorageError> {
        let (rollback, recv) = QueryMessage::rollback_recv();
        self.executor.send(rollback).map_err(|e| {
            Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(Box::new(e))))
        })?;
        let res = recv
            .await
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::RecvError(e))))?;
        res.map_err(|e| Error::ExecutorQuery(Box::new(e)))?;
        Ok(())
    }
}

impl Sql {
    async fn fetch_transaction_calls(
        &self,
        transaction_hash: &str,
    ) -> Result<Vec<torii_proto::TransactionCall>, StorageError> {
        use crate::utils::sql_string_to_felts;

        let calls = sqlx::query(
            "SELECT contract_address, entrypoint, calldata, call_type, caller_address
             FROM transaction_calls
             WHERE transaction_hash = ?",
        )
        .bind(transaction_hash)
        .fetch_all(&self.pool)
        .await?;

        let mut transaction_calls = Vec::new();
        for row in calls {
            let contract_address = row.try_get::<String, _>("contract_address")?;
            let entrypoint = row.try_get::<String, _>("entrypoint")?;
            let calldata = row.try_get::<String, _>("calldata")?;
            let call_type = row.try_get::<String, _>("call_type")?;
            let caller_address = row.try_get::<String, _>("caller_address")?;

            let call = torii_proto::TransactionCall {
                contract_address: Felt::from_str(&contract_address)
                    .map_err(|e| Error::Parse(ParseError::FromStr(e)))?,
                entrypoint,
                calldata: sql_string_to_felts(&calldata),
                call_type: CallType::from_str(&call_type).map_err(Error::Proto)?,
                caller_address: Felt::from_str(&caller_address)
                    .map_err(|e| Error::Parse(ParseError::FromStr(e)))?,
            };

            transaction_calls.push(call);
        }

        Ok(transaction_calls)
    }

    async fn fetch_transaction_models(
        &self,
        transaction_hash: &str,
    ) -> Result<Vec<Felt>, StorageError> {
        let models =
            sqlx::query("SELECT model_id FROM transaction_models WHERE transaction_hash = ?")
                .bind(transaction_hash)
                .fetch_all(&self.pool)
                .await?;

        let mut unique_models = Vec::new();
        for row in models {
            let model_id = row.try_get::<String, _>("model_id")?;
            unique_models
                .push(Felt::from_str(&model_id).map_err(|e| Error::Parse(ParseError::FromStr(e)))?);
        }

        Ok(unique_models)
    }
}
