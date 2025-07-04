use std::collections::HashSet;
use std::sync::Arc;

use dojo_types::primitive::SqlType;
use dojo_types::schema::Ty;
use sqlx::{Pool, Sqlite};
use starknet::core::types::Felt;
use tokio::sync::mpsc::UnboundedSender;
use torii_cache::Cache;
use torii_storage::types::{Contract, Cursor};
use torii_storage::Storage;

use crate::error::{Error, ParseError};
use crate::executor::error::ExecutorQueryError;
use crate::executor::{Argument, QueryMessage};
use crate::utils::utc_dt_string_from_timestamp;
use torii_sqlite_types::{Hook, ModelIndices};

pub mod constants;
pub mod cursor;
pub mod error;
pub mod executor;
pub mod model;
pub mod query;
pub mod simple_broker;
pub mod storage;
pub mod utils;

pub use torii_sqlite_types as types;

#[derive(Debug, Clone, Default)]
pub struct SqlConfig {
    pub all_model_indices: bool,
    pub model_indices: Vec<ModelIndices>,
    pub historical_models: HashSet<Felt>,
    pub hooks: Vec<Hook>,
}

impl SqlConfig {
    pub fn is_historical(&self, selector: &Felt) -> bool {
        self.historical_models.contains(selector)
    }
}

#[derive(Debug, Clone)]
pub struct Sql {
    pub pool: Pool<Sqlite>,
    pub executor: UnboundedSender<QueryMessage>,
    pub config: SqlConfig,
    pub cache: Option<Arc<dyn Cache>>,
}

impl Sql {
    pub async fn new(
        pool: Pool<Sqlite>,
        executor: UnboundedSender<QueryMessage>,
        contracts: &[Contract],
    ) -> Result<Self, Error> {
        Self::new_with_config(pool, executor, contracts, Default::default()).await
    }

    pub async fn new_with_config(
        pool: Pool<Sqlite>,
        executor: UnboundedSender<QueryMessage>,
        contracts: &[Contract],
        config: SqlConfig,
    ) -> Result<Self, Error> {
        for contract in contracts {
            executor.send(QueryMessage::other(
                "INSERT OR IGNORE INTO contracts (id, contract_address, contract_type) VALUES (?, \
                 ?, ?)"
                    .to_string(),
                vec![
                    Argument::FieldElement(contract.address),
                    Argument::FieldElement(contract.address),
                    Argument::String(contract.r#type.to_string()),
                ],
            )).map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;
        }

        let db = Self {
            pool: pool.clone(),
            executor,
            config,
            cache: None,
        };

        db.execute().await?;

        Ok(db)
    }

    pub fn with_cache(self, cache: Arc<dyn Cache>) -> Self {
        Self {
            cache: Some(cache),
            ..self
        }
    }

    fn set_entity_model(
        &self,
        model_name: &str,
        event_id: &str,
        entity_id: &str,
        entity: &Ty,
        block_timestamp: u64,
    ) -> Result<(), Error> {
        let mut columns = vec![
            "internal_id".to_string(),
            "internal_event_id".to_string(),
            "internal_executed_at".to_string(),
            "internal_updated_at".to_string(),
            if entity_id.starts_with("event:") {
                "internal_event_message_id".to_string()
            } else {
                "internal_entity_id".to_string()
            },
        ];

        let mut arguments = vec![
            Argument::String(entity_id.to_string()),
            Argument::String(event_id.to_string()),
            Argument::String(utc_dt_string_from_timestamp(block_timestamp)),
            Argument::String(chrono::Utc::now().to_rfc3339()),
            Argument::String(entity_id.trim_start_matches("event:").to_string()),
        ];

        fn collect_members(
            prefix: &str,
            ty: &Ty,
            columns: &mut Vec<String>,
            arguments: &mut Vec<Argument>,
        ) -> Result<(), Error> {
            match ty {
                Ty::Struct(s) => {
                    for member in &s.children {
                        let column_name = if prefix.is_empty() {
                            member.name.clone()
                        } else {
                            format!("{}.{}", prefix, member.name)
                        };
                        collect_members(&column_name, &member.ty, columns, arguments)?;
                    }
                }
                Ty::Enum(e) => {
                    columns.push(format!("\"{}\"", prefix));
                    arguments.push(Argument::String(e.to_sql_value()));

                    if let Some(option_idx) = e.option {
                        let option = &e.options[option_idx as usize];
                        if let Ty::Tuple(t) = &option.ty {
                            if t.is_empty() {
                                return Ok(());
                            }
                        }
                        let variant_path = format!("{}.{}", prefix, option.name);
                        collect_members(&variant_path, &option.ty, columns, arguments)?;
                    }
                }
                Ty::Tuple(t) => {
                    for (idx, member) in t.iter().enumerate() {
                        let column_name = if prefix.is_empty() {
                            format!("{}", idx)
                        } else {
                            format!("{}.{}", prefix, idx)
                        };
                        collect_members(&column_name, member, columns, arguments)?;
                    }
                }
                Ty::Array(array) => {
                    columns.push(format!("\"{}\"", prefix));
                    let values = array
                        .iter()
                        .map(|v| v.to_json_value())
                        .collect::<Result<Vec<_>, _>>()?;
                    arguments.push(Argument::String(
                        serde_json::to_string(&values)
                            .map_err(|e| Error::Parse(ParseError::FromJsonStr(e)))?,
                    ));
                }
                Ty::Primitive(ty) => {
                    columns.push(format!("\"{}\"", prefix));
                    arguments.push(Argument::String(ty.to_sql_value()));
                }
                Ty::ByteArray(b) => {
                    columns.push(format!("\"{}\"", prefix));
                    arguments.push(Argument::String(b.clone()));
                }
            }
            Ok(())
        }

        // Collect all columns and arguments recursively
        collect_members("", entity, &mut columns, &mut arguments)?;

        // Try to insert first - this will only succeed if the entity doesn't exist
        let placeholders: Vec<&str> = arguments.iter().map(|_| "?").collect();
        let insert_statement = format!(
            "INSERT INTO [{}] ({}) VALUES ({}) ON CONFLICT(internal_id) DO UPDATE SET {}",
            model_name,
            columns.join(","),
            placeholders.join(","),
            columns
                .iter()
                .skip(1) // Skip internal_id which is the primary key
                .map(|col| format!("{0}=excluded.{0}", col))
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Execute the single query
        self.executor
            .send(QueryMessage::other(insert_statement, arguments))
            .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_model_query(
        &self,
        path: Vec<String>,
        model: &Ty,
        schema_diff: Option<&Ty>,
        upgrade_diff: Option<&Ty>,
    ) -> Result<(), Error> {
        let table_id = path[0].clone(); // Use only the root path component
        let mut columns = Vec::new();
        let mut indices = Vec::new();
        let mut alter_table_queries = Vec::new();

        // Start building the create table query with internal columns
        let mut create_table_query = format!(
            "CREATE TABLE IF NOT EXISTS [{table_id}] (internal_id TEXT NOT NULL PRIMARY KEY, \
             internal_event_id TEXT NOT NULL, internal_entity_id TEXT, internal_event_message_id \
             TEXT, "
        );

        indices.push(format!(
            "CREATE INDEX IF NOT EXISTS [idx_{table_id}_internal_entity_id] ON [{table_id}] \
             ([internal_entity_id]);"
        ));
        indices.push(format!(
            "CREATE INDEX IF NOT EXISTS [idx_{table_id}_internal_event_message_id] ON \
             [{table_id}] ([internal_event_message_id]);"
        ));

        // Recursively add columns for all nested type
        self.add_columns_recursive(
            &path,
            model,
            &mut columns,
            &mut alter_table_queries,
            &mut indices,
            &table_id,
            schema_diff,
            upgrade_diff,
            false,
        )?;

        // Add all columns to the create table query
        for column in columns {
            create_table_query.push_str(&format!("{}, ", column));
        }

        // Add internal timestamps
        create_table_query.push_str("internal_executed_at DATETIME NOT NULL, ");
        create_table_query
            .push_str("internal_created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ");
        create_table_query
            .push_str("internal_updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ");

        // Add foreign key constraints
        create_table_query.push_str("FOREIGN KEY (internal_entity_id) REFERENCES entities(id), ");
        create_table_query
            .push_str("FOREIGN KEY (internal_event_message_id) REFERENCES event_messages(id));");

        // Execute the queries
        if upgrade_diff.is_some() || schema_diff.is_some() {
            for alter_query in alter_table_queries {
                self.executor
                    .send(QueryMessage::other(alter_query, vec![]))
                    .map_err(|e| {
                        Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e)))
                    })?;
            }
        } else {
            self.executor
                .send(QueryMessage::other(create_table_query, vec![]))
                .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;
        }

        // Create indices
        for index_query in indices {
            self.executor
                .send(QueryMessage::other(index_query, vec![]))
                .map_err(|e| Error::ExecutorQuery(Box::new(ExecutorQueryError::SendError(e))))?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn add_columns_recursive(
        &self,
        path: &[String],
        ty: &Ty,
        columns: &mut Vec<String>,
        alter_table_queries: &mut Vec<String>,
        indices: &mut Vec<String>,
        table_id: &str,
        schema_diff: Option<&Ty>,
        upgrade_diff: Option<&Ty>,
        is_key: bool,
    ) -> Result<(), Error> {
        let column_prefix = if path.len() > 1 {
            path[1..].join(".")
        } else {
            String::new()
        };

        let add_index = |indices: &mut Vec<String>, name: &str| {
            let model_indices = self
                .config
                .model_indices
                .iter()
                .find(|m| m.model_tag == table_id);

            if model_indices.is_some_and(|m| m.fields.contains(&name.to_string()))
                || (model_indices.is_none() && (self.config.all_model_indices || is_key))
            {
                indices.push(format!(
                    "CREATE INDEX IF NOT EXISTS [idx_{table_id}_{name}] ON [{table_id}] ([{name}]);"
                ));
            }
        };

        let mut add_column = |name: &str, sql_type: &str, indices: &mut Vec<String>| {
            columns.push(format!("[{name}] {sql_type}"));
            add_index(indices, name);
        };

        let mut alter_column = |name: &str, sql_type: &str, indices: &mut Vec<String>| {
            alter_table_queries.push(format!(
                "ALTER TABLE [{table_id}] ADD COLUMN [{name}] {sql_type}"
            ));
            add_index(indices, name);
        };

        let modify_column = |alter_table_queries: &mut Vec<String>,
                             name: &str,
                             sql_type: &str,
                             sql_value: &str| {
            alter_table_queries.push(format!(
            "CREATE TEMPORARY TABLE [tmp_values_{name}] AS SELECT internal_id, [{name}] FROM [{table_id}]"
        ));
            alter_table_queries.push(format!("DROP INDEX IF EXISTS [idx_{table_id}_{name}]"));
            alter_table_queries.push(format!("ALTER TABLE [{table_id}] DROP COLUMN [{name}]"));
            alter_table_queries.push(format!(
                "ALTER TABLE [{table_id}] ADD COLUMN [{name}] {sql_type}"
            ));
            alter_table_queries.push(format!(
                "UPDATE [{table_id}] SET [{name}] = (SELECT {sql_value} FROM [tmp_values_{name}] \
             WHERE [tmp_values_{name}].internal_id = [{table_id}].internal_id)"
            ));
            alter_table_queries.push(format!("DROP TABLE [tmp_values_{name}]"));
            alter_table_queries.push(format!(
                "CREATE INDEX IF NOT EXISTS [idx_{table_id}_{name}] ON [{table_id}] ([{name}]);"
            ));
        };

        match ty {
            Ty::Struct(s) => {
                let struct_upgrade_diff = upgrade_diff.and_then(|d| d.as_struct());
                let struct_schema_diff = schema_diff.and_then(|d| d.as_struct());

                for member in &s.children {
                    let member_upgrade_diff = struct_upgrade_diff
                        .and_then(|diff| diff.children.iter().find(|m| m.name == member.name))
                        .map(|m| &m.ty);
                    let member_schema_diff = struct_schema_diff
                        .and_then(|diff| diff.children.iter().find(|m| m.name == member.name))
                        .map(|m| &m.ty);

                    let mut new_path = path.to_vec();
                    new_path.push(member.name.clone());

                    self.add_columns_recursive(
                        &new_path,
                        &member.ty,
                        columns,
                        alter_table_queries,
                        indices,
                        table_id,
                        member_schema_diff,
                        member_upgrade_diff,
                        member.key,
                    )?;
                }
            }
            Ty::Tuple(t) => {
                let tuple_upgrade_diff = upgrade_diff.and_then(|d| d.as_tuple());
                let tuple_schema_diff = schema_diff.and_then(|d| d.as_tuple());

                for (idx, member) in t.iter().enumerate() {
                    let member_upgrade_diff = tuple_upgrade_diff.and_then(|diff| diff.get(idx));
                    let member_schema_diff = tuple_schema_diff.and_then(|diff| diff.get(idx));

                    let mut new_path = path.to_vec();
                    new_path.push(idx.to_string());

                    self.add_columns_recursive(
                        &new_path,
                        member,
                        columns,
                        alter_table_queries,
                        indices,
                        table_id,
                        member_schema_diff,
                        member_upgrade_diff,
                        is_key,
                    )?;
                }
            }
            Ty::Array(_) => {
                let column_name = if column_prefix.is_empty() {
                    "value".to_string()
                } else {
                    column_prefix
                };

                if upgrade_diff.is_some() {
                    modify_column(
                        alter_table_queries,
                        &column_name,
                        "TEXT",
                        &format!("[{column_name}]"),
                    );
                } else if schema_diff.is_some() {
                    alter_column(&column_name, "TEXT", indices);
                } else {
                    add_column(&column_name, "TEXT", indices);
                }
            }
            Ty::Enum(e) => {
                let enum_upgrade_diff = upgrade_diff.and_then(|d| d.as_enum());
                let enum_schema_diff = schema_diff.and_then(|d| d.as_enum());

                let column_name = if column_prefix.is_empty() {
                    "option".to_string()
                } else {
                    column_prefix
                };

                let all_options = e
                    .options
                    .iter()
                    .map(|c| format!("'{}'", c.name))
                    .collect::<Vec<_>>()
                    .join(", ");

                let sql_type = format!(
                "TEXT CONSTRAINT [{column_name}_check] CHECK([{column_name}] IN ({all_options}))"
            );

                // If new variants of an enum are added, without the enum itself being added through an upgrade
                // to the model, then we should consider this as an upgrade. The reason is that for this specific
                // case, we only need to modify the column and its constraints. Not add it.
                if enum_upgrade_diff.is_some()
                    || (schema_diff.is_some() && schema_diff.unwrap() != ty)
                {
                    modify_column(
                        alter_table_queries,
                        &column_name,
                        &sql_type,
                        &format!("[{column_name}]"),
                    );
                } else if enum_schema_diff.is_some() {
                    // In the case where the enum is being added to the model, we need to add the column and its constraints
                    alter_column(&column_name, &sql_type, indices);
                } else {
                    // And in the default case where we have no upgrade at all (nor to the model, nor to the enum)
                    // just add the column and its constraints as part of the create query.
                    add_column(&column_name, &sql_type, indices);
                }

                for child in &e.options {
                    let variant_upgrade_diff = enum_upgrade_diff
                        .and_then(|diff| diff.options.iter().find(|v| v.name == child.name))
                        .map(|v| &v.ty);
                    let variant_schema_diff = enum_schema_diff
                        .and_then(|diff| diff.options.iter().find(|v| v.name == child.name))
                        .map(|v| &v.ty);

                    if let Ty::Tuple(tuple) = &child.ty {
                        if tuple.is_empty() {
                            continue;
                        }
                    }

                    let mut new_path = path.to_vec();
                    new_path.push(child.name.clone());

                    self.add_columns_recursive(
                        &new_path,
                        &child.ty,
                        columns,
                        alter_table_queries,
                        indices,
                        table_id,
                        variant_schema_diff,
                        variant_upgrade_diff,
                        is_key,
                    )?;
                }
            }
            Ty::ByteArray(_) => {
                let column_name = if column_prefix.is_empty() {
                    "value".to_string()
                } else {
                    column_prefix
                };

                if upgrade_diff.is_some() {
                    modify_column(
                        alter_table_queries,
                        &column_name,
                        "TEXT",
                        &format!("[{column_name}]"),
                    );
                } else if schema_diff.is_some() {
                    alter_column(&column_name, "TEXT", indices);
                } else {
                    add_column(&column_name, "TEXT", indices);
                }
            }
            Ty::Primitive(p) => {
                let column_name = if column_prefix.is_empty() {
                    "value".to_string()
                } else {
                    column_prefix
                };

                if let Some(upgrade_diff) = upgrade_diff {
                    if let Some(old_primitive) = upgrade_diff.as_primitive() {
                        let sql_value = if old_primitive.to_sql_type() == SqlType::Integer
                            && p.to_sql_type() == SqlType::Text
                        {
                            format!("'0x' || printf('%064x', [{column_name}])")
                        } else {
                            format!("[{column_name}]")
                        };
                        modify_column(
                            alter_table_queries,
                            &column_name,
                            p.to_sql_type().as_ref(),
                            &sql_value,
                        );
                    }
                } else if schema_diff.is_some() {
                    alter_column(&column_name, p.to_sql_type().as_ref(), indices);
                } else {
                    add_column(&column_name, p.to_sql_type().as_ref(), indices);
                }
            }
        }

        Ok(())
    }
}
