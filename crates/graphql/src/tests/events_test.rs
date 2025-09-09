#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_graphql::dynamic::Schema;
    use serde_json::Value;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use starknet::core::types::Felt;
    use starknet::providers::jsonrpc::HttpTransport;
    use starknet::providers::JsonRpcClient;
    use tokio::sync::broadcast;
    use torii_messaging::{Messaging, MessagingConfig};
    use torii_sqlite::executor::Executor;
    use torii_sqlite::Sql;
    use torii_storage::proto::{ContractDefinition, ContractType};
    use url::Url;

    use crate::schema::build_schema;
    use crate::tests::{run_graphql_query, Connection, Event};

    async fn events_query(schema: &Schema, args: &str) -> Value {
        let query = format!(
            r#"
          {{
            events {} {{
              totalCount
              edges {{
                cursor
                node {{
                  id
                  keys
                  data
                  transactionHash
                  executedAt
                }}
              }}
              pageInfo {{
                hasPreviousPage
                hasNextPage
                startCursor
                endCursor
              }}
            }}
          }}
        "#,
            args
        );

        let result = run_graphql_query(schema, &query).await;
        result
            .get("events")
            .ok_or("events not found")
            .unwrap()
            .clone()
    }

    #[sqlx::test(migrations = "../migrations", fixtures("./fixtures/events.sql"))]
    async fn test_events_query(
        options: SqlitePoolOptions,
        mut connect_options: SqliteConnectOptions,
    ) -> Result<()> {
        // enable regex
        connect_options = connect_options.with_regexp();

        let pool = options.connect_with(connect_options).await?;

        // Set up storage and messaging
        let (shutdown_tx, _) = broadcast::channel(1);
        let url: Url = "https://www.example.com".parse().unwrap();
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), provider.clone())
                .await
                .unwrap();
        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let storage = Arc::new(
            Sql::new(
                pool.clone(),
                sender,
                &[ContractDefinition {
                    address: Felt::ZERO,
                    r#type: ContractType::WORLD,
                }],
            )
            .await
            .unwrap(),
        );

        let messaging = Arc::new(Messaging::new(
            MessagingConfig::default(),
            storage.clone(),
            provider.clone(),
        ));

        let schema = build_schema(&pool, messaging, storage).await?;

        let result = events_query(&schema, "(keys: [\"0x1\"])").await;
        let connection: Connection<Event> = serde_json::from_value(result.clone())?;
        let event = connection.edges.first().unwrap();
        assert_eq!(connection.total_count, 1);
        assert_eq!(event.node.id, "0x1");
        assert_eq!(event.node.executed_at, "2024-03-19T16:32:10+00:00");

        let result = events_query(&schema, "(keys: [\"0x2\", \"*\", \"0x1\"])").await;
        let connection: Connection<Event> = serde_json::from_value(result.clone())?;
        let event = connection.edges.first().unwrap();
        assert_eq!(connection.total_count, 1);
        assert_eq!(event.node.id, "0x2");
        assert_eq!(event.node.executed_at, "2024-03-19T16:32:10+00:00");

        let result = events_query(&schema, "(keys: [\"*\", \"0x1\"])").await;
        let connection: Connection<Event> = serde_json::from_value(result.clone())?;
        let event = connection.edges.first().unwrap();
        assert_eq!(connection.total_count, 1);
        assert_eq!(event.node.id, "0x3");
        assert_eq!(event.node.executed_at, "2024-03-19T16:32:10+00:00");

        Ok(())
    }
}
