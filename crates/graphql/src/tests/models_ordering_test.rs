#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_graphql::dynamic::Schema;
    use serde_json::Value;
    use starknet::core::types::Felt;
    use tempfile::NamedTempFile;
    use tokio::sync::broadcast;
    use torii_messaging::{Messaging, MessagingConfig};
    use torii_sqlite::executor::Executor;
    use torii_sqlite::Sql;
    use torii_storage::proto::{Contract, ContractType};

    use crate::schema::build_schema;
    use crate::tests::{run_graphql_query, spinup_types_test, Connection, WorldModel};

    async fn world_model_query(schema: &Schema, arg: &str) -> Value {
        let query = format!(
            r#"
          {{
             models {} {{
              totalCount
              edges {{
                cursor
                node {{
                    id
                    name
                    classHash
                    transactionHash
                    createdAt
                }}
              }}
              pageInfo {{
                startCursor
                hasPreviousPage
                hasNextPage
                startCursor
                endCursor
              }}
            }}
          }}
        "#,
            arg,
        );

        let result = run_graphql_query(schema, &query).await;
        result
            .get("models")
            .ok_or("models not found")
            .unwrap()
            .clone()
    }

    // End to end test spins up a test sequencer and deploys types-test project, this takes a while
    // to run so combine all related tests into one
    #[tokio::test(flavor = "multi_thread")]
    async fn models_ordering_test() -> Result<()> {
        let tempfile = NamedTempFile::new().unwrap();
        let path = tempfile.path().to_string_lossy();
        let (pool, provider) = spinup_types_test(&path).await?;

        // Set up storage and messaging
        let (shutdown_tx, _) = broadcast::channel(1);
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
                &[Contract {
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

        let schema = build_schema(&pool, messaging, storage).await.unwrap();

        // default params, test entity relationship, test nested types
        let world_model = world_model_query(&schema, "").await;
        let connection: Connection<WorldModel> = serde_json::from_value(world_model).unwrap();

        connection.edges.first().unwrap();
        connection.edges.get(1).unwrap();
        connection.edges.get(2).unwrap();

        // by default is ordered by id
        // assert_eq!(&first_model.node.name, "Subrecord");
        // assert_eq!(&second_model.node.name, "RecordSibling");
        // assert_eq!(&last_model.node.name, "Record");

        // *** ORDER TESTING ***

        // order on name string ASC (number)
        let world_model =
            world_model_query(&schema, "(order: {field: NAME, direction: ASC})").await;
        let connection: Connection<WorldModel> = serde_json::from_value(world_model).unwrap();
        let first_model = connection.edges.first().unwrap();
        let second_model = connection.edges.get(1).unwrap();
        let third_model = connection.edges.get(2).unwrap();
        let last_model = connection.edges.get(3).unwrap();
        assert_eq!(&first_model.node.name, "Record");
        assert_eq!(&second_model.node.name, "RecordLogged");
        assert_eq!(&third_model.node.name, "RecordSibling");
        assert_eq!(&last_model.node.name, "Subrecord");
        Ok(())
    }
}
