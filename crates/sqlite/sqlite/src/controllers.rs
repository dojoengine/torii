use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;

use crate::{error::ControllerSyncError, Sql};


pub struct ControllersSync {
    sql: Sql,
    cursor: RwLock<Option<DateTime<Utc>>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ControllerAccount {
    pub username: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ControllersData {
    pub controllers: Controllers,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Controllers {
    pub edges: Vec<ControllerEdge>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ControllerEdge {
    pub node: Controller,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Controller {
    pub account: ControllerAccount,
    pub address: String,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ControllersResponse {
    pub data: ControllersData,
}

impl ControllersSync {
    pub async fn new(sql: Sql) -> Self {
        let cursor: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT deployed_at FROM controllers ORDER BY deployed_at DESC LIMIT 1",
        )
        .fetch_optional(&sql.pool)
        .await
        .expect("Should be able to read cursor from controllers table");

        Self { sql, cursor: RwLock::new(cursor) }
    }

    pub async fn sync(&self) -> Result<usize, ControllerSyncError> {
        // graphQL query to get the controllers api.cartridge.gg/graphql
        let query = format!(r#"
        query {{
          controllers(where:{{
            createdAtGT:"{}"
          }}, orderBy:{{
            field:CREATED_AT,
            direction:ASC
          }}) {{
            edges {{
              node {{
                address
                createdAt
                account {{
                  username
                }}
              }}
            }}
          }}
        }}"#, self.cursor.read().await.unwrap_or_default().to_rfc3339());

        // send the query to the graphQL endpoint
        let response = reqwest::Client::new()
            .post("https://api.cartridge.gg/query")
            .json(&json!({
                "query": query,
            }))
            .send()
            .await
            .map_err(ControllerSyncError::Reqwest)?;

        let body: ControllersResponse = response.json().await?;

        let controllers = body.data.controllers.edges.iter().map(|c| c.node.clone()).collect::<Vec<_>>();
        let num_controllers = controllers.len();

        for controller in controllers {
            self.sql.add_controller(&controller.account.username, &controller.address, controller.created_at).await?;
            *self.cursor.write().await = Some(controller.created_at);
        }

        Ok(num_controllers)
    }
}
