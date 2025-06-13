use std::{str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use sqlx::SqlitePool;
use starknet_crypto::Felt;
use tokio::sync::RwLock;
use tracing::warn;

use crate::{error::ControllerSyncError, Sql};

const CARTRIDGE_API_QUERY_URL: &str = "https://api.cartridge.gg/query";

#[derive(Debug)]
pub struct ControllersSync {
    sql: Sql,
    cursor: RwLock<Option<DateTime<Utc>>>,
    api_url: String,
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

        Self {
            sql,
            cursor: RwLock::new(cursor),
            api_url: CARTRIDGE_API_QUERY_URL.to_string(),
        }
    }

    pub fn with_api_url(mut self, url: String) -> Self {
        self.api_url = url;
        self
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.sql.pool
    }

    async fn fetch_controllers(&self) -> Result<Vec<Controller>, ControllerSyncError> {
        let query = format!(
            r#"
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
        }}"#,
            self.cursor.read().await.unwrap_or_default().to_rfc3339()
        );

        let mut attempts = 0;
        const MAX_RETRIES: u32 = 3;
        const INITIAL_BACKOFF: Duration = Duration::from_secs(2);
        let response = loop {
            attempts += 1;
            let result = reqwest::Client::new()
                .post(&self.api_url)
                .json(&json!({
                    "query": query,
                }))
                .send()
                .await;

            match result {
                Ok(resp) if resp.status().is_success() => break resp,
                Ok(resp) if attempts < MAX_RETRIES => {
                    let error_text = resp.text().await?;
                    warn!(error_text, "Error fetching controllers, retrying.");
                    let backoff = INITIAL_BACKOFF * (1 << (attempts - 1));
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Ok(resp) => {
                    let error_text = resp.text().await?;
                    return Err(ControllerSyncError::ApiError(error_text));
                }
                Err(_) if attempts < MAX_RETRIES => {
                    let backoff = INITIAL_BACKOFF * (1 << (attempts - 1));
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => return Err(ControllerSyncError::Reqwest(e)),
            }
        };

        let body: ControllersResponse = response.json().await?;

        Ok(body
            .data
            .controllers
            .edges
            .into_iter()
            .map(|e| e.node)
            .collect())
    }

    pub async fn sync(&self) -> Result<usize, ControllerSyncError> {
        let controllers = self.fetch_controllers().await?;
        let num_controllers = controllers.len();

        for controller in controllers {
            let felt_addr = Felt::from_str(&controller.address).unwrap();
            let padded_address = format!("{:#066x}", felt_addr);

            let e = self
                .sql
                .add_controller(
                    &controller.account.username,
                    &padded_address,
                    controller.created_at,
                )
                .await;

            e?;

            *self.cursor.write().await = Some(controller.created_at);
        }

        Ok(num_controllers)
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use super::*;
    use mockito::Server;
    use serde_json::json;
    use sqlx::Row;
    use starknet::providers::{jsonrpc::HttpTransport, JsonRpcClient};
    use starknet_crypto::Felt;
    use tokio::sync::broadcast;
    use url::Url;

    const CARTRIDGE_NODE_MAINNET: &str = "https://api.cartridge.gg/x/starknet/mainnet";

    async fn new_sql_test(shutdown_tx: broadcast::Sender<()>) -> Sql {
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
            Url::parse(CARTRIDGE_NODE_MAINNET).unwrap(),
        )));
        Sql::new_tmp_file(
            Felt::from_str("0x123").unwrap(),
            provider.clone(),
            shutdown_tx,
        )
        .await
    }

    #[tokio::test]
    async fn test_fetch_controllers_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/query")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "data": {
                        "controllers": {
                            "edges": [
                                {
                                    "node": {
                                        "address": "0x123",
                                        "createdAt": "2024-03-20T12:00:00Z",
                                        "account": {
                                            "username": "test_user"
                                        }
                                    }
                                }
                            ]
                        }
                    }
                })
                .to_string(),
            )
            .create_async()
            .await;

        let (shutdown_tx, _) = broadcast::channel(1);

        let sql = new_sql_test(shutdown_tx).await;
        let sync = ControllersSync::new(sql)
            .await
            .with_api_url(server.url() + "/query");

        let result = sync.fetch_controllers().await;
        assert!(result.is_ok());
        let controllers = result.unwrap();
        assert_eq!(controllers.len(), 1);
        assert_eq!(controllers[0].address, "0x123");
        assert_eq!(controllers[0].account.username, "test_user");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_controllers_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/query")
            .with_status(500)
            .with_header("content-type", "application/json")
            .with_body("Internal Server Error")
            .expect(3)
            .create_async()
            .await;

        let (shutdown_tx, _) = broadcast::channel(1);
        let sql = new_sql_test(shutdown_tx).await;

        let sync = ControllersSync::new(sql)
            .await
            .with_api_url(server.url() + "/query");

        let result = sync.fetch_controllers().await;
        assert!(result.is_err());

        match result {
            Err(ControllerSyncError::ApiError(msg)) => {
                assert_eq!(msg, "Internal Server Error");
            }
            _ => panic!("Expected ApiError"),
        }

        mock.assert_async().await;
    }

    #[ignore = "This test requires the executor to work from test utils, to be fixed."]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sync() {
        let (shutdown_tx, _) = broadcast::channel(1);
        let sql = new_sql_test(shutdown_tx).await;

        let ctrls = ControllersSync::new(sql).await;

        let num_controllers = ctrls.sync().await.unwrap();
        assert!(num_controllers > 0);

        let stored_controllers = sqlx::query("SELECT address FROM controllers")
            .fetch_all(ctrls.pool())
            .await
            .unwrap();

        for row in stored_controllers {
            let address: String = row.get("address");
            assert_eq!(address.len(), 66);
            assert!(address.starts_with("0x"));
            assert!(address[2..].chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[tokio::test]
    async fn test_fetch_controllers_api_empty_future() {
        let (shutdown_tx, _) = broadcast::channel(1);
        let sql = new_sql_test(shutdown_tx).await;

        // Insert a controller in the future which should always yield an empty result.
        let timestamp = chrono::Utc::now() + chrono::Duration::days(10);
        sqlx::query(
            "INSERT INTO controllers (id, username, address, deployed_at) VALUES (?, ?, ?, ?)",
        )
        .bind("test_user")
        .bind("test_user")
        .bind("0x123")
        .bind(timestamp.to_rfc3339())
        .execute(&sql.pool)
        .await
        .unwrap();

        let sync = ControllersSync::new(sql).await;

        let result = sync.fetch_controllers().await;
        assert!(result.is_ok());
        let controllers = result.unwrap();
        assert!(controllers.is_empty());
    }
}
