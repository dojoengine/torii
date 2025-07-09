use std::{str::FromStr, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use starknet_crypto::Felt;
use tokio::sync::RwLock;
use torii_storage::{
    proto::{ControllerQuery, OrderBy, OrderDirection, Pagination, PaginationDirection},
    Storage,
};
use tracing::warn;

use crate::error::Error;

const CARTRIDGE_API_QUERY_URL: &str = "https://api.cartridge.gg/query";

#[derive(Debug)]
pub struct ControllersSync {
    storage: Arc<dyn Storage>,
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
    pub async fn new(storage: Arc<dyn Storage>) -> Result<Self, Error> {
        // Our controllers are sorted by deployed_at in descending order, so we can use the first one as the cursor.
        let cursor: Option<DateTime<Utc>> = storage
            .controllers(
                &ControllerQuery {
                    contract_addresses: vec![],
                    usernames: vec![],
                    pagination: Pagination {
                        cursor: None,
                        limit: Some(1),
                        direction: PaginationDirection::Forward,
                        order_by: vec![OrderBy {
                            field: "deployed_at".to_string(),
                            direction: OrderDirection::Desc,
                        }],
                    },
                },
            )
            .await?
            .items
            .first()
            .map(|c| c.deployed_at);

        Ok(Self {
            storage,
            cursor: RwLock::new(cursor),
            api_url: CARTRIDGE_API_QUERY_URL.to_string(),
        })
    }

    pub fn with_api_url(mut self, url: String) -> Self {
        self.api_url = url;
        self
    }

    async fn fetch_controllers(&self) -> Result<Vec<Controller>, Error> {
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
                    return Err(Error::ApiError(error_text));
                }
                Err(_) if attempts < MAX_RETRIES => {
                    let backoff = INITIAL_BACKOFF * (1 << (attempts - 1));
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => return Err(Error::Reqwest(e)),
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

    /// Syncs the controllers from the Cartridge API to the database.
    ///
    /// Returns the number of controllers synced.
    pub async fn sync(&self) -> Result<usize, Error> {
        let controllers = self.fetch_controllers().await?;
        let num_controllers = controllers.len();

        for controller in controllers {
            let felt_addr = Felt::from_str(&controller.address).unwrap();
            let padded_address = format!("{:#066x}", felt_addr);

            let e = self
                .storage
                .add_controller(
                    &controller.account.username,
                    &padded_address,
                    controller.created_at,
                )
                .await;

            e.map_err(Error::Storage)?;

            *self.cursor.write().await = Some(controller.created_at);
        }

        Ok(num_controllers)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use mockito::Server;
    use serde_json::json;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use starknet::providers::{jsonrpc::HttpTransport, JsonRpcClient};
    use tempfile::NamedTempFile;
    use tokio::sync::broadcast;
    use torii_sqlite::{executor::Executor, Sql};
    use url::Url;

    const CARTRIDGE_NODE_MAINNET: &str = "https://api.cartridge.gg/x/starknet/mainnet";

    async fn bootstrap_sql(
        path: &str,
        shutdown_tx: broadcast::Sender<()>,
        provider: Arc<JsonRpcClient<HttpTransport>>,
    ) -> Sql {
        let options = SqliteConnectOptions::from_str(path)
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .unwrap();
        sqlx::migrate!("../migrations").run(&pool).await.unwrap();

        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), Arc::clone(&provider))
                .await
                .unwrap();

        tokio::spawn(async move { executor.run().await.unwrap() });

        Sql::new(pool.clone(), sender, &[]).await.unwrap()
    }
    #[tokio::test]
    async fn test_fetch_controllers_success() {
        let mut server = Server::new_async().await;
        server
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
        let tempfile = NamedTempFile::new().unwrap();
        let path = tempfile.path().to_string_lossy();
        let sql = bootstrap_sql(
            &path,
            shutdown_tx.clone(),
            Arc::new(JsonRpcClient::new(HttpTransport::new(
                Url::parse(CARTRIDGE_NODE_MAINNET).unwrap(),
            ))),
        )
        .await;
        let sync = ControllersSync::new(Arc::new(sql))
            .await
            .unwrap()
            .with_api_url(server.url() + "/query");

        let result = sync.fetch_controllers().await;
        assert!(result.is_ok());
        let controllers = result.unwrap();
        assert_eq!(controllers.len(), 1);
        assert_eq!(controllers[0].address, "0x123");
        assert_eq!(controllers[0].account.username, "test_user");
    }

    #[tokio::test]
    async fn test_fetch_controllers_error() {
        let mut server = Server::new_async().await;
        server
            .mock("POST", "/query")
            .with_status(500)
            .with_header("content-type", "application/json")
            .with_body("Internal Server Error")
            .expect(3)
            .create_async()
            .await;

        let (shutdown_tx, _) = broadcast::channel(1);
        let tempfile = NamedTempFile::new().unwrap();
        let path = tempfile.path().to_string_lossy();
        let sql = bootstrap_sql(
            &path,
            shutdown_tx.clone(),
            Arc::new(JsonRpcClient::new(HttpTransport::new(
                Url::parse(CARTRIDGE_NODE_MAINNET).unwrap(),
            ))),
        )
        .await;

        let sync = ControllersSync::new(Arc::new(sql))
            .await
            .unwrap()
            .with_api_url(server.url() + "/query");

        let result = sync.fetch_controllers().await;
        assert!(result.is_err());

        match result {
            Err(Error::ApiError(msg)) => {
                assert_eq!(msg, "Internal Server Error");
            }
            _ => panic!("Expected ApiError"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sync() {
        let (shutdown_tx, _) = broadcast::channel(1);
        let tempfile = NamedTempFile::new().unwrap();
        let path = tempfile.path().to_string_lossy();
        let sql = bootstrap_sql(
            &path,
            shutdown_tx.clone(),
            Arc::new(JsonRpcClient::new(HttpTransport::new(
                Url::parse(CARTRIDGE_NODE_MAINNET).unwrap(),
            ))),
        )
        .await;

        let ctrls = ControllersSync::new(Arc::new(sql)).await.unwrap();

        let num_controllers = ctrls.sync().await.unwrap();
        assert!(num_controllers > 0);

        ctrls.storage.execute().await.unwrap();

        let stored_controllers = ctrls
            .storage
            .controllers(&ControllerQuery {
                contract_addresses: vec![],
                usernames: vec![],
                pagination: Pagination::default(),
            })
            .await
            .unwrap()
            .items;

        assert_eq!(stored_controllers.len(), num_controllers);
    }

    #[tokio::test]
    async fn test_fetch_controllers_api_empty_future() {
        let (shutdown_tx, _) = broadcast::channel(1);
        let tempfile = NamedTempFile::new().unwrap();
        let path = tempfile.path().to_string_lossy();

        let sql = bootstrap_sql(
            &path,
            shutdown_tx.clone(),
            Arc::new(JsonRpcClient::new(HttpTransport::new(
                Url::parse(CARTRIDGE_NODE_MAINNET).unwrap(),
            ))),
        )
        .await;

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

        let sync = ControllersSync::new(Arc::new(sql)).await.unwrap();

        let result = sync.fetch_controllers().await;
        assert!(result.is_ok());
        let controllers = result.unwrap();
        assert!(controllers.is_empty());
    }

    #[tokio::test]
    async fn test_sync_incremental() {
        let mut server = Server::new_async().await;

        // First sync - mock initial controller
        server
            .mock("POST", "/query")
            .match_body(mockito::Matcher::Regex(
                ".*createdAtGT.*1970-01-01T00:00:00\\+00:00.*".to_string(),
            ))
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
                                            "username": "user1"
                                        }
                                    }
                                }
                            ]
                        }
                    }
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        // Second sync - mock new controller (should use cursor from first sync)
        server
            .mock("POST", "/query")
            .match_body(mockito::Matcher::Regex(
                ".*createdAtGT.*2024-03-20T12:00:00\\+00:00.*".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "data": {
                        "controllers": {
                            "edges": [
                                {
                                    "node": {
                                        "address": "0x456",
                                        "createdAt": "2024-03-20T13:00:00Z",
                                        "account": {
                                            "username": "user2"
                                        }
                                    }
                                }
                            ]
                        }
                    }
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;

        let (shutdown_tx, _) = broadcast::channel(1);
        let tempfile = NamedTempFile::new().unwrap();
        let path = tempfile.path().to_string_lossy();
        let sql = bootstrap_sql(
            &path,
            shutdown_tx.clone(),
            Arc::new(JsonRpcClient::new(HttpTransport::new(
                Url::parse(CARTRIDGE_NODE_MAINNET).unwrap(),
            ))),
        )
        .await;

        let sync = ControllersSync::new(Arc::new(sql))
            .await
            .unwrap()
            .with_api_url(server.url() + "/query");

        // First sync - should fetch 1 controller
        let num_controllers_1 = sync.sync().await.unwrap();
        assert_eq!(num_controllers_1, 1);

        // Second sync - should only fetch the new controller (cursor filters out the first one)
        let num_controllers_2 = sync.sync().await.unwrap();
        assert_eq!(num_controllers_2, 1);

        // Execute to persist to database
        sync.storage.execute().await.unwrap();

        // Verify total controllers in database (should be 2 total)
        let stored_controllers = sync
            .storage
            .controllers(&ControllerQuery {
                contract_addresses: vec![],
                usernames: vec![],
                pagination: Pagination::default(),
            })
            .await
            .unwrap()
            .items;

        assert_eq!(stored_controllers.len(), 2);

        // Verify the controllers are the ones we expect
        let usernames: Vec<&str> = stored_controllers
            .iter()
            .map(|c| c.username.as_str())
            .collect();
        assert!(usernames.contains(&"user1"));
        assert!(usernames.contains(&"user2"));
    }
}
