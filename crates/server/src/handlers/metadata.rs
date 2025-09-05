use std::net::IpAddr;
use std::str::FromStr;

use http::{Request, Response, StatusCode};
use hyper::Body;
use sqlx::SqlitePool;
use starknet::core::types::U256;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::JsonRpcClient;
use starknet_crypto::Felt;
use torii_processors::erc::fetch_token_metadata;
use torii_sqlite::constants::TOKENS_TABLE;
use tracing::{debug, error};

use super::Handler;

pub struct MetadataHandler {
    pool: std::sync::Arc<SqlitePool>,
    provider_url: String,
}

impl MetadataHandler {
    pub fn new(pool: std::sync::Arc<SqlitePool>, provider_url: String) -> Self {
        Self { pool, provider_url }
    }
}

#[async_trait::async_trait]
impl Handler for MetadataHandler {
    fn should_handle(&self, req: &Request<Body>) -> bool {
        req.uri().path().starts_with("/metadata/reindex/")
            && req.method() == http::Method::POST
    }

    async fn handle(&self, req: Request<Body>, _client_addr: IpAddr) -> Response<Body> {
        let path = req.uri().path();
        
        // Remove "/metadata/reindex/" prefix
        let path = match path.strip_prefix("/metadata/reindex/") {
            Some(p) => p,
            None => {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"error":"Invalid path"}"#))
                    .unwrap();
            }
        };

        // Split the path and validate format - expecting "contract_address/token_id"
        let parts: Vec<&str> = path.split('/').collect();

        if parts.len() != 2 {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"error":"Invalid path format. Expected: /metadata/reindex/{contract_address}/{token_id}"}"#))
                .unwrap();
        }

        // Validate and parse contract_address
        if !parts[0].starts_with("0x") {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"error":"Invalid contract address format"}"#))
                .unwrap();
        }

        let contract_address = match Felt::from_str(parts[0]) {
            Ok(addr) => addr,
            Err(e) => {
                error!(error = ?e, "Failed to parse contract address");
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"error":"Invalid contract address: {}"}}"#, e)))
                    .unwrap();
            }
        };

        // Validate and parse token_id
        if !parts[1].starts_with("0x") {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"error":"Invalid token ID format"}"#))
                .unwrap();
        }

        let token_id_str = parts[1].strip_prefix("0x").unwrap_or(parts[1]);
        let token_id = match U256::from_str_radix(token_id_str, 16) {
            Ok(id) => id,
            Err(e) => {
                error!(error = ?e, "Failed to parse token ID");
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"error":"Invalid token ID: {}"}}"#, e)))
                    .unwrap();
            }
        };

        let token_key = format!("{}:{}", parts[0], parts[1]);

        debug!(
            contract_address = format!("{:#x}", contract_address),
            token_id = %token_id,
            "Reindexing metadata for token"
        );

        // Create provider
        let provider = match self.provider_url.parse() {
            Ok(url) => JsonRpcClient::new(HttpTransport::new(url)),
            Err(e) => {
                error!(error = ?e, "Failed to parse provider URL");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"error":"Invalid provider configuration"}"#))
                    .unwrap();
            }
        };

        // Fetch new metadata
        let metadata = match fetch_token_metadata(contract_address, token_id, &provider).await {
            Ok(metadata) => metadata,
            Err(e) => {
                error!(
                    error = ?e,
                    contract_address = format!("{:#x}", contract_address),
                    token_id = %token_id,
                    "Failed to fetch metadata"
                );
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"error":"Failed to fetch metadata: {}"}}"#,
                        e
                    )))
                    .unwrap();
            }
        };

        // Update metadata in database
        let result = sqlx::query(&format!(
            "UPDATE {} SET metadata = ? WHERE id = ?",
            TOKENS_TABLE
        ))
        .bind(&metadata)
        .bind(&token_key)
        .execute(self.pool.as_ref())
        .await;

        match result {
            Ok(result) => {
                if result.rows_affected() > 0 {
                    debug!(
                        contract_address = format!("{:#x}", contract_address),
                        token_id = %token_id,
                        "Successfully updated metadata"
                    );
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .body(Body::from(format!(
                            r#"{{"success":true,"message":"Metadata updated successfully","token_id":"{}","metadata":{}}}"#,
                            token_key, metadata
                        )))
                        .unwrap()
                } else {
                    error!(
                        contract_address = format!("{:#x}", contract_address),
                        token_id = %token_id,
                        "Token not found in database"
                    );
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"error":"Token not found"}"#))
                        .unwrap()
                }
            }
            Err(e) => {
                error!(
                    error = ?e,
                    contract_address = format!("{:#x}", contract_address),
                    token_id = %token_id,
                    "Failed to update metadata in database"
                );
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"error":"Database update failed: {}"}}"#,
                        e
                    )))
                    .unwrap()
            }
        }
    }
}