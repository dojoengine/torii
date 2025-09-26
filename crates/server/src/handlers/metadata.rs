use std::str::FromStr;
use std::sync::Arc;
use std::{fmt::Debug, net::IpAddr};

use crypto_bigint::U256;
use http::{Request, Response, StatusCode};
use hyper::Body;
use starknet::providers::Provider;
use starknet_crypto::Felt;
use torii_processors::erc::fetch_token_metadata;
use torii_storage::proto::TokenId;
use torii_storage::Storage;
use tracing::{debug, error};

use super::Handler;

pub(crate) const LOG_TARGET: &str = "torii::server::handlers::metadata";

#[derive(Debug)]
pub struct MetadataHandler<P: Provider + Sync + Send + Debug, S: Storage> {
    storage: Arc<S>,
    provider: P,
}

impl<P: Provider + Sync + Send + Debug, S: Storage> MetadataHandler<P, S> {
    pub fn new(storage: Arc<S>, provider: P) -> Self {
        Self { storage, provider }
    }
}

#[async_trait::async_trait]
impl<P: Provider + Sync + Send + Debug, S: Storage> Handler for MetadataHandler<P, S> {
    fn should_handle(&self, req: &Request<Body>) -> bool {
        req.uri().path().starts_with("/metadata/reindex/")
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
                error!(target: LOG_TARGET, error = ?e, "Failed to parse contract address");
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"error":"Invalid contract address: {}"}}"#,
                        e
                    )))
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
        let token_id: starknet::core::types::U256 = U256::from_be_hex(token_id_str).into();

        let token_key = format!("{}:{}", parts[0], parts[1]);

        debug!(
            target: LOG_TARGET,
            contract_address = format!("{:#x}", contract_address),
            token_id = format!("{:#x}", token_id),
            "Reindexing metadata for token"
        );

        // Fetch new metadata
        let metadata = match fetch_token_metadata(contract_address, token_id, &self.provider).await
        {
            Ok(metadata) => metadata,
            Err(e) => {
                error!(
                    target: LOG_TARGET,
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

        // Update metadata using storage layer
        let result = self
            .storage
            .update_token_metadata(TokenId::Nft(contract_address, token_id), metadata.clone())
            .await;

        match result {
            Ok(()) => {
                // Execute the transaction
                let execute_result = self.storage.execute().await;
                match execute_result {
                    Ok(()) => {
                        debug!(
                            target: LOG_TARGET,
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
                    }
                    Err(e) => {
                        error!(
                            target: LOG_TARGET,
                            error = ?e,
                            contract_address = format!("{:#x}", contract_address),
                            token_id = %token_id,
                            "Failed to execute storage transaction"
                        );
                        Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header("content-type", "application/json")
                            .body(Body::from(format!(
                                r#"{{"error":"Transaction execution failed: {}"}}"#,
                                e
                            )))
                            .unwrap()
                    }
                }
            }
            Err(e) => {
                error!(
                    target: LOG_TARGET,
                    error = ?e,
                    contract_address = format!("{:#x}", contract_address),
                    token_id = %token_id,
                    "Failed to update metadata"
                );
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"error":"Metadata update failed: {}"}}"#,
                        e
                    )))
                    .unwrap()
            }
        }
    }
}
