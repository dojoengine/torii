use std::sync::LazyLock;
use std::time::Duration;

use futures_util::TryStreamExt;
use ipfs_api_backend_hyper::{IpfsApi, IpfsClient, TryFromUri};
use reqwest::Client;
use tokio_util::bytes::Bytes;
use tracing::debug;

use crate::{
    constants::{IPFS_CLIENT_PASSWORD, IPFS_CLIENT_URL, IPFS_CLIENT_USERNAME},
    error::HttpError,
};

// Global clients
static HTTP_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
        .expect("Failed to create HTTP client")
});

static IPFS_CLIENT: LazyLock<IpfsClient> = LazyLock::new(|| {
    IpfsClient::from_str(IPFS_CLIENT_URL)
        .expect("Failed to create IPFS client")
        .with_credentials(IPFS_CLIENT_USERNAME, IPFS_CLIENT_PASSWORD)
});

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const REQ_MAX_RETRIES: u32 = 5;

/// Fetch content from HTTP URL with retries
pub async fn fetch_content_from_http(url: &str) -> Result<Bytes, HttpError> {
    let mut retries = 0;
    let mut backoff = INITIAL_BACKOFF;

    loop {
        match HTTP_CLIENT.get(url).send().await {
            Ok(response) => {
                if !response.status().is_success() {
                    return Err(HttpError::StatusCode(
                        response.status(),
                        response.text().await.unwrap_or_default(),
                    ));
                }
                return response.bytes().await.map_err(HttpError::Reqwest);
            }
            Err(e) => {
                if retries >= REQ_MAX_RETRIES {
                    return Err(HttpError::Reqwest(e));
                }
                debug!(error = ?e, retry = retries, "Request failed, retrying after backoff");
                tokio::time::sleep(backoff).await;
                retries += 1;
                backoff *= 2;
            }
        }
    }
}

/// Fetch content from IPFS with retries
pub async fn fetch_content_from_ipfs(cid: &str) -> Result<Bytes, ipfs_api_backend_hyper::Error> {
    let mut retries = 0;
    let mut backoff = INITIAL_BACKOFF;

    loop {
        match IPFS_CLIENT
            .cat(cid)
            .map_ok(|chunk| chunk.to_vec())
            .try_concat()
            .await
        {
            Ok(stream) => return Ok(Bytes::from(stream)),
            Err(e) => {
                if retries >= REQ_MAX_RETRIES {
                    return Err(e);
                }
                debug!(error = ?e, retry = retries, "Request failed, retrying after backoff");
                tokio::time::sleep(backoff).await;
                retries += 1;
                backoff *= 2;
            }
        }
    }
}
