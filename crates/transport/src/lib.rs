use async_trait::async_trait;
use log::trace;
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Serialize};
use tokio::time;

use crate::{
    jsonrpc::{transports::JsonRpcTransport, JsonRpcMethod, JsonRpcResponse},
    ProviderRequestData,
};

/// A [`JsonRpcTransport`] implementation that uses HTTP connections.
#[derive(Debug, Clone)]
pub struct HttpTransport {
    client: Client,
    url: Url,
    headers: Vec<(String, String)>,
    max_retries: u32,
    retry_delay_ms: u64,
}

#[derive(Debug, Serialize)]
struct JsonRpcRequest<T> {
    id: u64,
    jsonrpc: &'static str,
    method: JsonRpcMethod,
    params: T,
}

impl HttpTransport {
    /// Constructs [`HttpTransport`] from a JSON-RPC server URL, using default HTTP client settings.
    /// Defaults to 3 retries with a 500ms base delay.
    ///
    /// To use custom HTTP settings (e.g. proxy, timeout), use
    /// [`new_with_client`](fn.new_with_client) instead.
    pub fn new(url: impl Into<Url>) -> Self {
        Self::new_with_client(url, Client::new())
    }

    /// Constructs [`HttpTransport`] from a JSON-RPC server URL and a custom `reqwest` client.
    /// Defaults to 3 retries with a 500ms base delay.
    pub fn new_with_client(url: impl Into<Url>, client: Client) -> Self {
        Self {
            client,
            url: url.into(),
            headers: vec![],
            max_retries: 3, // Default max retries
            retry_delay_ms: 500, // Default base delay in ms
        }
    }

    /// Sets the maximum number of retries for requests.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the base delay in milliseconds for exponential backoff.
    pub fn with_retry_delay_ms(mut self, retry_delay_ms: u64) -> Self {
        self.retry_delay_ms = retry_delay_ms;
        self
    }

    /// Consumes the current [`HttpTransport`] instance and returns a new one with the header
    /// appended. Same as calling [`add_header`](fn.add_header).
    pub fn with_header(self, name: String, value: String) -> Self {
        let mut headers = self.headers;
        headers.push((name, value));

        Self {
            client: self.client,
            url: self.url,
            headers,
            max_retries: self.max_retries,
            retry_delay_ms: self.retry_delay_ms,
        }
    }

    /// Adds a custom HTTP header to be sent for requests.
    pub fn add_header(&mut self, name: String, value: String) {
        self.headers.push((name, value))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl JsonRpcTransport for HttpTransport {
    type Error = HttpTransportError;

    async fn send_request<P, R>(
        &self,
        method: JsonRpcMethod,
        params: P,
    ) -> Result<JsonRpcResponse<R>, Self::Error>
    where
        P: Serialize + Send,
        R: DeserializeOwned,
    {
        let mut attempts = 0;
        let mut last_error: Option<HttpTransportError> = None;

        while attempts <= self.max_retries {
            let request_body_data = JsonRpcRequest {
                id: 1,
                jsonrpc: "2.0",
                method,
                params: &params,
            };

            let request_body_json = match serde_json::to_string(&request_body_data) {
                Ok(json) => json,
                Err(e) => return Err(HttpTransportError::Json(e)),
            };
            
            trace!("Sending request via JSON-RPC (attempt {}): {}", attempts + 1, request_body_json);

            let mut request_builder = self
                .client
                .post(self.url.clone())
                .body(request_body_json.clone())
                .header("Content-Type", "application/json");

            for (name, value) in &self.headers {
                request_builder = request_builder.header(name, value);
            }

            match request_builder.send().await {
                Ok(response) => {
                    let response_text = match response.text().await {
                        Ok(text) => text,
                        Err(e) => {
                            last_error = Some(HttpTransportError::Reqwest(e));
                            attempts += 1;
                            if attempts <= self.max_retries && self.retry_delay_ms > 0 {
                                let delay = self.retry_delay_ms * (2u64.pow(attempts -1));
                                time::sleep(time::Duration::from_millis(delay)).await;
                            }
                            continue;
                        }
                    };
                    trace!("Response from JSON-RPC: {}", response_text);
                    match serde_json::from_str(&response_text) {
                        Ok(parsed) => return Ok(parsed),
                        Err(e) => return Err(HttpTransportError::Json(e)),
                    }
                }
                Err(e) => {
                    last_error = Some(HttpTransportError::Reqwest(e));
                    attempts += 1;
                    if attempts <= self.max_retries && self.retry_delay_ms > 0 {
                        let delay = self.retry_delay_ms * (2u64.pow(attempts - 1));
                        time::sleep(time::Duration::from_millis(delay)).await;
                    }
                }
            }
        }
        Err(HttpTransportError::RetriesExhausted { 
            max_retries: self.max_retries, 
            last_error: Box::new(last_error.unwrap_or_else(|| HttpTransportError::Reqwest(reqwest::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "Unknown error during request processing")))))
        })
    }

    async fn send_requests<R>(
        &self,
        requests_data: R,
    ) -> Result<Vec<JsonRpcResponse<serde_json::Value>>, Self::Error>
    where
        R: AsRef<[ProviderRequestData]> + Send + Sync,
    {
        let mut attempts = 0;
        let mut last_error: Option<HttpTransportError> = None;

        let original_request_bodies: Vec<_> = requests_data
            .as_ref()
            .iter()
            .enumerate()
            .map(|(ind, request_item)| JsonRpcRequest {
                id: ind as u64,
                jsonrpc: "2.0",
                method: request_item.jsonrpc_method(),
                params: request_item,
            })
            .collect();
        
        let request_count = original_request_bodies.len();

        while attempts <= self.max_retries {
            let request_body_json = match serde_json::to_string(&original_request_bodies) {
                Ok(json) => json,
                Err(e) => return Err(HttpTransportError::Json(e)),
            };
            trace!("Sending batch request via JSON-RPC (attempt {}): {}", attempts + 1, request_body_json);

            let mut request_builder = self
                .client
                .post(self.url.clone())
                .body(request_body_json)
                .header("Content-Type", "application/json");

            for (name, value) in &self.headers {
                request_builder = request_builder.header(name, value);
            }

            match request_builder.send().await {
                Ok(response) => {
                    let response_text = match response.text().await {
                        Ok(text) => text,
                        Err(e) => {
                            last_error = Some(HttpTransportError::Reqwest(e));
                            attempts += 1;
                            if attempts <= self.max_retries && self.retry_delay_ms > 0 {
                                let delay = self.retry_delay_ms * (2u64.pow(attempts -1));
                                time::sleep(time::Duration::from_millis(delay)).await;
                            }
                            continue;
                        }
                    };
                    trace!("Response from JSON-RPC: {}", response_text);

                    let parsed_response_batch: Vec<JsonRpcResponse<serde_json::Value>> = 
                        match serde_json::from_str(&response_text) {
                            Ok(parsed) => parsed,
                            Err(e) => return Err(HttpTransportError::Json(e)),
                    };

                    let mut responses_ordered: Vec<Option<JsonRpcResponse<serde_json::Value>>> = vec![];
                    responses_ordered.resize(request_count, None);

                    for response_item in parsed_response_batch {
                        let id = match &response_item {
                            JsonRpcResponse::Success { id, .. } | JsonRpcResponse::Error { id, .. } => {
                                *id as usize
                            }
                        };

                        if id >= request_count {
                            return Err(HttpTransportError::UnexpectedResponseId(id as u64));
                        }
                        responses_ordered[id] = Some(response_item);
                    }

                    if responses_ordered.iter().any(Option::is_none) {
                        last_error = Some(HttpTransportError::UnexpectedResponseId(request_count as u64));
                        attempts += 1;
                        if attempts <= self.max_retries && self.retry_delay_ms > 0 {
                             let delay = self.retry_delay_ms * (2u64.pow(attempts -1));
                             time::sleep(time::Duration::from_millis(delay)).await;
                        }
                        continue;
                    }

                    return Ok(responses_ordered.into_iter().flatten().collect::<Vec<_>>());
                }
                Err(e) => {
                    last_error = Some(HttpTransportError::Reqwest(e));
                    attempts += 1;
                    if attempts <= self.max_retries && self.retry_delay_ms > 0 {
                        let delay = self.retry_delay_ms * (2u64.pow(attempts-1));
                        time::sleep(time::Duration::from_millis(delay)).await;
                    }
                }
            }
        }
        Err(HttpTransportError::RetriesExhausted { 
            max_retries: self.max_retries, 
            last_error: Box::new(last_error.unwrap_or_else(|| HttpTransportError::Reqwest(reqwest::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "Unknown error during batch request processing")))))
        })
    }
}
