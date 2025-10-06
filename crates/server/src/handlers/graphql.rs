use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper::{Body, Request, Response};
use hyper_reverse_proxy::ReverseProxy;
use tokio::time::timeout;
use tracing::error;

use super::Handler;

pub(crate) const LOG_TARGET: &str = "torii::server::handlers::graphql";

// Timeout for regular GraphQL requests (not WebSocket which are long-lived)
const GRAPHQL_PROXY_TIMEOUT: Duration = Duration::from_secs(60);

pub struct GraphQLHandler {
    pub(crate) graphql_addr: Option<SocketAddr>,
    grpc_proxy_client: Arc<ReverseProxy<HttpConnector<GaiResolver>>>,
    websocket_proxy_client: Arc<ReverseProxy<HttpConnector<GaiResolver>>>,
}

impl GraphQLHandler {
    pub fn new(
        graphql_addr: Option<SocketAddr>,
        grpc_proxy_client: Arc<ReverseProxy<HttpConnector<GaiResolver>>>,
        websocket_proxy_client: Arc<ReverseProxy<HttpConnector<GaiResolver>>>,
    ) -> Self {
        Self { graphql_addr, grpc_proxy_client, websocket_proxy_client }
    }
}

impl std::fmt::Debug for GraphQLHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphQLHandler")
            .field("graphql_addr", &self.graphql_addr)
            .finish()
    }
}

#[async_trait::async_trait]
impl Handler for GraphQLHandler {
    fn should_handle(&self, req: &Request<Body>) -> bool {
        req.uri().path().starts_with("/graphql")
    }

    async fn handle(&self, req: Request<Body>, client_addr: IpAddr) -> Response<Body> {
        if let Some(addr) = self.graphql_addr {
            let graphql_addr = format!("http://{}", addr);

            // WebSocket connections are long-lived and shouldn't have timeouts
            let is_websocket = crate::proxy::is_websocket_upgrade(&req);

            if is_websocket {
                // No timeout for WebSocket upgrades
                match self.websocket_proxy_client
                    .call(client_addr, &graphql_addr, req)
                    .await
                {
                    Ok(response) => response,
                    Err(_error) => {
                        error!(target: LOG_TARGET, error = ?_error, "WebSocket proxy error");
                        Response::builder()
                            .status(StatusCode::BAD_GATEWAY)
                            .body(Body::empty())
                            .unwrap()
                    }
                }
            } else {
                // Regular GraphQL requests get a timeout
                match timeout(
                    GRAPHQL_PROXY_TIMEOUT,
                    self.grpc_proxy_client.call(client_addr, &graphql_addr, req)
                ).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(_error)) => {
                        error!(target: LOG_TARGET, error = ?_error, "GraphQL proxy error");
                        Response::builder()
                            .status(StatusCode::BAD_GATEWAY)
                            .body(Body::empty())
                            .unwrap()
                    }
                    Err(_) => {
                        error!(target: LOG_TARGET, "GraphQL request timeout after {:?}", GRAPHQL_PROXY_TIMEOUT);
                        Response::builder()
                            .status(StatusCode::GATEWAY_TIMEOUT)
                            .body(Body::empty())
                            .unwrap()
                    }
                }
            }
        } else {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap()
        }
    }
}
