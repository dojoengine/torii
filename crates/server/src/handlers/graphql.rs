use std::net::{IpAddr, SocketAddr};

use http::StatusCode;
use hyper::{Body, Request, Response};
use tracing::error;

use super::Handler;

pub(crate) const LOG_TARGET: &str = "torii::server::handlers::graphql";

#[derive(Debug)]
pub struct GraphQLHandler {
    pub(crate) graphql_addr: Option<SocketAddr>,
}

impl GraphQLHandler {
    pub fn new(graphql_addr: Option<SocketAddr>) -> Self {
        Self { graphql_addr }
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

            // Use WebSocket-compatible client for WebSocket upgrade requests
            let result = if crate::proxy::is_websocket_upgrade(&req) {
                crate::proxy::WEBSOCKET_PROXY_CLIENT
                    .call(client_addr, &graphql_addr, req)
                    .await
            } else {
                crate::proxy::PROXY_CLIENT
                    .call(client_addr, &graphql_addr, req)
                    .await
            };

            match result {
                Ok(response) => response,
                Err(_error) => {
                    error!(target: LOG_TARGET, "GraphQL proxy error: {:?}", _error);
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())
                        .unwrap()
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
