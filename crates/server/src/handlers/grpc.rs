use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use http::header::CONTENT_TYPE;
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper::{Body, Request, Response, StatusCode};
use hyper_reverse_proxy::ReverseProxy;
use tokio::time::timeout;
use tracing::error;

use super::Handler;

pub(crate) const LOG_TARGET: &str = "torii::server::handlers::grpc";

// Default timeout for gRPC requests (60 seconds, can be overridden by grpc-timeout header)
const GRPC_PROXY_TIMEOUT: Duration = Duration::from_secs(60);

pub struct GrpcHandler {
    grpc_addr: Option<SocketAddr>,
    proxy_client: Arc<ReverseProxy<HttpConnector<GaiResolver>>>,
}

impl GrpcHandler {
    pub fn new(
        grpc_addr: Option<SocketAddr>,
        proxy_client: Arc<ReverseProxy<HttpConnector<GaiResolver>>>,
    ) -> Self {
        Self {
            grpc_addr,
            proxy_client,
        }
    }
}

impl std::fmt::Debug for GrpcHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcHandler")
            .field("grpc_addr", &self.grpc_addr)
            .finish()
    }
}

#[async_trait::async_trait]
impl Handler for GrpcHandler {
    fn should_handle(&self, req: &Request<Body>) -> bool {
        req.headers()
            .get(CONTENT_TYPE)
            .and_then(|ct| ct.to_str().ok())
            .map(|ct| ct.starts_with("application/grpc"))
            .unwrap_or(false)
    }

    async fn handle(&self, req: Request<Body>, client_addr: IpAddr) -> Response<Body> {
        if let Some(grpc_addr) = self.grpc_addr {
            let grpc_addr = format!("http://{}", grpc_addr);

            // Wrap proxy call with timeout to prevent indefinite hangs
            match timeout(
                GRPC_PROXY_TIMEOUT,
                self.proxy_client.call(client_addr, &grpc_addr, req),
            )
            .await
            {
                Ok(Ok(response)) => response,
                Ok(Err(_error)) => {
                    error!(target: LOG_TARGET, error = ?_error, "gRPC proxy error");
                    Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .body(Body::empty())
                        .unwrap()
                }
                Err(_) => {
                    error!(target: LOG_TARGET, "gRPC request timeout after {:?}", GRPC_PROXY_TIMEOUT);
                    Response::builder()
                        .status(StatusCode::GATEWAY_TIMEOUT)
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
