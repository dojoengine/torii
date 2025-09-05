pub mod graphql;
pub mod grpc;
pub mod mcp;
pub mod metadata;
pub mod sql;
pub mod static_files;

use std::net::IpAddr;

use http::{Method, StatusCode};
use hyper::{Body, Request, Response};

#[async_trait::async_trait]
pub trait Handler: Send + Sync + std::fmt::Debug {
    // Check if this handler should handle the given request
    fn should_handle(&self, req: &Request<Body>) -> bool;

    // Handle the request
    async fn handle(&self, req: Request<Body>, client_addr: IpAddr) -> Response<Body>;

    async fn extract_query(&self, req: Request<Body>) -> Result<String, Response<Body>> {
        match *req.method() {
            Method::GET => {
                // Get the query from the query params
                let params = req.uri().query().unwrap_or_default();
                form_urlencoded::parse(params.as_bytes())
                    .find(|(key, _)| key == "q" || key == "query")
                    .map(|(_, value)| value.to_string())
                    .ok_or(
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from("Missing 'q' or 'query' parameter."))
                            .unwrap(),
                    )
            }
            Method::POST => {
                // Get the query from request body
                let body_bytes = hyper::body::to_bytes(req.into_body()).await.map_err(|_| {
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Failed to read query from request body"))
                        .unwrap()
                })?;
                String::from_utf8(body_bytes.to_vec()).map_err(|_| {
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Invalid query"))
                        .unwrap()
                })
            }
            _ => Err(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::from("Only GET and POST methods are allowed"))
                .unwrap()),
        }
    }
}
