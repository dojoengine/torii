use std::convert::Infallible;
use std::fs::File;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow;
use http::header::CONTENT_TYPE;
use http::{HeaderName, Method};
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper::server::conn::AddrStream;
use hyper::service::make_service_fn;
use hyper::{Body, Client, Request, Response, Server, StatusCode};
use hyper_reverse_proxy::ReverseProxy;
use rustls::{Certificate, PrivateKey, ServerConfig};
use serde_json::json;
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tokio_rustls::TlsAcceptor;
use tower::ServiceBuilder;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::handlers::graphql::GraphQLHandler;
use crate::handlers::grpc::GrpcHandler;
use crate::handlers::mcp::McpHandler;
use crate::handlers::sql::SqlHandler;
use crate::handlers::static_files::StaticHandler;
use crate::handlers::Handler;

const DEFAULT_ALLOW_HEADERS: [&str; 13] = [
    "accept",
    "origin",
    "content-type",
    "access-control-allow-origin",
    "upgrade",
    "x-grpc-web",
    "x-grpc-timeout",
    "x-user-agent",
    "connection",
    "sec-websocket-key",
    "sec-websocket-version",
    "grpc-accept-encoding",
    "grpc-encoding",
];
const DEFAULT_EXPOSED_HEADERS: [&str; 4] = [
    "grpc-status",
    "grpc-message",
    "grpc-status-details-bin",
    "grpc-encoding",
];
const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

lazy_static::lazy_static! {
    pub(crate) static ref GRAPHQL_PROXY_CLIENT: ReverseProxy<HttpConnector<GaiResolver>> = {
        ReverseProxy::new(
            Client::builder()
             .build_http(),
        )
    };

    pub(crate) static ref GRPC_PROXY_CLIENT: ReverseProxy<HttpConnector<GaiResolver>> = {
        ReverseProxy::new(
            Client::builder()
             .http2_only(true)
             .build_http(),
        )
    };
}

#[derive(Debug)]
pub struct Proxy {
    addr: SocketAddr,
    allowed_origins: Option<Vec<String>>,
    handlers: Arc<RwLock<Vec<Box<dyn Handler>>>>,
    version_spec: String,
    tls_config: Option<Arc<ServerConfig>>,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

impl Proxy {
    pub fn new(
        addr: SocketAddr,
        allowed_origins: Option<Vec<String>>,
        grpc_addr: Option<SocketAddr>,
        graphql_addr: Option<SocketAddr>,
        artifacts_addr: Option<SocketAddr>,
        pool: Arc<SqlitePool>,
        version_spec: String,
    ) -> Self {
        let handlers: Arc<RwLock<Vec<Box<dyn Handler>>>> = Arc::new(RwLock::new(vec![
            Box::new(GraphQLHandler::new(graphql_addr)),
            Box::new(GrpcHandler::new(grpc_addr)),
            Box::new(McpHandler::new(pool.clone())),
            Box::new(SqlHandler::new(pool.clone())),
            Box::new(StaticHandler::new(artifacts_addr)),
        ]));

        Self {
            addr,
            allowed_origins,
            handlers,
            version_spec,
            tls_config: None,
        }
    }

    pub fn with_tls_config(mut self, tls_config: TlsConfig) -> anyhow::Result<Self> {
        let server_config = Self::load_tls_config(&tls_config)?;
        self.tls_config = Some(Arc::new(server_config));
        Ok(self)
    }

    fn load_tls_config(config: &TlsConfig) -> anyhow::Result<ServerConfig> {
        // Load certificates
        let cert_file = File::open(&config.cert_path)?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs = rustls_pemfile::certs(&mut cert_reader)?
            .into_iter()
            .map(Certificate)
            .collect::<Vec<_>>();

        // Load private key
        let key_file = File::open(&config.key_path)?;
        let mut key_reader = BufReader::new(key_file);
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut key_reader)?;

        // Try RSA keys if PKCS8 fails
        if keys.is_empty() {
            let key_file = File::open(&config.key_path)?;
            let mut key_reader = BufReader::new(key_file);
            keys = rustls_pemfile::rsa_private_keys(&mut key_reader)?;
        }

        if keys.is_empty() {
            anyhow::bail!("No private keys found in key file");
        }

        let key = PrivateKey(keys[0].clone());

        // Create server config
        let server_config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;

        Ok(server_config)
    }

    pub async fn set_graphql_addr(&self, addr: SocketAddr) {
        let mut handlers = self.handlers.write().await;
        handlers[0] = Box::new(GraphQLHandler::new(Some(addr)));
    }

    fn create_cors_layer(&self) -> Option<CorsLayer> {
        let cors = CorsLayer::new()
            .max_age(DEFAULT_MAX_AGE)
            .allow_methods([Method::GET, Method::POST])
            .allow_headers(
                DEFAULT_ALLOW_HEADERS
                    .iter()
                    .cloned()
                    .map(HeaderName::from_static)
                    .collect::<Vec<HeaderName>>(),
            )
            .expose_headers(
                DEFAULT_EXPOSED_HEADERS
                    .iter()
                    .cloned()
                    .map(HeaderName::from_static)
                    .collect::<Vec<HeaderName>>(),
            );

        self.allowed_origins
            .clone()
            .map(|allowed_origins| match allowed_origins.as_slice() {
                [origin] if origin == "*" => cors.allow_origin(AllowOrigin::mirror_request()),
                origins => cors.allow_origin(
                    origins
                        .iter()
                        .map(|o| {
                            let _ = o.parse::<http::Uri>().expect("Invalid URI");
                            o.parse().expect("Invalid origin")
                        })
                        .collect::<Vec<_>>(),
                ),
            })
    }

    pub async fn start(
        &self,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let addr = self.addr;
        let tls_config = self.tls_config.clone();

        // Configure server with or without TLS
        if let Some(tls_config) = tls_config {
            let tls_acceptor = TlsAcceptor::from(tls_config);
            let cors_layer = self.create_cors_layer();

            // For HTTPS, we need to manually accept connections and handle TLS
            let listener = tokio::net::TcpListener::bind(addr).await?;

            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, remote_addr)) => {
                                let tls_acceptor = tls_acceptor.clone();
                                let handlers = self.handlers.clone();
                                let version_spec = self.version_spec.clone();
                                let cors_layer = cors_layer.clone();

                                tokio::spawn(async move {
                                    match tls_acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            let service = ServiceBuilder::new()
                                                .option_layer(cors_layer)
                                                .service_fn(move |req| {
                                                    let handlers = handlers.clone();
                                                    let version_spec = version_spec.clone();
                                                    async move {
                                                        let handlers = handlers.read().await;
                                                        handle(remote_addr.ip(), req, &handlers, &version_spec).await
                                                    }
                                                });

                                            if let Err(e) = hyper::server::conn::Http::new()
                                                .serve_connection(tls_stream, service)
                                                .await
                                            {
                                                eprintln!("Error serving connection: {}", e);
                                            }
                                        }
                                        Err(_) => {
                                            // TLS handshake failures are typically user errors (HTTP to HTTPS)
                                            // so we don't log them to avoid noise
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                eprintln!("Failed to accept connection: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }

            Ok(())
        } else {
            // HTTP server
            let cors_layer = self.create_cors_layer();
            let make_svc = make_service_fn(move |conn: &AddrStream| {
                let remote_addr = conn.remote_addr().ip();
                let handlers = self.handlers.clone();
                let version_spec = self.version_spec.clone();
                let cors_layer = cors_layer.clone();

                let service =
                    ServiceBuilder::new()
                        .option_layer(cors_layer)
                        .service_fn(move |req| {
                            let handlers = handlers.clone();
                            let version_spec = version_spec.clone();
                            async move {
                                let handlers = handlers.read().await;
                                handle(remote_addr, req, &handlers, &version_spec).await
                            }
                        });

                async { Ok::<_, Infallible>(service) }
            });

            let server = Server::bind(&addr);
            server
                .serve(make_svc)
                .with_graceful_shutdown(async move {
                    shutdown_rx.recv().await.ok();
                })
                .await
                .map_err(anyhow::Error::from)
        }
    }
}

async fn handle(
    client_ip: IpAddr,
    req: Request<Body>,
    handlers: &[Box<dyn Handler>],
    version_spec: &str,
) -> Result<Response<Body>, Infallible> {
    for handler in handlers.iter() {
        if handler.should_handle(&req) {
            return Ok(handler.handle(req, client_ip).await);
        }
    }

    // Default response if no handler matches
    let json = json!({
        "service": "torii",
        "version": version_spec,
        "success": true,

    });

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(json.to_string()))
        .unwrap())
}
