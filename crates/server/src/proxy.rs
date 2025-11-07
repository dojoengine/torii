use std::convert::Infallible;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow;
use camino::Utf8PathBuf;
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
use starknet::providers::Provider;
use tokio::sync::RwLock;
use tokio_rustls::TlsAcceptor;
use torii_storage::Storage;
use tower::ServiceBuilder;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{debug, warn};

use crate::handlers::graphql::GraphQLHandler;
use crate::handlers::grpc::GrpcHandler;
use crate::handlers::mcp::McpHandler;
use crate::handlers::metadata::MetadataHandler;
use crate::handlers::r#static::StaticHandler;
use crate::handlers::sql::SqlHandler;
use crate::handlers::Handler;

pub const LOG_TARGET: &str = "torii::server::proxy";

/// Configuration for proxy client connection settings
#[derive(Debug, Clone)]
pub struct ProxySettings {
    /// TCP keepalive interval in seconds (0 to disable)
    pub tcp_keepalive_interval: u64,
    /// HTTP/2 keepalive interval in seconds (0 to disable)
    pub http2_keepalive_interval: u64,
    /// HTTP/2 keepalive timeout in seconds
    pub http2_keepalive_timeout: u64,
}

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

/// Create a gRPC-compatible HTTP/2 proxy client with configurable keepalive settings
pub fn create_grpc_proxy_client(
    settings: &ProxySettings,
) -> ReverseProxy<HttpConnector<GaiResolver>> {
    let mut http_connector = HttpConnector::new();

    // TCP keepalive to detect dead connections
    if settings.tcp_keepalive_interval > 0 {
        http_connector.set_keepalive(Some(Duration::from_secs(settings.tcp_keepalive_interval)));
    }
    // TCP nodelay for lower latency (important for gRPC)
    http_connector.set_nodelay(true);

    // Build client with HTTP/2 keepalive settings to match gRPC server and detect stale connections
    // Enable adaptive window sizing for optimal streaming performance
    let client = if settings.http2_keepalive_interval > 0 && settings.http2_keepalive_timeout > 0 {
        Client::builder()
            .http2_only(true)
            .http2_keep_alive_interval(Duration::from_secs(settings.http2_keepalive_interval))
            .http2_keep_alive_timeout(Duration::from_secs(settings.http2_keepalive_timeout))
            .http2_keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .build(http_connector)
    } else if settings.http2_keepalive_interval > 0 {
        Client::builder()
            .http2_only(true)
            .http2_keep_alive_interval(Duration::from_secs(settings.http2_keepalive_interval))
            .http2_keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .build(http_connector)
    } else {
        Client::builder()
            .http2_only(true)
            .http2_adaptive_window(true)
            .build(http_connector)
    };

    ReverseProxy::new(client)
}

/// Create a WebSocket-compatible HTTP/1.1 proxy client
pub fn create_websocket_proxy_client() -> ReverseProxy<HttpConnector<GaiResolver>> {
    ReverseProxy::new(Client::builder().build_http())
}

// Helper function to check if a request is a WebSocket upgrade request
pub fn is_websocket_upgrade(req: &Request<Body>) -> bool {
    req.headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_lowercase() == "websocket")
        .unwrap_or(false)
        && req
            .headers()
            .get("connection")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_lowercase().contains("upgrade"))
            .unwrap_or(false)
}

pub struct Proxy<P: Provider + Sync + Send + Debug + 'static> {
    addr: SocketAddr,
    allowed_origins: Option<Vec<String>>,
    handlers: Arc<RwLock<Vec<Box<dyn Handler>>>>,
    version_spec: String,
    tls_config: Option<Arc<ServerConfig>>,
    grpc_proxy_client: Arc<ReverseProxy<HttpConnector<GaiResolver>>>,
    websocket_proxy_client: Arc<ReverseProxy<HttpConnector<GaiResolver>>>,
    _provider: std::marker::PhantomData<P>,
}

impl<P: Provider + Sync + Send + Debug + 'static> std::fmt::Debug for Proxy<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proxy")
            .field("addr", &self.addr)
            .field("allowed_origins", &self.allowed_origins)
            .field("version_spec", &self.version_spec)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

impl<P: Provider + Sync + Send + Debug + 'static> Proxy<P> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<S: Storage + 'static>(
        addr: SocketAddr,
        allowed_origins: Option<Vec<String>>,
        grpc_addr: Option<SocketAddr>,
        graphql_addr: Option<SocketAddr>,
        artifacts_dir: Utf8PathBuf,
        pool: Arc<SqlitePool>,
        storage: Arc<S>,
        provider: P,
        version_spec: String,
        proxy_settings: ProxySettings,
    ) -> Self {
        // Create proxy clients with configured settings
        let grpc_proxy_client = Arc::new(create_grpc_proxy_client(&proxy_settings));
        let websocket_proxy_client = Arc::new(create_websocket_proxy_client());

        let handlers: Arc<RwLock<Vec<Box<dyn Handler>>>> = Arc::new(RwLock::new(vec![
            Box::new(GraphQLHandler::new(
                graphql_addr,
                grpc_proxy_client.clone(),
                websocket_proxy_client.clone(),
            )),
            Box::new(GrpcHandler::new(grpc_addr, grpc_proxy_client.clone())),
            Box::new(McpHandler::new(pool.clone())),
            Box::new(MetadataHandler::new(storage.clone(), provider)),
            Box::new(SqlHandler::new(pool.clone())),
            Box::new(StaticHandler::new(artifacts_dir, (*pool).clone())),
        ]));

        Self {
            addr,
            allowed_origins,
            handlers,
            version_spec,
            tls_config: None,
            grpc_proxy_client,
            websocket_proxy_client,
            _provider: std::marker::PhantomData,
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
        let mut server_config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        server_config.alpn_protocols =
            vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];

        Ok(server_config)
    }

    pub async fn set_graphql_addr(&self, addr: SocketAddr) {
        let mut handlers = self.handlers.write().await;
        handlers[0] = Box::new(GraphQLHandler::new(
            Some(addr),
            self.grpc_proxy_client.clone(),
            self.websocket_proxy_client.clone(),
        ));
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
                                                .with_upgrades() // Enable connection upgrades for WebSocket over TLS
                                                .await
                                            {
                                                // Connection errors are common in production (client disconnects, timeouts, etc.)
                                                // Log at debug level to reduce noise, but include remote address for debugging
                                                debug!(target: LOG_TARGET, remote_addr = %remote_addr, error = ?e, "Serving connection.");
                                            }
                                        }
                                        Err(_) => {
                                            // TLS handshake failures are typically user errors (HTTP to HTTPS)
                                            // Log the client address for debugging purposes
                                            debug!(target: LOG_TARGET, remote_addr = %remote_addr, "Failed TLS handshake.");
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                warn!(target: LOG_TARGET, error = ?e, "Failed to accept connection.");
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
