use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use camino::Utf8PathBuf;
use merge_options::MergeOptions;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use starknet::core::types::Felt;
use torii_proto::{ContractDefinition, ContractType};
use torii_sqlite_types::{Aggregation, AggregatorConfig, Hook, HookEvent, ModelIndices, SortOrder};

pub const DEFAULT_HTTP_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_METRICS_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
pub const DEFAULT_METRICS_PORT: u16 = 9200;
pub const DEFAULT_EVENTS_CHUNK_SIZE: u64 = 1024;
pub const DEFAULT_BLOCKS_CHUNK_SIZE: u64 = 10240;
pub const DEFAULT_BATCH_CHUNK_SIZE: usize = 1024;
pub const DEFAULT_POLLING_INTERVAL: u64 = 500;
pub const DEFAULT_MAX_CONCURRENT_TASKS: usize = 100;
pub const DEFAULT_RELAY_PORT: u16 = 9090;
pub const DEFAULT_RELAY_WEBRTC_PORT: u16 = 9091;
pub const DEFAULT_RELAY_WEBSOCKET_PORT: u16 = 9092;
pub const DEFAULT_GRPC_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
pub const DEFAULT_GRPC_PORT: u16 = 50051;
pub const DEFAULT_GRPC_SUBSCRIPTION_BUFFER_SIZE: usize = 256;
pub const DEFAULT_GRPC_TCP_KEEPALIVE_SECS: u64 = 60;
pub const DEFAULT_GRPC_HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 30;
pub const DEFAULT_GRPC_HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

pub const DEFAULT_ERC_MAX_METADATA_TASKS: usize = 100;
pub const DEFAULT_DATABASE_WAL_AUTO_CHECKPOINT: u64 = 10000;
/// Default WAL size threshold for TRUNCATE checkpoint (100MB in bytes)
pub const DEFAULT_DATABASE_WAL_TRUNCATE_SIZE_THRESHOLD: u64 = 100 * 1024 * 1024;
/// Default interval in seconds between PRAGMA optimize runs after commits (1 hour)
pub const DEFAULT_DATABASE_OPTIMIZE_INTERVAL: u64 = 3600;
pub const DEFAULT_DATABASE_BUSY_TIMEOUT: u64 = 60_000;
pub const DEFAULT_DATABASE_ACQUIRE_TIMEOUT: u64 = 30_000;
pub const DEFAULT_DATABASE_IDLE_TIMEOUT: u64 = 600_000;
pub const DEFAULT_DATABASE_MAX_CONNECTIONS: u32 = 100;
pub const DEFAULT_MESSAGING_MAX_AGE: u64 = 300_000;
pub const DEFAULT_MESSAGING_FUTURE_TOLERANCE: u64 = 60_000;

// Activity tracking defaults
/// Default session timeout in seconds (1 hour)
pub const DEFAULT_ACTIVITY_SESSION_TIMEOUT: u64 = 3600;
/// Default days to keep activity records (30 days)
pub const DEFAULT_ACTIVITY_RETENTION_DAYS: u64 = 30;

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "Relay options")]
pub struct RelayOptions {
    /// Port to serve Libp2p TCP & UDP Quic transports
    #[arg(
        long = "relay.port",
        value_name = "PORT",
        default_value_t = DEFAULT_RELAY_PORT,
        help = "Port to serve Libp2p TCP & UDP Quic transports."
    )]
    pub port: u16,

    /// Port to serve Libp2p WebRTC transport
    #[arg(
        long = "relay.webrtc_port",
        value_name = "PORT",
        default_value_t = DEFAULT_RELAY_WEBRTC_PORT,
        help = "Port to serve Libp2p WebRTC transport."
    )]
    pub webrtc_port: u16,

    /// Port to serve Libp2p WebRTC transport
    #[arg(
        long = "relay.websocket_port",
        value_name = "PORT",
        default_value_t = DEFAULT_RELAY_WEBSOCKET_PORT,
        help = "Port to serve Libp2p WebRTC transport."
    )]
    pub websocket_port: u16,

    /// Path to a local identity key file. If not specified, a new identity will be generated
    #[arg(
        long = "relay.local_key_path",
        value_name = "PATH",
        help = "Path to a local identity key file. If not specified, a new identity will be \
                generated."
    )]
    pub local_key_path: Option<String>,

    /// Path to a local certificate file. If not specified, a new certificate will be generated
    /// for WebRTC connections
    #[arg(
        long = "relay.cert_path",
        value_name = "PATH",
        help = "Path to a local certificate file. If not specified, a new certificate will be \
                generated for WebRTC connections."
    )]
    pub cert_path: Option<String>,

    /// A list of other torii relays to connect to and sync with.
    /// Right now, only offchain messages broadcasted by the relay will be synced.
    #[arg(
        long = "relay.peers",
        value_delimiter = ',',
        help = "A list of other torii relays to connect to and sync with."
    )]
    pub peers: Vec<String>,
}

impl Default for RelayOptions {
    fn default() -> Self {
        Self {
            port: DEFAULT_RELAY_PORT,
            webrtc_port: DEFAULT_RELAY_WEBRTC_PORT,
            websocket_port: DEFAULT_RELAY_WEBSOCKET_PORT,
            local_key_path: None,
            cert_path: None,
            peers: vec![],
        }
    }
}

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "Indexing options")]
pub struct IndexingOptions {
    /// Chunk size of the events page when indexing using events
    #[arg(long = "indexing.events_chunk_size", default_value_t = DEFAULT_EVENTS_CHUNK_SIZE, help = "Chunk size of the events page to fetch from the sequencer.")]
    pub events_chunk_size: u64,

    /// Number of blocks to process before commiting to DB
    #[arg(long = "indexing.blocks_chunk_size", default_value_t = DEFAULT_BLOCKS_CHUNK_SIZE, help = "Number of blocks to process before commiting to DB.")]
    pub blocks_chunk_size: u64,

    /// Enable indexing pending blocks
    #[arg(
        long = "indexing.preconfirmed",
        alias = "indexing.pending",
        default_value_t = true,
        help = "Whether or not to index pending blocks."
    )]
    #[serde(alias = "pending")]
    pub preconfirmed: bool,

    /// Polling interval in ms
    #[arg(
        long = "indexing.polling_interval",
        default_value_t = DEFAULT_POLLING_INTERVAL,
        help = "Polling interval in ms for Torii to check for new events."
    )]
    pub polling_interval: u64,

    /// Maximum number of concurrent tasks used for processing parallelizable events.
    #[arg(
        long = "indexing.max_concurrent_tasks",
        default_value_t = DEFAULT_MAX_CONCURRENT_TASKS,
        help = "Maximum number of concurrent tasks processing parallelizable events."
    )]
    pub max_concurrent_tasks: usize,

    /// Whether or not to index world transactions
    #[arg(
        long = "indexing.transactions",
        default_value_t = false,
        help = "Whether or not to index world transactions and keep them in the database."
    )]
    pub transactions: bool,

    /// ERC contract addresses to index
    #[arg(
        long = "indexing.contracts",
        value_delimiter = ',',
        value_parser = parse_erc_contract,
        help = "The list of contracts to index, in the following format: contract_type:address or contract_type:address:starting_block. Supported contract types include ERC20, ERC721, ERC1155, WORLD, UDC, OTHER."
    )]
    #[serde(deserialize_with = "deserialize_contracts")]
    #[serde(serialize_with = "serialize_contracts")]
    pub contracts: Vec<ContractDefinition>,

    /// Namespaces to index
    #[arg(
        long = "indexing.namespaces",
        value_delimiter = ',',
        help = "The namespaces of the world that torii should index. If empty, all namespaces \
                will be indexed."
    )]
    pub namespaces: Vec<String>,

    /// Models to index
    #[arg(
        long = "indexing.models",
        value_delimiter = ',',
        help = "The models of the world that torii should index. If empty, all models will be indexed."
    )]
    pub models: Vec<String>,

    /// The block number to start indexing the world from.
    ///
    /// Warning: In the current implementation, this will break the indexing of tokens, if any.
    /// Since the tokens require the chain to be indexed from the beginning, to ensure correct
    /// balance updates.
    #[arg(
        long = "indexing.world_block",
        help = "The block number to start indexing from.",
        default_value_t = 0
    )]
    pub world_block: u64,

    /// Whether or not to index Cartridge controllers.
    #[arg(
        long = "indexing.controllers",
        default_value_t = false,
        help = "Whether or not to index Cartridge controllers."
    )]
    pub controllers: bool,

    /// Whether or not to read models from the block number they were registered in.
    /// If false, models will be read from the latest block.
    #[arg(
        long = "indexing.strict_model_reader",
        default_value_t = false,
        help = "Whether or not to read models from the block number they were registered in."
    )]
    pub strict_model_reader: bool,

    /// The chunk size to use for batch requests.
    #[arg(
        long = "indexing.batch_chunk_size",
        default_value_t = DEFAULT_BATCH_CHUNK_SIZE,
        help = "The chunk size to use for batch requests. This is used to split the requests into smaller chunks to avoid overwhelming the provider and potentially running into issues."
    )]
    pub batch_chunk_size: usize,

    /// Whether or not to index external contract registration events.
    #[arg(
        long = "indexing.external_contracts",
        default_value_t = true,
        help = "Whether or not to index external contract registration events."
    )]
    pub external_contracts: bool,

    /// Comma separated list of external contract instance names to index.
    /// If empty, all external contracts will be indexed (when external_contracts is enabled).
    #[arg(
        long = "indexing.external_contract_whitelist",
        value_delimiter = ',',
        help = "Comma separated list of external contract instance names to index. If empty, all external contracts will be indexed (when external_contracts is enabled)."
    )]
    pub external_contract_whitelist: Vec<String>,
}

impl Default for IndexingOptions {
    fn default() -> Self {
        Self {
            events_chunk_size: DEFAULT_EVENTS_CHUNK_SIZE,
            blocks_chunk_size: DEFAULT_BLOCKS_CHUNK_SIZE,
            batch_chunk_size: DEFAULT_BATCH_CHUNK_SIZE,
            preconfirmed: true,
            polling_interval: DEFAULT_POLLING_INTERVAL,
            max_concurrent_tasks: DEFAULT_MAX_CONCURRENT_TASKS,
            transactions: false,
            contracts: vec![],
            namespaces: vec![],
            models: vec![],
            world_block: 0,
            controllers: false,
            strict_model_reader: false,
            external_contracts: true,
            external_contract_whitelist: vec![],
        }
    }
}

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, Default, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "Events indexing options")]
pub struct EventsOptions {
    /// Whether or not to index raw events
    #[arg(
        long = "events.raw",
        default_value_t = false,
        help = "Whether or not to index raw events."
    )]
    pub raw: bool,
}

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "HTTP server options")]
pub struct ServerOptions {
    /// HTTP server listening interface.
    #[arg(long = "http.addr", value_name = "ADDRESS")]
    #[arg(default_value_t = DEFAULT_HTTP_ADDR)]
    pub http_addr: IpAddr,

    /// HTTP server listening port.
    #[arg(long = "http.port", value_name = "PORT")]
    #[arg(default_value_t = DEFAULT_HTTP_PORT)]
    pub http_port: u16,

    /// Comma separated list of domains from which to accept cross origin requests.
    #[arg(long = "http.cors_origins")]
    #[arg(value_delimiter = ',')]
    pub http_cors_origins: Option<Vec<String>>,

    /// Path to the SSL certificate file (.pem)
    #[arg(
        long = "http.tls_cert_path",
        value_name = "PATH",
        help = "Path to the SSL certificate file (.pem). If provided, the server will use HTTPS instead of HTTP."
    )]
    pub tls_cert_path: Option<String>,

    /// Path to the SSL private key file (.pem)
    #[arg(
        long = "http.tls_key_path",
        value_name = "PATH",
        help = "Path to the SSL private key file (.pem). Required when tls_cert_path is provided."
    )]
    pub tls_key_path: Option<String>,

    /// Use mkcert to generate and install local development certificates
    #[arg(
        long = "http.mkcert",
        help = "Use mkcert to automatically generate and install local development certificates for HTTPS. This will create certificates for localhost and 127.0.0.1."
    )]
    pub mkcert: bool,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            http_addr: DEFAULT_HTTP_ADDR,
            http_port: DEFAULT_HTTP_PORT,
            http_cors_origins: None,
            tls_cert_path: None,
            tls_key_path: None,
            mkcert: false,
        }
    }
}

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "Metrics options")]
pub struct MetricsOptions {
    /// Enable metrics.
    ///
    /// For now, metrics will still be collected even if this flag is not set. This only
    /// controls whether the metrics server is started or not.
    #[arg(long)]
    pub metrics: bool,

    /// The metrics will be served at the given address.
    #[arg(requires = "metrics")]
    #[arg(long = "metrics.addr", value_name = "ADDRESS")]
    #[arg(default_value_t = DEFAULT_METRICS_ADDR)]
    pub metrics_addr: IpAddr,

    /// The metrics will be served at the given port.
    #[arg(requires = "metrics")]
    #[arg(long = "metrics.port", value_name = "PORT")]
    #[arg(default_value_t = DEFAULT_METRICS_PORT)]
    pub metrics_port: u16,
}

impl Default for MetricsOptions {
    fn default() -> Self {
        Self {
            metrics: false,
            metrics_addr: DEFAULT_METRICS_ADDR,
            metrics_port: DEFAULT_METRICS_PORT,
        }
    }
}

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "ERC options")]
pub struct ErcOptions {
    /// The maximum number of concurrent tasks to use for indexing ERC721 and ERC1155 token
    /// metadata.
    #[arg(
        long = "erc.max_metadata_tasks",
        default_value_t = DEFAULT_ERC_MAX_METADATA_TASKS,
        help = "The maximum number of concurrent tasks to use for indexing ERC721 and ERC1155 token metadata."
    )]
    pub max_metadata_tasks: usize,

    /// Path to a directory to store ERC artifacts
    #[arg(long)]
    pub artifacts_path: Option<Utf8PathBuf>,
}

impl Default for ErcOptions {
    fn default() -> Self {
        Self {
            max_metadata_tasks: DEFAULT_ERC_MAX_METADATA_TASKS,
            artifacts_path: None,
        }
    }
}

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "Messaging options")]
pub struct MessagingOptions {
    /// Maximum age in milliseconds for message timestamps to be considered valid
    #[arg(
        long = "messaging.max_age",
        default_value_t = DEFAULT_MESSAGING_MAX_AGE,
        help = "Maximum age in milliseconds for message timestamps to be considered valid. Messages older than this will be rejected."
    )]
    pub max_age: u64,

    /// Maximum milliseconds in the future that message timestamps are allowed
    #[arg(
        long = "messaging.future_tolerance",
        default_value_t = DEFAULT_MESSAGING_FUTURE_TOLERANCE,
        help = "Maximum milliseconds in the future that message timestamps are allowed. Helps prevent clock skew issues."
    )]
    pub future_tolerance: u64,

    /// Whether timestamps are required in messages
    #[arg(
        long = "messaging.require_timestamp",
        default_value_t = false,
        help = "Whether timestamps are required in all messages. If false, timestamps are optional but validated when present."
    )]
    pub require_timestamp: bool,
}

impl Default for MessagingOptions {
    fn default() -> Self {
        Self {
            max_age: DEFAULT_MESSAGING_MAX_AGE,
            future_tolerance: DEFAULT_MESSAGING_FUTURE_TOLERANCE,
            require_timestamp: false,
        }
    }
}

pub const DEFAULT_DATABASE_PAGE_SIZE: u64 = 32_768;
/// Negative value is used to determine number of KiB to use for cache. Currently set as 512MB, 25%
/// of the RAM of the smallest slot instance.
pub const DEFAULT_DATABASE_CACHE_SIZE: i64 = -500_000;
/// Default soft memory limit in bytes (1GB)
pub const DEFAULT_DATABASE_SOFT_MEMORY_LIMIT: u64 = 1024 * 1024 * 1024;
/// Default hard memory limit in bytes (2GB)
pub const DEFAULT_DATABASE_HARD_MEMORY_LIMIT: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "SQL options")]
pub struct SqlOptions {
    /// Whether model tables should default to having indices on all columns
    #[arg(
        long = "sql.all_model_indices",
        default_value_t = false,
        help = "If true, creates indices on all columns of model tables by default. If false, \
                only key fields columns of model tables will have indices."
    )]
    pub all_model_indices: bool,

    /// Specify which fields should have indices for specific models
    /// Format: "model_name:field1,field2;another_model:field3,field4"
    #[arg(
        long = "sql.model_indices",
        value_delimiter = ';',
        value_parser = parse_model_indices,
        help = "Specify which fields should have indices for specific models. Format: \"model_name:field1,field2;another_model:field3,field4\""
    )]
    pub model_indices: Vec<ModelIndices>,

    /// Models that are going to be treated as historical during indexing. Applies to event
    /// messages and entities. A list of the model tags (namespace-name)
    #[arg(
        long = "sql.historical",
        value_delimiter = ',',
        help = "Models that are going to be treated as historical during indexing."
    )]
    pub historical: Vec<String>,

    /// The page size to use for the database. The page size must be a power of two between 512 and
    /// 65536 inclusive.
    #[arg(
        long = "sql.page_size",
        default_value_t = DEFAULT_DATABASE_PAGE_SIZE,
        help = "The page size to use for the database. The page size must be a power of two between 512 and 65536 inclusive."
    )]
    pub page_size: u64,

    /// Cache size to use for the database.
    #[arg(
        long = "sql.cache_size",
        default_value_t = DEFAULT_DATABASE_CACHE_SIZE,
        help = "The cache size to use for the database. A positive value determines a number of pages, a negative value determines a number of KiB."
    )]
    pub cache_size: i64,

    /// A set of SQL statements to execute after some specific events.
    /// Like after a model has been registered, or after an entity model has been updated etc...
    #[arg(
        long = "sql.hooks",
        value_delimiter = ',',
        value_parser = parse_hook,
        help = "A set of SQL statements to execute after some specific events."
    )]
    pub hooks: Vec<Hook>,

    /// A directory containing custom migrations to run.
    #[arg(
        long = "sql.migrations",
        value_name = "PATH",
        help = "A directory containing custom migrations to run."
    )]
    pub migrations: Option<PathBuf>,

    /// Aggregator configurations
    /// Format: "aggregator_id:model_tag:group_by:aggregation:order"
    #[arg(
        long = "sql.aggregators",
        value_delimiter = ';',
        value_parser = parse_aggregator_config,
        help = "Aggregator configurations. Format: \"aggregator_id:model_tag:group_by:aggregation:order\". \
                group_by can be comma-separated for multiple fields (e.g., 'player,task_id'). \
                Aggregation can be: field_name (latest value), +1/count (count events), max:field (highest), min:field (lowest), sum:field (accumulate), avg:field (average). \
                Order can be 'asc' or 'desc'. Multiple configs separated by ';'. \
                Examples: 'top_scores:ns-Player:player:score:desc' or 'progression:ns-Trophy:player,task:+1:desc' or 'avg_score:ns-Game:player:avg:score:desc'"
    )]
    pub aggregators: Vec<AggregatorConfig>,

    /// The pages interval to autocheckpoint.
    #[arg(
        long = "sql.wal_autocheckpoint",
        default_value_t = DEFAULT_DATABASE_WAL_AUTO_CHECKPOINT,
        help = "The pages interval to autocheckpoint."
    )]
    pub wal_autocheckpoint: u64,

    /// Size threshold in bytes for WAL file before performing a TRUNCATE checkpoint.
    /// This is checked periodically during execute operations.
    #[arg(
        long = "sql.wal_truncate_size_threshold",
        default_value_t = DEFAULT_DATABASE_WAL_TRUNCATE_SIZE_THRESHOLD,
        help = "Size threshold in bytes for WAL file before performing a TRUNCATE checkpoint. Set to 0 to disable. Default is 100MB."
    )]
    pub wal_truncate_size_threshold: u64,

    /// Interval in seconds between PRAGMA optimize runs after transaction commits.
    /// This intelligently updates query planner statistics for better performance.
    #[arg(
        long = "sql.optimize_interval",
        default_value_t = DEFAULT_DATABASE_OPTIMIZE_INTERVAL,
        help = "Interval in seconds between PRAGMA optimize runs after transaction commits. Set to 0 to disable automatic optimization. Default is 3600 seconds (1 hour)."
    )]
    pub optimize_interval: u64,

    /// The timeout before the database is considered busy.
    #[arg(
        long = "sql.busy_timeout",
        default_value_t = DEFAULT_DATABASE_BUSY_TIMEOUT,
        help = "The timeout before the database is considered busy. Helpful in situations where \
                the database is locked for a long time."
    )]
    pub busy_timeout: u64,

    /// The timeout when acquiring a connection from the pool.
    #[arg(
        long = "sql.acquire_timeout",
        default_value_t = DEFAULT_DATABASE_ACQUIRE_TIMEOUT,
        help = "The timeout in milliseconds when acquiring a connection from the pool. This \
                prevents immediate failures when all connections are busy."
    )]
    pub acquire_timeout: u64,

    /// The timeout before idle connections are closed.
    #[arg(
        long = "sql.idle_timeout",
        default_value_t = DEFAULT_DATABASE_IDLE_TIMEOUT,
        help = "The timeout in milliseconds before idle connections are closed and removed \
                from the pool."
    )]
    pub idle_timeout: u64,

    /// The maximum number of connections in the readonly pool.
    #[arg(
        long = "sql.max_connections",
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        help = "The maximum number of connections in the readonly connection pool. This \
                controls how many concurrent read operations can be performed."
    )]
    pub max_connections: u32,

    /// Soft memory limit in bytes for SQLite operations. When exceeded, SQLite will try to free
    /// memory by reducing cache size and other optimizations.
    #[arg(
        long = "sql.soft_memory_limit",
        default_value_t = DEFAULT_DATABASE_SOFT_MEMORY_LIMIT,
        help = "Soft memory limit in bytes for SQLite operations. When exceeded, SQLite will try \
                to free memory by reducing cache size and other optimizations."
    )]
    pub soft_memory_limit: u64,

    /// Hard memory limit in bytes for SQLite operations. When exceeded, SQLite will abort
    /// operations to prevent excessive memory usage.
    #[arg(
        long = "sql.hard_memory_limit",
        default_value_t = DEFAULT_DATABASE_HARD_MEMORY_LIMIT,
        help = "Hard memory limit in bytes for SQLite operations. When exceeded, SQLite will \
                abort operations to prevent excessive memory usage."
    )]
    pub hard_memory_limit: u64,
}

impl Default for SqlOptions {
    fn default() -> Self {
        Self {
            all_model_indices: false,
            model_indices: vec![],
            historical: vec![],
            page_size: DEFAULT_DATABASE_PAGE_SIZE,
            cache_size: DEFAULT_DATABASE_CACHE_SIZE,
            wal_autocheckpoint: DEFAULT_DATABASE_WAL_AUTO_CHECKPOINT,
            wal_truncate_size_threshold: DEFAULT_DATABASE_WAL_TRUNCATE_SIZE_THRESHOLD,
            optimize_interval: DEFAULT_DATABASE_OPTIMIZE_INTERVAL,
            busy_timeout: DEFAULT_DATABASE_BUSY_TIMEOUT,
            acquire_timeout: DEFAULT_DATABASE_ACQUIRE_TIMEOUT,
            idle_timeout: DEFAULT_DATABASE_IDLE_TIMEOUT,
            max_connections: DEFAULT_DATABASE_MAX_CONNECTIONS,
            soft_memory_limit: DEFAULT_DATABASE_SOFT_MEMORY_LIMIT,
            hard_memory_limit: DEFAULT_DATABASE_HARD_MEMORY_LIMIT,
            hooks: vec![],
            migrations: None,
            aggregators: vec![],
        }
    }
}

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "Activity tracking options")]
pub struct ActivityOptions {
    /// Enable activity tracking for user sessions
    /// NOTE: Requires --indexing.transactions to be enabled
    #[arg(
        long = "activity.enabled",
        default_value_t = true,
        help = "Whether to track user activity sessions. When enabled, aggregates transaction \
                calls into sessions for efficient activity queries. Requires transaction indexing \
                to be enabled (--indexing.transactions)."
    )]
    pub enabled: bool,

    /// Session timeout in seconds
    #[arg(
        long = "activity.session_timeout",
        default_value_t = DEFAULT_ACTIVITY_SESSION_TIMEOUT,
        help = "Duration in seconds of inactivity before starting a new session. Default is 3600 \
                seconds (1 hour)."
    )]
    pub session_timeout: u64,

    /// Days to retain activity records
    // #[arg(
    //     long = "activity.retention_days",
    //     default_value_t = DEFAULT_ACTIVITY_RETENTION_DAYS,
    //     help = "Number of days to keep activity records before cleanup. Set to 0 to keep forever. \
    //             Default is 30 days."
    // )]
    // pub retention_days: u64,

    /// Entrypoints to exclude from activity tracking
    #[arg(
        long = "activity.excluded_entrypoints",
        value_delimiter = ',',
        help = "Comma-separated list of entrypoints to exclude from activity tracking. Useful for \
                filtering out wrapper functions or system calls. Defaults include: \
                execute_from_outside_v3, request_random, submit_random, assert_consumed, \
                deployContract, set_name, register_model, entities, init_contract, upgrade_model, \
                emit_events, emit_event, set_metadata"
    )]
    pub excluded_entrypoints: Vec<String>,
}

impl Default for ActivityOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            session_timeout: DEFAULT_ACTIVITY_SESSION_TIMEOUT,
            // retention_days: DEFAULT_ACTIVITY_RETENTION_DAYS,
            excluded_entrypoints: vec![],
        }
    }
}

#[derive(Default, Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "Snapshot options")]
pub struct SnapshotOptions {
    /// Snapshot URL to download
    #[arg(long = "snapshot.url", help = "The snapshot URL to download.")]
    pub url: Option<String>,

    /// Optional version of the remote snapshot torii version
    #[arg(
        long = "snapshot.version",
        help = "Optional version of the torii the snapshot has been made from. This is only used to give a warning if there is a version mismatch between the snapshot and this torii."
    )]
    pub version: Option<String>,
}

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "Runner options")]
pub struct RunnerOptions {
    /// Open World Explorer on the browser.
    #[arg(
        long = "runner.explorer",
        default_value_t = false,
        help = "Open World Explorer on the browser."
    )]
    pub explorer: bool,

    /// Check if contracts are deployed before starting torii.
    #[arg(
        long = "runner.check_contracts",
        default_value_t = false,
        help = "Check if contracts are deployed before starting torii."
    )]
    pub check_contracts: bool,

    /// Number of threads for the query runtime (GraphQL/gRPC API).
    #[arg(
        long = "runner.query_threads",
        default_value_t = 0,
        help = "Number of threads for the query runtime handling GraphQL and gRPC API requests. \
                If 0, uses adaptive allocation based on CPU count and workload."
    )]
    pub query_threads: usize,

    /// Number of threads for the indexer runtime.
    #[arg(
        long = "runner.indexer_threads",
        default_value_t = 0,
        help = "Number of threads for the indexer runtime handling block processing and event indexing. \
                If 0, uses adaptive allocation. During heavy indexing, more threads are allocated to indexer."
    )]
    pub indexer_threads: usize,

    /// Runtime allocation strategy for balancing indexing vs query performance.
    #[arg(
        long = "runner.allocation_strategy",
        default_value = "adaptive",
        help = "Strategy for allocating CPU resources: \
                'adaptive' - automatically adjusts based on workload, \
                'query_priority' - prioritizes query responsiveness, \
                'indexer_priority' - prioritizes indexing throughput, \
                'balanced' - equal allocation between indexer and queries"
    )]
    pub allocation_strategy: String,
}

impl Default for RunnerOptions {
    fn default() -> Self {
        Self {
            explorer: false,
            check_contracts: false,
            query_threads: 0,
            indexer_threads: 0,
            allocation_strategy: "adaptive".to_string(),
        }
    }
}

#[derive(Debug, clap::Args, Clone, Serialize, Deserialize, PartialEq, MergeOptions)]
#[serde(default)]
#[command(next_help_heading = "GRPC options")]
pub struct GrpcOptions {
    /// gRPC server listening interface.
    #[arg(long = "grpc.addr", value_name = "ADDRESS")]
    #[arg(default_value_t = DEFAULT_GRPC_ADDR)]
    pub grpc_addr: IpAddr,

    /// gRPC server listening port.
    #[arg(long = "grpc.port", value_name = "PORT")]
    #[arg(default_value_t = DEFAULT_GRPC_PORT)]
    pub grpc_port: u16,

    /// The buffer size for the subscription channel.
    #[arg(
        long = "grpc.subscription_buffer_size",
        default_value_t = DEFAULT_GRPC_SUBSCRIPTION_BUFFER_SIZE,
        help = "The buffer size for the subscription channel."
    )]
    pub subscription_buffer_size: usize,

    /// Whether or not to broadcast optimistic updates to the subscribers.
    #[arg(
        long = "grpc.optimistic",
        default_value_t = false,
        help = "Whether or not to broadcast optimistic updates to the subscribers. If enabled, \
                the subscribers will receive optimistic updates for the events that are not yet \
                committed to the database."
    )]
    pub optimistic: bool,

    /// TCP keepalive interval in seconds. Set to 0 to disable.
    #[arg(
        long = "grpc.tcp_keepalive_interval",
        default_value_t = DEFAULT_GRPC_TCP_KEEPALIVE_SECS,
        help = "TCP keepalive interval in seconds for gRPC connections. Set to 0 to disable TCP keepalive."
    )]
    pub tcp_keepalive_interval: u64,

    /// HTTP/2 keepalive interval in seconds. Set to 0 to disable.
    #[arg(
        long = "grpc.http2_keepalive_interval",
        default_value_t = DEFAULT_GRPC_HTTP2_KEEPALIVE_INTERVAL_SECS,
        help = "HTTP/2 keepalive interval in seconds for gRPC connections. Set to 0 to disable HTTP/2 keepalive."
    )]
    pub http2_keepalive_interval: u64,

    /// HTTP/2 keepalive timeout in seconds.
    #[arg(
        long = "grpc.http2_keepalive_timeout",
        default_value_t = DEFAULT_GRPC_HTTP2_KEEPALIVE_TIMEOUT_SECS,
        help = "HTTP/2 keepalive timeout in seconds for gRPC connections. How long to wait for keepalive ping responses."
    )]
    pub http2_keepalive_timeout: u64,

    /// Maximum size in bytes for gRPC messages (both incoming and outgoing).
    #[arg(
        long = "grpc.max_message_size",
        default_value_t = DEFAULT_GRPC_MAX_MESSAGE_SIZE,
        help = "Maximum size in bytes for gRPC messages (both incoming and outgoing). Default is 16MB."
    )]
    pub max_message_size: usize,
}

impl GrpcOptions {
    /// Convert GrpcOptions to Duration values for gRPC configuration
    pub fn tcp_keepalive_duration(&self) -> Option<Duration> {
        if self.tcp_keepalive_interval == 0 {
            None
        } else {
            Some(Duration::from_secs(self.tcp_keepalive_interval))
        }
    }

    pub fn http2_keepalive_interval_duration(&self) -> Option<Duration> {
        if self.http2_keepalive_interval == 0 {
            None
        } else {
            Some(Duration::from_secs(self.http2_keepalive_interval))
        }
    }

    pub fn http2_keepalive_timeout_duration(&self) -> Option<Duration> {
        if self.http2_keepalive_timeout == 0 {
            None
        } else {
            Some(Duration::from_secs(self.http2_keepalive_timeout))
        }
    }
}

impl Default for GrpcOptions {
    fn default() -> Self {
        Self {
            grpc_addr: DEFAULT_GRPC_ADDR,
            grpc_port: DEFAULT_GRPC_PORT,
            subscription_buffer_size: DEFAULT_GRPC_SUBSCRIPTION_BUFFER_SIZE,
            optimistic: false,
            tcp_keepalive_interval: DEFAULT_GRPC_TCP_KEEPALIVE_SECS,
            http2_keepalive_interval: DEFAULT_GRPC_HTTP2_KEEPALIVE_INTERVAL_SECS,
            http2_keepalive_timeout: DEFAULT_GRPC_HTTP2_KEEPALIVE_TIMEOUT_SECS,
            max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
        }
    }
}

// Parses clap cli argument which is expected to be in the format:
// - model-tag:field1,field2;othermodel-tag:field3,field4
fn parse_model_indices(part: &str) -> anyhow::Result<ModelIndices> {
    let parts = part.split(':').collect::<Vec<&str>>();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid model indices format"));
    }

    let model_tag = parts[0].to_string();
    let fields = parts[1]
        .split(',')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    Ok(ModelIndices { model_tag, fields })
}

// Parses clap cli argument which is expected to be in the format:
// - event:event_data:statement
fn parse_hook(part: &str) -> anyhow::Result<Hook> {
    let parts: Vec<&str> = part.split(':').collect();
    if parts.len() != 3 {
        return Err(anyhow::anyhow!(
            "Invalid hook format. Expected 'event:event_data:statement'"
        ));
    }

    let event_type = parts[0];
    let event_data: Vec<String> = parts[1]
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if event_data.is_empty() {
        return Err(anyhow::anyhow!("Event data cannot be empty"));
    }

    let event = match event_type {
        "model_registered" => {
            if event_data.len() != 1 {
                return Err(anyhow::anyhow!(
                    "model_registered event requires exactly one model tag"
                ));
            }
            HookEvent::ModelRegistered {
                model_tag: event_data[0].clone(),
            }
        }
        "model_updated" => {
            if event_data.len() != 1 {
                return Err(anyhow::anyhow!(
                    "model_updated event requires exactly one model tag"
                ));
            }
            HookEvent::ModelUpdated {
                model_tag: event_data[0].clone(),
            }
        }
        "model_deleted" => {
            if event_data.len() != 1 {
                return Err(anyhow::anyhow!(
                    "model_deleted event requires exactly one model tag"
                ));
            }
            HookEvent::ModelDeleted {
                model_tag: event_data[0].clone(),
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid event type. Expected 'model_registered', 'model_updated' or \
                 'model_deleted'"
            ));
        }
    };

    Ok(Hook {
        event,
        statement: parts[2].to_string(),
    })
}

// Parses clap cli argument which is expected to be in the format:
// - contract_type:address or contract_type:address:starting_block
fn parse_erc_contract(part: &str) -> anyhow::Result<ContractDefinition> {
    match part.split(':').collect::<Vec<&str>>().as_slice() {
        [r#type, address] => {
            let r#type = r#type.parse::<ContractType>()?;

            let address = Felt::from_str(address)
                .with_context(|| format!("Expected address, found {}", address))?;
            Ok(ContractDefinition {
                address,
                r#type,
                starting_block: None,
            })
        }
        [r#type, address, starting_block] => {
            let r#type = r#type.parse::<ContractType>()?;

            let address = Felt::from_str(address)
                .with_context(|| format!("Expected address, found {}", address))?;

            let starting_block = starting_block.parse::<u64>()
                .with_context(|| format!("Expected block number, found {}", starting_block))?;

            Ok(ContractDefinition {
                address,
                r#type,
                starting_block: Some(starting_block),
            })
        }
        _ => Err(anyhow::anyhow!("Invalid contract format. Expected format: contract_type:address or contract_type:address:starting_block")),
    }
}

// Add this function to handle TOML deserialization
fn deserialize_contracts<'de, D>(deserializer: D) -> Result<Vec<ContractDefinition>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let contracts: Vec<String> = Vec::deserialize(deserializer)?;
    contracts
        .iter()
        .map(|s| parse_erc_contract(s).map_err(serde::de::Error::custom))
        .collect()
}

fn serialize_contracts<S>(
    contracts: &Vec<ContractDefinition>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut seq = serializer.serialize_seq(Some(contracts.len()))?;

    for contract in contracts {
        seq.serialize_element(&contract.to_string())?;
    }

    seq.end()
}

// Parses clap cli argument which is expected to be in the format:
// - aggregator_id:model_tag:group_by:aggregation:order
// - aggregator_id:model_tag:group_by:aggregation_type:field_path:order (for aggregations with field)
fn parse_aggregator_config(part: &str) -> anyhow::Result<AggregatorConfig> {
    let parts: Vec<&str> = part.split(':').collect();

    // Can be 5 parts (simple field or count) or 6 parts (aggregation:field)
    if parts.len() != 5 && parts.len() != 6 {
        return Err(anyhow::anyhow!(
            "Invalid aggregator config format. Expected \
             'aggregator_id:model_tag:group_by:aggregation:order' or \
             'aggregator_id:model_tag:group_by:aggregation_type:field:order'"
        ));
    }

    let (aggregation, order_idx) = if parts.len() == 5 {
        // Simple format: aggregator_id:model_tag:group_by:aggregation:order
        let agg = match parts[3] {
            "+1" | "increment" | "count" => Aggregation::Count,
            field_path => Aggregation::Latest(field_path.to_string()),
        };
        (agg, 4)
    } else {
        // Extended format: aggregator_id:model_tag:group_by:aggregation_type:field:order
        let agg = match parts[3].to_lowercase().as_str() {
            "max" => Aggregation::Max(parts[4].to_string()),
            "min" => Aggregation::Min(parts[4].to_string()),
            "sum" => Aggregation::Sum(parts[4].to_string()),
            "avg" | "average" => Aggregation::Avg(parts[4].to_string()),
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid aggregation type. Expected 'max', 'min', 'sum', or 'avg'"
                ));
            }
        };
        (agg, 5)
    };

    let order = match parts[order_idx].to_lowercase().as_str() {
        "desc" => SortOrder::Desc,
        "asc" => SortOrder::Asc,
        _ => {
            return Err(anyhow::anyhow!("Invalid order. Expected 'asc' or 'desc'"));
        }
    };

    // Parse group_by - can be comma-separated for multiple fields
    let group_by: Vec<String> = parts[2]
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if group_by.is_empty() {
        return Err(anyhow::anyhow!("group_by cannot be empty"));
    }

    Ok(AggregatorConfig {
        id: parts[0].to_string(),
        model_tag: parts[1].to_string(),
        group_by,
        aggregation,
        order,
    })
}
