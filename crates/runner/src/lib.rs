//! Torii binary executable.
//!
//! ## Feature Flags
//!
//! - `jemalloc`: Uses [jemallocator](https://github.com/tikv/jemallocator) as the global allocator.
//!   This is **not recommended on Windows**. See [here](https://rust-lang.github.io/rfcs/1974-global-allocators.html#jemalloc)
//!   for more info.
//! - `jemalloc-prof`: Enables [jemallocator's](https://github.com/tikv/jemallocator) heap profiling
//!   and leak detection functionality. See [jemalloc's opt.prof](https://jemalloc.net/jemalloc.3.html#opt.prof)
//!   documentation for usage details. This is **not recommended on Windows**. See [here](https://rust-lang.github.io/rfcs/1974-global-allocators.html#jemalloc)
//!   for more info.

use std::cmp;
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use camino::Utf8PathBuf;
use dojo_metrics::exporters::prometheus::PrometheusRecorder;
use dojo_types::naming::try_compute_selector_from_tag;
use futures::future::join_all;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::Executor as SqlxExecutor;
use sqlx::SqlitePool;
use starknet::core::types::{BlockId, BlockTag};
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider};
use starknet_crypto::Felt;
use tempfile::{NamedTempFile, TempDir};
use terminal_size::{terminal_size, Height, Width};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use torii_broker::types::ModelUpdate;
use torii_broker::MemoryBroker;
#[cfg(feature = "redis")]
use torii_broker::{init_redis, start_all_listeners};
use torii_cache::InMemoryCache;
use torii_cli::ToriiArgs;
use torii_controllers::sync::ControllersSync;
use torii_grpc_server::GrpcConfig;
use torii_indexer::engine::{Engine, EngineConfig};
use torii_indexer::{FetcherConfig, FetchingFlags, IndexingFlags};
use torii_libp2p_relay::Relay;
use torii_messaging::{Messaging, MessagingConfig};
use torii_processors::{EventProcessorConfig, Processors};
use torii_server::proxy::{Proxy, ProxySettings};
use torii_sqlite::executor::Executor;
use torii_sqlite::{Sql, SqlConfig};
use torii_storage::proto::{ContractDefinition, ContractType};
use torii_storage::ReadOnlyStorage;
use tracing::{debug, error, info, info_span, warn, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::form_urlencoded;

mod constants;

use crate::constants::LOG_TARGET;
const MIN_THREADS: usize = 1;

#[derive(Debug, Clone)]
pub enum AllocationStrategy {
    Adaptive,
    QueryPriority,
    IndexerPriority,
    Balanced,
}

impl From<&str> for AllocationStrategy {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "adaptive" => AllocationStrategy::Adaptive,
            "query_priority" => AllocationStrategy::QueryPriority,
            "indexer_priority" => AllocationStrategy::IndexerPriority,
            "balanced" => AllocationStrategy::Balanced,
            _ => AllocationStrategy::Adaptive,
        }
    }
}

#[derive(Debug)]
pub struct RuntimeAllocation {
    pub query_threads: usize,
    pub indexer_threads: usize,
    pub main_threads: usize,
}

impl RuntimeAllocation {
    pub fn calculate(
        cpu_count: usize,
        strategy: &AllocationStrategy,
        query_override: usize,
        indexer_override: usize,
    ) -> Self {
        // Reserve at least 1 thread for main runtime (proxy, messaging, etc.)
        let available_threads = cpu_count.saturating_sub(1);

        let (query_threads, indexer_threads) = match strategy {
            AllocationStrategy::QueryPriority => {
                // 70% query, 30% indexer
                let query = ((available_threads * 7) / 10)
                    .max(MIN_THREADS)
                    .min(available_threads);
                let indexer = available_threads
                    .saturating_sub(query)
                    .max(MIN_THREADS)
                    .min(available_threads);
                (query, indexer)
            }
            AllocationStrategy::IndexerPriority => {
                // 30% query, 70% indexer
                let indexer = ((available_threads * 7) / 10)
                    .max(MIN_THREADS)
                    .min(available_threads);
                let query = available_threads
                    .saturating_sub(indexer)
                    .max(MIN_THREADS)
                    .min(available_threads);
                (query, indexer)
            }
            AllocationStrategy::Balanced => {
                // 50% each
                let half = available_threads / 2;
                (
                    half.max(MIN_THREADS).min(available_threads),
                    half.max(MIN_THREADS).min(available_threads),
                )
            }
            AllocationStrategy::Adaptive => {
                // Default: 60% query, 40% indexer (queries are user-facing)
                let query = ((available_threads * 6) / 10)
                    .max(MIN_THREADS)
                    .min(available_threads);
                let indexer = available_threads
                    .saturating_sub(query)
                    .max(MIN_THREADS)
                    .min(available_threads);
                (query, indexer)
            }
        };

        Self {
            query_threads: if query_override > 0 {
                query_override.max(MIN_THREADS).min(cpu_count)
            } else {
                query_threads.max(MIN_THREADS)
            },
            indexer_threads: if indexer_override > 0 {
                indexer_override.max(MIN_THREADS).min(cpu_count)
            } else {
                indexer_threads.max(MIN_THREADS)
            },
            main_threads: 1, // Keep main runtime lightweight
        }
    }
}

// Structure to hold runtime and its handle for proper shutdown
struct ManagedRuntime {
    runtime: tokio::runtime::Runtime,
    handle: tokio::runtime::Handle,
}

impl ManagedRuntime {
    fn new(threads: usize, name: &str, stack_size: usize) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name(name)
            .thread_stack_size(stack_size)
            .enable_all()
            .build()
            .expect("Failed to create runtime");

        let handle = runtime.handle().clone();

        Self { runtime, handle }
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    // Shutdown runtime in a blocking context to avoid the panic
    fn shutdown(self) {
        self.runtime.shutdown_background();
    }
}

// Function to create a configurable query runtime
fn create_query_runtime(threads: usize) -> ManagedRuntime {
    ManagedRuntime::new(threads, "torii-query", 2 * 1024 * 1024) // 2MB stack for complex queries
}

// Function to create a dedicated indexer runtime
fn create_indexer_runtime(threads: usize) -> ManagedRuntime {
    ManagedRuntime::new(threads, "torii-indexer", 1024 * 1024) // 1MB stack (less than queries)
}

// Config sqlite memstatus
fn config_sqlite_memstatus(enable: bool) {
    unsafe {
        libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_MEMSTATUS, enable as i32)
    };
}

/// Creates a responsive progress bar template based on terminal size
fn create_progress_bar_template() -> String {
    let (terminal_width, msg_width) = if let Some((Width(w), Height(_))) = terminal_size() {
        // Calculate appropriate widths based on terminal size
        let width = w as usize;
        let min_width = 80;
        let max_width = 120;
        let effective_width = cmp::max(min_width, cmp::min(width, max_width));

        // Calculate message width first (needs space for " XX.Xs" format)
        let msg_width = (effective_width / 8).clamp(8, 20); // Ensure at least 8 chars for seconds

        // Calculate progress bar width (reserve space for other elements)
        // " {spinner:.yellow} snapshot [BAR] {bytes}/{total_bytes} Downloading{msg}"
        let reserved_space = 45 + msg_width; // Space for spinner, labels, bytes, and message
        let bar_width = if effective_width > reserved_space {
            // Use most of the available space for the bar
            let available_space = effective_width - reserved_space;
            cmp::min(60, (available_space * 8) / 10) // Max 60 chars, 80% of available
        } else {
            30 // Minimum bar width
        };

        (bar_width, msg_width)
    } else {
        // Default values if terminal size cannot be determined
        (40, 20)
    };

    format!(
        " {{spinner:.yellow}} snapshot [{{bar:{}.cyan/blue}}] {{bytes}}/{{total_bytes}} Downloading{{wide_msg:>{}.blue}}",
        terminal_width, msg_width
    )
}

#[derive(Debug)]
pub struct Runner {
    args: ToriiArgs,
    version_spec: String,
}

impl Runner {
    pub fn new(args: ToriiArgs, version_spec: String) -> Self {
        Self { args, version_spec }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        // dump the config to the given path if it is provided
        if let Some(dump_config) = &self.args.dump_config {
            let mut dump = self.args.clone();
            // remove the config and dump_config params from the dump
            dump.config = None;
            dump.dump_config = None;

            let config = toml::to_string_pretty(&dump)?;
            std::fs::write(dump_config, config)?;
        }

        // Add world to list of generic contracts if it is provided
        if let Some(world_address) = self.args.world_address {
            self.args.indexing.contracts.push(ContractDefinition {
                address: world_address,
                r#type: ContractType::WORLD,
                starting_block: None,
            });
        }

        // Setup cancellation for graceful shutdown
        let (shutdown_tx, _) = broadcast::channel(1);

        let shutdown_tx_clone = shutdown_tx.clone();
        ctrlc::set_handler(move || {
            let _ = shutdown_tx_clone.send(());
        })
        .expect("Error setting Ctrl-C handler");

        // Initialize Redis broker if configured (for multi-replica deployments)
        #[cfg(feature = "redis")]
        if let Some(redis_url) = &self.args.broker.redis_url {
            match init_redis(redis_url).await {
                Ok(()) => {
                    info!(target: LOG_TARGET, redis_url = %redis_url, "Redis broker initialized for distributed subscriptions");
                    // Start Redis subscription listeners for all update types
                    start_all_listeners().await;
                    info!(target: LOG_TARGET, "Redis subscription listeners started - subscriptions will work across replicas");
                }
                Err(e) => {
                    error!(target: LOG_TARGET, redis_url = %redis_url, error = ?e, "Failed to initialize Redis broker");
                    return Err(anyhow::anyhow!(
                        "Failed to connect to Redis at {}: {}. Multi-replica subscriptions require Redis.",
                        redis_url,
                        e
                    ));
                }
            }
        }

        let transport = HttpTransport::new(self.args.rpc.clone()).with_header(
            "User-Agent".to_string(),
            format!("Torii/{}", self.version_spec),
        );
        let provider: Arc<_> = JsonRpcClient::new(transport).into();

        // Check provider spec version. We only support v0.9.
        let supported_spec = "0.9";
        let spec_version = provider.spec_version().await?;
        if !spec_version.starts_with(supported_spec) {
            return Err(anyhow::anyhow!(
                "Provider spec version is not supported. Please use a provider that supports v{supported_spec}. Got: {spec_version}. You might need to add a `rpc/v{}` to the end of the URL.",
                supported_spec.replace('.', "_")
            ));
        }

        // Verify contracts are deployed
        if self.args.runner.check_contracts {
            let undeployed =
                verify_contracts_deployed(&provider, &self.args.indexing.contracts).await?;
            if !undeployed.is_empty() {
                return Err(anyhow::anyhow!(
                    "The following contracts are not deployed: {:?}",
                    undeployed
                ));
            }
        }

        let tempfile = NamedTempFile::new()?;
        let database_path = if let Some(db_dir) = &self.args.db_dir {
            // Create the directory if it doesn't exist
            std::fs::create_dir_all(db_dir)?;
            // Set the database file path inside the directory
            db_dir.join("torii.db")
        } else {
            tempfile.path().to_path_buf()
        };

        // Download snapshot if URL is provided
        if let Some(snapshot_url) = self.args.snapshot.url {
            // We don't wanna download our snapshot into an existing database. So only proceed if we don't have an existing db dir
            // or if we have a tempfile path.
            if self.args.db_dir.is_none() || !database_path.exists() {
                info!(target: LOG_TARGET, url = %snapshot_url, path = %database_path.display(), "Downloading snapshot...");

                // Check for version mismatch
                if let Some(snapshot_version) = self.args.snapshot.version {
                    if snapshot_version != self.version_spec {
                        warn!(
                            target: LOG_TARGET,
                            snapshot_version = %snapshot_version,
                            current_version = %self.version_spec,
                            "Snapshot version mismatch. This may cause issues."
                        );
                    }
                }

                let client = reqwest::Client::new();
                if let Err(e) =
                    stream_snapshot_into_file(&snapshot_url, &database_path, &client).await
                {
                    error!(target: LOG_TARGET, error = ?e, "Failed to download snapshot.");
                    // Decide if we should exit or continue with a fresh DB
                    // For now, let's exit as the user explicitly requested a snapshot.
                    return Err(e);
                }
                info!(target: LOG_TARGET, "Snapshot downloaded successfully.");
            } else {
                error!(target: LOG_TARGET, "A database already exists at the given path. If you want to download a new snapshot, please delete the existing database file or provide a different path.");
                return Err(anyhow::anyhow!(
                    "Database file already exists at the specified path."
                ));
            }
        }

        // Calculate optimal runtime allocation early for database configuration
        let cpu_count = num_cpus::get();
        let strategy = AllocationStrategy::from(self.args.runner.allocation_strategy.as_str());
        let allocation = RuntimeAllocation::calculate(
            cpu_count,
            &strategy,
            self.args.runner.query_threads,
            self.args.runner.indexer_threads,
        );

        let mut options = SqliteConnectOptions::from_str(&database_path.to_string_lossy())?
            .create_if_missing(true)
            .with_regexp();

        // Optimize SQLite threading for parallelizable operations
        // SQLite uses auxiliary threads for sorting, indexing, and complex queries
        // Default is 0 (no parallelization), so we enable it for better performance
        // Set to number of available CPUs for maximum parallelization potential
        let sqlite_threads = cpu_count;
        options = options.pragma("threads", sqlite_threads.to_string());

        // Advanced performance settings optimized for indexing + query workload
        options = options.auto_vacuum(SqliteAutoVacuum::None);
        options = options.journal_mode(SqliteJournalMode::Wal);
        options = options.shared_cache(self.args.sql.shared_cache);

        // Use NORMAL for better performance during heavy indexing
        // FULL would be safer but much slower for writes
        options = options.synchronous(SqliteSynchronous::Normal);
        options = options.optimize_on_close(true, None);

        // Performance tuning based on workload
        options = options.pragma("cache_size", self.args.sql.cache_size.to_string());
        options = options.pragma("page_size", self.args.sql.page_size.to_string());

        // Optimize WAL checkpointing for heavy write workloads
        options = options.pragma(
            "wal_autocheckpoint",
            self.args.sql.wal_autocheckpoint.to_string(),
        );

        // Increase busy timeout for concurrent access
        options = options.pragma("busy_timeout", self.args.sql.busy_timeout.to_string());
        options = options.pragma(
            "soft_heap_limit",
            self.args.sql.soft_memory_limit.to_string(),
        );
        options = options.pragma(
            "hard_heap_limit",
            self.args.sql.hard_memory_limit.to_string(),
        );

        // Enable memory status tracking globally
        config_sqlite_memstatus(true);

        // Additional performance optimizations for indexing workload
        let temp_store = self.args.sql.temp_store.clone();
        options = options.pragma("temp_store", temp_store);
        options = options.pragma("mmap_size", self.args.sql.mmap_size.to_string());
        options = options.pragma(
            "journal_size_limit",
            self.args.sql.journal_size_limit.to_string(),
        );

        // Write pool: NO memory limits - critical for indexing performance
        let write_pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(self.args.sql.acquire_timeout))
            .idle_timeout(Some(Duration::from_millis(self.args.sql.idle_timeout)))
            .connect_with(options.clone())
            .await?;

        // Aggressive WAL cleanup
        write_pool
            .execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await?;

        // Readonly pool
        let readonly_options = options.read_only(true);

        // Use more connections for readonly pool to handle concurrent queries
        let max_readonly_connections = cmp::max(
            self.args.sql.max_connections,
            (allocation.query_threads * 2) as u32, // 2 connections per query thread
        );

        let readonly_pool = SqlitePoolOptions::new()
            .min_connections(cmp::min(4, max_readonly_connections)) // Keep some connections warm
            .max_connections(max_readonly_connections)
            .acquire_timeout(Duration::from_millis(self.args.sql.acquire_timeout))
            .idle_timeout(Some(Duration::from_millis(self.args.sql.idle_timeout)))
            .connect_with(readonly_options)
            .await?;

        let mut migrate_handle = write_pool.acquire().await?;
        if let Some(migrations) = self.args.sql.migrations {
            // Create a temporary directory to combine migrations
            let temp_migrations = TempDir::new()?;

            // Copy default migrations first
            let default_migrations_dir =
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../migrations");
            for entry in std::fs::read_dir(default_migrations_dir)? {
                let entry = entry?;
                let target = temp_migrations.path().join(entry.file_name());
                std::fs::copy(entry.path(), target)?;
            }

            // Copy custom migrations
            for entry in std::fs::read_dir(&migrations)? {
                let entry = entry?;
                let target = temp_migrations.path().join(entry.file_name());
                std::fs::copy(entry.path(), target)?;
            }

            // Run combined migrations
            let migrator = sqlx::migrate::Migrator::new(temp_migrations.path()).await?;
            migrator.run(&mut migrate_handle).await?;
        } else {
            sqlx::migrate!("../migrations")
                .run(&mut migrate_handle)
                .await?;
        }

        // Optimize database after schema changes (migrations/indexes)
        sqlx::query("PRAGMA optimize")
            .execute(&mut *migrate_handle)
            .await?;

        drop(migrate_handle);

        if self.args.sql.all_model_indices && !self.args.sql.model_indices.is_empty() {
            warn!(
                target: LOG_TARGET,
                "all_model_indices is true, which will override any specific indices in model_indices"
            );
        }

        // Validate activity tracking configuration
        if self.args.activity.activity_enabled && !self.args.indexing.transactions {
            return Err(anyhow::anyhow!(
                "Activity tracking is enabled but transaction indexing is disabled. \
                 Activity tracking requires transaction data to function. \
                 Please enable transaction indexing with --indexing.transactions or \
                 disable activity tracking with --activity.enabled=false"
            ));
        }

        let historical_models = self.args.sql.historical.clone().into_iter().try_fold(
            HashSet::new(),
            |mut acc, tag| {
                let selector = try_compute_selector_from_tag(&tag)
                    .map_err(|_| anyhow::anyhow!("Invalid model tag: {}", tag))?;
                acc.insert(selector);
                Ok::<HashSet<Felt>, anyhow::Error>(acc)
            },
        )?;

        // Build excluded entrypoints set - use defaults if not specified
        let default_excluded = [
            "execute_from_outside_v3",
            "request_random",
            "submit_random",
            "assert_consumed",
            "deployContract",
            "set_name",
            "register_model",
            "entities",
            "init_contract",
            "upgrade_model",
            "emit_events",
            "emit_event",
            "set_metadata",
        ];

        let activity_excluded_entrypoints: HashSet<String> =
            if self.args.activity.excluded_entrypoints.is_empty() {
                default_excluded.iter().map(|s| s.to_string()).collect()
            } else {
                self.args
                    .activity
                    .excluded_entrypoints
                    .iter()
                    .cloned()
                    .collect()
            };

        let sql_config = SqlConfig {
            all_model_indices: self.args.sql.all_model_indices,
            model_indices: self.args.sql.model_indices.clone(),
            historical_models: historical_models.clone(),
            hooks: self.args.sql.hooks.clone(),
            aggregators: self.args.sql.aggregators.clone(),
            wal_truncate_size_threshold: self.args.sql.wal_truncate_size_threshold,
            optimize_interval: self.args.sql.optimize_interval,
            activity_enabled: self.args.activity.activity_enabled,
            activity_session_timeout: self.args.activity.session_timeout,
            activity_excluded_entrypoints,
            token_attributes: self.args.erc.token_attributes,
            trait_counts: self.args.erc.trait_counts,
            achievement_registration_model_name: self.args.achievement.registration_model_name,
            achievement_progression_model_name: self.args.achievement.progression_model_name,
            search_max_results: self.args.search.max_results,
            search_min_query_length: self.args.search.min_query_length,
            search_prefix_matching: self.args.search.prefix_matching,
            search_return_snippets: self.args.search.return_snippets,
            search_snippet_length: self.args.search.snippet_length,
        };

        let (mut executor, sender) = Executor::new_with_config(
            write_pool.clone(),
            shutdown_tx.clone(),
            provider.clone(),
            sql_config.clone(),
            database_path.clone(),
        )
        .await?;
        let executor_handle = tokio::spawn(async move { executor.run().await });

        let db = Sql::new_with_config(
            readonly_pool.clone(),
            sender.clone(),
            &self.args.indexing.contracts,
            sql_config.clone(),
        )
        .await?;
        let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
        let db = db.with_cache(cache.clone());

        let processors = Arc::new(Processors::default());

        let mut indexing_flags = IndexingFlags::empty();
        if self.args.events.raw {
            indexing_flags.insert(IndexingFlags::RAW_EVENTS);
        }
        let mut fetching_flags = FetchingFlags::empty();
        if self.args.indexing.transactions {
            fetching_flags.insert(FetchingFlags::TRANSACTIONS);
        }
        if self.args.indexing.transaction_receipts {
            fetching_flags.insert(FetchingFlags::TRANSACTION_RECEIPTS);
        }
        if self.args.indexing.preconfirmed {
            fetching_flags.insert(FetchingFlags::PRECONFIRMED_BLOCK);
        }

        let storage = Arc::new(db.clone());
        let controllers = if self.args.indexing.controllers {
            Some(Arc::new(
                ControllersSync::new(storage.clone()).await.unwrap(),
            ))
        } else {
            None
        };

        // Scale max_concurrent_tasks based on indexer threads for better CPU utilization
        let optimal_concurrent_tasks = if self.args.indexing.max_concurrent_tasks == 100 {
            // Default value, scale with indexer threads
            (allocation.indexer_threads * 8).clamp(50, 500) // 8 tasks per thread, reasonable bounds
        } else {
            // User specified, respect their choice
            self.args.indexing.max_concurrent_tasks
        };

        debug!(target: LOG_TARGET,
            cpu_count = cpu_count,
            strategy = ?strategy,
            query_threads = allocation.query_threads,
            indexer_threads = allocation.indexer_threads,
            max_concurrent_tasks = optimal_concurrent_tasks,
            "Runtime allocation calculated"
        );

        let mut engine: Engine<Arc<JsonRpcClient<HttpTransport>>> = Engine::new_with_controllers(
            storage.clone(),
            cache.clone(),
            provider.clone(),
            processors.clone(),
            EngineConfig {
                max_concurrent_tasks: optimal_concurrent_tasks,
                fetcher_config: FetcherConfig {
                    batch_chunk_size: self.args.indexing.batch_chunk_size,
                    blocks_chunk_size: self.args.indexing.blocks_chunk_size,
                    events_chunk_size: self.args.indexing.events_chunk_size,
                    world_block: self.args.indexing.world_block,
                    flags: fetching_flags,
                },
                polling_interval: Duration::from_millis(self.args.indexing.polling_interval),
                flags: indexing_flags,
                event_processor_config: EventProcessorConfig {
                    strict_model_reader: self.args.indexing.strict_model_reader,
                    namespaces: self.args.indexing.namespaces.into_iter().collect(),
                    historical_models,
                    max_metadata_tasks: self.args.erc.max_metadata_tasks,
                    models: self.args.indexing.models.clone().into_iter().collect(),
                    external_contracts: self.args.indexing.external_contracts,
                    external_contract_whitelist: self
                        .args
                        .indexing
                        .external_contract_whitelist
                        .clone()
                        .into_iter()
                        .collect(),
                    metadata_updates: self.args.erc.metadata_updates,
                    metadata_update_whitelist: self
                        .args
                        .erc
                        .metadata_update_whitelist
                        .iter()
                        .filter_map(|s| Felt::from_hex(s.trim()).ok())
                        .collect(),
                    metadata_update_blacklist: self
                        .args
                        .erc
                        .metadata_update_blacklist
                        .iter()
                        .filter_map(|s| Felt::from_hex(s.trim()).ok())
                        .collect(),
                    metadata_updates_only_at_head: self.args.erc.metadata_updates_only_at_head,
                    async_metadata_updates: self.args.erc.async_metadata_updates,
                },
                world_block: self.args.indexing.world_block,
            },
            shutdown_tx.clone(),
            controllers,
        );

        let shutdown_rx = shutdown_tx.subscribe();
        let temp_dir = TempDir::new()?;
        let artifacts_path = self
            .args
            .erc
            .artifacts_path
            .unwrap_or_else(|| Utf8PathBuf::from(temp_dir.path().to_str().unwrap()));

        tokio::fs::create_dir_all(&artifacts_path).await?;
        let absolute_path = artifacts_path.canonicalize_utf8()?;

        // Create messaging instance with configuration
        let messaging_config = MessagingConfig {
            max_age: self.args.messaging.max_age,
            future_tolerance: self.args.messaging.future_tolerance,
            require_timestamp: self.args.messaging.require_timestamp,
        };
        let messaging = Arc::new(Messaging::new(
            messaging_config,
            storage.clone(),
            provider.clone(),
        ));

        let (mut libp2p_relay_server, cross_messaging_tx) = Relay::new_with_peers(
            messaging.clone(),
            self.args.relay.port,
            self.args.relay.webrtc_port,
            self.args.relay.websocket_port,
            self.args.relay.local_key_path,
            self.args.relay.cert_path,
            self.args.relay.peers,
        )
        .expect("Failed to start libp2p relay server");

        let grpc_bind_addr = SocketAddr::new(self.args.grpc.grpc_addr, self.args.grpc.grpc_port);
        let (grpc_addr, grpc_server) = torii_grpc_server::new(
            shutdown_rx,
            storage.clone(),
            messaging.clone(),
            cross_messaging_tx,
            readonly_pool.clone(),
            GrpcConfig {
                subscription_buffer_size: self.args.grpc.subscription_buffer_size,
                optimistic: self.args.grpc.optimistic,
                tcp_keepalive_interval: Duration::from_secs(self.args.grpc.tcp_keepalive_interval),
                http2_keepalive_interval: Duration::from_secs(
                    self.args.grpc.http2_keepalive_interval,
                ),
                http2_keepalive_timeout: Duration::from_secs(
                    self.args.grpc.http2_keepalive_timeout,
                ),
                max_message_size: self.args.grpc.max_message_size,
            },
            Some(grpc_bind_addr),
        )
        .await?;

        let addr = SocketAddr::new(self.args.server.http_addr, self.args.server.http_port);

        let mut proxy_server = Proxy::new(
            addr,
            self.args
                .server
                .http_cors_origins
                .filter(|cors_origins| !cors_origins.is_empty()),
            Some(grpc_addr),
            None,
            absolute_path.clone(),
            Arc::new(readonly_pool.clone()),
            storage.clone(),
            provider.clone(),
            self.version_spec.clone(),
            ProxySettings {
                tcp_keepalive_interval: self.args.grpc.tcp_keepalive_interval,
                http2_keepalive_interval: self.args.grpc.http2_keepalive_interval,
                http2_keepalive_timeout: self.args.grpc.http2_keepalive_timeout,
            },
        );

        // Handle mkcert certificate generation
        let (final_cert_path, final_key_path) = if self.args.server.mkcert {
            if self.args.server.tls_cert_path.is_some() || self.args.server.tls_key_path.is_some() {
                warn!(target: LOG_TARGET, "mkcert flag is set but explicit TLS paths are also provided. Using explicit paths.");
                (
                    self.args.server.tls_cert_path.clone(),
                    self.args.server.tls_key_path.clone(),
                )
            } else {
                match generate_mkcert_certificates().await {
                    Ok((cert_path, key_path)) => {
                        info!(target: LOG_TARGET, cert_path = %cert_path, key_path = %key_path, "Successfully generated mkcert certificates");
                        (Some(cert_path), Some(key_path))
                    }
                    Err(e) => {
                        warn!(target: LOG_TARGET, error = ?e, "Failed to generate mkcert certificates. Falling back to HTTP.");
                        (None, None)
                    }
                }
            }
        } else {
            (
                self.args.server.tls_cert_path.clone(),
                self.args.server.tls_key_path.clone(),
            )
        };

        // Configure TLS if certificates are provided
        if let (Some(cert_path), Some(key_path)) = (&final_cert_path, &final_key_path) {
            let tls_config = torii_server::TlsConfig {
                cert_path: cert_path.clone(),
                key_path: key_path.clone(),
            };

            info!(target: LOG_TARGET, "Starting HTTPS server with TLS certificates");
            proxy_server = proxy_server
                .with_tls_config(tls_config)
                .map_err(|e| anyhow::anyhow!("Failed to configure TLS: {}", e))?;
        } else if final_cert_path.is_some() || final_key_path.is_some() {
            warn!(target: LOG_TARGET, "TLS configuration incomplete. Both tls_cert_path and tls_key_path are required for HTTPS. Falling back to HTTP.");
        }

        let proxy_server = Arc::new(proxy_server);

        let graphql_server = spawn_rebuilding_graphql_server(
            shutdown_tx.clone(),
            readonly_pool.into(),
            proxy_server.clone(),
            messaging.clone(),
            storage.clone(),
        );

        let protocol = if final_cert_path.is_some() && final_key_path.is_some() {
            "https"
        } else {
            "http"
        };

        let gql_endpoint = format!("{}://{}/graphql", protocol, addr);
        let mcp_endpoint = format!("{}://{}/mcp", protocol, addr);
        let sql_endpoint = format!("{}://{}/sql", protocol, addr);

        let encoded: String = form_urlencoded::byte_serialize(
            gql_endpoint.replace("0.0.0.0", "localhost").as_bytes(),
        )
        .collect();
        let explorer_url = format!("https://worlds.dev/torii?url={}", encoded);
        info!(target: LOG_TARGET, endpoint = %addr, protocol = %protocol, "Starting torii endpoint.");
        info!(target: LOG_TARGET, endpoint = %grpc_addr, "Serving gRPC endpoint.");
        info!(target: LOG_TARGET, endpoint = %gql_endpoint, "Serving Graphql playground.");
        info!(target: LOG_TARGET, endpoint = %sql_endpoint, "Serving SQL playground.");
        info!(target: LOG_TARGET, endpoint = %mcp_endpoint, "Serving MCP endpoint.");
        info!(target: LOG_TARGET, url = %explorer_url, "Serving World Explorer.");
        info!(target: LOG_TARGET, path = %artifacts_path, "Serving ERC artifacts at path");

        if self.args.runner.explorer {
            if let Err(e) = webbrowser::open(&explorer_url) {
                error!(target: LOG_TARGET, error = ?e, "Failed to open World Explorer in browser.");
            }
        }

        if self.args.metrics.metrics {
            let addr = SocketAddr::new(
                self.args.metrics.metrics_addr,
                self.args.metrics.metrics_port,
            );
            info!(target: LOG_TARGET, %addr, "Starting metrics endpoint.");
            let prometheus_handle = PrometheusRecorder::install("torii")?;
            let server = dojo_metrics::Server::new(prometheus_handle).with_process_metrics();
            tokio::spawn(server.start(addr));
        }

        // Create dedicated runtimes
        let query_runtime = create_query_runtime(allocation.query_threads);
        let indexer_runtime = create_indexer_runtime(allocation.indexer_threads);

        // Move engine to dedicated indexer runtime for CPU isolation
        let engine_handle = indexer_runtime
            .handle()
            .spawn(async move { engine.start().await });

        let proxy_server_handle =
            tokio::spawn(async move { proxy_server.start(shutdown_tx.subscribe()).await });

        // Spawn user-facing query services on dedicated API runtime for better performance isolation
        let graphql_server_handle = query_runtime.handle().spawn(graphql_server);

        let grpc_server_handle = query_runtime.handle().spawn(grpc_server);

        let libp2p_relay_server_handle =
            tokio::spawn(async move { libp2p_relay_server.run().await });

        // Macro to handle task results uniformly
        macro_rules! handle_task {
            ($result:expr, $name:literal) => {
                match $result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(anyhow::anyhow!("{} failed: {}", $name, e)),
                    Err(e) => Err(anyhow::anyhow!("{} task panicked: {}", $name, e)),
                }
            };
            // For tasks that return () directly (no inner Result)
            ($result:expr, $name:literal, void) => {
                $result
                    .map_err(|e| anyhow::anyhow!("{} task panicked: {}", $name, e))
                    .map(|_| ())
            };
        }

        // Wait for shutdown signal or any task completion
        let result = tokio::select! {
            res = engine_handle => handle_task!(res, "Engine"),
            res = executor_handle => handle_task!(res, "Executor"),
            res = proxy_server_handle => handle_task!(res, "Proxy server"),
            res = graphql_server_handle => handle_task!(res, "GraphQL server", void),
            res = grpc_server_handle => handle_task!(res, "gRPC server"),
            res = libp2p_relay_server_handle => handle_task!(res, "LibP2P relay", void),
            _ = dojo_utils::signal::wait_signals() => {
                info!(target: LOG_TARGET, "Shutdown signal received, cleaning up...");
                Ok(())
            },
        };

        // Properly shutdown runtimes
        query_runtime.shutdown();
        indexer_runtime.shutdown();

        info!(target: LOG_TARGET, "Shutdown complete");
        result
    }
}

async fn spawn_rebuilding_graphql_server<P: Provider + Sync + Send + Clone + Debug + 'static>(
    shutdown_tx: Sender<()>,
    pool: Arc<SqlitePool>,
    proxy_server: Arc<Proxy<P>>,
    messaging: Arc<Messaging<P>>,
    storage: Arc<dyn ReadOnlyStorage>,
) {
    let mut broker = MemoryBroker::<ModelUpdate>::subscribe();

    loop {
        let shutdown_rx = shutdown_tx.subscribe();
        let (new_addr, new_server) =
            torii_graphql::server::new(shutdown_rx, &pool, messaging.clone(), storage.clone())
                .await;

        tokio::spawn(new_server);

        proxy_server.set_graphql_addr(new_addr).await;

        // Break the loop if there are no more events
        if broker.next().await.is_none() {
            break;
        } else {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn verify_contracts_deployed(
    provider: &JsonRpcClient<HttpTransport>,
    contracts: &[ContractDefinition],
) -> anyhow::Result<Vec<ContractDefinition>> {
    // Create a future for each contract verification
    let verification_futures = contracts.iter().map(|contract| {
        let contract = contract.clone();
        async move {
            let result = provider
                .get_class_at(BlockId::Tag(BlockTag::PreConfirmed), contract.address)
                .await;
            (contract, result)
        }
    });

    // Run all verifications concurrently
    let results = join_all(verification_futures).await;

    // Collect undeployed contracts
    let undeployed = results
        .into_iter()
        .filter_map(|(contract, result)| match result {
            Ok(_) => None,
            Err(_) => Some(contract),
        })
        .collect();

    Ok(undeployed)
}

/// Streams a snapshot into a file, displaying progress and handling potential errors.
///
/// # Arguments
/// * `url` - The URL to download from.
/// * `destination_path` - The path to save the downloaded file.
/// * `client` - An instance of `reqwest::Client`.
///
/// # Returns
/// * `Ok(())` if the download is successful.
/// * `Err(anyhow::Error)` if any error occurs during download or file writing.
async fn stream_snapshot_into_file(
    url: &str,
    destination_path: &Path,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let response = client.get(url).send().await?.error_for_status()?;
    let total_size = response.content_length().unwrap_or(0);

    let span = info_span!("download_snapshot", url);
    span.pb_set_style(
        &indicatif::ProgressStyle::default_bar()
            .template(&create_progress_bar_template())?
            .progress_chars("⣿⣤⠀")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    span.pb_set_length(total_size);
    span.pb_set_message(&format!(" {:.1}s", 0.0));

    let instrumented_future = async {
        let mut file = File::create(destination_path).await?;
        let mut downloaded: u64 = 0;
        let mut stream = response.bytes_stream();
        let start_time = std::time::Instant::now();

        while let Some(item) = stream.next().await {
            let chunk = item?;
            file.write_all(&chunk).await?;
            let new = cmp::min(downloaded.saturating_add(chunk.len() as u64), total_size);
            downloaded = new;
            let elapsed = start_time.elapsed().as_secs_f64();
            Span::current().pb_set_position(new);
            Span::current().pb_set_message(&format!(" {:.1}s", elapsed));
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        Span::current().pb_set_message(&format!(" {:.1}s", elapsed));
        Ok(())
    }
    .instrument(span);

    instrumented_future.await
}

async fn generate_mkcert_certificates() -> anyhow::Result<(String, String)> {
    // Check if mkcert is installed
    let check_output = tokio::process::Command::new("mkcert")
        .arg("-version")
        .output()
        .await;

    if check_output.is_err() {
        return Err(anyhow::anyhow!("mkcert is not installed. Please install mkcert first: https://github.com/FiloSottile/mkcert"));
    }

    // Create directory for certificates in temp dir
    let cert_dir = std::env::temp_dir().join("torii-certs");
    tokio::fs::create_dir_all(&cert_dir).await?;

    let cert_path = cert_dir.join("localhost.pem");
    let key_path = cert_dir.join("localhost-key.pem");

    // Install the CA certificate
    let install_output = tokio::process::Command::new("mkcert")
        .arg("-install")
        .output()
        .await?;

    if !install_output.status.success() {
        return Err(anyhow::anyhow!(
            "Failed to install mkcert CA: {}",
            String::from_utf8_lossy(&install_output.stderr)
        ));
    }

    // Generate certificates for localhost and 127.0.0.1
    let output = tokio::process::Command::new("mkcert")
        .arg("-cert-file")
        .arg(&cert_path)
        .arg("-key-file")
        .arg(&key_path)
        .arg("localhost")
        .arg("127.0.0.1")
        .output()
        .await?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "Failed to generate mkcert certificates: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok((
        cert_path.to_string_lossy().to_string(),
        key_path.to_string_lossy().to_string(),
    ))
}
