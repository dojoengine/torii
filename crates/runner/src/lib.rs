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
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use camino::Utf8PathBuf;
use constants::UDC_ADDRESS;
use dojo_metrics::exporters::prometheus::PrometheusRecorder;
use dojo_types::naming::compute_selector_from_tag;
use dojo_world::contracts::world::WorldContractReader;
use futures::future::join_all;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::SqlitePool;
use starknet::core::types::{BlockId, BlockTag};
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider};
use tempfile::{NamedTempFile, TempDir};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use torii_cli::ToriiArgs;
use torii_grpc_server::GrpcConfig;
use torii_indexer::engine::{Engine, EngineConfig, IndexingFlags};
use torii_libp2p_relay::Relay;
use torii_processors::{EventProcessorConfig, Processors};
use torii_server::proxy::Proxy;
use torii_sqlite::cache::ModelCache;
use torii_sqlite::executor::Executor;
use torii_sqlite::simple_broker::SimpleBroker;
use torii_sqlite::types::{Contract, ContractType, Model};
use torii_sqlite::{Sql, SqlConfig};
use tracing::{error, info, info_span, warn, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::form_urlencoded;

mod constants;

use crate::constants::LOG_TARGET;

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

        let world_address = if let Some(world_address) = self.args.world_address {
            world_address
        } else {
            return Err(anyhow::anyhow!("Please specify a world address."));
        };

        self.args.indexing.contracts.push(Contract {
            address: world_address,
            r#type: ContractType::WORLD,
        });

        if self.args.indexing.controllers {
            self.args.indexing.contracts.push(Contract {
                address: UDC_ADDRESS,
                r#type: ContractType::UDC,
            });
        }

        // Setup cancellation for graceful shutdown
        let (shutdown_tx, _) = broadcast::channel(1);

        let shutdown_tx_clone = shutdown_tx.clone();
        ctrlc::set_handler(move || {
            let _ = shutdown_tx_clone.send(());
        })
        .expect("Error setting Ctrl-C handler");

        let transport = HttpTransport::new(self.args.rpc.clone()).with_header(
            "User-Agent".to_string(),
            format!("Torii/{}", self.version_spec),
        );
        let provider: Arc<_> = JsonRpcClient::new(transport).into();

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

        let mut options = SqliteConnectOptions::from_str(&database_path.to_string_lossy())?
            .create_if_missing(true)
            .with_regexp();

        // Set the number of threads based on CPU count
        let cpu_count = std::thread::available_parallelism().unwrap().get();
        let thread_count = cmp::min(cpu_count, 8);
        options = options.pragma("threads", thread_count.to_string());

        // Performance settings
        options = options.auto_vacuum(SqliteAutoVacuum::None);
        options = options.journal_mode(SqliteJournalMode::Wal);
        options = options.synchronous(SqliteSynchronous::Normal);
        options = options.optimize_on_close(true, None);
        options = options.pragma("cache_size", self.args.sql.cache_size.to_string());
        options = options.pragma("page_size", self.args.sql.page_size.to_string());
        options = options.pragma(
            "wal_autocheckpoint",
            self.args.sql.wal_autocheckpoint.to_string(),
        );
        options = options.pragma("busy_timeout", self.args.sql.busy_timeout.to_string());

        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(self.args.indexing.max_concurrent_tasks as u32)
            .connect_with(options.clone())
            .await?;

        let readonly_options = options.read_only(true);
        let readonly_pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(100)
            .connect_with(readonly_options)
            .await?;

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
            migrator.run(&pool).await?;
        } else {
            sqlx::migrate!("../migrations").run(&pool).await?;
        }

        // Get world address
        let world = WorldContractReader::new(world_address, provider.clone());

        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), provider.clone()).await?;
        let executor_handle = tokio::spawn(async move { executor.run().await });

        let model_cache = Arc::new(ModelCache::new(readonly_pool.clone()).await?);

        if self.args.sql.all_model_indices && !self.args.sql.model_indices.is_empty() {
            warn!(
                target: LOG_TARGET,
                "all_model_indices is true, which will override any specific indices in model_indices"
            );
        }

        let historical_models = self
            .args
            .sql
            .historical
            .clone()
            .into_iter()
            .map(|tag| compute_selector_from_tag(&tag))
            .collect::<HashSet<_>>();
        let db = Sql::new_with_config(
            pool.clone(),
            sender.clone(),
            &self.args.indexing.contracts,
            model_cache.clone(),
            SqlConfig {
                all_model_indices: self.args.sql.all_model_indices,
                model_indices: self.args.sql.model_indices.clone(),
                historical_models: historical_models.clone(),
                hooks: self.args.sql.hooks.clone(),
                max_metadata_tasks: self.args.erc.max_metadata_tasks,
            },
        )
        .await?;

        let processors = Processors::default();

        let mut flags = IndexingFlags::empty();
        if self.args.indexing.transactions {
            flags.insert(IndexingFlags::TRANSACTIONS);
        }
        if self.args.events.raw {
            flags.insert(IndexingFlags::RAW_EVENTS);
        }
        if self.args.indexing.pending {
            flags.insert(IndexingFlags::PENDING_BLOCKS);
        }

        let mut engine: Engine<Arc<JsonRpcClient<HttpTransport>>> = Engine::new(
            world,
            db.clone(),
            provider.clone(),
            processors,
            EngineConfig {
                max_concurrent_tasks: self.args.indexing.max_concurrent_tasks,
                blocks_chunk_size: self.args.indexing.blocks_chunk_size,
                events_chunk_size: self.args.indexing.events_chunk_size,
                batch_chunk_size: self.args.indexing.batch_chunk_size,
                polling_interval: Duration::from_millis(self.args.indexing.polling_interval),
                flags,
                event_processor_config: EventProcessorConfig {
                    strict_model_reader: self.args.indexing.strict_model_reader,
                    namespaces: self.args.indexing.namespaces.into_iter().collect(),
                    historical_models,
                },
                world_block: self.args.indexing.world_block,
            },
            shutdown_tx.clone(),
            &self.args.indexing.contracts,
        );

        let shutdown_rx = shutdown_tx.subscribe();
        let (grpc_addr, grpc_server) = torii_grpc_server::new(
            shutdown_rx,
            &readonly_pool,
            world_address,
            model_cache,
            GrpcConfig {
                subscription_buffer_size: self.args.grpc.subscription_buffer_size,
            },
        )
        .await?;

        let temp_dir = TempDir::new()?;
        let artifacts_path = self
            .args
            .erc
            .artifacts_path
            .unwrap_or_else(|| Utf8PathBuf::from(temp_dir.path().to_str().unwrap()));

        tokio::fs::create_dir_all(&artifacts_path).await?;
        let absolute_path = artifacts_path.canonicalize_utf8()?;

        let (artifacts_addr, artifacts_server) = torii_server::artifacts::new(
            shutdown_tx.subscribe(),
            &absolute_path,
            readonly_pool.clone(),
        )
        .await?;

        let mut libp2p_relay_server = Relay::new_with_peers(
            db,
            provider.clone(),
            self.args.relay.port,
            self.args.relay.webrtc_port,
            self.args.relay.websocket_port,
            self.args.relay.local_key_path,
            self.args.relay.cert_path,
            self.args.relay.peers,
        )
        .expect("Failed to start libp2p relay server");

        let addr = SocketAddr::new(self.args.server.http_addr, self.args.server.http_port);

        let proxy_server = Arc::new(Proxy::new(
            addr,
            self.args
                .server
                .http_cors_origins
                .filter(|cors_origins| !cors_origins.is_empty()),
            Some(grpc_addr),
            None,
            Some(artifacts_addr),
            Arc::new(readonly_pool.clone()),
            self.version_spec.clone(),
        ));

        let graphql_server = spawn_rebuilding_graphql_server(
            shutdown_tx.clone(),
            readonly_pool.into(),
            proxy_server.clone(),
        );

        let gql_endpoint = format!("{addr}/graphql");
        let mcp_endpoint = format!("{addr}/mcp");
        let sql_endpoint = format!("{addr}/sql");

        let encoded: String = form_urlencoded::byte_serialize(
            gql_endpoint.replace("0.0.0.0", "localhost").as_bytes(),
        )
        .collect();
        let explorer_url = format!("https://worlds.dev/torii?url={}", encoded);
        info!(target: LOG_TARGET, endpoint = %addr, "Starting torii endpoint.");
        info!(target: LOG_TARGET, endpoint = %gql_endpoint, "Serving Graphql playground.");
        info!(target: LOG_TARGET, endpoint = %sql_endpoint, "Serving SQL playground.");
        info!(target: LOG_TARGET, endpoint = %mcp_endpoint, "Serving MCP endpoint.");
        info!(target: LOG_TARGET, url = %explorer_url, "Serving World Explorer.");
        info!(target: LOG_TARGET, path = %artifacts_path, "Serving ERC artifacts at path");

        if self.args.runner.explorer {
            if let Err(e) = webbrowser::open(&explorer_url) {
                error!(target: LOG_TARGET, error = ?e, "Opening World Explorer in the browser.");
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

        let engine_handle = tokio::spawn(async move { engine.start().await });
        let proxy_server_handle =
            tokio::spawn(async move { proxy_server.start(shutdown_tx.subscribe()).await });
        let graphql_server_handle = tokio::spawn(graphql_server);
        let grpc_server_handle = tokio::spawn(grpc_server);
        let libp2p_relay_server_handle =
            tokio::spawn(async move { libp2p_relay_server.run().await });
        let artifacts_server_handle = tokio::spawn(artifacts_server);

        tokio::select! {
            res = engine_handle => res??,
            res = executor_handle => res??,
            res = proxy_server_handle => res??,
            res = graphql_server_handle => res?,
            res = grpc_server_handle => res??,
            res = libp2p_relay_server_handle => res?,
            res = artifacts_server_handle => res?,
            _ = dojo_utils::signal::wait_signals() => {},
        };

        Ok(())
    }
}

async fn spawn_rebuilding_graphql_server(
    shutdown_tx: Sender<()>,
    pool: Arc<SqlitePool>,
    proxy_server: Arc<Proxy>,
) {
    let mut broker = SimpleBroker::<Model>::subscribe();

    loop {
        let shutdown_rx = shutdown_tx.subscribe();
        let (new_addr, new_server) = torii_graphql::server::new(shutdown_rx, &pool).await;

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
    contracts: &[Contract],
) -> anyhow::Result<Vec<Contract>> {
    // Create a future for each contract verification
    let verification_futures = contracts.iter().map(|contract| {
        let contract = *contract;
        async move {
            let result = provider
                .get_class_at(BlockId::Tag(BlockTag::Pending), contract.address)
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
            .template(
                "{msg} [{elapsed_precise}] \n[{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {percent}%",
            )?
            .progress_chars("█▓░"),
    );
    span.pb_set_length(total_size);
    span.pb_set_message(&format!("Downloading {}", url));

    let instrumented_future = async {
        let mut file = File::create(destination_path).await?;
        let mut downloaded: u64 = 0;
        let mut stream = response.bytes_stream();

        while let Some(item) = stream.next().await {
            let chunk = item?;
            file.write_all(&chunk).await?;
            let new = cmp::min(downloaded.saturating_add(chunk.len() as u64), total_size);
            downloaded = new;
            Span::current().pb_set_position(new);
        }

        Span::current().pb_set_message("Downloaded snapshot successfully");
        Ok(())
    }
    .instrument(span);

    instrumented_future.await
}
