use std::str::FromStr;
use std::sync::Arc;

use crate::cache::ModelCache;
use crate::executor::Executor;
use crate::types::{Contract, ContractType};
use crate::Sql;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use starknet::core::types::Felt;
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::JsonRpcClient;
use tempfile::NamedTempFile;
use tokio::sync::broadcast;

impl Sql {
    /// Creates a new temporary file and returns a new Sql instance.
    /// Only the world contract is added to the contracts list.
    /// TODO: think about a more mature interface for this one when more tests are using it.
    pub async fn new_tmp_file(
        world_address: Felt,
        provider: Arc<JsonRpcClient<HttpTransport>>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Self {
        let tempfile = NamedTempFile::new().unwrap();
        let path = tempfile.path().to_string_lossy();

        let mut options = SqliteConnectOptions::from_str(&path)
            .unwrap()
            .create_if_missing(true)
            .with_regexp();

        // Performance settings
        options = options.auto_vacuum(SqliteAutoVacuum::None);
        options = options.journal_mode(SqliteJournalMode::Wal);
        options = options.synchronous(SqliteSynchronous::Normal);
        options = options.optimize_on_close(true, None);
        options = options.pragma("cache_size", "-500000");
        options = options.pragma("page_size", "32768");
        options = options.pragma("wal_autocheckpoint", "1000");
        options = options.pragma("busy_timeout", "60000");

        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(options)
            .await
            .unwrap();

        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        let (mut executor, sender) = Executor::new(pool.clone(), shutdown_tx.clone(), provider)
            .await
            .unwrap();

        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let model_cache = Arc::new(ModelCache::new(pool.clone()).await.unwrap());

        Sql::new(
            pool.clone(),
            sender,
            &[Contract {
                address: world_address,
                r#type: ContractType::WORLD,
            }],
            model_cache,
        )
        .await
        .unwrap()
    }
}
