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

use clap::Parser;
use cli::Cli;
use torii_runner::Runner;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

mod cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set the global tracing subscriber
    let filter_layer =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("torii=info"));

    let indicatif_layer = IndicatifLayer::new();

    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(filter_layer)
        .with(indicatif_layer)
        .init();

    let args = Cli::parse().args.with_config_file()?;
    let runner = Runner::new(args, env!("TORII_VERSION_SPEC").to_string());
    runner.run().await?;
    Ok(())
}
