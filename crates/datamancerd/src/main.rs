//! `datamancerd` — the standalone datamancer server.
//!
//! A thin binary that wraps the `datamancer` library and serves multiple
//! consumer processes on the same host. It introduces **no** new ordering,
//! transport, or event-model semantics; its job is composition + process
//! lifecycle + a control surface:
//!
//! 1. Build a [`datamancer::Datamancer`] from a TOML config.
//! 2. Accept client connections over a Unix-domain control socket; per client
//!    create a multiplexing client session wired to a per-client iceoryx2
//!    data-plane service, and publish the diagnostics snapshot on the
//!    diagnostics plane.
//! 3. Hold authoritative sessions alive as the cross-process lifecycle anchor.
//! 4. Expose a control surface for runtime `subscribe`/`unsubscribe`.
//! 5. Graceful shutdown: stop accepting, flush sinks + tap log, drain.
//!
//! Access control is **filesystem permissions on the control socket only**
//! (same-host, single-operator). This is **not** a network-safe surface.
#![forbid(unsafe_code)]

mod config;
mod control;
mod error;
mod server;
mod shutdown;

use std::path::PathBuf;

use clap::Parser;

use crate::config::Config;
use crate::error::Result;

/// Command-line arguments.
#[derive(Debug, Parser)]
#[command(
    name = "datamancerd",
    about = "Standalone datamancer market-data server"
)]
struct Args {
    /// Path to the TOML config file.
    #[arg(long, short)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "datamancerd failed");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    server::Server::bootstrap(config).await?.run().await
}
