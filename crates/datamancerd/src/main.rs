//! `datamancerd` — the standalone datamancer server.
//!
//! A thin binary that wraps the `datamancer` library and serves multiple
//! consumer processes on the same host. It introduces **no** new ordering,
//! transport, or event-model semantics; its job is composition + process
//! lifecycle + a control surface:
//!
//! 1. Build a [`datamancer::Datamancer`] from a TOML config.
//! 2. Accept client connections over a control endpoint — a Unix-domain socket
//!    on Unix, an owner-only-DACL named pipe on Windows (`win_control`); per
//!    client create a multiplexing client session wired to a per-client
//!    iceoryx2 data-plane service, and publish the diagnostics snapshot on the
//!    diagnostics plane.
//! 3. Hold authoritative sessions alive as the cross-process lifecycle anchor.
//! 4. Expose a control surface for runtime `subscribe`/`unsubscribe`.
//! 5. Graceful shutdown: stop accepting, flush sinks + tap log, drain.
//!
//! Access control is **same-host, single-operator**: Unix gates each
//! privileged op on the peer's uid (`SO_PEERCRED`); Windows restricts the pipe
//! with an owner-only DACL so only the daemon's user can open it (see
//! `win_control`). This is **not** a network-safe surface.
//!
//! EXT-1: the crate is `#![forbid(unsafe_code)]` everywhere except Windows,
//! where the named-pipe transport needs Win32 FFI. There it relaxes to
//! `#![deny(unsafe_code)]` with a *single* scoped `#[allow(unsafe_code)]`
//! confined to the audited `win_control` module.
#![cfg_attr(not(windows), forbid(unsafe_code))]
#![cfg_attr(windows, deny(unsafe_code))]

mod config;
mod config_class;
mod config_hub;
mod control;
mod credentials;
mod error;
mod paths;
mod server;
mod shutdown;
mod single_instance;
#[cfg(feature = "web-ui")]
mod web;
#[cfg(windows)]
mod win_control;
#[cfg(feature = "ws")]
mod ws;

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
    /// Path to the TOML config file. Defaults to the platform config
    /// directory (scaffolded with a commented default on first run).
    #[arg(long, short)]
    config: Option<std::path::PathBuf>,
}

/// Best-effort read of just the `[log]` section, before the lock/scaffold
/// path runs. Never fails: any problem falls back to defaults, and the real
/// `Config::load` reports it properly later.
fn peek_log_config(explicit: Option<&std::path::Path>) -> config::LogConfig {
    // Unlike `Config`, this reads a full config file but only cares about
    // `[log]` — it must not reject the file for unrelated unknown fields (the
    // real `Config` type is exhaustive; this peek is deliberately not).
    #[derive(serde::Deserialize, Default)]
    struct Peek {
        #[serde(default)]
        log: config::LogConfig,
    }
    let path = match explicit {
        Some(p) => p.to_path_buf(),
        None => match paths::default_config_path() {
            Some(p) => p,
            None => return config::LogConfig::default(),
        },
    };
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| toml::from_str::<Peek>(&s).ok())
        .map(|p| p.log)
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    let log = peek_log_config(args.config.as_deref());
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log.level.clone()));
    match log.format {
        config::LogFormat::Text => tracing_subscriber::fmt().with_env_filter(filter).init(),
        config::LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init(),
    }

    match run(args).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "datamancerd failed");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<()> {
    // Acquire the global single-instance lock before touching any shared
    // resource (config scaffold, tap-log/cache DBs, iceoryx2 node). Held for
    // the whole process; released by the kernel on exit. A second launch —
    // whatever config it is given — fails here.
    let _instance = single_instance::InstanceLock::acquire()?;
    let config_path = paths::resolve_config_path(args.config)?;
    tracing::info!(path = %config_path.display(), "loading config");
    let config = Config::load(&config_path)?;
    server::Server::bootstrap(config, config_path)
        .await?
        .run()
        .await
}

#[cfg(test)]
mod main_tests {
    use super::*;

    #[test]
    fn peek_log_config_reads_valid_section() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[log]\nlevel = \"debug\"\nformat = \"json\"\n").expect("write");
        let log = peek_log_config(Some(&path));
        assert_eq!(log.level, "debug");
        assert_eq!(log.format, config::LogFormat::Json);
    }

    #[test]
    fn peek_log_config_defaults_on_missing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("missing.toml");
        let log = peek_log_config(Some(&path));
        assert_eq!(log, config::LogConfig::default());
    }

    #[test]
    fn peek_log_config_defaults_on_unparseable_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "this is not [ valid toml").expect("write");
        let log = peek_log_config(Some(&path));
        assert_eq!(log, config::LogConfig::default());
    }

    #[test]
    fn peek_log_config_ignores_unrelated_unknown_fields() {
        // The peek must not `deny_unknown_fields` on the whole document —
        // only `[log]` matters here; the real `Config::load` validates the
        // rest later.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "[provider.alpaca]\naccount_type = \"paper\"\n\n[log]\nlevel = \"warn\"\n",
        )
        .expect("write");
        let log = peek_log_config(Some(&path));
        assert_eq!(log.level, "warn");
        assert_eq!(log.format, config::LogFormat::Text);
    }
}
