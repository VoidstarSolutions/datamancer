//! Cycle-4 health/observability e2e: spawn the real daemon, assert the
//! health op, disabled-provider enrichment, hot enable, and the push plane.
//! Needs a live iceoryx2 runtime:
//! `cargo test -p datamancerd --test health_observability_e2e -- --ignored --test-threads=1`
//!
//! Mirrors `config_service_e2e.rs`'s fixtures exactly: zero `[provider.*]`
//! sections, `service_prefix = "health-e2e"`, `publish_interval_ms = 200`,
//! scrubbed `ALPACA_*` env, `DATAMANCER_CREDENTIALS_FILE` pinned to the
//! tempdir, and the shared `stop_daemon()` pid-from-lockfile helper — all
//! copied locally per that file's pattern rather than imported across test
//! binaries. Like `credential_broker_e2e.rs`, the daemon is spawned directly
//! via `Command` (not the facade's spawner) so its env can be scrubbed; the
//! facade then `ensure`s against the already-running socket.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use datamancer_client::app::{AppHandle, EnsureConfig};
use datamancer_core::ProviderState;
use tokio_stream::StreamExt;

/// Write a daemon config with **no** provider sections and return the
/// config/socket paths. Same shape as `config_service_e2e.rs`'s
/// `write_config_no_providers`, with the service prefix/publish interval
/// this suite's brief calls for.
fn write_config_no_providers(dir: &std::path::Path) -> (PathBuf, PathBuf) {
    let socket = dir.join("control.sock");
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[server]
admin_socket = "{}"
service_prefix = "health-e2e"

[diagnostics]
publish_interval_ms = 200
"#,
            socket.display()
        ),
    )
    .unwrap();
    (config, socket)
}

fn ensure_cfg(dir: &std::path::Path, name: &str, socket: PathBuf, config: PathBuf) -> EnsureConfig {
    let mut cfg = EnsureConfig::new(env!("CARGO_BIN_EXE_datamancerd"), name);
    cfg.config_path = Some(config);
    cfg.control_socket = Some(socket);
    cfg.log_path = Some(dir.join("daemon.log"));
    cfg.ready_timeout = Duration::from_secs(15);
    cfg
}

/// Kill the daemon we spawned: recover the pid from the single-instance
/// lockfile (documented as the holder's pid) and TERM it. Same helper as
/// `config_service_e2e.rs`/`app_facade_e2e.rs`/`credential_broker_e2e.rs`.
fn stop_daemon() {
    let lock = directories::ProjectDirs::from("", "", "datamancer")
        .unwrap()
        .data_dir()
        .join("datamancerd.lock");
    if let Ok(pid) = std::fs::read_to_string(&lock) {
        let pid = pid.trim().to_string();
        if !pid.is_empty() {
            let _ = std::process::Command::new("kill").arg(&pid).status();
            std::thread::sleep(Duration::from_millis(1500));
        }
    }
}

#[tokio::test]
#[ignore = "needs a live iceoryx2 runtime and spawns the daemon binary"]
#[allow(clippy::too_many_lines)] // one sequential e2e narrative; splitting hides the story
async fn health_reflects_disabled_enabled_and_pushes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (config, socket) = write_config_no_providers(dir.path());

    // 1. Spawn with zero [provider.*] sections. Spawn directly (not through
    // the facade's spawner) with all four ALPACA_* env vars scrubbed and
    // DATAMANCER_CREDENTIALS_FILE pinned to the tempdir — same env hygiene
    // as `credential_broker_e2e.rs`/`config_service_e2e.rs`.
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_datamancerd"))
        .arg("--config")
        .arg(&config)
        .env(
            "DATAMANCER_CREDENTIALS_FILE",
            dir.path().join("credentials.json"),
        )
        .env_remove("ALPACA_PAPER_API_KEY_ID")
        .env_remove("ALPACA_PAPER_API_SECRET_KEY")
        .env_remove("ALPACA_LIVE_API_KEY_ID")
        .env_remove("ALPACA_LIVE_API_SECRET_KEY")
        .spawn()
        .expect("spawn datamancerd");

    let bind_deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(
            Instant::now() < bind_deadline,
            "daemon socket never appeared"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let cfg = ensure_cfg(dir.path(), "health-e2e", socket.clone(), config.clone());
    let (mut handle, events) = AppHandle::ensure(cfg)
        .await
        .expect("ensure must find the already-running daemon");

    // 2. health(): schema_version == 2, daemon.version == daemon crate
    // version, credential_backend set, every provider Disabled.
    let health = handle.health().await.expect("health");
    assert_eq!(
        health.schema_version, 2,
        "schema_version must be pinned to 2"
    );
    assert_eq!(
        health.daemon.version.as_deref(),
        Some(handle.daemon_version()),
        "daemon.version must be daemon-stamped to the running daemon crate version"
    );
    assert!(
        health.daemon.credential_backend.is_some(),
        "daemon.credential_backend must be stamped"
    );
    assert!(
        !health.providers.is_empty(),
        "every compiled-in provider must be enumerated even with zero [provider.*] sections"
    );
    assert!(
        health
            .providers
            .iter()
            .all(|p| p.state == ProviderState::Disabled),
        "every provider must be Disabled with no [provider.*] sections present: {:?}",
        health.providers
    );

    // 3. configure-provider alpaca_crypto {account_type: "paper"} ->
    // health(): alpaca_crypto no longer Disabled, alpaca (untouched) still
    // Disabled.
    handle
        .configure_provider(
            "alpaca-crypto",
            serde_json::json!({"account_type": "paper"}),
        )
        .await
        .expect("configure_provider alpaca-crypto");

    let health_after_configure = handle
        .health()
        .await
        .expect("health after configure-provider");
    let alpaca_crypto_state = health_after_configure
        .providers
        .iter()
        .find(|p| p.provider.as_str() == "alpaca-crypto")
        .expect("alpaca-crypto must be enumerated")
        .state;
    assert_ne!(
        alpaca_crypto_state,
        ProviderState::Disabled,
        "alpaca-crypto must leave Disabled once configured, even without credentials"
    );
    let alpaca_state = health_after_configure
        .providers
        .iter()
        .find(|p| p.provider.as_str() == "alpaca")
        .expect("alpaca must be enumerated")
        .state;
    assert_eq!(
        alpaca_state,
        ProviderState::Disabled,
        "alpaca must remain Disabled — only alpaca-crypto was configured"
    );

    // 4. watch_health(): a view arrives within 3s (publish cadence 200ms)
    // and carries the same schema_version and provider states.
    let mut stream = handle.watch_health();
    let pushed = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("a health view must arrive on the push plane within 3s")
        .expect("the push stream must not end while the daemon is alive");
    assert_eq!(pushed.schema_version, health_after_configure.schema_version);
    let pushed_alpaca_crypto_state = pushed
        .providers
        .iter()
        .find(|p| p.provider.as_str() == "alpaca-crypto")
        .expect("alpaca-crypto must be enumerated in the pushed view")
        .state;
    assert_ne!(pushed_alpaca_crypto_state, ProviderState::Disabled);
    let pushed_alpaca_state = pushed
        .providers
        .iter()
        .find(|p| p.provider.as_str() == "alpaca")
        .expect("alpaca must be enumerated in the pushed view")
        .state;
    assert_eq!(pushed_alpaca_state, ProviderState::Disabled);
    drop(stream);

    // 5. shutdown_daemon; drop.
    drop(events);
    handle.shutdown_daemon().await.expect("shutdown_daemon");

    let status = child
        .wait()
        .expect("daemon process must exit after shutdown");
    assert!(status.success(), "daemon must exit 0 on graceful shutdown");

    stop_daemon();
}
