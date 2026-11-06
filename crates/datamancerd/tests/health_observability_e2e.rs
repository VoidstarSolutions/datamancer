//! Cycle-4 health/observability e2e: spawn the real daemon, assert the
//! health op, disabled-provider enrichment, hot enable, and the push plane.
//! Needs a live iceoryx2 runtime:
//! `cargo test -p datamancerd --test health_observability_e2e -- --ignored --test-threads=1`
//!
//! The per-surface health test runs against the hermetic scripted
//! provider and needs the `scripted-provider` feature:
//! `cargo test -p datamancerd --features scripted-provider --test health_observability_e2e -- --ignored --test-threads=1`
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

#[cfg(feature = "scripted-provider")]
const SCRIPTED_PROVIDER_ID: &str = "scripted";
#[cfg(feature = "scripted-provider")]
const FAILING_SYMBOL: &str = "FAIL";

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

/// Best-effort safety net around the spawned daemon `Child`: if a later
/// assertion panics before the end-of-test `shutdown_daemon`/`wait()`
/// sequence runs, `Drop` kills and reaps the child rather than leaking it
/// and its hold on the host-global single-instance lock. Not the primary
/// shutdown path — the graceful `AppHandle::shutdown_daemon` + `child.wait()`
/// flow is unchanged; `kill`/`wait` on an already-exited (reaped) process
/// simply errors, which is ignored, so the guard is a harmless no-op on the
/// success path.
struct DaemonGuard(std::process::Child);

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
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
    // as `credential_broker_e2e.rs`/`config_service_e2e.rs`. Wrapped in
    // `DaemonGuard` so a panic in any assertion below still kills/reaps the
    // process instead of leaking it.
    let mut child = DaemonGuard(
        std::process::Command::new(env!("CARGO_BIN_EXE_datamancerd"))
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
            .expect("spawn datamancerd"),
    );

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

    // 2. health(): schema_version == 3, daemon.version == daemon crate
    // version, credential_backend set, every provider Disabled.
    let health = handle.health().await.expect("health");
    assert_eq!(
        health.schema_version, 3,
        "schema_version must be pinned to 3"
    );
    // This test binary IS the datamancerd crate, so its CARGO_PKG_VERSION
    // is the daemon crate version — assert against the constant directly
    // rather than the facade's ping-derived echo (daemon-vs-daemon would be
    // circular).
    assert_eq!(
        health.daemon.version.as_deref(),
        Some(env!("CARGO_PKG_VERSION")),
        "daemon.version must be daemon-stamped to the daemon crate version"
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
    assert_eq!(
        alpaca_crypto_state,
        ProviderState::Idle,
        "alpaca-crypto must leave Disabled once configured, even without credentials, \
         and read Idle (enabled, nothing subscribed) rather than Connecting"
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
    // and carries the same schema_version and provider states. The service
    // is history_size(1), so a late joiner's FIRST sample can predate the
    // hot-apply above — loop under one deadline until a view shows
    // alpaca-crypto off Disabled rather than asserting on the first sample.
    let mut stream = handle.watch_health();
    let pushed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let view = stream
                .next()
                .await
                .expect("the push stream must not end while the daemon is alive");
            let alpaca_crypto = view
                .providers
                .iter()
                .find(|p| p.provider.as_str() == "alpaca-crypto")
                .expect("alpaca-crypto must be enumerated in the pushed view");
            if alpaca_crypto.state != ProviderState::Disabled {
                return view;
            }
        }
    })
    .await
    .expect(
        "no pushed health view showed alpaca-crypto off Disabled within 3s \
         (publish cadence 200ms) — push plane stale or hot-apply not reflected",
    );
    assert_eq!(pushed.schema_version, 3, "pushed view must carry schema 3");
    assert_eq!(pushed.schema_version, health_after_configure.schema_version);
    let pushed_alpaca_state = pushed
        .providers
        .iter()
        .find(|p| p.provider.as_str() == "alpaca")
        .expect("alpaca must be enumerated in the pushed view")
        .state;
    assert_eq!(
        pushed_alpaca_state,
        ProviderState::Disabled,
        "alpaca must remain Disabled in the pushed view — only alpaca-crypto was configured"
    );
    drop(stream);

    // 5. shutdown_daemon; drop.
    drop(events);
    handle.shutdown_daemon().await.expect("shutdown_daemon");

    let status = child
        .0
        .wait()
        .expect("daemon process must exit after shutdown");
    assert!(status.success(), "daemon must exit 0 on graceful shutdown");

    stop_daemon();
}

/// Write a daemon config whose only provider is the scripted one (feature
/// `scripted-provider`). The history delay is the same 400 ms the scripted
/// `daemon_e2e` tests use: a query that completes before the client's
/// data-plane subscriber attaches is reaped unseen (`open_query`'s
/// documented attach race), so the fetch must outlast the attach.
#[cfg(feature = "scripted-provider")]
fn write_config_scripted(dir: &std::path::Path) -> (PathBuf, PathBuf) {
    let socket = dir.join("control.sock");
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[provider.scripted]
history_delay_ms = 400
history_prints = 3
failing_symbol = "{FAILING_SYMBOL}"

[server]
admin_socket = "{}"
service_prefix = "health-e2e-surfaces"

[diagnostics]
publish_interval_ms = 200
"#,
            socket.display()
        ),
    )
    .unwrap();
    (config, socket)
}

/// The scripted provider's `surfaces` from a view, by provider id.
#[cfg(feature = "scripted-provider")]
fn scripted_surfaces(view: &datamancer_core::HealthView) -> datamancer_core::ProviderSurfaces {
    view.providers
        .iter()
        .find(|p| p.provider.as_str() == SCRIPTED_PROVIDER_ID)
        .unwrap_or_else(|| panic!("scripted provider must be enumerated: {:?}", view.providers))
        .surfaces
        .clone()
}

#[cfg(feature = "scripted-provider")]
fn scripted_instrument(symbol: &str) -> datamancer_core::Instrument {
    datamancer_core::Instrument::new(
        datamancer_core::ProviderId::from_static(SCRIPTED_PROVIDER_ID),
        datamancer_core::AssetClass::Equity,
        symbol,
    )
}

/// Run one history query through the facade up to its terminal
/// `SessionClosing` (the query stream's end marker: a subscriber that reads
/// on past it polls the reaped service forever) and return how many data
/// events it delivered.
#[cfg(feature = "scripted-provider")]
async fn drain_query(handle: &mut AppHandle, symbol: &str) -> usize {
    use datamancer_client::spec::QuerySpec;
    use datamancer_core::{ControlKind, EventKind, MarketEvent, Timestamp};

    let spec = QuerySpec::new(
        &scripted_instrument(symbol),
        EventKind::Trade,
        Timestamp(1_000_000_000),
        Timestamp(2_000_000_000),
    )
    .expect("equity is a wire asset class");
    let (_id, mut stream) = handle.query(&spec).await.expect("query accepted");
    let mut data = 0;
    let drained = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(ev) = stream.next().await {
            match ev {
                MarketEvent::Control(c) if c.kind == ControlKind::SessionClosing => return true,
                MarketEvent::Control(_) => {}
                _ => data += 1,
            }
        }
        false
    })
    .await
    .expect("the query must reach SessionClosing within 10s");
    assert!(drained, "the query stream ended without SessionClosing");
    data
}

/// Per-surface health over the daemon's `health` op and push plane: the
/// scripted provider's history surface fails on the `FAILING_SYMBOL` query and
/// recovers on the next ordinary query while its live surface stays `Unknown`,
/// a view from the push plane carries the same surfaces, and a live subscribe
/// reads `Ok` on the live surface though the provider emits no connect
/// controls.
#[cfg(feature = "scripted-provider")]
#[tokio::test]
#[ignore = "needs a live iceoryx2 runtime and spawns the daemon binary"]
#[allow(clippy::too_many_lines)] // one sequential e2e narrative
async fn health_reports_the_history_and_live_surfaces_apart() {
    use datamancer_client::spec::SubscriptionSpec;
    use datamancer_core::SurfaceOutcome;

    let dir = tempfile::tempdir().expect("tempdir");
    let (config, socket) = write_config_scripted(dir.path());

    let mut child = DaemonGuard(
        std::process::Command::new(env!("CARGO_BIN_EXE_datamancerd"))
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
            .expect("spawn datamancerd"),
    );
    let bind_deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(
            Instant::now() < bind_deadline,
            "daemon socket never appeared"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let cfg = ensure_cfg(
        dir.path(),
        "health-e2e-surfaces",
        socket.clone(),
        config.clone(),
    );
    let (mut handle, events) = AppHandle::ensure(cfg)
        .await
        .expect("ensure must find the already-running daemon");

    // 1. Nothing used yet: both surfaces Unknown; the provider itself is Idle
    // (enabled, nothing requested).
    let health = handle.health().await.expect("health");
    assert_eq!(health.schema_version, 3);
    let s = scripted_surfaces(&health);
    assert_eq!(s.history, SurfaceOutcome::Unknown);
    assert_eq!(s.live, SurfaceOutcome::Unknown);
    let scripted_state = health
        .providers
        .iter()
        .find(|p| p.provider.as_str() == SCRIPTED_PROVIDER_ID)
        .expect("scripted")
        .state;
    assert_eq!(scripted_state, ProviderState::Idle);

    // 2. A failing history fetch: the query completes with no data (the
    // failure is in-band), and the history surface alone reads Failing with
    // the provider's message.
    assert_eq!(drain_query(&mut handle, FAILING_SYMBOL).await, 0);
    let health = handle.health().await.expect("health after the failure");
    let s = scripted_surfaces(&health);
    let SurfaceOutcome::Failing {
        at: failed_at,
        error,
    } = &s.history
    else {
        panic!("{s:?}");
    };
    assert!(error.contains(FAILING_SYMBOL), "{s:?}");
    assert!(*failed_at <= health.daemon.captured_at, "{s:?}");
    assert_eq!(s.live, SurfaceOutcome::Unknown, "live untouched");
    let failed_at = *failed_at;

    // 3. The next ordinary query succeeds: Ok, error cleared.
    assert_eq!(drain_query(&mut handle, "AAPL").await, 3);
    let health = handle.health().await.expect("health after the success");
    let s = scripted_surfaces(&health);
    assert!(
        matches!(s.history, SurfaceOutcome::Ok { at } if at >= failed_at),
        "{s:?}"
    );
    assert_eq!(s.live, SurfaceOutcome::Unknown);

    // 4. The push plane carries the same surfaces (history_size(1): wait for
    // a sample published after the success rather than the first one).
    let mut stream = handle.watch_health();
    let pushed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let view = stream
                .next()
                .await
                .expect("the push stream must not end while the daemon is alive");
            if matches!(scripted_surfaces(&view).history, SurfaceOutcome::Ok { .. }) {
                return view;
            }
        }
    })
    .await
    .expect("no pushed view showed the history surface Ok within 3s");
    assert_eq!(pushed.schema_version, 3);
    assert_eq!(
        scripted_surfaces(&pushed),
        s,
        "push plane and health op agree"
    );
    drop(stream);

    // 5. A live subscribe: the provider sends no connect control, and the
    // accepted subscribe is the evidence.
    let spec = SubscriptionSpec::new(
        &scripted_instrument("AAPL"),
        datamancer_core::EventKind::Trade,
    )
    .expect("equity is a wire asset class");
    handle.subscribe(&spec).await.expect("subscribe accepted");
    let health = handle.health().await.expect("health after the subscribe");
    let s = scripted_surfaces(&health);
    assert!(matches!(s.live, SurfaceOutcome::Ok { .. }), "{s:?}");

    drop(events);
    handle.shutdown_daemon().await.expect("shutdown_daemon");
    let status = child
        .0
        .wait()
        .expect("daemon process must exit after shutdown");
    assert!(status.success(), "daemon must exit 0 on graceful shutdown");
    stop_daemon();
}
