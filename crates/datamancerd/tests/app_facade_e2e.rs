//! End-to-end: the app facade against the real binary. `#[ignore]`d — needs
//! a live iceoryx2 runtime; run with
//! `cargo test -p datamancerd --test app_facade_e2e -- --ignored --test-threads=1`.
//! The single-instance lock is per-user host-global: these tests cannot run
//! alongside a real datamancerd or in parallel with `daemon_e2e`.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use datamancer_client::app::{AppHandle, EnsureConfig};

/// Minimal daemon config in a tempdir; socket path returned alongside.
/// Mirrors `daemon_e2e.rs`'s fixture (same provider + `[server]` sections,
/// plus `[diagnostics]`) — overrides `admin_socket` to the tempdir.
fn write_config(dir: &Path) -> (PathBuf, PathBuf) {
    let socket = dir.join("control.sock");
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[provider.alpaca_crypto]
account_type = "paper"
venue = "us"

[server]
admin_socket = "{}"
service_prefix = "app-facade-e2e"

[diagnostics]
publish_interval_ms = 200
"#,
            socket.display()
        ),
    )
    .unwrap();
    (config, socket)
}

/// `text` as one POSIX shell word.
fn shell_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', r"'\''"))
}

/// A launcher for the daemon under test with its credential store pinned to a
/// `credentials.json` in `dir`: the facade spawns with this process's
/// environment, so the pin rides on the binary it is told to run.
fn write_pinned_launcher(dir: &Path) -> PathBuf {
    let launcher = dir.join("datamancerd-pinned");
    std::fs::write(
        &launcher,
        format!(
            "#!/bin/sh\nexport DATAMANCER_CREDENTIALS_FILE={}\nexec {} \"$@\"\n",
            shell_quote(&dir.join("credentials.json").display().to_string()),
            shell_quote(env!("CARGO_BIN_EXE_datamancerd")),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o700)).unwrap();
    launcher
}

fn ensure_cfg(dir: &Path, name: &str) -> EnsureConfig {
    let (config, socket) = write_config(dir);
    let mut cfg = EnsureConfig::new(write_pinned_launcher(dir), name);
    cfg.config_path = Some(config);
    cfg.control_socket = Some(socket);
    cfg.log_path = Some(dir.join("daemon.log"));
    cfg.ready_timeout = Duration::from_secs(15);
    cfg
}

/// Kill the daemon we spawned: the facade detaches, so recover the pid from
/// the single-instance lockfile (documented as the holder's pid) and TERM it.
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
#[ignore = "needs live iceoryx2 runtime and host-global single-instance lock"]
async fn ensure_spawns_daemon_and_health_reports_version() {
    let dir = tempfile::tempdir().unwrap();
    let (mut handle, _events) = AppHandle::ensure(ensure_cfg(dir.path(), "e2e-a"))
        .await
        .expect("ensure must spawn and connect");
    assert!(!handle.daemon_version().is_empty());
    let health = handle.health().await.expect("health");
    assert_eq!(
        health.daemon.version.as_deref(),
        Some(handle.daemon_version())
    );
    assert_eq!(health.daemon.credential_backend.as_deref(), Some("file"));
    assert!(
        !health.providers.is_empty(),
        "configured provider must appear"
    );
    handle.close().await.expect("close");
    stop_daemon();
}

#[tokio::test]
#[ignore = "needs live iceoryx2 runtime and host-global single-instance lock"]
async fn concurrent_ensures_share_one_daemon() {
    let dir = tempfile::tempdir().unwrap();
    // Same socket + config: both race to spawn; the lock arbitrates.
    let cfg_a = ensure_cfg(dir.path(), "e2e-race-a");
    let cfg_b = ensure_cfg(dir.path(), "e2e-race-b");
    let (ra, rb) = tokio::join!(AppHandle::ensure(cfg_a), AppHandle::ensure(cfg_b));
    let (mut a, _ea) = ra.expect("racer A must succeed (spawn or lost-race connect)");
    let (b, _eb) = rb.expect("racer B must succeed (spawn or lost-race connect)");
    assert_eq!(a.daemon_version(), b.daemon_version());
    // Both clients registered on the one daemon.
    let snapshot = a.snapshot().await.expect("snapshot");
    assert!(snapshot.client_sessions.len() >= 2);
    let _ = b.close().await;
    let _ = a.close().await;
    stop_daemon();
}
