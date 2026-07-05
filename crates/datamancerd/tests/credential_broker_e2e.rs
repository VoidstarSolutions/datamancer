//! End-to-end: the credential broker against the real binary, with NO
//! ALPACA_* env vars in the daemon's environment — credentials arrive only
//! via set-credentials. `#[ignore]`d: needs live iceoryx2 + paper credentials
//! in the TEST's env. Run:
//! `cargo test -p datamancerd --test credential_broker_e2e -- --ignored --test-threads=1`

use std::path::PathBuf;
use std::time::{Duration, Instant};

use datamancer_client::ClientError;
use datamancer_client::app::{AppHandle, EnsureConfig};
use datamancer_client::codes;
use datamancer_client::spec::SubscriptionSpec;
use datamancer_core::{ConnectionState, ProviderCredentials};

/// Minimal daemon config in a tempdir; socket path returned alongside.
/// Same shape as `app_facade_e2e.rs`'s fixture.
fn write_config(dir: &std::path::Path) -> (PathBuf, PathBuf) {
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
service_prefix = "credential-broker-e2e"

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
/// `app_facade_e2e.rs`.
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
#[ignore = "needs live iceoryx2 runtime, paper credentials, host-global lock"]
#[allow(clippy::too_many_lines)] // one sequential 7-step e2e narrative; splitting hides the story
async fn broker_provisions_credentials_and_provider_connects() {
    let dir = tempfile::tempdir().unwrap();
    let (config, socket) = write_config(dir.path());

    // 1. Spawn datamancerd directly (NOT through the facade's spawner) with
    //    all four ALPACA_* env vars scrubbed — the point of this test is that
    //    credentials arrive ONLY through the broker, never the env fallback.
    //    DATAMANCER_CREDENTIALS_FILE pins the broker's store to this
    //    tempdir: the test must never read, write, or clear the developer's
    //    real keychain entries.
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

    // Wait for the control socket to appear (daemon bound).
    let bind_deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(
            Instant::now() < bind_deadline,
            "daemon socket never appeared"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // 2. Connect the facade to the already-running daemon (`ensure` finds it
    //    via the socket rather than spawning a second one) and assert health:
    //    the provider is present and the credential backend is active.
    let cfg = ensure_cfg(dir.path(), "cred-e2e", socket.clone(), config.clone());
    let (mut handle, _events) = AppHandle::ensure(cfg)
        .await
        .expect("ensure must find the already-running daemon");

    let health = handle.health().await.expect("health");
    assert!(
        !health.providers.is_empty(),
        "configured provider must appear in health"
    );
    assert!(
        health.daemon.credential_backend.is_some(),
        "credential_backend must be Some: the broker is active"
    );

    // 3. Without any credentials provisioned (env scrubbed, nothing in the
    //    store yet), get-credentials must fail with credentials_missing.
    match handle.get_credentials("alpaca-crypto").await {
        Err(ClientError::Control { code, .. }) => {
            assert_eq!(code, codes::CREDENTIALS_MISSING);
        }
        other => panic!("expected credentials_missing, got {other:?}"),
    }

    // 4. set_credentials with the TEST's own paper key pair (never the
    //    daemon's environment — the daemon has none).
    let key_id = std::env::var("ALPACA_PAPER_API_KEY_ID")
        .expect("ALPACA_PAPER_API_KEY_ID must be set in the test's own environment");
    let secret = std::env::var("ALPACA_PAPER_API_SECRET_KEY")
        .expect("ALPACA_PAPER_API_SECRET_KEY must be set in the test's own environment");
    let creds = ProviderCredentials::ApiKeyPair {
        key_id: key_id.clone(),
        secret: secret.clone(),
    };
    handle
        .set_credentials("alpaca-crypto", creds.clone())
        .await
        .expect("set_credentials");

    // 5. Subscribe BTC/USD trades; within a bounded wait, the provider
    //    transitions to Connected: the watch seeded None, set hot-applied,
    //    the streaming task connected with the brokered credentials.
    let spec = SubscriptionSpec {
        provider: "alpaca-crypto".to_string(),
        asset_class: datamancer_client::spec::AssetClassCfg::Crypto,
        symbol: "BTC/USD".to_string(),
        kind: datamancer_client::spec::EventKindCfg::Trade,
        scope: datamancer_client::spec::ScopeCfg::default(),
        persistence: datamancer_client::spec::PersistenceCfg::default(),
    };
    handle.subscribe(&spec).await.expect("subscribe");

    let connect_deadline = Instant::now() + Duration::from_secs(20);
    let mut connected = false;
    while Instant::now() < connect_deadline {
        let snapshot = handle.snapshot().await.expect("snapshot");
        if snapshot
            .providers
            .iter()
            .any(|p| p.connection_state == ConnectionState::Connected)
        {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        connected,
        "provider never reached Connected within the bounded wait after set_credentials"
    );

    // 6. get_credentials round-trips the pair; clear_credentials ok; a
    //    second get is credentials_missing again while the stream stays up
    //    (clear does not un-apply — a recorded deviation from the spec).
    let got = handle
        .get_credentials("alpaca-crypto")
        .await
        .expect("get_credentials");
    assert_eq!(got, creds);

    handle
        .clear_credentials("alpaca-crypto")
        .await
        .expect("clear_credentials");

    match handle.get_credentials("alpaca-crypto").await {
        Err(ClientError::Control { code, .. }) => {
            assert_eq!(code, codes::CREDENTIALS_MISSING);
        }
        other => panic!("expected credentials_missing after clear, got {other:?}"),
    }

    // The stream must still be up: clear does not un-apply the live provider.
    let snapshot = handle.snapshot().await.expect("snapshot after clear");
    assert!(
        snapshot
            .providers
            .iter()
            .any(|p| p.connection_state == ConnectionState::Connected),
        "provider must remain Connected after clear-credentials (clear does not un-apply)"
    );

    // 7. close + stop_daemon.
    handle.close().await.expect("close");
    let _ = child.kill();
    let _ = child.wait();
    stop_daemon();
}
