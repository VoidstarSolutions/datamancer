//! End-to-end: the config service (cycle 3) against the real binary.
//!
//! `config_service_boots_disabled_and_shuts_down_cleanly` needs no live
//! network and no provider credentials — it boots the daemon with **zero**
//! provider sections, exercises `ping`/`get-config`/`configure-provider`
//! (including the `unknown_config_field` error path), and `shutdown`. It
//! still spawns the real binary and needs a live iceoryx2 runtime, so it is
//! `#[ignore]`d like every other daemon e2e in this crate.
//!
//! `config_service_enables_and_disables_a_provider_live` additionally needs
//! live Alpaca paper credentials in the **test's own** environment
//! (`ALPACA_PAPER_API_KEY_ID` / `ALPACA_PAPER_API_SECRET_KEY`) — the daemon
//! itself never sees them via env (all four `ALPACA_*` vars are scrubbed from
//! its environment; `DATAMANCER_CREDENTIALS_FILE` pins its credential store
//! to a tempdir, so this test never touches the developer's real keychain).
//! Run either with:
//!
//! ```text
//! cargo test -p datamancerd --test config_service_e2e -- --ignored --nocapture
//! ```

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use datamancer_client::ClientError;
use datamancer_client::app::{AppHandle, Applied, EnsureConfig};
use datamancer_client::codes;
use datamancer_client::spec::SubscriptionSpec;
use datamancer_core::{ConnectionState, ProviderCredentials};

/// Write a daemon config with **no** provider sections and return the
/// config/socket paths.
fn write_config_no_providers(dir: &std::path::Path) -> (PathBuf, PathBuf) {
    let socket = dir.join("control.sock");
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[server]
admin_socket = "{}"
service_prefix = "config-service-e2e"

[diagnostics]
publish_interval_ms = 200
"#,
            socket.display()
        ),
    )
    .unwrap();
    (config, socket)
}

/// Kill the daemon we spawned: recover the pid from the single-instance
/// lockfile (documented as the holder's pid) and TERM it. Same helper as
/// `credential_broker_e2e.rs`/`app_facade_e2e.rs`.
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

/// Send one JSON line and read one JSON reply line (raw UDS round trip, no
/// facade — same helper shape as `daemon_e2e.rs`).
fn round_trip(socket: &std::path::Path, request: &str) -> serde_json::Value {
    let stream = UnixStream::connect(socket).expect("connect socket");
    let mut writer = stream.try_clone().expect("clone");
    writer.write_all(request.as_bytes()).expect("write");
    writer.write_all(b"\n").expect("write nl");
    writer.flush().expect("flush");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read reply");
    serde_json::from_str(&line).expect("parse reply")
}

/// Spawn the daemon directly (not through the facade's spawner) with all
/// four `ALPACA_*` env vars scrubbed and its credential store pinned to a
/// tempdir — same env hygiene as `credential_broker_e2e.rs`.
fn spawn_daemon(
    dir: &std::path::Path,
    config: &std::path::Path,
    socket: &std::path::Path,
) -> Child {
    let child = Command::new(env!("CARGO_BIN_EXE_datamancerd"))
        .arg("--config")
        .arg(config)
        .env("DATAMANCER_CREDENTIALS_FILE", dir.join("credentials.json"))
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
    child
}

/// Non-network: boot with zero providers, exercise `get-config` and the
/// `unknown_config_field` error path, then `shutdown`. No live iceoryx2 data
/// flow, but the daemon still creates an iceoryx2 node at startup, so this
/// stays `#[ignore]`d alongside the rest of the suite (see module doc).
#[test]
#[ignore = "needs a live iceoryx2 runtime"]
fn config_service_boots_disabled_and_shuts_down_cleanly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (config, socket) = write_config_no_providers(dir.path());
    let mut child = spawn_daemon(dir.path(), &config, &socket);

    // `ping` succeeds against a zero-provider boot.
    let ping = round_trip(&socket, r#"{"op":"ping"}"#);
    assert_eq!(ping["ok"], serde_json::Value::Bool(true));

    // `get-config`: ungated, no restart pending, no provider sections.
    let got = round_trip(&socket, r#"{"op":"get-config"}"#);
    assert_eq!(got["ok"], serde_json::Value::Bool(true));
    assert_eq!(got["restart_required"], serde_json::Value::Bool(false));
    let providers = got["config"]["provider"]
        .as_object()
        .expect("provider table present");
    assert!(
        providers.values().all(serde_json::Value::is_null),
        "boot config carried no [provider.*] sections (every compiled-in id is null/disabled): {providers:?}"
    );

    // `configure-provider` with an unknown field in the section -> stable
    // error code, config left untouched.
    let bad = round_trip(
        &socket,
        r#"{"op":"configure-provider","provider":"alpaca-crypto","settings":{"not_a_real_field":true}}"#,
    );
    assert_eq!(bad["ok"], serde_json::Value::Bool(false));
    assert_eq!(bad["code"], codes::UNKNOWN_CONFIG_FIELD);

    let after_bad = round_trip(&socket, r#"{"op":"get-config"}"#);
    let providers_after = after_bad["config"]["provider"]
        .as_object()
        .expect("provider table present");
    assert!(
        providers_after.values().all(serde_json::Value::is_null),
        "a rejected configure-provider must not mutate the persisted config: {providers_after:?}"
    );

    // `shutdown`: ok reply, and the process actually exits.
    let shutdown = round_trip(&socket, r#"{"op":"shutdown"}"#);
    assert_eq!(shutdown["ok"], serde_json::Value::Bool(true));

    let status = child
        .wait()
        .expect("daemon process must exit after shutdown");
    assert!(status.success(), "daemon must exit 0 on graceful shutdown");

    stop_daemon();
}

/// Live network: enable a provider through the config service, subscribe
/// and see events flow, then disable it again and confirm a fresh subscribe
/// no longer works — all driven entirely through the config-service ops
/// rather than a hand-edited config file.
#[tokio::test]
#[ignore = "needs live iceoryx2 runtime, paper credentials, host-global lock"]
#[allow(clippy::too_many_lines)] // one sequential e2e narrative; splitting hides the story
async fn config_service_enables_and_disables_a_provider_live() {
    let dir = tempfile::tempdir().unwrap();
    let (config, socket) = write_config_no_providers(dir.path());
    let mut child = spawn_daemon(dir.path(), &config, &socket);

    let mut cfg = EnsureConfig::new(env!("CARGO_BIN_EXE_datamancerd"), "config-svc-e2e");
    cfg.config_path = Some(config.clone());
    cfg.control_socket = Some(socket.clone());
    cfg.log_path = Some(dir.path().join("daemon.log"));
    cfg.ready_timeout = Duration::from_secs(15);

    let (mut handle, events) = AppHandle::ensure(cfg)
        .await
        .expect("ensure must find the already-running daemon");

    // 2. get-config: no provider sections enabled, no restart pending.
    let got = handle.get_config().await.expect("get_config");
    assert!(!got.restart_required);
    assert!(got.config["provider"]["alpaca_crypto"].is_null());

    // 3. Without a configured/enabled provider, the daemon requires
    // configure-provider before data flows: subscribing against a disabled
    // provider must fail rather than silently succeed.
    let spec = SubscriptionSpec {
        provider: "alpaca-crypto".to_string(),
        asset_class: datamancer_client::spec::AssetClassCfg::Crypto,
        symbol: "BTC/USD".to_string(),
        kind: datamancer_client::spec::EventKindCfg::Trade,
        scope: datamancer_client::spec::ScopeCfg::default(),
        persistence: datamancer_client::spec::PersistenceCfg::default(),
    };
    let pre_configure = handle.subscribe(&spec).await;
    assert!(
        pre_configure.is_err(),
        "subscribing to a disabled/unconfigured provider must fail: {pre_configure:?}"
    );

    // 4. set-credentials works while the provider is disabled — the
    // credential hub seeds a watch for every compiled-in id regardless of
    // whether its config section is present.
    let key_id = std::env::var("ALPACA_PAPER_API_KEY_ID")
        .expect("ALPACA_PAPER_API_KEY_ID must be set in the test's own environment");
    let secret = std::env::var("ALPACA_PAPER_API_SECRET_KEY")
        .expect("ALPACA_PAPER_API_SECRET_KEY must be set in the test's own environment");
    handle
        .set_credentials(
            "alpaca-crypto",
            ProviderCredentials::ApiKeyPair {
                key_id: key_id.clone(),
                secret: secret.clone(),
            },
        )
        .await
        .expect("set_credentials while disabled");

    // 5. configure-provider enables it; provider.* is Hot, so this always
    // applies live.
    let applied = handle
        .configure_provider(
            "alpaca-crypto",
            serde_json::json!({"account_type": "paper", "venue": "us"}),
        )
        .await
        .expect("configure_provider");
    assert_eq!(applied, Applied::Live);

    // 6. get-config: the section is now present (non-null).
    let after_configure = handle.get_config().await.expect("get_config after enable");
    assert!(
        !after_configure.config["provider"]["alpaca_crypto"].is_null(),
        "configured provider section must appear (non-null) in get-config: {:?}",
        after_configure.config["provider"]
    );

    // 7. Subscribe BTC/USD trades; within a bounded wait, the provider
    // reaches Connected and the subscribe succeeds.
    handle
        .subscribe(&spec)
        .await
        .expect("subscribe after configure-provider");

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
        "provider never reached Connected within the bounded wait after configure-provider"
    );

    // 8. remove-provider disables it again; stored credentials are left in
    // place (documented, not re-asserted here — see credential_broker_e2e).
    let removed = handle
        .remove_provider("alpaca-crypto")
        .await
        .expect("remove_provider");
    assert_eq!(removed, Applied::Live);

    let after_remove = handle.get_config().await.expect("get_config after remove");
    assert!(
        after_remove.config["provider"]["alpaca_crypto"].is_null(),
        "removed provider section must be null again in get-config: {:?}",
        after_remove.config["provider"]
    );

    // A get-credentials round trip still works: remove-provider does not
    // clear the store.
    let creds = handle
        .get_credentials("alpaca-crypto")
        .await
        .expect("get_credentials must still work after remove-provider");
    assert_eq!(
        creds,
        ProviderCredentials::ApiKeyPair { key_id, secret },
        "remove-provider must leave stored credentials untouched"
    );

    // A fresh subscribe on a fresh client now fails (disabled again).
    match handle.subscribe(&spec).await {
        Err(ClientError::Control { .. }) => {}
        other => panic!(
            "expected a control error subscribing to a just-disabled provider, got {other:?}"
        ),
    }

    // Drop the data-plane event stream before asking the daemon to shut
    // down: tearing down the client-side iceoryx2 subscriber while the
    // daemon (and its node) is still alive avoids a slow/blocked cleanup
    // against an already-torn-down publisher.
    drop(events);

    // 9. shutdown -> ok, and the process exits cleanly.
    handle.shutdown_daemon().await.expect("shutdown_daemon");

    let status = child
        .wait()
        .expect("daemon process must exit after shutdown");
    assert!(status.success(), "daemon must exit 0 on graceful shutdown");

    stop_daemon();
}
