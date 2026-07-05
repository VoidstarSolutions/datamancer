//! The web layer's handle to the daemon's config file.
//!
//! Holds the resolved config path and the exact [`Config`] the daemon booted
//! with, plus the [`ConfigHub`](crate::config_hub::ConfigHub) that is the
//! daemon's sole hot-path config writer. `write` delegates the full
//! validate→persist→apply sequence to the hub — this layer no longer touches
//! the file directly. `restart_required` is `true` when either the on-disk
//! file's cold fields diverge from the boot config, or the on-disk file
//! diverges from what the hub currently has applied (an external hand-edit
//! the hub hasn't seen yet) — a hand-edited hot field still needs a restart
//! to take effect, since hand edits are boot-time only.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::Config;
use crate::config_class::cold_divergence;
use crate::config_hub::ConfigHub;
use crate::error::{DaemonError, Result};

/// Placeholder emitted in place of a real secret (currently `[ws].auth_token`)
/// on the read/response path, and recognized on write as "keep the stored value
/// unchanged". A UI shows this like a masked password field: submitting it back
/// verbatim means "don't touch the secret".
pub(crate) const REDACTED_SECRET: &str = "<redacted>";

/// Cheap-`Clone` (Arc-backed) config-file handle for web handlers.
#[derive(Clone)]
pub struct ConfigState {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    boot: Config,
    hub: Arc<ConfigHub>,
    restart_required: AtomicBool,
    // Serializes read-disk/write so concurrent `PUT /api/config` calls don't
    // race on the shared `<path>.tmp` sibling file or interleave flag updates.
    io_lock: tokio::sync::Mutex<()>,
}

impl ConfigState {
    /// Build from the resolved config path, the boot-time config, and the
    /// hub that owns all hot-path writes.
    #[must_use]
    pub fn new(path: PathBuf, boot: Config, hub: Arc<ConfigHub>) -> Self {
        Self {
            inner: Arc::new(Inner {
                path,
                boot,
                hub,
                restart_required: AtomicBool::new(false),
                io_lock: tokio::sync::Mutex::new(()),
            }),
        }
    }

    /// The config file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// The config the daemon booted with.
    #[allow(dead_code)] // Not yet consumed by a handler; kept for future diffing UI.
    #[must_use]
    pub fn boot(&self) -> &Config {
        &self.inner.boot
    }

    /// Latest known restart-required flag (recomputed by `read_disk`/`write`).
    #[must_use]
    pub fn restart_required(&self) -> bool {
        self.inner.restart_required.load(Ordering::Relaxed)
    }

    /// Read and parse the on-disk config (no validation — this is the display
    /// path and must show external hand-edits). Recomputes the restart flag.
    ///
    /// # Errors
    ///
    /// [`DaemonError::ConfigRead`] / [`DaemonError::ConfigParse`] on failure.
    pub async fn read_disk(&self) -> Result<Config> {
        let _guard = self.inner.io_lock.lock().await;
        let text = tokio::fs::read_to_string(&self.inner.path)
            .await
            .map_err(|source| DaemonError::ConfigRead {
                path: self.inner.path.clone(),
                source,
            })?;
        let config = Config::parse(&text)?;
        self.store_disk_flag(&config).await;
        Ok(config)
    }

    /// Validate and apply `config` via the hub (the sole hot-path writer):
    /// validate → persist → apply, then recompute the restart flag from the
    /// hub's cold-divergence verdict. Nothing is written (and the flag is
    /// unchanged) on failure.
    ///
    /// Serialized against concurrent `read_disk`/`write` calls: two in-flight
    /// `PUT /api/config` requests would otherwise race the preserve-on-write
    /// secret read against each other or interleave restart-flag updates.
    ///
    /// # Errors
    ///
    /// Propagates [`crate::config_hub::ConfigHub::apply_full`] errors.
    pub async fn write(&self, config: &Config) -> Result<()> {
        let _guard = self.inner.io_lock.lock().await;
        let mut config = config.clone();
        // Preserve-on-write: a caller that echoed back the redacted GET view
        // sends the placeholder in place of the real token. Restore it from the
        // current on-disk config (read under the same lock as the write, so it
        // is consistent with what we are about to overwrite). Read/parse
        // failures **propagate** — the write aborts rather than silently
        // clearing the token (which would disable WS auth) just because the
        // file we are about to overwrite could not be read. A successful parse
        // that simply has no stored token yields `None` (nothing to preserve),
        // so the literal placeholder is still never persisted as a secret.
        if let Some(ws) = config.ws.as_mut()
            && ws.auth_token.as_deref() == Some(REDACTED_SECRET)
        {
            let text = tokio::fs::read_to_string(&self.inner.path)
                .await
                .map_err(|source| DaemonError::ConfigRead {
                    path: self.inner.path.clone(),
                    source,
                })?;
            let current = Config::parse(&text)?;
            ws.auth_token = current.ws.and_then(|w| w.auth_token);
        }
        let restart_required = self.inner.hub.apply_full(config).await?;
        self.inner
            .restart_required
            .store(restart_required, Ordering::Relaxed);
        Ok(())
    }

    /// Recompute the restart flag for the `read_disk` (display) path: a hand
    /// edit is flagged whenever its cold fields diverge from the boot config,
    /// or whenever it diverges at all from what the hub currently has applied
    /// (an edit the hub hasn't seen — and, being hot-only-at-boot, never will
    /// apply live — still needs a restart to take effect).
    async fn store_disk_flag(&self, on_disk: &Config) {
        let hub_current = self.inner.hub.current().await;
        let restart_required =
            !cold_divergence(&self.inner.boot, on_disk).is_empty() || *on_disk != hub_current;
        self.inner
            .restart_required
            .store(restart_required, Ordering::Relaxed);
    }
}

use axum::Json;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::control::codes;

/// The `GET`/`PUT /api/config` success payload.
#[derive(Serialize)]
struct ConfigView {
    config: Config,
    restart_required: bool,
    path: String,
}

#[derive(Serialize)]
struct ConfigError {
    code: &'static str,
    message: String,
}

fn error_response(status: StatusCode, code: &'static str, message: String) -> Response {
    (status, Json(ConfigError { code, message })).into_response()
}

fn view(state: &ConfigState, mut config: Config) -> Response {
    redact_secrets(&mut config);
    let body = ConfigView {
        restart_required: state.restart_required(),
        path: state.path().display().to_string(),
        config,
    };
    (StatusCode::OK, Json(body)).into_response()
}

/// Replace secret-bearing fields with [`REDACTED_SECRET`] before the config is
/// serialized to a web client. The real values never leave the process on the
/// API surface; a caller that echoes the placeholder back on `PUT` has it
/// restored from disk in [`ConfigState::write`] (preserve-on-write).
fn redact_secrets(config: &mut Config) {
    if let Some(ws) = config.ws.as_mut()
        && ws.auth_token.is_some()
    {
        ws.auth_token = Some(REDACTED_SECRET.to_string());
    }
}

/// `GET /api/config` — the on-disk config (shows external hand-edits) plus the
/// restart-required flag and the file path.
pub(crate) async fn get_config(State(state): State<ConfigState>) -> Response {
    match state.read_disk().await {
        Ok(config) => view(&state, config),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            codes::CONFIG,
            e.to_string(),
        ),
    }
}

/// `PUT /api/config` — validate and atomically write a full config. The one
/// mutating route on the web surface; guarded by loopback bind (transport),
/// JSON content type (axum `Json` rejects others with 415, which also forces a
/// CORS preflight for cross-origin scripts), and a same-origin Origin/Host
/// check (blocks non-preflighted cross-site sends).
pub(crate) async fn put_config(
    State(state): State<ConfigState>,
    headers: HeaderMap,
    payload: std::result::Result<Json<Config>, JsonRejection>,
) -> Response {
    if !same_origin_ok(&headers) {
        return error_response(
            StatusCode::FORBIDDEN,
            codes::BAD_REQUEST,
            "cross-origin config writes are not allowed".to_string(),
        );
    }
    let Json(config) = match payload {
        Ok(json) => json,
        Err(JsonRejection::MissingJsonContentType(_)) => {
            return error_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                codes::BAD_REQUEST,
                "config writes require Content-Type: application/json".to_string(),
            );
        }
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, codes::BAD_REQUEST, e.to_string());
        }
    };
    match state.write(&config).await {
        Ok(()) => view(&state, config),
        Err(e @ (DaemonError::ConfigInvalid(_) | DaemonError::ConfigSerialize(_))) => {
            error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                codes::CONFIG,
                e.to_string(),
            )
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            codes::CONFIG,
            e.to_string(),
        ),
    }
}

/// `true` when `Origin` (if present) and `Host` (if present) are loopback.
/// The UI is same-origin on a loopback bind, so any non-loopback value means a
/// cross-site request that slipped past content-type preflighting.
fn same_origin_ok(headers: &HeaderMap) -> bool {
    fn loopback_host(hostport: &str) -> bool {
        // `[::1]:8080`, `[::1]`, `127.0.0.1:8080`, `localhost` forms.
        let host = hostport
            .strip_prefix('[')
            .and_then(|rest| rest.split(']').next())
            .map_or_else(|| hostport.split(':').next().unwrap_or(""), |v6| v6);
        host == "127.0.0.1" || host == "localhost" || host == "::1"
    }
    let origin_ok = headers
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .is_none_or(|origin| origin.strip_prefix("http://").is_some_and(loopback_host));
    let host_ok = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .is_none_or(loopback_host);
    origin_ok && host_ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AccountTypeCfg;

    const MINIMAL: &str = "[provider.alpaca]\naccount_type = \"paper\"\n";

    fn config_state(path: PathBuf, boot: Config) -> ConfigState {
        let (hub, _sources) = crate::config_hub::ConfigHub::bootstrap(boot.clone(), path.clone());
        ConfigState::new(path, boot, hub)
    }

    fn boot_state(dir: &std::path::Path) -> (ConfigState, Config) {
        let path = dir.join("config.toml");
        let boot = Config::parse(MINIMAL).expect("parse");
        boot.save(&path).expect("seed file");
        (config_state(path, boot.clone()), boot)
    }

    #[tokio::test]
    async fn restart_required_tracks_disk_vs_boot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (state, boot) = boot_state(dir.path());
        assert!(!state.restart_required(), "boot state matches disk");

        // A changed config flips the flag on write.
        let mut changed = boot.clone();
        changed.session.resume_buffer_events = 42;
        state.write(&changed).await.expect("write");
        assert!(state.restart_required());
        assert_eq!(state.read_disk().await.expect("read"), changed);

        // Restoring the boot config clears it (parsed equality, not bytes).
        state.write(&boot).await.expect("restore");
        assert!(!state.restart_required());
    }

    #[tokio::test]
    async fn read_disk_reflects_external_edits() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (state, _boot) = boot_state(dir.path());
        // Hand-edit on disk behind the daemon's back.
        std::fs::write(state.path(), "[provider.alpaca]\naccount_type = \"live\"\n")
            .expect("hand edit");
        let disk = state.read_disk().await.expect("read");
        assert_eq!(
            disk.provider.alpaca.expect("alpaca").account_type,
            AccountTypeCfg::Live
        );
        assert!(state.restart_required(), "external edit shows up");
    }

    #[tokio::test]
    async fn write_invalid_config_changes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (state, boot) = boot_state(dir.path());
        let invalid = Config::parse(
            "[cache]\nbackend = \"embedded\"\npath = \"./same.db\"\n\n[tap_log]\nbackend = \"embedded\"\npath = \"./same.db\"\n",
        )
        .expect("parse");
        state.write(&invalid).await.expect_err("must reject");
        assert_eq!(state.read_disk().await.expect("read"), boot);
        assert!(!state.restart_required());
    }

    const WITH_TOKEN: &str = "[provider.alpaca]\naccount_type = \"paper\"\n\n[ws]\nenabled = true\nauth_token = \"super-secret-token\"\n";

    fn token_of(config: &Config) -> Option<&str> {
        config.ws.as_ref().and_then(|w| w.auth_token.as_deref())
    }

    #[test]
    fn redact_secrets_masks_present_token_only() {
        let mut with = Config::parse(WITH_TOKEN).expect("parse");
        redact_secrets(&mut with);
        assert_eq!(token_of(&with), Some(REDACTED_SECRET));

        // An absent token stays absent (no spurious placeholder).
        let mut without = Config::parse(MINIMAL).expect("parse");
        redact_secrets(&mut without);
        assert_eq!(token_of(&without), None);
    }

    #[tokio::test]
    async fn get_config_response_never_contains_the_token() {
        use axum::body::to_bytes;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let boot = Config::parse(WITH_TOKEN).expect("parse");
        boot.save(&path).expect("seed");
        let state = config_state(path, boot);

        let resp = get_config(State(state)).await;
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.expect("body");
        let text = String::from_utf8(bytes.to_vec()).expect("utf8");
        assert!(!text.contains("super-secret-token"), "leaked token: {text}");
        assert!(text.contains(REDACTED_SECRET), "no placeholder: {text}");
    }

    #[tokio::test]
    async fn write_preserves_redacted_token_from_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let boot = Config::parse(WITH_TOKEN).expect("parse");
        boot.save(&path).expect("seed");
        let state = config_state(path, boot.clone());

        // The UI echoes back the redacted view unchanged: token == placeholder.
        let mut echoed = boot.clone();
        echoed.ws.as_mut().unwrap().auth_token = Some(REDACTED_SECRET.to_string());
        state.write(&echoed).await.expect("write");

        // The real token is restored on disk, and the round-trip is a no-op.
        let disk = state.read_disk().await.expect("read");
        assert_eq!(token_of(&disk), Some("super-secret-token"));
        assert!(
            !state.restart_required(),
            "placeholder round-trip must not flag restart"
        );
    }

    #[tokio::test]
    async fn write_sets_a_genuinely_new_token() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let boot = Config::parse(WITH_TOKEN).expect("parse");
        boot.save(&path).expect("seed");
        let state = config_state(path, boot.clone());

        let mut changed = boot.clone();
        changed.ws.as_mut().unwrap().auth_token = Some("rotated-token".to_string());
        state.write(&changed).await.expect("write");
        assert_eq!(
            token_of(&state.read_disk().await.expect("read")),
            Some("rotated-token")
        );
    }

    #[tokio::test]
    async fn write_never_persists_the_literal_placeholder() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        // Seed with NO stored token.
        let boot =
            Config::parse("[provider.alpaca]\naccount_type = \"paper\"\n\n[ws]\nenabled = true\n")
                .expect("parse");
        boot.save(&path).expect("seed");
        let state = config_state(path, boot.clone());

        let mut echoed = boot.clone();
        echoed.ws.as_mut().unwrap().auth_token = Some(REDACTED_SECRET.to_string());
        state.write(&echoed).await.expect("write");
        // No token to restore -> cleared, never the literal placeholder.
        assert_eq!(token_of(&state.read_disk().await.expect("read")), None);
    }

    #[tokio::test]
    async fn write_aborts_rather_than_clearing_token_when_disk_unreadable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let boot = Config::parse(WITH_TOKEN).expect("parse");
        boot.save(&path).expect("seed");
        let state = config_state(path.clone(), boot.clone());

        // The on-disk file is corrupted behind the daemon's back, so the real
        // token can no longer be recovered for a placeholder round-trip.
        std::fs::write(&path, "this is not valid toml {{{").expect("corrupt");

        let mut echoed = boot.clone();
        echoed.ws.as_mut().unwrap().auth_token = Some(REDACTED_SECRET.to_string());
        // The write must fail loudly, not silently clear auth on save.
        state
            .write(&echoed)
            .await
            .expect_err("must not silently disable auth");

        // Nothing was written: the file is left exactly as it was, so no
        // cleared-auth config replaced it.
        let raw = std::fs::read_to_string(&path).expect("read raw");
        assert_eq!(raw, "this is not valid toml {{{");
    }

    #[tokio::test]
    async fn hot_only_web_edit_does_not_require_restart_and_applies_live() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let boot = Config::parse("[provider]\n").unwrap();
        boot.save(&path).unwrap();
        let (hub, sources) = crate::config_hub::ConfigHub::bootstrap(boot.clone(), path.clone());
        let state = ConfigState::new(path, boot.clone(), hub);

        let mut edited = boot.clone();
        edited.provider.alpaca = Some(crate::config::AlpacaSection {
            account_type: crate::config::AccountTypeCfg::Live,
        });
        state.write(&edited).await.expect("write");
        assert!(
            !state.restart_required(),
            "hot-only edit must not require restart"
        );
        assert!(sources.alpaca.current().is_some(), "hot edit applies live");
    }

    #[tokio::test]
    async fn cold_web_edit_requires_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let boot = Config::parse("[provider]\n").unwrap();
        boot.save(&path).unwrap();
        let (hub, _sources) = crate::config_hub::ConfigHub::bootstrap(boot.clone(), path.clone());
        let state = ConfigState::new(path, boot.clone(), hub);

        let mut edited = boot.clone();
        edited.session.resume_buffer_events = 42;
        state.write(&edited).await.expect("write");
        assert!(state.restart_required());
    }
}
