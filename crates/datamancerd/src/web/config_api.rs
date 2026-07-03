//! The web layer's handle to the daemon's config file.
//!
//! Holds the resolved config path and the exact [`Config`] the daemon booted
//! with. The daemon's runtime is immutable after boot (apply-on-restart), so
//! this handle only reads and rewrites the *file*; `restart_required` is
//! parsed-config inequality between the on-disk file and the boot config —
//! a save that restores the boot config clears the flag even though comments
//! were lost.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::Config;
use crate::error::{DaemonError, Result};

/// Cheap-`Clone` (Arc-backed) config-file handle for web handlers.
#[derive(Clone)]
pub struct ConfigState {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    boot: Config,
    restart_required: AtomicBool,
    // Serializes read-disk/write so concurrent `PUT /api/config` calls don't
    // race on the shared `<path>.tmp` sibling file or interleave flag updates.
    io_lock: tokio::sync::Mutex<()>,
}

impl ConfigState {
    /// Build from the resolved config path and the boot-time config.
    #[must_use]
    pub fn new(path: PathBuf, boot: Config) -> Self {
        Self {
            inner: Arc::new(Inner {
                path,
                boot,
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
        self.store_flag(&config);
        Ok(config)
    }

    /// Validate and atomically write `config` to the file, then recompute the
    /// restart flag. Nothing is written (and the flag is unchanged) on failure.
    ///
    /// Serialized against concurrent `read_disk`/`write` calls: two in-flight
    /// `PUT /api/config` requests would otherwise share the fixed
    /// `<path>.tmp` sibling file and could tear each other's write or race
    /// the restart-required flag.
    ///
    /// # Errors
    ///
    /// Propagates [`Config::save`] errors.
    pub async fn write(&self, config: &Config) -> Result<()> {
        let _guard = self.inner.io_lock.lock().await;
        let config = config.clone();
        let path = self.inner.path.clone();
        // `save` is small-file blocking I/O; keep it off the shared runtime.
        let config_result =
            tokio::task::spawn_blocking(move || config.save(&path).map(|()| config))
                .await
                .map_err(|e| {
                    DaemonError::Io(std::io::Error::other(format!(
                        "config write task failed: {e}"
                    )))
                })?;
        let saved = config_result?;
        self.store_flag(&saved);
        Ok(())
    }

    fn store_flag(&self, on_disk: &Config) {
        self.inner
            .restart_required
            .store(*on_disk != self.inner.boot, Ordering::Relaxed);
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

fn view(state: &ConfigState, config: Config) -> Response {
    let body = ConfigView {
        restart_required: state.restart_required(),
        path: state.path().display().to_string(),
        config,
    };
    (StatusCode::OK, Json(body)).into_response()
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

    fn boot_state(dir: &std::path::Path) -> (ConfigState, Config) {
        let path = dir.join("config.toml");
        let boot = Config::parse(MINIMAL).expect("parse");
        boot.save(&path).expect("seed file");
        (ConfigState::new(path, boot.clone()), boot)
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
        let invalid = Config::parse("[provider]\n").expect("parse");
        state.write(&invalid).await.expect_err("must reject");
        assert_eq!(state.read_disk().await.expect("read"), boot);
        assert!(!state.restart_required());
    }
}
