//! App-facing facade for datamancerd (spec 2026-07-05, cycle 1): find a
//! running daemon or spawn one, connect, and expose typed health.
//!
//! Adds **no** protocol semantics — every capability maps to control-surface
//! ops a hand-rolled client could issue. Spawn is detached and unsupervised:
//! the daemon is a shared host service that outlives the app that spawned it;
//! if it dies, the event stream ends and the app calls [`AppHandle::ensure`]
//! again (reconnect-by-recreate).

mod error;
mod lifecycle;
mod platform;

pub use error::{EnsureError, ReadyDiagnosis};

use std::path::PathBuf;
use std::time::Duration;

use datamancer_core::{HealthView, InstrumentInfo, ProviderId, SystemSnapshot};

use crate::Client as _;
use crate::error::ClientError;
use crate::iceoryx2::{Iceoryx2Client, Iceoryx2ClientError, Iceoryx2Config};
use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

/// The multiplexed event stream (same contract as the underlying
/// [`crate::Client`] impl: `(instrument, seq)`-ordered, loss never silent).
pub type AppEvents = <Iceoryx2Client as crate::Client>::Events;

/// Gate the connection on the daemon's reported version.
fn check_version(daemon: &str) -> Result<(), EnsureError> {
    let client = env!("CARGO_PKG_VERSION");
    if lifecycle::version_compatible(client, daemon) {
        Ok(())
    } else {
        Err(EnsureError::VersionSkew {
            daemon: daemon.to_string(),
            client: client.to_string(),
        })
    }
}

/// Reduce a snapshot and stamp the daemon version onto it.
fn fill_health(snapshot: &SystemSnapshot, daemon_version: &str) -> HealthView {
    let mut view = HealthView::from_snapshot(snapshot, HealthView::DEFAULT_STALE_AFTER_NS);
    view.daemon.version = Some(daemon_version.to_string());
    view
}

/// Parameters for `AppHandle::ensure` (`AppHandle` lands with the facade).
#[derive(Debug, Clone)]
pub struct EnsureConfig {
    /// The datamancerd binary to spawn if none is running. Explicit — no
    /// `PATH` search (a bundling app knows its sidecar's location; guessing
    /// invites version skew and PATH hijack).
    pub daemon_binary: PathBuf,
    /// Daemon config file. `None` = the daemon's platform default (which
    /// self-scaffolds on first run).
    pub config_path: Option<PathBuf>,
    /// Control socket. `None` = `crate::default_control_socket()`.
    pub control_socket: Option<PathBuf>,
    /// This client's name for `open-client` (unique per daemon).
    pub client_name: String,
    /// Bound on spawn-to-ready. Default 10 s.
    pub ready_timeout: Duration,
    /// Spawned daemon's stdout/stderr destination. `None` = the platform
    /// default (`crate::paths::default_daemon_log()`, Task 5).
    pub log_path: Option<PathBuf>,
    /// Forwarded to the iceoryx2 client (idle poll sleep).
    pub poll_interval: Duration,
    /// Forwarded to the iceoryx2 client (local event buffer bound).
    pub event_buffer: usize,
}

impl EnsureConfig {
    /// Defaults: 10 s ready timeout, 1 ms poll, 8192-event buffer, platform
    /// socket/config/log paths.
    #[must_use]
    pub fn new(daemon_binary: impl Into<PathBuf>, client_name: impl Into<String>) -> Self {
        Self {
            daemon_binary: daemon_binary.into(),
            config_path: None,
            control_socket: None,
            client_name: client_name.into(),
            ready_timeout: Duration::from_secs(10),
            log_path: None,
            poll_interval: Duration::from_millis(1),
            event_buffer: 8192,
        }
    }
}

/// The app-facing daemon handle: found-or-spawned, connected, versioned.
///
/// Holds the same-host [`Iceoryx2Client`] and adds no protocol semantics —
/// every method maps to control-surface ops.
pub struct AppHandle {
    client: Iceoryx2Client,
    daemon_version: String,
}

impl AppHandle {
    /// Find a running daemon at the (default or configured) control socket,
    /// or spawn `cfg.daemon_binary` detached and await readiness; then
    /// connect. Losing a spawn race to another app's daemon is success.
    ///
    /// # Errors
    ///
    /// [`EnsureError`] — each variant is app-actionable (see its docs).
    pub async fn ensure(cfg: EnsureConfig) -> Result<(Self, AppEvents), EnsureError> {
        let socket = cfg
            .control_socket
            .clone()
            .or_else(crate::default_control_socket)
            .ok_or(EnsureError::NoSocketPath)?;
        let log_path = cfg
            .log_path
            .clone()
            .or_else(crate::paths::default_daemon_log)
            .ok_or(EnsureError::NoSocketPath)?;
        let daemon_version = lifecycle::ensure_daemon(
            &platform::TokioEndpoint,
            &platform::ProcessSpawner::new(log_path),
            &cfg,
            &socket,
        )
        .await?;
        check_version(&daemon_version)?;
        let (client, events) = Iceoryx2Client::connect(Iceoryx2Config {
            control_socket: socket,
            client_name: cfg.client_name.clone(),
            poll_interval: cfg.poll_interval,
            event_buffer: cfg.event_buffer,
        })
        .await?;
        Ok((
            Self {
                client,
                daemon_version,
            },
            events,
        ))
    }

    /// The daemon version reported at connect (`ping`).
    #[must_use]
    pub fn daemon_version(&self) -> &str {
        &self.daemon_version
    }

    /// Typed health for app rendering: the daemon snapshot reduced to
    /// [`HealthView`], with `daemon.version` filled from the handshake.
    ///
    /// # Errors
    ///
    /// Propagates the underlying `snapshot` control/transport failure.
    pub async fn health(&mut self) -> Result<HealthView, ClientError<Iceoryx2ClientError>> {
        let snapshot = self.client.snapshot().await?;
        Ok(fill_health(&snapshot, &self.daemon_version))
    }

    /// See [`crate::Client::subscribe`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::subscribe`].
    pub async fn subscribe(
        &mut self,
        spec: &SubscriptionSpec,
    ) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client.subscribe(spec).await
    }

    /// See [`crate::Client::unsubscribe`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::unsubscribe`].
    pub async fn unsubscribe(
        &mut self,
        spec: &UnsubscribeSpec,
    ) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client.unsubscribe(spec).await
    }

    /// See [`crate::Client::instruments`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::instruments`].
    pub async fn instruments(
        &mut self,
        provider: Option<&ProviderId>,
    ) -> Result<Vec<InstrumentInfo>, ClientError<Iceoryx2ClientError>> {
        self.client.instruments(provider).await
    }

    /// The raw diagnostics snapshot (prefer [`Self::health`] for rendering).
    ///
    /// # Errors
    ///
    /// See [`crate::Client::snapshot`].
    pub async fn snapshot(&mut self) -> Result<SystemSnapshot, ClientError<Iceoryx2ClientError>> {
        self.client.snapshot().await
    }

    /// Graceful close of this client (the daemon keeps running — deliberate
    /// daemon stop is a cycle-3 capability).
    ///
    /// # Errors
    ///
    /// See [`crate::Client::close`].
    pub async fn close(self) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skew_gate_produces_typed_error() {
        // Derive an incompatible daemon version from our own (next major)
        // instead of hardcoding one, so crate version bumps can't silently
        // make the "skewed" fixture compatible again.
        let major: u64 = env!("CARGO_PKG_VERSION")
            .split('.')
            .next()
            .and_then(|m| m.parse().ok())
            .expect("CARGO_PKG_VERSION starts with a numeric major");
        let skewed = format!("{}.0.0", major + 1);
        match check_version(&skewed) {
            Err(EnsureError::VersionSkew { daemon, client }) => {
                assert_eq!(daemon, skewed);
                assert_eq!(client, env!("CARGO_PKG_VERSION"));
            }
            other => panic!("expected VersionSkew, got {other:?}"),
        }
        assert!(check_version(env!("CARGO_PKG_VERSION")).is_ok());
    }

    #[test]
    fn health_fill_sets_daemon_version() {
        use datamancer_core::{CacheSnapshot, HealthView, SystemSnapshot, Timestamp};
        let snap = SystemSnapshot::new(
            Timestamp(1),
            vec![],
            CacheSnapshot::new(vec![], None),
            vec![],
            vec![],
        );
        let view = fill_health(&snap, "0.1.0");
        assert_eq!(view.daemon.version.as_deref(), Some("0.1.0"));
        assert_eq!(view.schema_version, HealthView::SCHEMA_VERSION);
    }
}
