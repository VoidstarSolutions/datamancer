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

/// The daemon's current configuration (TOML as JSON) and whether any
/// cold-classified field awaits a restart.
#[derive(Debug, Clone, PartialEq)]
pub struct DaemonConfig {
    pub config: serde_json::Value,
    pub restart_required: bool,
}

/// How a mutating config op took effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// Applied to the running daemon.
    Live,
    /// Persisted; takes effect at the next daemon start.
    RestartRequired,
}

use std::path::PathBuf;
use std::time::Duration;

use datamancer_core::{
    HealthView, InstrumentInfo, ProviderCredentials, ProviderId, SystemSnapshot,
};

use crate::Client as _;
use crate::error::ClientError;
use crate::iceoryx2::{Iceoryx2Client, Iceoryx2ClientError, Iceoryx2Config};
use crate::protocol::uds::{Reply, Request};
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

/// Reduce a snapshot and stamp the daemon version and credential backend
/// onto it (both from the `ping` handshake, not the snapshot itself).
fn fill_health(
    snapshot: &SystemSnapshot,
    daemon_version: &str,
    credential_backend: Option<&str>,
) -> HealthView {
    let mut view = HealthView::from_snapshot(snapshot, HealthView::DEFAULT_STALE_AFTER_NS);
    view.daemon.version = Some(daemon_version.to_string());
    view.daemon.credential_backend = credential_backend.map(str::to_string);
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
    daemon_hello: lifecycle::DaemonHello,
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
        let daemon_hello = lifecycle::ensure_daemon(
            &platform::TokioEndpoint,
            &platform::ProcessSpawner::new(log_path),
            &cfg,
            &socket,
        )
        .await?;
        check_version(&daemon_hello.version)?;
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
                daemon_hello,
            },
            events,
        ))
    }

    /// The daemon version reported at connect (`ping`).
    #[must_use]
    pub fn daemon_version(&self) -> &str {
        &self.daemon_hello.version
    }

    /// Typed health for app rendering: the daemon snapshot reduced to
    /// [`HealthView`], with `daemon.version` and `daemon.credential_backend`
    /// filled from the handshake.
    ///
    /// # Errors
    ///
    /// Propagates the underlying `snapshot` control/transport failure.
    pub async fn health(&mut self) -> Result<HealthView, ClientError<Iceoryx2ClientError>> {
        let snapshot = self.client.snapshot().await?;
        Ok(fill_health(
            &snapshot,
            &self.daemon_hello.version,
            self.daemon_hello.credential_backend.as_deref(),
        ))
    }

    /// Store (create or rotate) provider credentials in the daemon's broker.
    /// Applies live: a configured provider reconnects with the new
    /// credentials.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with the stable codes (`unknown_provider`,
    /// `bad_request`, `credential_backend_unavailable`, `permission_denied`)
    /// or a transport failure.
    pub async fn set_credentials(
        &mut self,
        provider: &str,
        credentials: ProviderCredentials,
    ) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client
            .control_request(&Request::SetCredentials {
                provider: provider.to_string(),
                credentials,
            })
            .await
            .map(|_| ())
    }

    /// Read back stored credentials for `provider`.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with `unknown_provider`, `credentials_missing`,
    /// `credential_backend_unavailable`, or `permission_denied`; or a
    /// transport failure (including a malformed ok reply missing the
    /// `credentials` payload).
    pub async fn get_credentials(
        &mut self,
        provider: &str,
    ) -> Result<ProviderCredentials, ClientError<Iceoryx2ClientError>> {
        let reply = self
            .client
            .control_request(&Request::GetCredentials {
                provider: provider.to_string(),
            })
            .await?;
        reply.credentials.ok_or_else(|| {
            ClientError::Transport(Iceoryx2ClientError::Protocol(
                "ok get-credentials reply missing credentials payload".to_string(),
            ))
        })
    }

    /// Remove stored credentials for `provider`. Does **not** unapply a
    /// running provider's already-live credentials — those persist until the
    /// provider restarts.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with the stable codes (`unknown_provider`,
    /// `credential_backend_unavailable`, `permission_denied`) or a transport
    /// failure.
    pub async fn clear_credentials(
        &mut self,
        provider: &str,
    ) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client
            .control_request(&Request::ClearCredentials {
                provider: provider.to_string(),
            })
            .await
            .map(|_| ())
    }

    /// Fetch the daemon's current config.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with stable codes, or a transport failure
    /// (including a malformed ok reply missing the `config` payload).
    pub async fn get_config(&mut self) -> Result<DaemonConfig, ClientError<Iceoryx2ClientError>> {
        let reply = self.client.control_request(&Request::GetConfig).await?;
        daemon_config_from(&reply)
    }

    /// Enable (or re-configure) a compiled-in provider. `settings` is the
    /// provider's config-section shape, e.g.
    /// `json!({"account_type": "live"})`; pass `json!({})` for defaults.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with `unknown_provider`,
    /// `unknown_config_field`, `bad_request`, or `permission_denied`; or a
    /// transport failure.
    pub async fn configure_provider(
        &mut self,
        provider: &str,
        settings: serde_json::Value,
    ) -> Result<Applied, ClientError<Iceoryx2ClientError>> {
        let reply = self
            .client
            .control_request(&Request::ConfigureProvider {
                provider: provider.to_string(),
                settings,
            })
            .await?;
        Ok(applied_from(&reply))
    }

    /// Disable a compiled-in provider. Stored credentials are untouched;
    /// re-enabling reuses them.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with `unknown_provider` or
    /// `permission_denied`; or a transport failure.
    pub async fn remove_provider(
        &mut self,
        provider: &str,
    ) -> Result<Applied, ClientError<Iceoryx2ClientError>> {
        let reply = self
            .client
            .control_request(&Request::RemoveProvider {
                provider: provider.to_string(),
            })
            .await?;
        Ok(applied_from(&reply))
    }

    /// Deliberately stop the daemon (graceful drain). Consumes the handle:
    /// the connection is gone once the daemon exits.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with `permission_denied`, or a transport
    /// failure sending the request.
    pub async fn shutdown_daemon(mut self) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client
            .control_request(&Request::Shutdown)
            .await
            .map(|_| ())
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

    /// Graceful close of this client (the daemon keeps running — see
    /// [`Self::shutdown_daemon`] to stop it deliberately).
    ///
    /// # Errors
    ///
    /// See [`crate::Client::close`].
    pub async fn close(self) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client.close().await
    }
}

/// Map an ok `get-config` reply to [`DaemonConfig`], or a transport error if
/// the reply is missing its `config` payload.
fn daemon_config_from(reply: &Reply) -> Result<DaemonConfig, ClientError<Iceoryx2ClientError>> {
    let config = reply.config.clone().ok_or_else(|| {
        ClientError::Transport(Iceoryx2ClientError::Protocol(
            "ok get-config reply missing config payload".to_string(),
        ))
    })?;
    Ok(DaemonConfig {
        config,
        restart_required: reply.restart_required.unwrap_or(false),
    })
}

/// Map an ok config-mutation reply's `applied` field to [`Applied`].
fn applied_from(reply: &Reply) -> Applied {
    match reply.applied.as_deref() {
        Some("restart_required") => Applied::RestartRequired,
        _ => Applied::Live,
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
        let view = fill_health(&snap, "0.1.0", None);
        assert_eq!(view.daemon.version.as_deref(), Some("0.1.0"));
        assert_eq!(view.schema_version, HealthView::SCHEMA_VERSION);
    }

    #[test]
    fn health_fill_sets_backend_alongside_version() {
        use datamancer_core::{CacheSnapshot, SystemSnapshot, Timestamp};
        let snap = SystemSnapshot::new(
            Timestamp(1),
            vec![],
            CacheSnapshot::new(vec![], None),
            vec![],
            vec![],
        );
        let view = fill_health(&snap, "0.3.0", Some("keychain"));
        assert_eq!(view.daemon.version.as_deref(), Some("0.3.0"));
        assert_eq!(view.daemon.credential_backend.as_deref(), Some("keychain"));
        let older = fill_health(&snap, "0.3.0", None);
        assert!(older.daemon.credential_backend.is_none());
    }

    #[test]
    fn applied_from_maps_restart_required() {
        let mut reply = Reply::ok();
        reply.applied = Some("restart_required".to_string());
        assert_eq!(applied_from(&reply), Applied::RestartRequired);
    }

    #[test]
    fn applied_from_maps_live() {
        let reply = Reply::applied_live();
        assert_eq!(applied_from(&reply), Applied::Live);
    }

    #[test]
    fn applied_from_defaults_to_live_when_absent() {
        let reply = Reply::config(serde_json::json!({}), false);
        assert!(reply.applied.is_none());
        assert_eq!(applied_from(&reply), Applied::Live);
    }

    #[test]
    fn daemon_config_from_reads_config_and_restart_flag() {
        let reply = Reply::config(serde_json::json!({"providers": {}}), true);
        let config = daemon_config_from(&reply).expect("config payload present");
        assert_eq!(config.config, serde_json::json!({"providers": {}}));
        assert!(config.restart_required);
    }

    #[test]
    fn daemon_config_from_missing_config_is_transport_error() {
        let reply = Reply::applied_live();
        match daemon_config_from(&reply) {
            Err(ClientError::Transport(Iceoryx2ClientError::Protocol(msg))) => {
                assert!(msg.contains("config"));
            }
            other => panic!("expected Transport(Protocol(_)), got {other:?}"),
        }
    }
}
