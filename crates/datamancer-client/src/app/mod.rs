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
#[cfg(unix)]
mod platform;
#[cfg(windows)]
mod platform_windows;
#[cfg(windows)]
use platform_windows as platform;

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
    HealthView, InstrumentEntry, InstrumentInfo, ProviderCredentials, ProviderId, SystemSnapshot,
};

use crate::Client as _;
use crate::error::ClientError;
#[cfg(not(windows))]
use crate::iceoryx2::{Iceoryx2Client, Iceoryx2ClientError, Iceoryx2Config};
use crate::protocol::uds::{Reply, Request};
use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

// Windows hybrid: admin ops ride the owner-DACL named pipe, the data plane
// rides WS-loopback (iceoryx2 shm is not viable on Windows — Phase 4). Unix is
// a single `Iceoryx2Client` carrying both.
#[cfg(windows)]
use crate::pipe_control::PipeControlClient;
#[cfg(windows)]
pub use crate::pipe_control::PipeControlError;
#[cfg(windows)]
use crate::ws::{WsClient, WsConfig};

/// The multiplexed event stream (same contract as the underlying
/// [`crate::Client`] impl: `(instrument, seq)`-ordered, loss never silent).
#[cfg(not(windows))]
pub type AppEvents = <Iceoryx2Client as crate::Client>::Events;
#[cfg(windows)]
pub type AppEvents = <WsClient as crate::Client>::Events;

/// Transport error surfaced by the admin (control-plane) methods —
/// `ping`/`health`/credentials/config/`shutdown`. Unix: the iceoryx2 client's
/// error (control is UDS). Windows: the named-pipe control error (admin stays
/// on the owner-DACL pipe). Daemon rejections are `ClientError::Control` with a
/// stable code regardless; this is only the transport-failure type.
#[cfg(not(windows))]
pub type AdminError = Iceoryx2ClientError;
#[cfg(windows)]
pub type AdminError = PipeControlError;

/// Transport error surfaced by the data-plane methods —
/// `subscribe`/`unsubscribe`/`snapshot`/`instruments`/`capabilities`/`close`.
/// Unix: the iceoryx2 client's error. Windows: the WS-loopback client's error.
#[cfg(not(windows))]
pub type DataError = Iceoryx2ClientError;
#[cfg(windows)]
pub type DataError = crate::ws::WsClientError;

/// Default WS-loopback data endpoint for the Windows hybrid, used when
/// [`EnsureConfig::ws_data_url`] is `None`. Must match the daemon's `[ws]`
/// loopback scaffold in `datamancerd`'s `paths::default_config_toml`
/// (`bind = "127.0.0.1"`, `port = 9001`) — changing the scaffold port there
/// requires updating this default (or setting `ws_data_url` explicitly).
#[cfg(windows)]
const DEFAULT_WS_DATA_ENDPOINT: &str = "ws://127.0.0.1:9001";

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
    /// Windows hybrid only: the WS-loopback data endpoint (`ws://host:port`)
    /// the data plane dials. `None` = the loopback default
    /// (`ws://127.0.0.1:9001`), matching the daemon's `[ws]` scaffold. Ignored
    /// on unix (the data plane is iceoryx2 shm, which needs no TCP endpoint) —
    /// the field is `#[cfg(windows)]` so unix `EnsureConfig` is unchanged.
    #[cfg(windows)]
    pub ws_data_url: Option<String>,
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
            #[cfg(windows)]
            ws_data_url: None,
        }
    }
}

/// The app-facing daemon handle: found-or-spawned, connected, versioned.
///
/// Adds no protocol semantics — every method maps to control-surface ops. Unix
/// holds one same-host [`Iceoryx2Client`] carrying both control and shm data.
#[cfg(not(windows))]
pub struct AppHandle {
    client: Iceoryx2Client,
    daemon_hello: lifecycle::DaemonHello,
}

/// The app-facing daemon handle (Windows **hybrid**): admin ops ride the
/// owner-DACL named-pipe control connection (`admin`), the data plane rides
/// WS-loopback (`data`) since iceoryx2 shm is not viable on Windows. Two
/// independent connections; still no new protocol semantics.
#[cfg(windows)]
pub struct AppHandle {
    admin: PipeControlClient,
    data: WsClient,
    /// The WS health-push stream, subscribed eagerly at [`AppHandle::ensure`]
    /// and handed out (once) by [`AppHandle::watch_health`]. `None` if the
    /// eager subscribe failed (health degrades silently, matching the unix
    /// contract). Behind a `Mutex` so `watch_health` keeps its `&self` signature.
    health: std::sync::Mutex<Option<HealthStream>>,
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

        #[cfg(not(windows))]
        {
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

        // Windows hybrid: connect the admin plane (owner-verified pipe) and the
        // data plane (WS-loopback) as two independent connections. The pipe is
        // re-verified here (owner SID + integrity) even though `ensure_daemon`
        // already pinged it — `connect_verified` is the security boundary for
        // every privileged send, not a one-time probe.
        #[cfg(windows)]
        {
            let admin = PipeControlClient::connect(&socket).await?;
            let url = cfg
                .ws_data_url
                .clone()
                .unwrap_or_else(|| DEFAULT_WS_DATA_ENDPOINT.to_string());
            let (mut data, events) = WsClient::connect(WsConfig {
                url,
                auth_token: None,
                event_buffer: cfg.event_buffer,
            })
            .await?;
            // Eagerly subscribe to the health push so `watch_health` keeps its
            // `&self`, infallible signature. A subscribe failure degrades to no
            // health (unix contract: a setup failure just ends the stream), it
            // does not fail `ensure` — the data/admin planes are already up.
            let health = std::sync::Mutex::new(data.watch_health().await.ok());
            Ok((
                Self {
                    admin,
                    data,
                    health,
                    daemon_hello,
                },
                events,
            ))
        }
    }

    /// The daemon version reported at connect (`ping`).
    #[must_use]
    pub fn daemon_version(&self) -> &str {
        &self.daemon_hello.version
    }

    // --- internal plane routing (cfg-split so the public methods above stay
    // platform-neutral) ---

    /// Route an admin (control-plane) request and map the reply to the
    /// two-layer error model. Unix: the iceoryx2 client's UDS control. Windows:
    /// the owner-DACL named pipe — the reject-to-`ClientError::Control` mapping
    /// (which `Iceoryx2Client::control_request` does on unix) is applied here.
    #[cfg(not(windows))]
    async fn admin_request(&mut self, req: &Request) -> Result<Reply, ClientError<AdminError>> {
        self.client.control_request(req).await
    }

    #[cfg(windows)]
    async fn admin_request(&mut self, req: &Request) -> Result<Reply, ClientError<AdminError>> {
        let reply = self
            .admin
            .request(req)
            .await
            .map_err(ClientError::Transport)?;
        if reply.ok {
            Ok(reply)
        } else {
            Err(ClientError::Control {
                code: reply.code.unwrap_or_default(),
                message: reply.message.unwrap_or_default(),
            })
        }
    }

    /// The data-plane client. Unix: the shared iceoryx2 client (control + shm).
    /// Windows: the WS-loopback client.
    #[cfg(not(windows))]
    fn data_mut(&mut self) -> &mut Iceoryx2Client {
        &mut self.client
    }

    #[cfg(windows)]
    fn data_mut(&mut self) -> &mut WsClient {
        &mut self.data
    }

    /// The daemon's app-facing health view, reduced and stamped daemon-side
    /// (version, credential backend, `schema_version`). The `ensure` version
    /// gate makes daemon/client schema skew unrepresentable in practice; the
    /// `schema_version` field is the detectable degradation if it ever isn't.
    ///
    /// # Errors
    ///
    /// Propagates the underlying `health` control/transport failure.
    pub async fn health(&mut self) -> Result<HealthView, ClientError<AdminError>> {
        let reply = self.admin_request(&Request::Health).await?;
        reply
            .health
            .ok_or_else(|| admin_protocol_error("ok health reply missing health payload"))
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
    ) -> Result<(), ClientError<AdminError>> {
        self.admin_request(&Request::SetCredentials {
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
    ) -> Result<ProviderCredentials, ClientError<AdminError>> {
        let reply = self
            .admin_request(&Request::GetCredentials {
                provider: provider.to_string(),
            })
            .await?;
        reply.credentials.ok_or_else(|| {
            admin_protocol_error("ok get-credentials reply missing credentials payload")
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
    ) -> Result<(), ClientError<AdminError>> {
        self.admin_request(&Request::ClearCredentials {
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
    pub async fn get_config(&mut self) -> Result<DaemonConfig, ClientError<AdminError>> {
        let reply = self.admin_request(&Request::GetConfig).await?;
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
    ) -> Result<Applied, ClientError<AdminError>> {
        let reply = self
            .admin_request(&Request::ConfigureProvider {
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
    ) -> Result<Applied, ClientError<AdminError>> {
        let reply = self
            .admin_request(&Request::RemoveProvider {
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
    pub async fn shutdown_daemon(mut self) -> Result<(), ClientError<AdminError>> {
        self.admin_request(&Request::Shutdown).await.map(|_| ())
    }

    /// See [`crate::Client::subscribe`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::subscribe`].
    pub async fn subscribe(
        &mut self,
        spec: &SubscriptionSpec,
    ) -> Result<(), ClientError<DataError>> {
        self.data_mut().subscribe(spec).await
    }

    /// See [`crate::Client::unsubscribe`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::unsubscribe`].
    pub async fn unsubscribe(
        &mut self,
        spec: &UnsubscribeSpec,
    ) -> Result<(), ClientError<DataError>> {
        self.data_mut().unsubscribe(spec).await
    }

    /// See [`crate::Client::instruments`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::instruments`].
    pub async fn instruments(
        &mut self,
        provider: Option<&ProviderId>,
    ) -> Result<Vec<InstrumentInfo>, ClientError<DataError>> {
        self.data_mut().instruments(provider).await
    }

    /// See [`crate::Client::capabilities`].
    ///
    /// # Errors
    ///
    /// See [`crate::Client::capabilities`].
    pub async fn capabilities(
        &mut self,
        provider: &ProviderId,
        symbols: &[String],
    ) -> Result<Vec<InstrumentEntry>, ClientError<DataError>> {
        self.data_mut().capabilities(provider, symbols).await
    }

    /// The raw diagnostics snapshot (prefer [`Self::health`] for rendering).
    ///
    /// # Errors
    ///
    /// See [`crate::Client::snapshot`].
    pub async fn snapshot(&mut self) -> Result<SystemSnapshot, ClientError<DataError>> {
        self.data_mut().snapshot().await
    }

    /// Graceful close of this client (the daemon keeps running — see
    /// [`Self::shutdown_daemon`] to stop it deliberately).
    ///
    /// # Errors
    ///
    /// See [`crate::Client::close`].
    #[cfg(not(windows))]
    pub async fn close(self) -> Result<(), ClientError<DataError>> {
        self.client.close().await
    }

    /// Windows hybrid close: the WS `close` sends `CloseClient` to the daemon;
    /// the admin pipe (`self.admin`) drops here, releasing the control
    /// connection. The daemon keeps running (see [`Self::shutdown_daemon`]).
    ///
    /// # Errors
    ///
    /// See [`crate::Client::close`] (the WS data-plane close).
    #[cfg(windows)]
    pub async fn close(self) -> Result<(), ClientError<DataError>> {
        self.data.close().await
    }

    /// Subscribe to pushed health views on the `datamancer/health` plane (the
    /// daemon publishes on its diagnostics cadence, daemon-stamped the same
    /// way as [`Self::health`]). Late joiners immediately receive the most
    /// recent view (`history_size(1)`).
    ///
    /// This is a same-host shared-memory subscription independent of the
    /// control connection: it does not consume `self.client` and cannot fail
    /// synchronously — a setup failure (node/service open) ends the returned
    /// stream immediately instead. Drop the stream to stop the poll task.
    #[cfg(not(windows))]
    #[must_use]
    pub fn watch_health(&self) -> HealthStream {
        let (tx, rx) = tokio::sync::mpsc::channel::<HealthView>(4);
        tokio::task::spawn_blocking(move || {
            let Ok(node) = ::iceoryx2::prelude::NodeBuilder::new()
                .create::<::iceoryx2::prelude::ipc_threadsafe::Service>()
            else {
                return; // stream ends; caller observes termination
            };
            let Ok(subscriber) =
                datamancer_transport_iceoryx2::Iceoryx2HealthSubscriber::open(&node)
            else {
                return;
            };
            while !tx.is_closed() {
                match subscriber.receive() {
                    Ok(Some(view)) => {
                        if tx.blocking_send(view).is_err() {
                            return;
                        }
                    }
                    Ok(None) => std::thread::sleep(HEALTH_POLL_INTERVAL),
                    Err(_) => return,
                }
            }
        });
        tokio_stream::wrappers::ReceiverStream::new(rx)
    }

    /// Windows hybrid health push (WS-loopback, not iceoryx2 shm — Phase 4).
    /// The subscription is established **eagerly at [`Self::ensure`]** (a
    /// `watch-health` op over the WS data socket): the daemon then pushes
    /// daemon-stamped [`HealthView`]s on its diagnostics cadence, demuxed onto
    /// a dedicated channel by the WS reader task. This method hands out that
    /// stream.
    ///
    /// Two Windows-only nuances vs. the unix iceoryx2 arm, both consequences of
    /// the push riding the single WS socket (subscribed once) rather than an
    /// independent shm plane:
    /// - **Single-shot:** the first call returns the live stream; later calls
    ///   return an already-ended stream (the unix arm can be called repeatedly,
    ///   each opening a fresh shm subscriber).
    /// - **Silent degradation preserved:** if the eager subscribe failed at
    ///   `ensure` (it does not fail `ensure`), this returns an immediately-ended
    ///   stream — matching the unix contract that a setup failure just ends the
    ///   stream. The `&self`, infallible signature is identical across platforms.
    #[cfg(windows)]
    #[must_use]
    pub fn watch_health(&self) -> HealthStream {
        // A poisoned lock (impossible here — the guard is never held across a
        // panic-prone section) degrades to an ended stream rather than
        // panicking, keeping this truly infallible like the unix arm.
        self.health
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
            .unwrap_or_else(ended_health_stream)
    }
}

/// An immediately-ended [`HealthStream`] (sender dropped): the Windows
/// `watch_health` fallback when no live health subscription is available.
#[cfg(windows)]
fn ended_health_stream() -> HealthStream {
    let (_tx, rx) = tokio::sync::mpsc::channel::<HealthView>(1);
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// Push stream of daemon-stamped [`HealthView`]s (the `datamancer/health`
/// plane; the daemon publishes on its diagnostics cadence). The stream ends
/// if the subscription fails; drop the stream to stop the poll task.
pub type HealthStream = tokio_stream::wrappers::ReceiverStream<HealthView>;

/// Idle-poll sleep for [`AppHandle::watch_health`] (unix iceoryx2 arm only —
/// the Windows arm consumes a pushed WS channel, no polling). Not derived from
/// [`EnsureConfig::poll_interval`]: that value lives on the one-shot `ensure`
/// config and isn't retained on `AppHandle`, and this loop's cadence is
/// bounded above by the daemon's independent diagnostics publish interval
/// regardless, so a fixed short poll is simplest.
#[cfg(not(windows))]
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Construct an admin-plane transport `Protocol` error (a malformed ok reply
/// from the control plane). The concrete transport type is [`AdminError`],
/// which differs by platform (iceoryx2 UDS on unix, named pipe on Windows).
#[cfg(not(windows))]
fn admin_protocol_error(msg: &str) -> ClientError<AdminError> {
    ClientError::Transport(Iceoryx2ClientError::Protocol(msg.to_string()))
}
#[cfg(windows)]
fn admin_protocol_error(msg: &str) -> ClientError<AdminError> {
    ClientError::Transport(PipeControlError::Protocol(msg.to_string()))
}

/// Map an ok `get-config` reply to [`DaemonConfig`], or a transport error if
/// the reply is missing its `config` payload.
fn daemon_config_from(reply: &Reply) -> Result<DaemonConfig, ClientError<AdminError>> {
    let config = reply
        .config
        .clone()
        .ok_or_else(|| admin_protocol_error("ok get-config reply missing config payload"))?;
    Ok(DaemonConfig {
        config,
        restart_required: reply.restart_required.unwrap_or(false),
    })
}

/// Map an ok config-mutation reply's `applied` field to [`Applied`].
fn applied_from(reply: &Reply) -> Applied {
    match reply.applied.as_deref() {
        Some(crate::codes::RESTART_REQUIRED) => Applied::RestartRequired,
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

    // `fill_health` and its unit tests (`health_fill_sets_daemon_version`,
    // `health_fill_sets_backend_alongside_version`) were removed with the
    // client-side reduction: `health()` now sends `Request::Health` and the
    // daemon stamps `version`/`credential_backend` server-side (see
    // `datamancerd::server::Server::dispatch`'s `Request::Health` arm). The
    // actor isn't unit-testable without a live `Datamancer`/iceoryx2
    // runtime (no existing `dispatch` arm has a unit test for the same
    // reason), so the stamping assertions move to the Task 11 daemon e2e
    // test instead.

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
            // The concrete transport type is `AdminError` (platform-specific);
            // assert on its `Display` so the test holds on both the iceoryx2
            // (unix) and named-pipe (Windows) admin errors.
            Err(ClientError::Transport(e)) => {
                assert!(e.to_string().contains("config"));
            }
            other => panic!("expected Transport(_), got {other:?}"),
        }
    }
}
