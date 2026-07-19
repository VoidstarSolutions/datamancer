//! The daemon supervisor: process lifecycle, the per-client registry, the
//! control listener, the diagnostics ticker, and graceful shutdown.
//!
//! A single async **actor task** (`run`) owns the client registry and the
//! iceoryx2 [`Node`]; the control listener and per-connection readers send it
//! [`ServerCommand`]s over an `mpsc`, so no lock is ever held across an
//! `.await`. One iceoryx2 node per process; per-client sinks own their service
//! on it. Startup-session anchors hold authoritative sessions alive across
//! client presence (`always_on=true` for the whole process lifetime).

// Native Windows port, in progress (#29): this module's control-dispatch
// machinery (accept loop, per-connection handler, dispatch helpers,
// `ServerCommand` variants, unix-only fields) is reached only through the Unix
// control socket, so it is transitionally dead on Windows until the named-pipe
// transport revives it in Phase 3. Scoped allow — Unix/macOS stay lint-strict;
// remove when Phase 3 lands.
#![cfg_attr(windows, allow(dead_code, unused_imports))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datamancer::transport::{
    Iceoryx2DataSink, Iceoryx2DiagnosticsPublisher, Iceoryx2HealthPublisher,
};
use datamancer::{
    AssetClass, ClientSession, Datamancer, EventKind, HealthView, Instrument, ProviderId, Scope,
    Session, TapLog,
    traits::{EventSink, PublishOutcome},
};
use futures::StreamExt as _;
use iceoryx2::prelude::{NodeBuilder, ipc_threadsafe};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::control::{Reply, Request, SubscriptionSpec, codes, reply_from_library_error};
use crate::credentials::{CredentialHub, privileged_op_permitted};
use crate::error::{DaemonError, Result};
use crate::shutdown::{DrainClient, DrainRecorder, drain};

type Node = iceoryx2::prelude::Node<ipc_threadsafe::Service>;

/// Reduce a [`datamancer::SystemSnapshot`] into the app-facing
/// [`HealthView`] and stamp the two daemon-only fields the core reduction
/// leaves `None` (`version`, `credential_backend`) — the one place this
/// ritual happens, shared by the UDS `Request::Health` reply, the
/// diagnostics-plane health publisher, and (when `web-ui` is enabled) the
/// `/api/health` HTTP handler. Byte-identical behavior everywhere: same
/// [`HealthView::DEFAULT_STALE_AFTER_NS`], same `CARGO_PKG_VERSION` (all call
/// sites live in this crate, so the version constant is identical).
pub(crate) fn stamped_health_view(
    snapshot: &datamancer::SystemSnapshot,
    credential_backend: &str,
) -> HealthView {
    let mut view = HealthView::from_snapshot(snapshot, HealthView::DEFAULT_STALE_AFTER_NS);
    view.daemon.version = Some(env!("CARGO_PKG_VERSION").to_string());
    view.daemon.credential_backend = Some(credential_backend.to_string());
    view
}

/// One connected client: its multiplexing session, its per-client data-plane
/// sink, the pump task feeding the sink, and its service name.
struct ClientEntry {
    name: String,
    session: Option<ClientSession>,
    sink: Arc<dyn EventSink>,
    pump: JoinHandle<()>,
    service: String,
}

#[async_trait]
impl DrainClient for ClientEntry {
    fn name(&self) -> &str {
        &self.name
    }

    async fn flush_sink(&self) {
        if let Err(e) = self.sink.flush().await {
            tracing::warn!(client = %self.name, error = %e, "sink flush failed during drain");
        }
    }

    async fn close(&mut self) {
        if let Some(session) = self.session.take() {
            // Closing emits a terminal `SessionClosing` into the client stream.
            let _ = session.close().await;
        }
        // Let the pump deliver the remaining stream (incl. `SessionClosing`) into
        // the sink before the later flush, rather than severing it immediately.
        // The session close ends the stream, so the pump finishes on its own;
        // bound the wait so a wedged pump cannot hang shutdown, aborting only if
        // it overruns.
        let mut pump = std::mem::replace(&mut self.pump, tokio::spawn(async {}));
        if tokio::time::timeout(Duration::from_secs(2), &mut pump)
            .await
            .is_err()
        {
            pump.abort();
        }
    }
}

/// A command handed to the server actor by a control connection.
enum ServerCommand {
    /// A parsed control request plus the channel to reply on.
    Request {
        request: Request,
        reply: oneshot::Sender<Reply>,
    },
    /// A control connection for `client` hit EOF (emergency teardown).
    Disconnect { client: String },
}

/// Live handles to the embedded web server: its `serve` task, a one-shot
/// shutdown trigger, and the two snapshot-refresh tasks.
#[cfg(feature = "web-ui")]
struct WebHandles {
    serve: JoinHandle<std::io::Result<()>>,
    shutdown: oneshot::Sender<()>,
    refreshers: crate::web::refresh::Refreshers,
}

#[cfg(feature = "web-ui")]
impl WebHandles {
    /// Trigger graceful shutdown: signal the serve task to drain, await it under
    /// a short bound, then abort the refresh tasks.
    async fn shutdown(self) {
        let _ = self.shutdown.send(());
        if tokio::time::timeout(Duration::from_secs(5), self.serve)
            .await
            .is_err()
        {
            tracing::warn!("web server did not drain within timeout");
        }
        self.refreshers.abort();
    }
}

/// The daemon supervisor.
pub struct Server {
    dm: Datamancer,
    node: Node,
    tap_log: Option<Arc<dyn TapLog>>,
    /// Startup-session anchors, held for the process lifetime.
    anchors: Vec<Session>,
    clients: HashMap<String, ClientEntry>,
    next_client_id: u64,
    service_prefix: String,
    max_clients: usize,
    admin_socket: PathBuf,
    shutdown_timeout: Duration,
    diag_interval: Duration,
    /// Optional web-UI settings (Phase 6); `None` (or `enabled=false`) disables
    /// the embedded HTTP introspection surface.
    #[cfg(feature = "web-ui")]
    web: Option<crate::config::WebUiConfig>,
    /// Optional WS client-surface settings; `None` (or `enabled=false`) disables
    /// the remote WebSocket listener.
    #[cfg(feature = "ws")]
    ws: Option<crate::config::WsConfig>,
    /// The web layer's handle to the on-disk config file (Phase 6 config API).
    #[cfg(feature = "web-ui")]
    config_state: crate::web::ConfigState,
    /// The credential broker. Held here so `run` can hand a clone to the
    /// accept loop; credential ops never go through the actor's `dispatch`
    /// (blocking store I/O runs off-actor in `handle_connection`).
    hub: Arc<CredentialHub>,
    /// The config service: settings watches, persist-then-apply, hot
    /// provider ops. Dispatched off-actor in `handle_connection`, same as the
    /// credential hub (`get-config` ungated, the mutating ops peer-cred
    /// gated).
    config_hub: Arc<crate::config_hub::ConfigHub>,
    /// The active credential-store backend name (for `ping`); threaded in at
    /// bootstrap so the actor never touches the hub.
    credential_backend: &'static str,
    /// `true` once a shutdown signal has been observed; rejects new requests.
    draining: bool,
}

impl Server {
    /// Build the daemon from a validated [`Config`]: assemble the
    /// [`Datamancer`], create the one process-wide iceoryx2 node, and open the
    /// `[[startup_session]]` anchors.
    ///
    /// # Errors
    ///
    /// Propagates config/library/transport errors.
    pub async fn bootstrap(config: Config, config_path: std::path::PathBuf) -> Result<Self> {
        let admin_socket = config.server.admin_socket.clone();
        let service_prefix = config.server.service_prefix.clone();
        let max_clients = config.iceoryx2.max_clients;
        let shutdown_timeout = Duration::from_secs(config.server.shutdown_timeout_secs);
        let diag_interval = Duration::from_millis(config.diagnostics.publish_interval_ms);
        let startup_sessions = config.startup_session.clone();

        #[cfg(feature = "web-ui")]
        let web = config.web_ui.clone();
        #[cfg(feature = "ws")]
        let ws = config.ws.clone();
        tracing::debug!(
            live_state_ms = config.diagnostics.publish_interval_ms,
            cache_catalog_ms = config.diagnostics.cache_catalog_interval_ms,
            "diagnostics cadence (cache-catalog split deferred; single cadence in use)"
        );

        // Open the credential store and seed watch channels for every
        // compiled-in provider (so set-credentials works before a provider is
        // enabled). The deprecated env fallback applies only to configured providers.
        let env_fallback = config.configured_providers();
        let all_ids = crate::config::compiled_provider_ids();
        let (hub, sources) = CredentialHub::bootstrap(&all_ids, &env_fallback)?;
        let credential_backend = hub.backend_name();

        let (config_hub, provider_settings) =
            crate::config_hub::ConfigHub::bootstrap(config.clone(), config_path.clone());
        #[cfg(feature = "web-ui")]
        let config_state =
            crate::web::ConfigState::new(config_path.clone(), config.clone(), config_hub.clone());

        let built = config.build_runtime(&sources, provider_settings).await?;
        let dm = built.datamancer;
        let tap_log = built.tap_log;

        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .map_err(|e| DaemonError::Transport(format!("node create: {e:?}")))?;

        // `always_on=true` anchors are held for the process lifetime regardless
        // of client presence. `always_on=false` startup sessions are
        // refcount-driven warmth: with the shared authoritative registry they
        // are created on first client subscribe, so there is nothing to hold at
        // boot (a held-but-clientless session would defeat the refcount model).
        let mut anchors = Vec::new();
        for s in &startup_sessions {
            if !s.always_on {
                tracing::debug!(
                    symbol = %s.symbol,
                    "startup_session is refcount-driven (always_on=false); deferring to first client"
                );
                continue;
            }
            let scope = s.resolve_scope()?;
            let session = dm
                .session(
                    s.instrument(),
                    s.kind.into(),
                    scope,
                    crate::config::persistence_options(s.persistence),
                )
                .await?;
            tracing::info!(symbol = %s.symbol, "anchored always_on startup session");
            anchors.push(session);
        }

        Ok(Self {
            dm,
            node,
            tap_log,
            anchors,
            clients: HashMap::new(),
            next_client_id: 0,
            service_prefix,
            max_clients,
            admin_socket,
            shutdown_timeout,
            diag_interval,
            #[cfg(feature = "web-ui")]
            web,
            #[cfg(feature = "ws")]
            ws,
            #[cfg(feature = "web-ui")]
            config_state,
            hub,
            config_hub,
            credential_backend,
            draining: false,
        })
    }

    /// Run the daemon until a shutdown signal, then drain gracefully.
    ///
    /// # Errors
    ///
    /// Propagates control-socket bind errors.
    pub async fn run(mut self) -> Result<()> {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ServerCommand>(256);
        let accept = self.spawn_control(cmd_tx.clone())?;

        let publisher = Iceoryx2DiagnosticsPublisher::new(&self.node)
            .map_err(|e| DaemonError::Transport(format!("diagnostics publisher: {e:?}")))?;
        let health_publisher = Iceoryx2HealthPublisher::new(&self.node)
            .map_err(|e| DaemonError::Transport(format!("health publisher: {e:?}")))?;
        let diagnostics = spawn_diagnostics(
            self.dm.clone(),
            publisher,
            health_publisher,
            self.credential_backend,
            self.diag_interval,
        );

        #[cfg(feature = "web-ui")]
        let mut web_handles = self.start_web().await?;

        #[cfg(feature = "ws")]
        let (ws_task, ws_shutdown) = self.start_ws();

        #[cfg(unix)]
        tracing::info!(socket = %self.admin_socket.display(), "datamancerd listening");
        #[cfg(windows)]
        tracing::info!("datamancerd running; control surface not yet supported on Windows (#29)");

        // Platform terminate signal, selected on alongside Ctrl-C. Unix:
        // SIGTERM. Windows: console CTRL_SHUTDOWN. Both expose `recv()`.
        #[cfg(unix)]
        let mut terminate = unix_terminate()?;
        #[cfg(windows)]
        let mut terminate = tokio::signal::windows::ctrl_shutdown()?;
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("SIGINT received; shutting down");
                    break;
                }
                _ = terminate.recv() => {
                    #[cfg(unix)]
                    tracing::info!("SIGTERM received; shutting down");
                    #[cfg(windows)]
                    tracing::info!("CTRL_SHUTDOWN received; shutting down");
                    break;
                }
                maybe = cmd_rx.recv() => {
                    match maybe {
                        Some(cmd) => {
                            if self.handle(cmd).await.is_break() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        self.draining = true;

        // Drain the embedded web server first: stop serving introspection HTTP
        // before tearing down the data plane it reports on.
        #[cfg(feature = "web-ui")]
        if let Some(handles) = web_handles.take() {
            handles.shutdown().await;
        }

        // Stop accepting new WS clients and let in-flight connections tear down.
        #[cfg(feature = "ws")]
        {
            if let Some(trigger) = ws_shutdown {
                let _ = trigger.send(());
            }
            if tokio::time::timeout(Duration::from_secs(5), ws_task)
                .await
                .is_err()
            {
                tracing::warn!("ws surface did not drain within timeout");
            }
        }

        let recorder = DrainRecorder::default();
        let clients: Vec<Box<dyn DrainClient>> = self
            .clients
            .drain()
            .map(|(_, entry)| Box::new(entry) as Box<dyn DrainClient>)
            .collect();
        let drain_fut = drain(
            &recorder,
            move || accept.abort(),
            move || diagnostics.abort(),
            clients,
            std::mem::take(&mut self.anchors),
            self.tap_log.clone(),
        );

        // Drop the supervisor's own `cmd_tx` clone so `cmd_rx` closes once the
        // accept loop and connection tasks release theirs.
        drop(cmd_tx);

        if !drain_servicing_late_requests(drain_fut, &mut cmd_rx, self.shutdown_timeout).await {
            tracing::error!("shutdown drain exceeded timeout; forcing exit");
        }
        tracing::debug!(phases = ?recorder.entries(), "drain phases");
        let _ = std::fs::remove_file(&self.admin_socket);
        tracing::info!("datamancerd shutdown complete");
        Ok(())
    }

    /// Start the embedded web UI if it is configured and enabled: warm both
    /// snapshot swaps, spawn the two refresh tasks, install the optional metrics
    /// recorder, and spawn the loopback `serve` task. Returns `None` when the UI
    /// is disabled.
    ///
    /// # Errors
    ///
    /// Propagates a snapshot-warm failure or a bad bind address.
    #[cfg(feature = "web-ui")]
    async fn start_web(&self) -> Result<Option<WebHandles>> {
        use std::net::SocketAddr;

        let Some(web) = self.web.as_ref().filter(|w| w.enabled) else {
            return Ok(None);
        };

        let addr: SocketAddr = format!("{}:{}", web.bind, web.port)
            .parse()
            .map_err(|e| DaemonError::ConfigInvalid(format!("web bind address: {e}")))?;

        // Bind synchronously here (loopback enforced inside `bind`), so a bind
        // failure surfaces to the caller instead of being swallowed in the
        // detached serve task.
        let listener = crate::web::bind(addr).await?;

        #[cfg(feature = "metrics")]
        if let Err(e) = crate::web::metrics::install() {
            tracing::warn!(error = %e, "metrics recorder install failed; /metrics will 503");
        }

        // Warm both swaps before serving so a handler never serves an empty
        // snapshot.
        let mut refreshers =
            crate::web::refresh::Refreshers::warm(&self.dm, self.credential_backend).await?;
        refreshers.spawn(
            self.dm.clone(),
            web.live_state_cadence_ms,
            web.cache_catalog_cadence_ms,
        );

        let state = crate::web::AppState {
            snapshots: refreshers.state.clone(),
            config: self.config_state.clone(),
        };
        let assets_dir = web.assets_dir.clone();
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let serve = tokio::spawn(async move {
            crate::web::serve(state, listener, assets_dir, async move {
                let _ = shutdown_rx.await;
            })
            .await
        });

        Ok(Some(WebHandles {
            serve,
            shutdown,
            refreshers,
        }))
    }

    /// Start the WS client surface if enabled. Returns the serve task and its
    /// shutdown trigger (both `None`-equivalent when disabled: a no-op task and a
    /// dropped sender).
    #[cfg(feature = "ws")]
    fn start_ws(
        &self,
    ) -> (
        tokio::task::JoinHandle<std::io::Result<()>>,
        Option<oneshot::Sender<()>>,
    ) {
        let Some(cfg) = self.ws.as_ref().filter(|w| w.enabled).cloned() else {
            return (tokio::spawn(async { Ok(()) }), None);
        };
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let dm = self.dm.clone();
        let task = tokio::spawn(async move {
            crate::ws::serve(dm, cfg, async move {
                let _ = shutdown_rx.await;
            })
            .await
        });
        (task, Some(shutdown))
    }

    /// Spawn the control-surface accept task. Unix: bind the UDS control socket
    /// and accept connections (the peer-cred gate admits exactly the daemon's
    /// own uid). Windows: the named-pipe control transport and its token-SID
    /// gate are not yet implemented (#29), so this is a no-op placeholder task
    /// and the daemon runs without a control surface. Returns a uniform
    /// `JoinHandle<()>` either way so the drain's `accept.abort()` is shared.
    ///
    /// # Errors
    ///
    /// Propagates the control-socket bind error (Unix).
    #[cfg(unix)]
    fn spawn_control(&self, cmd_tx: mpsc::Sender<ServerCommand>) -> Result<JoinHandle<()>> {
        let listener = self.bind_socket()?;
        // The daemon's own effective uid: the privileged-op peer-cred gate
        // (credential ops, config mutation, shutdown) admits exactly this uid.
        let own_euid = rustix::process::geteuid().as_raw();
        Ok(tokio::spawn(accept_loop(
            listener,
            cmd_tx,
            self.dm.clone(),
            self.hub.clone(),
            self.config_hub.clone(),
            own_euid,
        )))
    }

    #[cfg(windows)]
    #[allow(clippy::unnecessary_wraps, clippy::unused_self)] // parity with the Unix arm
    fn spawn_control(&self, _cmd_tx: mpsc::Sender<ServerCommand>) -> Result<JoinHandle<()>> {
        tracing::warn!(
            "control socket not yet supported on Windows; running without a \
             control surface (native Windows support in progress, #29)"
        );
        Ok(tokio::spawn(async {}))
    }

    #[cfg(unix)]
    fn bind_socket(&self) -> Result<UnixListener> {
        if let Some(parent) = self.admin_socket.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        self.clear_stale_socket()?;
        UnixListener::bind(&self.admin_socket).map_err(|e| {
            DaemonError::from(std::io::Error::new(
                e.kind(),
                format!(
                    "binding control socket {}: {e}",
                    self.admin_socket.display()
                ),
            ))
        })
    }

    /// Remove a *stale* admin socket left by an unclean prior exit.
    ///
    /// The global single-instance lock (acquired before `run`) rules out another
    /// live `datamancerd`, so a socket at this path with no listener is
    /// necessarily stale and safe to remove. Refuses to delete a path that
    /// exists and is *not* a socket, so a misconfiguration cannot clobber an
    /// arbitrary file. Also refuses to steal a socket with a *live* listener:
    /// the lock excludes another daemon but not an unrelated program, so a live
    /// listener here means `admin_socket` is misconfigured onto a foreign
    /// service rather than left stale.
    #[cfg(unix)]
    fn clear_stale_socket(&self) -> Result<()> {
        use std::os::unix::fs::FileTypeExt;
        let meta = match std::fs::symlink_metadata(&self.admin_socket) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if !meta.file_type().is_socket() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "admin_socket path {} exists and is not a socket; refusing to remove it",
                    self.admin_socket.display()
                ),
            )
            .into());
        }
        // The lock rules out another datamancerd, but not an unrelated program.
        // A live listener here is therefore a foreign service, not a stale
        // socket — do not steal it.
        if std::os::unix::net::UnixStream::connect(&self.admin_socket).is_ok() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!(
                    "admin_socket {} is already in use by another process",
                    self.admin_socket.display()
                ),
            )
            .into());
        }
        // A socket with no listener is stale (unclean prior exit); safe to remove.
        std::fs::remove_file(&self.admin_socket)?;
        Ok(())
    }

    async fn handle(&mut self, cmd: ServerCommand) -> std::ops::ControlFlow<()> {
        match cmd {
            ServerCommand::Request { request, reply } => {
                if matches!(request, Request::Shutdown) && !self.draining {
                    tracing::info!("shutdown requested via control op");
                    // Best-effort: this reply is delivered via the oneshot
                    // before we break the actor loop below, but the
                    // connection task writes it out to the socket while the
                    // drain (started by the break) runs concurrently —
                    // same-host best-effort, not guaranteed to reach the
                    // caller before drain completes.
                    let _ = reply.send(Reply::ok());
                    return std::ops::ControlFlow::Break(());
                }
                let response = self.dispatch(request).await;
                let _ = reply.send(response);
            }
            ServerCommand::Disconnect { client } => {
                self.teardown_client(&client).await;
            }
        }
        std::ops::ControlFlow::Continue(())
    }

    async fn dispatch(&mut self, request: Request) -> Reply {
        if self.draining {
            return Reply::error(codes::SHUTTING_DOWN, "daemon is shutting down");
        }
        match request {
            Request::OpenClient {
                client,
                subscriptions,
            } => self.open_client(client, subscriptions).await,
            Request::Subscribe { client, spec } => self.subscribe(&client, spec).await,
            Request::Unsubscribe { client, spec } => {
                let instrument = Instrument::new(
                    ProviderId::new(spec.provider),
                    spec.asset_class.into(),
                    spec.symbol,
                );
                self.unsubscribe(&client, instrument, spec.kind.into())
                    .await
            }
            Request::CloseClient { client } => {
                if self.clients.contains_key(&client) {
                    self.teardown_client(&client).await;
                    Reply::ok()
                } else {
                    Reply::error(codes::UNKNOWN_CLIENT, format!("no such client {client:?}"))
                }
            }
            Request::ListClients => Reply::clients(self.clients.keys().cloned().collect()),
            Request::Ping => Reply::pong(env!("CARGO_PKG_VERSION"), self.credential_backend),
            Request::Snapshot => match self.dm.snapshot().await {
                Ok(snapshot) => Reply::snapshot(snapshot),
                Err(e) => reply_from_library_error(&e),
            },
            // Dispatched off-actor in `handle_connection` (a catalog request
            // awaits live provider REST and must not stall the actor). This
            // arm only exists for match exhaustiveness / defense in depth —
            // it should be unreachable in normal operation.
            Request::Instruments { provider } => {
                let filter = provider.map(ProviderId::new);
                match self.dm.instrument_catalog(filter.as_ref()).await {
                    Ok(catalog) => Reply::instruments(catalog),
                    Err(e) => reply_from_library_error(&e),
                }
            }
            // Dispatched off-actor in `handle_connection` (a capabilities
            // request awaits live provider REST and must not stall the
            // actor). This arm only exists for match exhaustiveness /
            // defense in depth — it should be unreachable in normal
            // operation.
            Request::Capabilities { provider, symbols } => {
                dispatch_capabilities(&self.dm, &provider, &symbols).await
            }
            // Dispatched off-actor in `handle_connection` (blocking store I/O
            // behind `spawn_blocking`, peer-cred gated there — the gate needs
            // the connection's peer uid, which never reaches the actor). This
            // arm only exists for match exhaustiveness / defense in depth —
            // it should be unreachable in normal operation.
            Request::SetCredentials { .. }
            | Request::GetCredentials { .. }
            | Request::ClearCredentials { .. } => Reply::error(
                codes::INTERNAL,
                "credential ops are dispatched off-actor; this arm is unreachable",
            ),
            // Dispatched off-actor in `handle_connection` (config-hub state; the
            // mutating ops and shutdown are peer-cred gated there). These arms only
            // exist for match exhaustiveness / defense in depth.
            Request::GetConfig
            | Request::ConfigureProvider { .. }
            | Request::RemoveProvider { .. } => Reply::error(
                codes::INTERNAL,
                "config ops are dispatched off-actor; this arm is unreachable",
            ),
            // `handle` intercepts Shutdown before dispatch; reaching here means the
            // daemon is already draining.
            Request::Shutdown => Reply::error(codes::SHUTTING_DOWN, "daemon is shutting down"),
            Request::Health => {
                let view = stamped_health_view(&self.dm.snapshot_live(), self.credential_backend);
                Reply::health(view)
            }
        }
    }

    async fn open_client(&mut self, client: String, subscriptions: Vec<SubscriptionSpec>) -> Reply {
        if self.clients.contains_key(&client) {
            return Reply::error(
                codes::DUPLICATE_CLIENT,
                format!("client {client:?} already open"),
            );
        }
        if self.clients.len() >= self.max_clients {
            return Reply::error(
                codes::SERVICE_CAP_EXCEEDED,
                format!("service cap {} exceeded", self.max_clients),
            );
        }

        let id = self.next_client_id;
        let sink = match Iceoryx2DataSink::new(&self.node, id) {
            Ok(sink) => Arc::new(sink) as Arc<dyn EventSink>,
            Err(e) => {
                return Reply::error(codes::INTERNAL, format!("data sink: {e:?}"));
            }
        };
        let service = format!("{}/data/{id}", self.service_prefix);

        let session = self.dm.client_session();
        // Apply seed subscriptions before taking the stream.
        for spec in subscriptions {
            let instrument = spec_instrument(&spec);
            if let Err(e) = session
                .subscribe(
                    instrument,
                    spec.kind.into(),
                    Scope::Live {
                        backfill_from: None,
                    },
                    crate::config::persistence_options(spec.persistence),
                )
                .await
            {
                return reply_from_library_error(&e);
            }
        }

        let stream = match session.take_events().await {
            Ok(stream) => stream,
            Err(e) => return reply_from_library_error(&e),
        };
        let pump = spawn_pump(stream, sink.clone());

        self.next_client_id += 1;
        self.clients.insert(
            client.clone(),
            ClientEntry {
                name: client,
                session: Some(session),
                sink,
                pump,
                service: service.clone(),
            },
        );
        Reply::service(service)
    }

    async fn subscribe(&mut self, client: &str, spec: SubscriptionSpec) -> Reply {
        let Some(entry) = self.clients.get(client) else {
            return Reply::error(codes::UNKNOWN_CLIENT, format!("no such client {client:?}"));
        };
        let Some(session) = entry.session.as_ref() else {
            return Reply::error(codes::SESSION_CLOSED, "client session is closing");
        };
        let instrument = spec_instrument(&spec);
        match session
            .subscribe(
                instrument,
                spec.kind.into(),
                Scope::Live {
                    backfill_from: None,
                },
                crate::config::persistence_options(spec.persistence),
            )
            .await
        {
            Ok(()) => Reply::ok(),
            Err(e) => reply_from_library_error(&e),
        }
    }

    async fn unsubscribe(
        &mut self,
        client: &str,
        instrument: Instrument,
        kind: EventKind,
    ) -> Reply {
        let Some(entry) = self.clients.get(client) else {
            return Reply::error(codes::UNKNOWN_CLIENT, format!("no such client {client:?}"));
        };
        let Some(session) = entry.session.as_ref() else {
            return Reply::error(codes::SESSION_CLOSED, "client session is closing");
        };
        match session.unsubscribe(instrument, kind).await {
            Ok(()) => Reply::ok(),
            Err(e) => reply_from_library_error(&e),
        }
    }

    async fn teardown_client(&mut self, client: &str) {
        if let Some(mut entry) = self.clients.remove(client) {
            tracing::info!(client, service = %entry.service, "tearing down client");
            entry.flush_sink().await;
            entry.close().await;
        }
    }
}

/// The instrument for a subscription spec.
fn spec_instrument(spec: &SubscriptionSpec) -> Instrument {
    Instrument::new(
        ProviderId::new(spec.provider.clone()),
        spec.asset_class.into(),
        spec.symbol.clone(),
    )
}

/// Reject a control request that carries unknown top-level JSON keys. `serde`'s
/// `deny_unknown_fields` cannot cover `Request` directly (it is an
/// internally-tagged enum, and `Subscribe` `#[serde(flatten)]`s its spec — both
/// disable the attribute), so compare the input object's keys against the keys
/// the parsed request round-trips to. Any input key absent from the canonical
/// form was silently ignored and is reported as an error.
fn reject_unknown_keys(line: &str, request: &Request) -> std::result::Result<(), String> {
    let input: serde_json::Value =
        serde_json::from_str(line).map_err(|e| format!("invalid request: {e}"))?;
    let canonical = serde_json::to_value(request).map_err(|e| format!("invalid request: {e}"))?;
    if let (Some(input), Some(canonical)) = (input.as_object(), canonical.as_object()) {
        for key in input.keys() {
            if !canonical.contains_key(key) {
                return Err(format!("unknown field `{key}`"));
            }
        }
    }
    Ok(())
}

/// Pump a client's multiplexed stream into its per-client sink, in arrival
/// order (`(instrument, seq)`; no cross-symbol order is created). A single
/// sequential pump preserves the stream's existing order.
fn spawn_pump(mut stream: datamancer::EventStream, sink: Arc<dyn EventSink>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(ev) = stream.next().await {
            match sink.publish(ev).await {
                PublishOutcome::Delivered => {}
                PublishOutcome::Rejected(_) => {
                    tracing::warn!("sink rejected event; stopping pump");
                    break;
                }
            }
        }
        if let Err(e) = sink.flush().await {
            tracing::warn!(error = %e, "final sink flush failed");
        }
    })
}

/// Accept control connections and spawn a reader per connection.
#[cfg(unix)]
async fn accept_loop(
    listener: UnixListener,
    cmd_tx: mpsc::Sender<ServerCommand>,
    dm: Datamancer,
    hub: Arc<CredentialHub>,
    config_hub: Arc<crate::config_hub::ConfigHub>,
    own_euid: u32,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(handle_connection(
                    stream,
                    cmd_tx.clone(),
                    dm.clone(),
                    hub.clone(),
                    config_hub.clone(),
                    own_euid,
                ));
            }
            Err(e) => {
                tracing::warn!(error = %e, "control accept failed");
            }
        }
    }
}

/// Look up per-instrument capabilities for a provider's symbols. Ungated
/// (a read op, like `Instruments`); awaits live provider REST, hence
/// dispatched off-actor.
async fn dispatch_capabilities(dm: &Datamancer, provider: &str, symbols: &[String]) -> Reply {
    let pid = ProviderId::new(provider.to_string());
    // `capabilities` is keyed on symbol; the asset class here is a placeholder
    // the provider overwrites with the authoritative class on the returned
    // entry (see `Provider::capabilities`). Do not treat it as a real class.
    let instruments: Vec<Instrument> = symbols
        .iter()
        .map(|s| Instrument::new(pid.clone(), AssetClass::Equity, s.clone()))
        .collect();
    match dm.instrument_capabilities(&pid, &instruments).await {
        Ok(entries) => Reply::capabilities(entries),
        Err(e) => reply_from_library_error(&e),
    }
}

/// Route an already-gated credential op to the credential hub.
async fn dispatch_credential_op(request: Request, hub: &CredentialHub) -> Reply {
    match request {
        Request::SetCredentials {
            provider,
            credentials,
        } => hub.set(&provider, credentials).await,
        Request::GetCredentials { provider } => hub.get(&provider).await,
        Request::ClearCredentials { provider } => hub.clear(&provider).await,
        // Narrowed by the caller's `matches!`.
        _ => Reply::error(codes::INTERNAL, "unreachable credential dispatch"),
    }
}

/// Route an already-gated config-mutation or shutdown op. `ConfigureProvider`
/// and `RemoveProvider` run against the config hub; `Shutdown` is forwarded
/// to the actor (a run-loop decision, not hub state). Returns `None` when the
/// actor's command channel is closed, signalling the caller should stop
/// servicing this connection.
async fn dispatch_config_op(
    request: Request,
    config_hub: &crate::config_hub::ConfigHub,
    cmd_tx: &mpsc::Sender<ServerCommand>,
) -> Option<Reply> {
    Some(match request {
        Request::ConfigureProvider { provider, settings } => {
            config_hub.configure_provider(&provider, settings).await
        }
        Request::RemoveProvider { provider } => config_hub.remove_provider(&provider).await,
        Request::Shutdown => {
            let (tx, rx) = oneshot::channel();
            if cmd_tx
                .send(ServerCommand::Request {
                    request: Request::Shutdown,
                    reply: tx,
                })
                .await
                .is_err()
            {
                return None;
            }
            match rx.await {
                Ok(reply) => reply,
                Err(_) => return None,
            }
        }
        // Narrowed by the caller's `matches!`.
        _ => Reply::error(codes::INTERNAL, "unreachable config dispatch"),
    })
}

/// One long-lived control connection. Reads newline-delimited JSON requests,
/// forwards each to the server actor, writes the reply line. On EOF, if this
/// connection had opened a client, signals an emergency teardown.
///
/// `Request::Instruments`, the credential ops, and the config-service ops
/// are dispatched here, off-actor, rather than forwarded to the actor: the
/// first awaits a live provider REST call, the credential ops do blocking
/// credential-store I/O (behind `spawn_blocking`) — neither may stall
/// unrelated control traffic on the single-actor loop. `get-config` is
/// ungated — credentials never live in the config, and the one
/// secret-shaped field, `[ws].auth_token`, is redacted in its reply (see
/// `ConfigHub::get_config`); the credential ops,
/// `configure-provider`/`remove-provider`, and `shutdown` are additionally
/// gated on the peer's uid matching the daemon's own effective uid, captured
/// per-connection before the stream is split. `shutdown` is forwarded to the
/// actor (a run-loop decision, not hub state) once the gate passes.
#[cfg(unix)]
async fn handle_connection(
    stream: UnixStream,
    cmd_tx: mpsc::Sender<ServerCommand>,
    dm: Datamancer,
    hub: Arc<CredentialHub>,
    config_hub: Arc<crate::config_hub::ConfigHub>,
    own_euid: u32,
) {
    // Kernel-reported peer credentials; unreadable peer = privileged ops
    // denied (never defaulted).
    let peer_uid = stream.peer_cred().ok().map(|c| c.uid());
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    let mut opened_client: Option<String> = None;

    loop {
        let Ok(Some(line)) = lines.next_line().await else {
            break;
        };
        if line.trim().is_empty() {
            continue;
        }
        let reply = match serde_json::from_str::<Request>(&line) {
            Ok(request) => {
                if let Err(detail) = reject_unknown_keys(&line, &request) {
                    Reply::error(codes::BAD_REQUEST, detail)
                } else if let Request::Instruments { provider } = &request {
                    let filter = provider.clone().map(ProviderId::new);
                    match dm.instrument_catalog(filter.as_ref()).await {
                        Ok(catalog) => Reply::instruments(catalog),
                        Err(e) => reply_from_library_error(&e),
                    }
                } else if let Request::Capabilities { provider, symbols } = &request {
                    dispatch_capabilities(&dm, provider, symbols).await
                } else if matches!(
                    &request,
                    Request::SetCredentials { .. }
                        | Request::GetCredentials { .. }
                        | Request::ClearCredentials { .. }
                ) {
                    if privileged_op_permitted(peer_uid, own_euid) {
                        dispatch_credential_op(request, &hub).await
                    } else {
                        Reply::error(
                            codes::PERMISSION_DENIED,
                            "credential ops require the daemon owner's uid",
                        )
                    }
                } else if matches!(&request, Request::GetConfig) {
                    config_hub.get_config().await
                } else if matches!(
                    &request,
                    Request::ConfigureProvider { .. }
                        | Request::RemoveProvider { .. }
                        | Request::Shutdown
                ) {
                    if privileged_op_permitted(peer_uid, own_euid) {
                        match dispatch_config_op(request, &config_hub, &cmd_tx).await {
                            Some(reply) => reply,
                            None => break,
                        }
                    } else {
                        Reply::error(
                            codes::PERMISSION_DENIED,
                            "config mutation and shutdown ops require the daemon owner's uid",
                        )
                    }
                } else {
                    let open_client_name = match &request {
                        Request::OpenClient { client, .. } => Some(client.clone()),
                        _ => None,
                    };
                    let (tx, rx) = oneshot::channel();
                    if cmd_tx
                        .send(ServerCommand::Request { request, reply: tx })
                        .await
                        .is_err()
                    {
                        break;
                    }
                    match rx.await {
                        Ok(reply) => {
                            // Arm EOF teardown only after a *successful* open, so a
                            // rejected open (e.g. duplicate-name) never causes the
                            // EOF path to tear down the existing client of that
                            // name.
                            if reply.ok
                                && let Some(name) = open_client_name
                            {
                                opened_client = Some(name);
                            }
                            reply
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(e) => Reply::error(codes::BAD_REQUEST, format!("invalid request: {e}")),
        };
        let Ok(mut buf) = serde_json::to_vec(&reply) else {
            continue;
        };
        buf.push(b'\n');
        if write.write_all(&buf).await.is_err() {
            break;
        }
    }

    // Emergency teardown on EOF for a client this connection opened.
    if let Some(client) = opened_client {
        let _ = cmd_tx.send(ServerCommand::Disconnect { client }).await;
    }
}

/// Spawn the diagnostics ticker: assemble the snapshot on cadence and publish
/// it on both the diagnostics plane and the health plane (one snapshot feeds
/// both — the health view is stamped with the daemon version and credential
/// backend, same as the `Request::Health` dispatch arm). `snapshot()` is
/// async (the cache catalog does I/O); awaiting it here keeps the ticker off
/// the actor's critical path.
fn spawn_diagnostics(
    dm: Datamancer,
    publisher: Iceoryx2DiagnosticsPublisher,
    health_publisher: Iceoryx2HealthPublisher,
    credential_backend: &'static str,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            match dm.snapshot().await {
                Ok(snapshot) => {
                    if let Err(e) = publisher.publish(&snapshot) {
                        tracing::warn!(error = %e, "diagnostics publish failed");
                    }
                    let view = stamped_health_view(&snapshot, credential_backend);
                    if let Err(e) = health_publisher.publish(&view) {
                        tracing::warn!(error = %e, "health publish failed");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "diagnostics snapshot failed");
                }
            }
        }
    })
}

/// Run `drain_fut` to completion while still servicing `cmd_rx`: the accept
/// loop is aborted at the start of `drain`, but connection tasks already
/// spawned may still send a request. Reply `shutting_down` promptly so a
/// producer never blocks forever on its reply channel (which would otherwise
/// leave a wedged connection task and could stall a clean exit). Returns
/// `false` if `shutdown_timeout` was exceeded.
async fn drain_servicing_late_requests(
    drain_fut: impl std::future::Future<Output = ()>,
    cmd_rx: &mut mpsc::Receiver<ServerCommand>,
    shutdown_timeout: Duration,
) -> bool {
    tokio::pin!(drain_fut);
    let drained = tokio::time::timeout(shutdown_timeout, async {
        let mut producers_open = true;
        loop {
            tokio::select! {
                () = &mut drain_fut => break,
                maybe = cmd_rx.recv(), if producers_open => match maybe {
                    Some(ServerCommand::Request { reply, .. }) => {
                        let _ = reply.send(Reply::error(
                            codes::SHUTTING_DOWN,
                            "daemon is shutting down",
                        ));
                    }
                    Some(ServerCommand::Disconnect { .. }) => {}
                    None => producers_open = false,
                },
            }
        }
    })
    .await;
    drained.is_ok()
}

/// A SIGTERM stream (Unix). Wrapped so `run` can `select!` on it.
#[cfg(unix)]
fn unix_terminate() -> Result<tokio::signal::unix::Signal> {
    Ok(tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn privileged_gate_requires_exact_uid_match() {
        use crate::credentials::privileged_op_permitted;
        assert!(privileged_op_permitted(Some(501), 501));
        assert!(!privileged_op_permitted(Some(502), 501));
        assert!(!privileged_op_permitted(None, 501));
    }

    #[test]
    fn reject_unknown_keys_accepts_omitted_settings_on_configure_provider() {
        let line = r#"{"op":"configure-provider","provider":"alpaca"}"#;
        let request: Request = serde_json::from_str(line).expect("parse");
        assert!(
            reject_unknown_keys(line, &request).is_ok(),
            "omitted `settings` must round-trip to the canonical `\"settings\":null` form"
        );
    }

    #[test]
    fn reject_unknown_keys_still_rejects_bogus_top_level_key_on_configure_provider() {
        let line = r#"{"op":"configure-provider","provider":"alpaca","bogus":1}"#;
        let request: Request = serde_json::from_str(line).expect("parse");
        assert!(reject_unknown_keys(line, &request).is_err());
    }

    /// `AppHandle::ensure`'s version gate (`datamancer-client`'s
    /// `app::check_version`) requires the daemon's `ping` version
    /// (`env!("CARGO_PKG_VERSION")`, stamped above in `Request::Ping`) to be
    /// major.minor-compatible with the client's own `CARGO_PKG_VERSION` —
    /// but `datamancerd` and `datamancer-client` version independently in
    /// this workspace. Nothing else enforces they stay in lockstep, so pin
    /// it here: this test fails the moment one crate is bumped without the
    /// other, which is the signal to bump both together.
    #[test]
    fn daemon_and_client_versions_stay_in_lockstep() {
        assert_eq!(
            env!("CARGO_PKG_VERSION"),
            datamancer_client::VERSION,
            "datamancerd and datamancer-client must be version-bumped together — \
             the ping version gate in AppHandle::ensure compares them"
        );
    }
}
