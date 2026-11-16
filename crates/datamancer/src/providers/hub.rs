//! The streaming hub behind an Alpaca provider's live sessions.
//!
//! A provider spawns one hub task (through its [`HubSlot`]) and hands every
//! session a [`SharedLiveHandle`] onto it. The hub owns the one upstream
//! connection: it connects through the provider's [`Connector`], applies the
//! aggregate subscription set, routes each event to the sink of the session
//! that owns its `(instrument, kind)`, and handles parking, backoff and
//! reconnects. Everything provider-specific sits behind [`Connector`],
//! [`Upstream`] and [`SubscriptionSet`]; the loop itself is generic, so the
//! per-message path stays monomorphic.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datamancer_core::{
    Control, ControlKind, DisconnectCause, Error, EventKind, Instrument, LiveHandle, MarketEvent,
    Result, Seq, Timestamp,
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use super::credentials::{AlpacaCredentials, CredentialsSource, PresentCredentials};
use super::runtime::{SettingsSource, WatchRead, Watched};
use crate::session::ReconnectPolicy;

/// How a failure's [`Classify::classify`] reads a failed connect, a refused
/// re-subscribe or a failed stream, and so what the hub does about it. A
/// rejected credential pair from a watched source parks the hub until a
/// rotation (retrying cannot help, provisioning can); one from a static
/// source, and everything transient, backs off. A session's subscribe the
/// upstream refuses is only answered by its class and never parks the hub;
/// the hub's own re-subscribe on a fresh connection, refused as a rejected
/// credential, ends that connection like a failed stream. Which of an
/// upstream's failures are which is the failure type's to say, in its
/// [`Classify`] impl.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamFailure {
    Unauthenticated,
    Transient,
}

/// Why the hub has no connection, which says what ends the wait besides a
/// settings change, and what a subscribe arriving meanwhile is refused
/// with: a credential park is `Error::Unauthenticated`, a disable is
/// `Error::ProviderDisabled`, a backoff is `Error::Provider`. An unsubscribe
/// arriving meanwhile is applied and succeeds.
#[derive(Debug, Clone, Copy)]
enum Disconnected {
    /// Parked until the settings re-enable the provider.
    Disabled,
    /// Parked until a watched credential source delivers its first value.
    MissingCredentials,
    /// Parked until a watched credential source rotates a rejected pair.
    RejectedCredentials,
    /// Sleeping out a backoff of this length before the next connect.
    Reconnecting(Duration),
}

impl Disconnected {
    /// The error `provider` refuses a subscribe with while in this state.
    fn refusal(self, provider: &str) -> Error {
        let provider = provider.to_string();
        match self {
            Self::Disabled => Error::ProviderDisabled { provider },
            Self::MissingCredentials => Error::Unauthenticated {
                provider,
                message: "waiting for credentials".to_string(),
            },
            Self::RejectedCredentials => Error::Unauthenticated {
                provider,
                message: "waiting for new credentials after auth rejection".to_string(),
            },
            Self::Reconnecting(_) => Error::Provider {
                provider,
                message: "provider is reconnecting".to_string(),
            },
        }
    }

    /// Whether a credentials rotation ends the wait in this state.
    const fn wakes_on_rotation(self) -> bool {
        matches!(self, Self::MissingCredentials | Self::RejectedCredentials)
    }
}

/// A provider's aggregate subscription set, as its upstream takes it.
pub(super) trait SubscriptionSet: Clone + Send + Sync + 'static {
    /// The set holding no pair.
    fn empty() -> Self;
    /// Whether the set holds no pair.
    fn is_empty(&self) -> bool;
    /// Add one `(instrument, kind)` pair; one already held stays as it is.
    fn insert(&mut self, instrument: &Instrument, kind: EventKind);
    /// Remove one `(instrument, kind)` pair; one not held changes nothing.
    fn remove(&mut self, instrument: &Instrument, kind: EventKind);
}

/// How a failure reads, as a [`StreamFailure`], which says what the hub
/// does about it. A property of the failure type, so every upstream that
/// fails the same way classifies the same way.
pub(super) trait Classify {
    /// Whether this failure is a rejected credential or transient.
    fn classify(&self) -> StreamFailure;
}

/// One connected upstream: the calls the hub makes on it.
pub(super) trait Upstream: Send {
    /// The subscription set this upstream takes.
    type Subs: SubscriptionSet;
    /// What a failed call returns.
    type Failure: std::error::Error + Classify + Send + Sync + 'static;
    /// Apply the aggregate subscription set `subs`.
    fn add(
        &mut self,
        subs: &Self::Subs,
    ) -> impl Future<Output = std::result::Result<(), Self::Failure>> + Send;
    /// Remove the pairs in `removal`.
    fn remove(
        &mut self,
        removal: &Self::Subs,
    ) -> impl Future<Output = std::result::Result<(), Self::Failure>> + Send;
    /// The next message, translated into 0..N events.
    ///
    /// The hub races this future against its other work and drops it
    /// whenever another branch wins, so dropping it mid-await must not lose
    /// a message.
    fn next_events(
        &mut self,
    ) -> impl Future<Output = std::result::Result<Vec<MarketEvent>, Self::Failure>> + Send;
    /// Close the connection.
    fn shut_down(self) -> impl Future<Output = ()> + Send;
}

/// What differs between providers: how to open the upstream. The sources
/// the hub reads are data, not behaviour, and come with the slot as
/// [`HubSources`]; how a failure reads is the failure type's own.
pub(super) trait Connector: Send + Sync + 'static {
    /// The provider id stamped on every control and error the hub emits.
    const PROVIDER_ID: &'static str;
    /// The runtime settings a connect reads.
    type Settings: Clone + Send + Sync + 'static;
    /// The connected upstream; its `Failure` is what a failed connect
    /// returns too.
    type Upstream: Upstream;
    /// Open the upstream with `settings` and `credentials`.
    fn connect(
        &self,
        settings: &Self::Settings,
        credentials: PresentCredentials,
    ) -> impl Future<Output = std::result::Result<Self::Upstream, FailureOf<Self>>> + Send;
}

/// The failure type of a connector's upstream.
pub(super) type FailureOf<C> = <<C as Connector>::Upstream as Upstream>::Failure;

/// What the hub reads from its provider's configuration: where the settings
/// and credentials come from, and how to back off between failed connects.
#[derive(Clone)]
pub(super) struct HubSources<S> {
    /// Where the settings come from; `None` parks the hub disabled.
    pub(super) settings: SettingsSource<S>,
    /// Where the credentials come from; none yet parks the hub.
    pub(super) credentials: CredentialsSource,
    /// The backoff policy between failed connects.
    pub(super) reconnect: ReconnectPolicy,
}

// ---------------------------------------------------------------------------
// Hub slot
// ---------------------------------------------------------------------------

/// A provider's hub task: none until the first live session, then one that
/// takes commands. The slot guards its own state, so a provider holds it
/// plainly and every session's `start_live` shares it; a hub whose task has
/// ended is replaced by the next session that attaches.
pub(super) struct HubSlot<C: Connector> {
    connector: Arc<C>,
    sources: HubSources<C::Settings>,
    state: std::sync::Mutex<SlotState>,
}

struct SlotState {
    active: Option<ActiveHub>,
    #[cfg(test)]
    spawns: usize,
}

struct ActiveHub {
    /// Closed once the hub task has ended, however it ended: that is how
    /// the slot tells a live hub from a dead one.
    cmd_tx: mpsc::Sender<HubCommand>,
    /// The hub exits on its own once the provider and every handle are
    /// gone (its command channel closes); the join handle is kept so a
    /// test can observe that.
    #[allow(dead_code, reason = "observed by the lifecycle test only")]
    task: JoinHandle<()>,
}

impl<C: Connector> HubSlot<C> {
    /// A slot with no hub running; a hub it spawns connects through
    /// `connector` and reads `sources`.
    pub(super) fn new(connector: C, sources: HubSources<C::Settings>) -> Self {
        Self {
            connector: Arc::new(connector),
            sources,
            state: std::sync::Mutex::new(SlotState {
                active: None,
                #[cfg(test)]
                spawns: 0,
            }),
        }
    }

    /// A session's handle onto the hub, with its events routed to `sink`;
    /// spawns the hub if none runs or the last one has ended.
    pub(super) fn attach(&self, sink: mpsc::Sender<MarketEvent>) -> SharedLiveHandle {
        SharedLiveHandle::new(self.ensure(), sink)
    }

    /// The slot's state. The lock is never held across an await, and
    /// [`ensure`](Self::ensure) writes the state only after its spawn has
    /// returned, so a lock poisoned by a spawn that panicked still guards a
    /// consistent state, which is taken as it is.
    fn state(&self) -> std::sync::MutexGuard<'_, SlotState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The hub's command sender, spawning the hub if none runs. A hub whose
    /// task has ended (a defensive exit, a panic in its connector) has
    /// dropped its receiver, so its sender is closed and it is replaced like
    /// an idle slot, rather than handed out to answer every later command
    /// with `SessionClosed`.
    fn ensure(&self) -> mpsc::Sender<HubCommand> {
        let mut state = self.state();
        if let Some(active) = &state.active
            && !active.cmd_tx.is_closed()
        {
            return active.cmd_tx.clone();
        }
        let (cmd_tx, cmd_rx) = mpsc::channel::<HubCommand>(64);
        let task = tokio::spawn(run_hub(
            Arc::clone(&self.connector),
            self.sources.clone(),
            cmd_rx,
        ));
        #[cfg(test)]
        {
            state.spawns += 1;
        }
        state.active = Some(ActiveHub {
            cmd_tx: cmd_tx.clone(),
            task,
        });
        cmd_tx
    }

    /// How many hub tasks this slot has spawned (test observability).
    #[cfg(test)]
    pub(super) fn spawns(&self) -> usize {
        self.state().spawns
    }

    /// Take the hub task out of the slot, dropping the slot's own command
    /// sender: once every handle is gone too, the task exits and the
    /// returned join handle resolves (test observability).
    #[cfg(test)]
    pub(super) fn take_task(&self) -> Option<JoinHandle<()>> {
        self.state().active.take().map(|active| active.task)
    }
}

// ---------------------------------------------------------------------------
// Hub command channel and live handle
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum HubCommand {
    Subscribe {
        instrument: Instrument,
        kind: EventKind,
        sink: mpsc::Sender<MarketEvent>,
        ack: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        instrument: Instrument,
        kind: EventKind,
        ack: oneshot::Sender<Result<()>>,
    },
}

/// One session's handle onto the shared hub: it carries the session's sink
/// to the hub on subscribe and cleans its route up on close. The socket
/// itself belongs to the hub, never to a handle.
pub(super) struct SharedLiveHandle {
    cmd_tx: mpsc::Sender<HubCommand>,
    /// Per-session sink the hub will route events to once subscribed.
    sink: mpsc::Sender<MarketEvent>,
    /// Tracks the pair this handle is subscribed to so `close` can clean it
    /// up. With a per-session handle this is at most one pair.
    subscribed: Mutex<Option<(Instrument, EventKind)>>,
}

impl SharedLiveHandle {
    /// A handle that commands the hub over `cmd_tx` and has its events
    /// routed to `sink`.
    fn new(cmd_tx: mpsc::Sender<HubCommand>, sink: mpsc::Sender<MarketEvent>) -> Self {
        Self {
            cmd_tx,
            sink,
            subscribed: Mutex::new(None),
        }
    }
}

#[async_trait]
impl LiveHandle for SharedLiveHandle {
    async fn subscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(HubCommand::Subscribe {
                instrument: instrument.clone(),
                kind,
                sink: self.sink.clone(),
                ack: tx,
            })
            .await
            .map_err(|_| Error::SessionClosed)?;
        let res = rx.await.map_err(|_| Error::SessionClosed)?;
        if res.is_ok() {
            *self.subscribed.lock().await = Some((instrument, kind));
        }
        res
    }

    async fn unsubscribe(&self, instrument: Instrument, kind: EventKind) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(HubCommand::Unsubscribe {
                instrument: instrument.clone(),
                kind,
                ack: tx,
            })
            .await
            .map_err(|_| Error::SessionClosed)?;
        let res = rx.await.map_err(|_| Error::SessionClosed)?;
        if res.is_ok() {
            let mut slot = self.subscribed.lock().await;
            if slot.as_ref() == Some(&(instrument, kind)) {
                *slot = None;
            }
        }
        res
    }

    async fn close(self: Box<Self>) -> Result<()> {
        // If the session never explicitly unsubscribed, do it now so the hub
        // drops the route. The hub itself is shared; its lifetime is tied to
        // the provider, not any one session.
        let pair = self.subscribed.lock().await.take();
        if let Some((instrument, kind)) = pair {
            let (tx, rx) = oneshot::channel();
            let _ = self
                .cmd_tx
                .send(HubCommand::Unsubscribe {
                    instrument,
                    kind,
                    ack: tx,
                })
                .await;
            let _ = rx.await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Hub task
// ---------------------------------------------------------------------------

/// The sinks of the live sessions, keyed by the pair each owns.
pub(super) type Routes = HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>;
/// The credentials watch of one connect cycle.
type CredentialsRx = Watched<Option<AlpacaCredentials>>;
/// The settings watch of one connect cycle.
type SettingsRx<S> = Watched<Option<S>>;

/// What the hub does once a connection, or the attempt at one, is over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    /// Resolve the sources again and connect.
    Reconnect,
    /// The provider and every handle are gone: exit.
    Exit,
}

/// Where a connection failed, for the log line and the disconnect reason.
#[derive(Debug, Clone, Copy)]
enum FailurePoint {
    /// The connect itself.
    Connect,
    /// The re-subscribe of the aggregate set on a fresh connection.
    Resubscribe,
    /// The connected stream.
    Stream,
}

impl FailurePoint {
    /// The warning logged for a failure here.
    fn log_message(self) -> &'static str {
        match self {
            Self::Connect => "market-data stream connect failed",
            Self::Resubscribe => "market-data stream re-subscribe refused",
            Self::Stream => "market-data stream failed",
        }
    }

    /// The reason the `ProviderDisconnected` control carries for a failure
    /// here, with the error's `chain`.
    fn reason(self, chain: &str) -> String {
        match self {
            Self::Connect => format!("connect failed: {chain}"),
            Self::Resubscribe => format!("re-subscribe refused: {chain}"),
            Self::Stream => format!("websocket: {chain}"),
        }
    }
}

/// One hub task: the connector and sources it connects with, the command
/// channel its handles reach it over, and the state that outlives any one
/// connection.
struct Hub<C: Connector> {
    connector: Arc<C>,
    sources: HubSources<C::Settings>,
    cmd_rx: mpsc::Receiver<HubCommand>,
    /// Per-session sinks keyed by the pair they own; the session registry's
    /// at-most-one-live-session-per-pair rule keeps this a function.
    routes: Routes,
    /// Authoritative aggregate subscription set, applied to every fresh socket.
    subs: <C::Upstream as Upstream>::Subs,
    /// The next backoff, doubled after each failed connect up to the
    /// policy's maximum and reset once a fresh connection has the
    /// subscription set applied.
    backoff_ms: u64,
    /// The cause the routes last heard a disconnect for, until the next
    /// connect, so a disable is told once however the hub comes to it.
    routes_heard: Option<DisconnectCause>,
    /// The credentials watch of the current cycle, taken at its read.
    cred_rx: CredentialsRx,
    /// The settings watch of the current cycle, taken at its read.
    settings_rx: SettingsRx<C::Settings>,
}

/// Run a provider's hub: connect, serve, and connect again, until the
/// provider and every handle are gone.
async fn run_hub<C: Connector>(
    connector: Arc<C>,
    sources: HubSources<C::Settings>,
    cmd_rx: mpsc::Receiver<HubCommand>,
) {
    let mut hub = Hub {
        backoff_ms: sources.reconnect.initial_backoff_ms,
        connector,
        sources,
        cmd_rx,
        routes: HashMap::new(),
        subs: <C::Upstream as Upstream>::Subs::empty(),
        routes_heard: None,
        cred_rx: Watched(None),
        settings_rx: Watched(None),
    };
    while hub.cycle().await == Next::Reconnect {}
}

impl<C: Connector> Hub<C> {
    /// One connect cycle: resolve the sources, connect, apply the aggregate
    /// subscription set and serve the stream until it ends, parking or
    /// backing off as its end demands.
    async fn cycle(&mut self) -> Next {
        let settings = match self.sources.settings.read() {
            WatchRead::Value { value, rx } => {
                self.settings_rx = rx;
                value
            }
            // Disabled: tell the routes, then park until the settings watch
            // delivers a value.
            WatchRead::Empty(rx) => {
                self.settings_rx = rx;
                self.tell_routes_disabled().await;
                return self.idle(Disconnected::Disabled).await;
            }
        };
        let credentials = match self.sources.credentials.read() {
            WatchRead::Value { value, rx } => {
                self.cred_rx = rx;
                value
            }
            // No credentials yet: wait for provisioning instead of hammering
            // bad auth, then resolve again.
            WatchRead::Empty(rx) => {
                self.cred_rx = rx;
                return self.idle(Disconnected::MissingCredentials).await;
            }
        };

        let mut upstream = match self.connector.connect(&settings, credentials).await {
            Ok(upstream) => {
                self.routes_heard = None;
                broadcast_control(
                    &self.routes,
                    ControlKind::ProviderConnected {
                        provider: C::PROVIDER_ID.to_string(),
                    },
                )
                .await;
                upstream
            }
            Err(err) => return self.fail(FailurePoint::Connect, &err).await,
        };

        // Re-apply the aggregate subscription set on a fresh socket. A
        // refusal that reads as a rejected credential is one, wherever the
        // server says so, and ends this connection like a failed stream; any
        // other refusal is broadcast and the stream stays up, since a
        // reconnect would only be refused again.
        let applied = if self.subs.is_empty() {
            Ok(())
        } else {
            upstream.add(&self.subs).await
        };
        match applied {
            Ok(()) => self.reset_backoff(),
            Err(err) => match err.classify() {
                StreamFailure::Unauthenticated => {
                    upstream.shut_down().await;
                    return self.fail(FailurePoint::Resubscribe, &err).await;
                }
                StreamFailure::Transient => {
                    broadcast_error::<C>(
                        &self.routes,
                        format!("re-subscribe failed: {}", error_chain(&err)),
                    )
                    .await;
                }
            },
        }

        self.serve(upstream).await
    }

    /// Serve the connected `upstream`: its messages, the handles' commands
    /// and the credential and settings watches, until the connection ends.
    async fn serve(&mut self, mut upstream: C::Upstream) -> Next {
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        // The provider and every handle are gone; no command
                        // can ever arrive again. Close the websocket and exit.
                        upstream.shut_down().await;
                        return Next::Exit;
                    };
                    let applied = handle_route_command::<C>(
                        &mut self.routes,
                        &mut self.subs,
                        &mut upstream,
                        cmd,
                    )
                    .await;
                    if applied {
                        self.reset_backoff();
                    }
                }
                next = upstream.next_events() => {
                    match next {
                        Ok(events) => {
                            for ev in events {
                                dispatch_event(&self.routes, ev).await;
                            }
                        }
                        Err(err) => {
                            drop(upstream);
                            return self.fail(FailurePoint::Stream, &err).await;
                        }
                    }
                }
                _ = self.cred_rx.changed() => {
                    tracing::info!(
                        provider = C::PROVIDER_ID,
                        "credentials changed; reconnecting"
                    );
                    // Same in-band control the failure path broadcasts,
                    // then reconnect at once with the new credentials.
                    self.broadcast_disconnected(
                        "credentials rotated".to_string(),
                        DisconnectCause::Error,
                    )
                    .await;
                    upstream.shut_down().await;
                    return Next::Reconnect;
                }
                settings = self.settings_rx.changed() => {
                    // The connection ends either way, and the routes hear it
                    // end before anything else.
                    if settings.is_some() {
                        tracing::info!(
                            provider = C::PROVIDER_ID,
                            "settings changed; reconnecting"
                        );
                        self.broadcast_disconnected(
                            "settings changed".to_string(),
                            DisconnectCause::Error,
                        )
                        .await;
                    } else {
                        self.tell_routes_disabled().await;
                    }
                    upstream.shut_down().await;
                    return Next::Reconnect;
                }
            }
        }
    }

    /// A failed connect, refused re-subscribe or failed stream: classify it,
    /// log it, tell every route how the provider disconnected, then park
    /// until the credentials rotate when it is a rejected credential from a
    /// watched source (retrying cannot help, provisioning can), or back off
    /// (a static source cannot rotate, and anything else may clear on its
    /// own).
    async fn fail(&mut self, point: FailurePoint, err: &FailureOf<C>) -> Next {
        let failure = err.classify();
        let unauthenticated = failure == StreamFailure::Unauthenticated;
        let chain = error_chain(err);
        tracing::warn!(
            provider = C::PROVIDER_ID,
            ?failure,
            error = %chain,
            "{}",
            point.log_message()
        );
        self.broadcast_disconnected(
            point.reason(&chain),
            if unauthenticated {
                DisconnectCause::Unauthenticated
            } else {
                DisconnectCause::Error
            },
        )
        .await;
        if unauthenticated && self.cred_rx.may_change() {
            return self.idle(Disconnected::RejectedCredentials).await;
        }
        self.back_off().await
    }

    /// Start the backoff ladder over: a fresh connection has its
    /// subscription set applied, so the next failure is a new one.
    fn reset_backoff(&mut self) {
        self.backoff_ms = self.sources.reconnect.initial_backoff_ms;
    }

    /// Tell the routes the provider was disabled, unless that is the last
    /// thing they heard.
    async fn tell_routes_disabled(&mut self) {
        if self.routes_heard == Some(DisconnectCause::Disabled) {
            return;
        }
        tracing::info!(provider = C::PROVIDER_ID, "provider disabled");
        self.broadcast_disconnected("provider disabled".to_string(), DisconnectCause::Disabled)
            .await;
    }

    /// Tell every route the provider disconnected, for `reason`.
    async fn broadcast_disconnected(&mut self, reason: String, cause: DisconnectCause) {
        self.routes_heard = Some(cause);
        broadcast_control(
            &self.routes,
            ControlKind::ProviderDisconnected {
                provider: C::PROVIDER_ID.to_string(),
                reason,
                cause,
            },
        )
        .await;
    }

    /// Wait, with no connection, until what ends `why` or a settings change,
    /// answering every command meanwhile as `why` says. The next cycle
    /// resolves the sources again; a value still missing lands back here
    /// rather than spinning. With every sender gone the hub still answers
    /// commands, so its shutdown (every handle dropped) still works.
    async fn idle(&mut self, why: Disconnected) -> Next {
        let mut clock = std::pin::pin!(async {
            match why {
                Disconnected::Reconnecting(delay) => tokio::time::sleep(delay).await,
                _ => std::future::pending().await,
            }
        });
        loop {
            tokio::select! {
                () = &mut clock => return Next::Reconnect,
                _ = self.cred_rx.changed(), if why.wakes_on_rotation() => {
                    return Next::Reconnect;
                }
                _ = self.settings_rx.changed() => return Next::Reconnect,
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        return Next::Exit;
                    };
                    self.handle_disconnected_command(cmd, why).await;
                }
            }
        }
    }

    /// Sleep out the current backoff, doubling it for the next time, and
    /// answer commands meanwhile without cutting the sleep short: a command
    /// is no reason to try the server again sooner. A settings change is,
    /// being a reconfiguration rather than a retry: it ends the sleep and
    /// the next cycle connects with the new settings, or parks disabled.
    async fn back_off(&mut self) -> Next {
        // Full jitter: pick a sleep uniformly in [0, backoff_ms]. With many
        // providers reconnecting after a shared upstream blip this avoids
        // synchronized retry storms; without it every one retries on the same
        // exponential ladder.
        let policy = &self.sources.reconnect;
        let delay_ms = if policy.jitter {
            fastrand::u64(0..=self.backoff_ms)
        } else {
            self.backoff_ms
        };
        self.backoff_ms = (self.backoff_ms * 2).min(policy.max_backoff_ms);
        self.idle(Disconnected::Reconnecting(Duration::from_millis(delay_ms)))
            .await
    }

    /// Answer a command that arrives while there is no connection. A
    /// subscribe is refused as `why` says, so the session sees a clear
    /// error rather than stalling. An unsubscribe has no socket to reach,
    /// so it is applied here, to the routes and the aggregate set, and
    /// answered `Ok`: a session that closes while the hub is parked or
    /// backing off takes its pair with it, rather than leaving it for the
    /// next connect to subscribe for nobody, against the account's symbol
    /// limit, until another session takes the pair.
    async fn handle_disconnected_command(&mut self, cmd: HubCommand, why: Disconnected) {
        match cmd {
            HubCommand::Subscribe { ack, .. } => {
                let _ = ack.send(Err(why.refusal(C::PROVIDER_ID)));
            }
            HubCommand::Unsubscribe {
                instrument,
                kind,
                ack,
            } => {
                self.subs.remove(&instrument, kind);
                end_route::<C>(&mut self.routes, instrument, kind).await;
                let _ = ack.send(Ok(()));
            }
        }
    }
}

/// Route one translated event to the sink owning its `(instrument, kind)`;
/// a control (no pair) fans out to every active sink so each session sees
/// provider-level state changes.
pub(super) async fn dispatch_event(routes: &Routes, ev: MarketEvent) {
    if let Some(key) = event_route_key(&ev) {
        if let Some(sink) = routes.get(&key) {
            let _ = sink.send(ev).await;
        }
        return;
    }
    for sink in routes.values() {
        let _ = sink.send(ev.clone()).await;
    }
}

fn event_route_key(ev: &MarketEvent) -> Option<(Instrument, EventKind)> {
    match ev {
        MarketEvent::Trade(t) => Some((t.instrument.clone(), EventKind::Trade)),
        MarketEvent::Quote(q) => Some((q.instrument.clone(), EventKind::Quote)),
        MarketEvent::Bar(b) => Some((b.instrument.clone(), EventKind::Bar(b.interval))),
        _ => None,
    }
}

/// The error a subscribe or unsubscribe is answered with when the upstream
/// refuses `what`.
fn ack_error<C: Connector>(what: &str, failure: &FailureOf<C>) -> Error {
    let provider = C::PROVIDER_ID.to_string();
    let message = format!("{what}: {}", error_chain(failure));
    match failure.classify() {
        StreamFailure::Unauthenticated => Error::Unauthenticated { provider, message },
        StreamFailure::Transient => Error::Provider { provider, message },
    }
}

/// Apply one session's subscribe or unsubscribe on the connected socket and
/// answer it. Returns whether a subscribe applied the aggregate set, since a
/// subscribe sends the whole set and its success is the set applied.
async fn handle_route_command<C: Connector>(
    routes: &mut Routes,
    subs: &mut <C::Upstream as Upstream>::Subs,
    upstream: &mut C::Upstream,
    cmd: HubCommand,
) -> bool {
    match cmd {
        HubCommand::Subscribe {
            instrument,
            kind,
            sink,
            ack,
        } => {
            let res = subscribe_route::<C>(routes, subs, upstream, instrument, kind, sink).await;
            let applied = res.is_ok();
            let _ = ack.send(res);
            applied
        }
        HubCommand::Unsubscribe {
            instrument,
            kind,
            ack,
        } => {
            let res = unsubscribe_route::<C>(routes, subs, upstream, instrument, kind).await;
            let _ = ack.send(res);
            false
        }
    }
}

/// Route `(instrument, kind)` to `sink` and subscribe it upstream. If the
/// upstream refuses, the route and the pair are dropped again, so a
/// reconnect does not apply a subscription the server rejected. That also
/// clears a route left behind by a session whose unsubscribe never arrived.
async fn subscribe_route<C: Connector>(
    routes: &mut Routes,
    subs: &mut <C::Upstream as Upstream>::Subs,
    upstream: &mut C::Upstream,
    instrument: Instrument,
    kind: EventKind,
    sink: mpsc::Sender<MarketEvent>,
) -> Result<()> {
    let key = (instrument, kind);
    routes.insert(key.clone(), sink);
    subs.insert(&key.0, kind);
    if let Err(failure) = upstream.add(subs).await {
        routes.remove(&key);
        subs.remove(&key.0, kind);
        return Err(ack_error::<C>("add_subscriptions", &failure));
    }
    // We are in the connected loop, so the provider is up.
    // `ProviderConnected` is otherwise only broadcast on the connect *edge*,
    // which a route that subscribes afterward misses — leaving its connection
    // state Unknown even as data flows. Tell the freshly-subscribed route the
    // provider is up.
    let route = routes.get(&key);
    broadcast_control_to(
        route,
        ControlKind::ProviderConnected {
            provider: C::PROVIDER_ID.to_string(),
        },
    )
    .await;
    broadcast_control_to(
        route,
        ControlKind::SubscriptionChanged {
            provider: C::PROVIDER_ID.to_string(),
            instrument: key.0.clone(),
            kind,
            active: true,
        },
    )
    .await;
    Ok(())
}

/// Remove the route for `(instrument, kind)`: drop the pair from `subs`,
/// remove it upstream, then drop the route and tell its sink the
/// subscription ended. A failed upstream removal keeps the pair in `subs`,
/// so a reconnect applies it again.
async fn unsubscribe_route<C: Connector>(
    routes: &mut Routes,
    subs: &mut <C::Upstream as Upstream>::Subs,
    upstream: &mut C::Upstream,
    instrument: Instrument,
    kind: EventKind,
) -> Result<()> {
    let key = (instrument, kind);
    subs.remove(&key.0, kind);
    let mut removal = <C::Upstream as Upstream>::Subs::empty();
    removal.insert(&key.0, kind);
    match upstream.remove(&removal).await {
        Ok(()) => {
            // Drop the route after the upstream ack so any tail events still
            // in flight are routable.
            end_route::<C>(routes, key.0, kind).await;
            Ok(())
        }
        Err(failure) => {
            // The pair is still streaming upstream, so a reconnect must apply it.
            subs.insert(&key.0, kind);
            Err(ack_error::<C>("remove_subscriptions", &failure))
        }
    }
}

/// Drop the route for `(instrument, kind)` and tell its sink, if it has one,
/// that the subscription ended.
async fn end_route<C: Connector>(routes: &mut Routes, instrument: Instrument, kind: EventKind) {
    let key = (instrument, kind);
    let removed = routes.remove(&key);
    broadcast_control_to(
        removed.as_ref(),
        ControlKind::SubscriptionChanged {
            provider: C::PROVIDER_ID.to_string(),
            instrument: key.0,
            kind,
            active: false,
        },
    )
    .await;
}

/// A control event stamped with the current wall clock.
pub(super) fn control_event(kind: ControlKind) -> MarketEvent {
    let now = wall_clock_ts();
    MarketEvent::Control(Control {
        source_ts: now,
        rx_ts: now,
        seq: Seq(0),
        kind,
    })
}

/// Fan a control out to every active sink.
async fn broadcast_control(routes: &Routes, kind: ControlKind) {
    let ev = control_event(kind);
    for sink in routes.values() {
        let _ = sink.send(ev.clone()).await;
    }
}

/// Fan one provider error out to every active sink, every copy carrying the
/// same fresh fan-out id.
async fn broadcast_error<C: Connector>(routes: &Routes, message: String) {
    broadcast_control(routes, ControlKind::fanout_error(C::PROVIDER_ID, message)).await;
}

/// Send a control to one sink, if present.
async fn broadcast_control_to(sink: Option<&mpsc::Sender<MarketEvent>>, kind: ControlKind) {
    if let Some(s) = sink {
        let _ = s.send(control_event(kind)).await;
    }
}

/// The wall clock as a [`Timestamp`].
pub(super) fn wall_clock_ts() -> Timestamp {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "i64 nanos since epoch representable until year 2262"
        )]
        let n = d.as_nanos() as i64;
        n
    });
    Timestamp(nanos)
}

/// Walk a `std::error::Error` source chain to a single string, so a wrapped
/// websocket or I/O cause lands in the control's reason and the log line
/// rather than only the outer variant's text.
pub(super) fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![err.to_string()];
    let mut src = err.source();
    while let Some(e) = src {
        parts.push(e.to_string());
        src = e.source();
    }
    parts.join(" -> ")
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use datamancer_core::{
        AssetClass, Bar, BarInterval, ErrorOrigin, FanoutId, Price, ProviderId, Quantity, Trade,
    };
    use tokio::sync::watch;

    use super::*;

    /// The fake connector's failure, carrying how it classifies.
    #[derive(Debug, Clone, Copy)]
    struct Refused(StreamFailure);

    impl std::fmt::Display for Refused {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "refused ({:?})", self.0)
        }
    }

    impl std::error::Error for Refused {}

    impl Classify for Refused {
        fn classify(&self) -> StreamFailure {
            self.0
        }
    }

    /// A subscription set that is just the pairs it holds.
    #[derive(Debug, Clone, Default)]
    struct PairSet(BTreeSet<(Instrument, EventKind)>);

    impl PairSet {
        /// The trade symbols held, in the shape a stock list's `trades`
        /// field takes: `None` when there are none.
        fn trades(&self) -> Option<Vec<String>> {
            let symbols: Vec<String> = self
                .0
                .iter()
                .filter(|(_, kind)| *kind == EventKind::Trade)
                .map(|(instrument, _)| instrument.symbol().to_string())
                .collect();
            (!symbols.is_empty()).then_some(symbols)
        }
    }

    impl SubscriptionSet for PairSet {
        fn empty() -> Self {
            Self::default()
        }

        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }

        fn insert(&mut self, instrument: &Instrument, kind: EventKind) {
            self.0.insert((instrument.clone(), kind));
        }

        fn remove(&mut self, instrument: &Instrument, kind: EventKind) {
            self.0.remove(&(instrument.clone(), kind));
        }
    }

    /// Records the upstream calls the hub makes; refuses the first `add`
    /// with `refuse_first_add` and every `add` with `refuse_add`, and yields
    /// `events` in order, then pends.
    #[derive(Default)]
    struct FakeUpstream {
        /// How many times `add` was called, readable after the upstream
        /// moves into the hub.
        adds: Arc<AtomicUsize>,
        removals: Vec<PairSet>,
        refuse_add: Option<Refused>,
        /// Refuse the first `add` only, with this.
        refuse_first_add: Option<Refused>,
        events: VecDeque<std::result::Result<Vec<MarketEvent>, Refused>>,
        /// Whether `shut_down` yields once before returning, as a real
        /// websocket close, network I/O, does.
        yields_on_shut_down: bool,
    }

    impl FakeUpstream {
        /// An upstream whose `next_events` pends forever.
        fn idle() -> Self {
            Self::default()
        }

        /// An upstream whose first `next_events` fails with `refused`.
        fn failing_with(refused: Refused) -> Self {
            Self {
                events: VecDeque::from([Err(refused)]),
                ..Self::default()
            }
        }

        /// An idle upstream that refuses every `add` with `refused`.
        fn refusing_adds(refused: Refused) -> Self {
            Self {
                refuse_add: Some(refused),
                ..Self::default()
            }
        }

        /// The counter of `add` calls, to keep after the upstream moves.
        fn add_count(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.adds)
        }
    }

    impl Upstream for FakeUpstream {
        type Subs = PairSet;
        type Failure = Refused;

        async fn add(&mut self, _subs: &PairSet) -> std::result::Result<(), Refused> {
            self.adds.fetch_add(1, Ordering::SeqCst);
            match self.refuse_first_add.take().or(self.refuse_add) {
                Some(refused) => Err(refused),
                None => Ok(()),
            }
        }

        async fn remove(&mut self, removal: &PairSet) -> std::result::Result<(), Refused> {
            self.removals.push(removal.clone());
            Ok(())
        }

        async fn next_events(&mut self) -> std::result::Result<Vec<MarketEvent>, Refused> {
            match self.events.pop_front() {
                Some(next) => next,
                None => std::future::pending().await,
            }
        }

        async fn shut_down(self) {
            if self.yields_on_shut_down {
                tokio::task::yield_now().await;
            }
        }
    }

    /// A connector that answers each connect from a script.
    struct FakeConnector {
        connects: std::sync::Mutex<VecDeque<std::result::Result<FakeUpstream, Refused>>>,
        connect_count: Arc<AtomicUsize>,
        /// Panic on the first `connect`, taking that hub task down with it;
        /// a hub spawned after it connects as scripted.
        panics_once: AtomicBool,
        /// Whether every `connect` yields once before resolving, as a real
        /// connect, network I/O, does.
        yields_in_connect: bool,
    }

    impl FakeConnector {
        /// Once the script runs out, `connect` pends forever.
        fn scripted(connects: Vec<std::result::Result<FakeUpstream, Refused>>) -> Self {
            Self {
                connects: std::sync::Mutex::new(connects.into()),
                connect_count: Arc::default(),
                panics_once: AtomicBool::new(false),
                yields_in_connect: false,
            }
        }

        fn never_connects() -> Self {
            Self::scripted(vec![])
        }

        /// A connector whose first `connect` panics, so that hub task ends at
        /// once; the next hub's connect pends forever.
        fn panicking_once() -> Self {
            Self {
                panics_once: AtomicBool::new(true),
                ..Self::never_connects()
            }
        }

        /// How many times the hub has called `connect`, readable after the
        /// connector moves into the hub.
        fn connect_count(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.connect_count)
        }
    }

    /// Static settings, `Env` credentials, and a backoff that starts at one
    /// second with no jitter, so a backoff sleep is never zero.
    fn static_sources() -> HubSources<()> {
        HubSources {
            settings: SettingsSource::Static(()),
            credentials: CredentialsSource::Env,
            reconnect: ReconnectPolicy {
                initial_backoff_ms: 1_000,
                max_backoff_ms: 30_000,
                jitter: false,
            },
        }
    }

    /// As `static_sources`, but credentials come from a `Watch` source that
    /// starts provisioned; the sender rotates them.
    fn watched_credentials() -> (watch::Sender<Option<AlpacaCredentials>>, HubSources<()>) {
        let (creds_tx, creds_rx) = watch::channel(Some(test_credentials()));
        let sources = HubSources {
            credentials: CredentialsSource::Watch(creds_rx),
            ..static_sources()
        };
        (creds_tx, sources)
    }

    /// `sources`, with settings from a `Watch` source that starts enabled;
    /// the sender changes or disables them.
    fn with_watched_settings(
        sources: HubSources<()>,
    ) -> (watch::Sender<Option<()>>, HubSources<()>) {
        let (settings_tx, settings_rx) = watch::channel(Some(()));
        let sources = HubSources {
            settings: SettingsSource::Watch(settings_rx),
            ..sources
        };
        (settings_tx, sources)
    }

    impl Connector for FakeConnector {
        const PROVIDER_ID: &'static str = "fake";
        type Settings = ();
        type Upstream = FakeUpstream;

        fn connect(
            &self,
            _settings: &(),
            _credentials: PresentCredentials,
        ) -> impl Future<Output = std::result::Result<FakeUpstream, Refused>> + Send {
            assert!(
                !self.panics_once.swap(false, Ordering::SeqCst),
                "the scripted connect panics"
            );
            self.connect_count.fetch_add(1, Ordering::SeqCst);
            let next = self
                .connects
                .lock()
                .expect("connect script poisoned")
                .pop_front();
            let yields = self.yields_in_connect;
            async move {
                if yields {
                    tokio::task::yield_now().await;
                }
                match next {
                    Some(result) => result,
                    None => std::future::pending().await,
                }
            }
        }
    }

    /// Spawn a hub on `connector` reading `sources`: its task and its command
    /// sender.
    fn spawn_hub(
        connector: FakeConnector,
        sources: HubSources<()>,
    ) -> (JoinHandle<()>, mpsc::Sender<HubCommand>) {
        let slot = HubSlot::new(connector, sources);
        let cmd_tx = slot.ensure();
        let task = slot.take_task().expect("the hub was spawned");
        (task, cmd_tx)
    }

    fn test_credentials() -> AlpacaCredentials {
        AlpacaCredentials {
            key_id: "test-key".to_string(),
            secret: "test-secret".to_string(),
        }
    }

    fn instrument(symbol: &str) -> Instrument {
        Instrument::new(
            ProviderId::from_static(FakeConnector::PROVIDER_ID),
            AssetClass::Equity,
            symbol,
        )
    }

    fn aapl() -> Instrument {
        instrument("AAPL")
    }

    /// The kinds of every control queued on `rx`, in order; a data event
    /// there is a test error.
    fn control_kinds(rx: &mut mpsc::Receiver<MarketEvent>) -> Vec<ControlKind> {
        let mut kinds = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            match ev {
                MarketEvent::Control(Control { kind, .. }) => kinds.push(kind),
                other => panic!("expected a control, got {other:?}"),
            }
        }
        kinds
    }

    /// The causes of the `ProviderDisconnected` controls queued on `rx`, in
    /// order, dropping every other control.
    fn disconnect_causes(rx: &mut mpsc::Receiver<MarketEvent>) -> Vec<DisconnectCause> {
        control_kinds(rx)
            .into_iter()
            .filter_map(|kind| match kind {
                ControlKind::ProviderDisconnected { cause, .. } => Some(cause),
                _ => None,
            })
            .collect()
    }

    /// A hub connected once with one route on AAPL trades, its sources with
    /// watched settings and credentials so a test can force a reconnect or
    /// a disable, and the connects the script answers after the first.
    struct ConnectedHub {
        settings_tx: watch::Sender<Option<()>>,
        creds_tx: watch::Sender<Option<AlpacaCredentials>>,
        connects: Arc<AtomicUsize>,
        cmd_tx: mpsc::Sender<HubCommand>,
        handle: Box<SharedLiveHandle>,
        rx: mpsc::Receiver<MarketEvent>,
    }

    impl ConnectedHub {
        /// Connect on an idle upstream and subscribe AAPL trades; `later`
        /// answers the connects after that one. The route's queue is drained
        /// of the connect's controls.
        async fn with_later_connects(
            later: Vec<std::result::Result<FakeUpstream, Refused>>,
        ) -> (JoinHandle<()>, Self) {
            let (creds_tx, sources) = watched_credentials();
            let (settings_tx, sources) = with_watched_settings(sources);
            let mut connects = vec![Ok(FakeUpstream::idle())];
            connects.extend(later);
            let connector = FakeConnector::scripted(connects);
            let connect_count = connector.connect_count();
            let (hub, cmd_tx) = spawn_hub(connector, sources);
            let (sink, mut rx) = mpsc::channel(8);
            let handle = Box::new(SharedLiveHandle::new(cmd_tx.clone(), sink));
            handle
                .subscribe(aapl(), EventKind::Trade)
                .await
                .expect("subscribes on the first connection");
            assert_eq!(connect_count.load(Ordering::SeqCst), 1);
            control_kinds(&mut rx);
            let connected = Self {
                settings_tx,
                creds_tx,
                connects: connect_count,
                cmd_tx,
                handle,
                rx,
            };
            (hub, connected)
        }

        fn connects(&self) -> usize {
            self.connects.load(Ordering::SeqCst)
        }

        /// Rotate the credentials, which reconnects the hub, and let it run
        /// into the next scripted connect.
        async fn rotate_credentials(&self) {
            self.creds_tx.send(Some(test_credentials())).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        /// Change the settings, which reconnects the hub, and let it run into
        /// the next scripted connect.
        async fn change_settings(&self) {
            self.settings_tx.send(Some(())).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        /// A second session's subscribe, to SPY trades, as the hub answers it
        /// now.
        async fn another_subscribe(&self) -> Result<()> {
            let (sink, _rx) = mpsc::channel(8);
            let other = SharedLiveHandle::new(self.cmd_tx.clone(), sink);
            other.subscribe(instrument("SPY"), EventKind::Trade).await
        }
    }

    /// A subscribe that lands while the hub sleeps out a backoff is refused
    /// at once as reconnecting rather than queued behind the next connect.
    #[tokio::test(start_paused = true)]
    async fn a_subscribe_during_backoff_is_refused_as_reconnecting() {
        let (hub, cmd_tx) = spawn_hub(
            FakeConnector::scripted(vec![Err(Refused(StreamFailure::Transient))]),
            static_sources(),
        );
        let (sink, _rx) = mpsc::channel(8);
        let handle = SharedLiveHandle::new(cmd_tx, sink);
        tokio::task::yield_now().await; // the hub reaches its backoff sleep
        let err = handle
            .subscribe(aapl(), EventKind::Trade)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::Provider { ref message, .. } if message == "provider is reconnecting"
        ));
        drop(hub);
    }

    /// A mid-stream rejection of the credentials parks the hub: it does not
    /// reconnect on its own, however long it waits, until they rotate.
    #[tokio::test(start_paused = true)]
    async fn a_mid_stream_auth_rejection_parks_until_credentials_rotate() {
        let (creds_tx, sources) = watched_credentials();
        let connector = FakeConnector::scripted(vec![
            Ok(FakeUpstream::failing_with(Refused(
                StreamFailure::Unauthenticated,
            ))),
            Ok(FakeUpstream::idle()),
        ]);
        let connects = connector.connect_count();
        let (_hub, _cmd_tx) = spawn_hub(connector, sources);
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            1,
            "a parked hub does not reconnect on its own"
        );
        creds_tx.send(Some(test_credentials())).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            2,
            "a rotation reconnects it"
        );
    }

    /// A connect refused as unauthenticated with credentials that cannot
    /// rotate does not park: the routes hear an unauthenticated disconnect,
    /// and the hub connects again once its backoff elapses.
    #[tokio::test(start_paused = true)]
    async fn an_unauthenticated_connect_with_unwatched_credentials_backs_off() {
        // A watched settings source, so a settings change can force the
        // reconnect after the route is in place.
        let (settings_tx, sources) = with_watched_settings(static_sources());
        assert!(matches!(sources.credentials, CredentialsSource::Env));
        let connector = FakeConnector::scripted(vec![
            Ok(FakeUpstream::idle()),
            Err(Refused(StreamFailure::Unauthenticated)),
        ]);
        let connects = connector.connect_count();
        let (_hub, cmd_tx) = spawn_hub(connector, sources);
        let (sink, mut rx) = mpsc::channel(8);
        let handle = SharedLiveHandle::new(cmd_tx, sink);
        handle
            .subscribe(aapl(), EventKind::Trade)
            .await
            .expect("subscribes on the first connection");

        settings_tx.send(Some(())).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(connects.load(Ordering::SeqCst), 2, "the hub reconnected");
        assert_eq!(
            disconnect_causes(&mut rx),
            [DisconnectCause::Error, DisconnectCause::Unauthenticated],
            "the settings change, then the refused connect"
        );

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            2,
            "the hub waits out its backoff"
        );
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            3,
            "the hub retries after its backoff instead of parking"
        );
    }

    /// A re-subscribe after a reconnect that the server refuses as a rejected
    /// credential is one: the routes hear an unauthenticated disconnect and,
    /// the credentials being watched, the hub parks until they rotate, as it
    /// does when the connect or the stream fails that way.
    #[tokio::test(start_paused = true)]
    async fn a_re_subscribe_refused_as_unauthenticated_parks_until_credentials_rotate() {
        let (_hub, mut hub) = ConnectedHub::with_later_connects(vec![
            Ok(FakeUpstream::refusing_adds(Refused(
                StreamFailure::Unauthenticated,
            ))),
            Ok(FakeUpstream::idle()),
        ])
        .await;
        hub.rotate_credentials().await;
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert_eq!(hub.connects(), 2, "parked: no reconnect on its own");
        assert_eq!(
            disconnect_causes(&mut hub.rx),
            [DisconnectCause::Error, DisconnectCause::Unauthenticated],
            "the rotation, then the refused re-subscribe"
        );
        assert!(
            matches!(
                hub.another_subscribe().await,
                Err(Error::Unauthenticated { .. })
            ),
            "a subscribe to the parked hub says why"
        );
        hub.rotate_credentials().await;
        assert_eq!(hub.connects(), 3, "a rotation reconnects it");
    }

    /// With credentials that cannot rotate, a re-subscribe refused as a
    /// rejected credential backs off like any other failure: the connects
    /// climb the backoff ladder instead of retrying at the initial delay
    /// forever, since a connect that succeeds only to have its re-subscribe
    /// refused has not succeeded.
    #[tokio::test(start_paused = true)]
    async fn a_re_subscribe_refused_as_unauthenticated_climbs_the_backoff_ladder() {
        let (settings_tx, sources) = with_watched_settings(static_sources());
        assert!(matches!(sources.credentials, CredentialsSource::Env));
        let mut connects = vec![Ok(FakeUpstream::idle())];
        connects.extend((0..60).map(|_| {
            Ok(FakeUpstream::refusing_adds(Refused(
                StreamFailure::Unauthenticated,
            )))
        }));
        let connector = FakeConnector::scripted(connects);
        let count = connector.connect_count();
        let (_hub, cmd_tx) = spawn_hub(connector, sources);
        let (sink, _rx) = mpsc::channel(1024);
        let handle = SharedLiveHandle::new(cmd_tx, sink);
        handle
            .subscribe(aapl(), EventKind::Trade)
            .await
            .expect("subscribes on the first connection");

        settings_tx.send(Some(())).unwrap();
        tokio::time::sleep(Duration::from_mins(2)).await;
        // The ladder 1, 2, 4, 8, 16, 30, 30 s puts the reconnects at 0, 1, 3,
        // 7, 15, 31, 61 and 91 s: eight of them, nine connects with the
        // first; retrying at the initial delay would make sixty.
        assert_eq!(
            count.load(Ordering::SeqCst),
            9,
            "the backoff climbs its ladder"
        );
    }

    /// A re-subscribe refused for a transient reason leaves the stream up but
    /// has not subscribed it: a stream that then drops has not succeeded
    /// either, so the connects climb the backoff ladder rather than retry at
    /// the initial delay forever.
    #[tokio::test(start_paused = true)]
    async fn a_transient_re_subscribe_refusal_does_not_reset_the_backoff() {
        let (settings_tx, sources) = with_watched_settings(static_sources());
        let mut connects = vec![Ok(FakeUpstream::idle())];
        connects.extend((0..120).map(|_| {
            Ok(FakeUpstream {
                events: VecDeque::from([Err(Refused(StreamFailure::Transient))]),
                ..FakeUpstream::refusing_adds(Refused(StreamFailure::Transient))
            })
        }));
        let connector = FakeConnector::scripted(connects);
        let count = connector.connect_count();
        let (_hub, cmd_tx) = spawn_hub(connector, sources);
        let (sink, _rx) = mpsc::channel(2048);
        let handle = SharedLiveHandle::new(cmd_tx, sink);
        handle
            .subscribe(aapl(), EventKind::Trade)
            .await
            .expect("subscribes on the first connection");

        settings_tx.send(Some(())).unwrap();
        tokio::time::sleep(Duration::from_mins(2)).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            9,
            "the backoff climbs its ladder"
        );
    }

    /// A session's subscribe applies the whole aggregate set, so one that
    /// succeeds on a fresh connection whose own re-subscribe was refused has
    /// the set applied after all, and starts the backoff ladder over.
    #[tokio::test(start_paused = true)]
    async fn a_subscribe_that_applies_the_set_resets_the_backoff() {
        let (_hub, mut hub) = ConnectedHub::with_later_connects(vec![
            Err(Refused(StreamFailure::Transient)),
            Err(Refused(StreamFailure::Transient)),
            Ok(FakeUpstream {
                refuse_first_add: Some(Refused(StreamFailure::Transient)),
                ..FakeUpstream::idle()
            }),
            Err(Refused(StreamFailure::Transient)),
            Ok(FakeUpstream::idle()),
        ])
        .await;
        // Two failed connects climb the ladder to four seconds; the third
        // connects but has its re-subscribe refused, so the ladder stays.
        hub.change_settings().await;
        tokio::time::sleep(Duration::from_millis(3_100)).await;
        assert_eq!(hub.connects(), 4, "connected with the re-subscribe refused");
        control_kinds(&mut hub.rx);
        hub.another_subscribe()
            .await
            .expect("the subscribe applies the set");

        // The next failed connect sleeps the initial second, not four.
        hub.change_settings().await;
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert_eq!(
            hub.connects(),
            6,
            "the ladder started over at the applied set"
        );
    }

    /// A re-subscribe refused for any other reason is broadcast as a provider
    /// error and leaves the stream up: a reconnect would only be refused again.
    #[tokio::test(start_paused = true)]
    async fn a_re_subscribe_refused_otherwise_leaves_the_stream_up() {
        let (_hub, mut hub) = ConnectedHub::with_later_connects(vec![Ok(
            FakeUpstream::refusing_adds(Refused(StreamFailure::Transient)),
        )])
        .await;
        hub.change_settings().await;
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert_eq!(hub.connects(), 2, "no reconnect");
        let Err(Error::Provider { message, .. }) = hub.another_subscribe().await else {
            panic!("a connected hub answers a refused subscribe as a provider error");
        };
        assert!(
            message.starts_with("add_subscriptions:"),
            "the subscribe reached the upstream, which refused it: {message}"
        );
        let kinds = control_kinds(&mut hub.rx);
        assert!(
            matches!(
                kinds.as_slice(),
                [
                    ControlKind::ProviderDisconnected { cause: DisconnectCause::Error, .. },
                    ControlKind::ProviderConnected { .. },
                    ControlKind::ProviderError { message, .. },
                ] if message.starts_with("re-subscribe failed:")
            ),
            "{kinds:?}"
        );
    }

    /// A hub parked on rejected credentials still hears a disable: the routes
    /// get a disabled disconnect, a subscribe answers disabled, and a
    /// re-enable connects again.
    #[tokio::test(start_paused = true)]
    async fn a_parked_hub_notices_a_disable() {
        let (_hub, mut hub) = ConnectedHub::with_later_connects(vec![
            Err(Refused(StreamFailure::Unauthenticated)),
            Ok(FakeUpstream::idle()),
        ])
        .await;
        hub.rotate_credentials().await;
        assert_eq!(hub.connects(), 2);
        assert_eq!(
            disconnect_causes(&mut hub.rx),
            [DisconnectCause::Error, DisconnectCause::Unauthenticated],
            "the rotation, then the refused connect"
        );

        hub.settings_tx.send(None).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            disconnect_causes(&mut hub.rx),
            [DisconnectCause::Disabled],
            "the disable reaches the routes of a parked hub"
        );
        assert!(
            matches!(
                hub.another_subscribe().await,
                Err(Error::ProviderDisabled { .. })
            ),
            "a subscribe answers disabled, not rejected credentials"
        );
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert_eq!(hub.connects(), 2, "disabled: no connect");

        hub.change_settings().await;
        assert_eq!(hub.connects(), 3, "re-enabled: connects again");
    }

    /// Watched credentials whose sender is gone can never rotate, so a
    /// rejection with them backs off and retries like one with static
    /// credentials, rather than parking for a rotation that cannot come.
    #[tokio::test(start_paused = true)]
    async fn a_rejection_with_the_credentials_sender_gone_backs_off() {
        let (creds_tx, sources) = watched_credentials();
        drop(creds_tx);
        let connector = FakeConnector::scripted(vec![
            Err(Refused(StreamFailure::Unauthenticated)),
            Ok(FakeUpstream::idle()),
        ]);
        let connects = connector.connect_count();
        let (_hub, cmd_tx) = spawn_hub(connector, sources);
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            2,
            "no rotation can come: the hub backed off and retried"
        );
        let (sink, _rx) = mpsc::channel(8);
        let handle = SharedLiveHandle::new(cmd_tx, sink);
        handle
            .subscribe(aapl(), EventKind::Trade)
            .await
            .expect("the retry connected");
    }

    /// A disable undone while the connection is still closing is a gap the
    /// routes hear about: the first connection's end is told before the
    /// second's start, whatever the settings say by the time the next cycle
    /// reads them.
    #[tokio::test(start_paused = true)]
    async fn a_disable_undone_during_shut_down_still_tells_the_routes_of_the_gap() {
        let (settings_tx, sources) = with_watched_settings(static_sources());
        let first = FakeUpstream {
            yields_on_shut_down: true,
            ..FakeUpstream::idle()
        };
        let connector = FakeConnector::scripted(vec![Ok(first), Ok(FakeUpstream::idle())]);
        let connects = connector.connect_count();
        let (_hub, cmd_tx) = spawn_hub(connector, sources);
        let (sink, mut rx) = mpsc::channel(8);
        let handle = SharedLiveHandle::new(cmd_tx, sink);
        handle
            .subscribe(aapl(), EventKind::Trade)
            .await
            .expect("subscribes on the first connection");
        control_kinds(&mut rx);

        settings_tx.send(None).unwrap();
        // The hub takes its settings arm and is inside the shut-down when
        // the re-enable lands.
        tokio::task::yield_now().await;
        settings_tx.send(Some(())).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(connects.load(Ordering::SeqCst), 2, "the hub reconnected");
        let kinds = control_kinds(&mut rx);
        assert!(
            matches!(
                kinds.as_slice(),
                [
                    ControlKind::ProviderDisconnected {
                        cause: DisconnectCause::Disabled,
                        ..
                    },
                    ControlKind::ProviderConnected { .. },
                ]
            ),
            "the first connection's end is told before the second's start: {kinds:?}"
        );
    }

    /// Watched credentials whose sender goes away while the connect is in
    /// flight can rotate no more either: the rejection that follows backs
    /// off and retries, rather than parking for a rotation that cannot come.
    #[tokio::test(start_paused = true)]
    async fn a_rejection_with_the_sender_gone_mid_connect_backs_off() {
        let (creds_tx, sources) = watched_credentials();
        let connector = FakeConnector {
            yields_in_connect: true,
            ..FakeConnector::scripted(vec![
                Err(Refused(StreamFailure::Unauthenticated)),
                Ok(FakeUpstream::idle()),
            ])
        };
        let connects = connector.connect_count();
        let (_hub, cmd_tx) = spawn_hub(connector, sources);
        // The hub has read its sources and is inside the connect.
        tokio::task::yield_now().await;
        assert_eq!(connects.load(Ordering::SeqCst), 1);
        drop(creds_tx);
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            2,
            "no rotation can come: the hub backed off and retried"
        );
        let (sink, _rx) = mpsc::channel(8);
        let handle = SharedLiveHandle::new(cmd_tx, sink);
        handle
            .subscribe(aapl(), EventKind::Trade)
            .await
            .expect("the retry connected");
    }

    /// A rotation that landed before its sender went away is a rotation the
    /// hub has: a rejection then reconnects with it at once, rather than
    /// sleeping a backoff for a pair it already holds.
    #[tokio::test(start_paused = true)]
    async fn a_rotation_landed_before_the_sender_went_is_used_at_once() {
        let (creds_tx, sources) = watched_credentials();
        let connector = FakeConnector {
            yields_in_connect: true,
            ..FakeConnector::scripted(vec![
                Err(Refused(StreamFailure::Unauthenticated)),
                Ok(FakeUpstream::idle()),
            ])
        };
        let connects = connector.connect_count();
        let (_hub, _cmd_tx) = spawn_hub(connector, sources);
        // The hub has read its sources and is inside the connect.
        tokio::task::yield_now().await;
        assert_eq!(connects.load(Ordering::SeqCst), 1);
        creds_tx.send(Some(test_credentials())).unwrap();
        drop(creds_tx);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            2,
            "the rotation that already landed is used at once"
        );
    }

    /// A disable that lands in the same poll as a credentials rotation still
    /// reaches the routes: whichever wake the parked hub takes, the routes
    /// hear the disabled disconnect once before the hub parks disabled.
    #[tokio::test(start_paused = true)]
    async fn a_disable_landing_with_a_rotation_reaches_the_routes() {
        // The parked hub's select! picks a ready arm at random, so the race
        // is run several times; one silent disable fails the test.
        for _ in 0..20 {
            let (_hub, mut hub) = ConnectedHub::with_later_connects(vec![
                Err(Refused(StreamFailure::Unauthenticated)),
                Ok(FakeUpstream::idle()),
            ])
            .await;
            hub.rotate_credentials().await;
            assert_eq!(hub.connects(), 2, "parked on the rejected credentials");
            control_kinds(&mut hub.rx);

            hub.creds_tx.send(Some(test_credentials())).unwrap();
            hub.settings_tx.send(None).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert!(
                matches!(
                    hub.another_subscribe().await,
                    Err(Error::ProviderDisabled { .. })
                ),
                "the hub parked disabled"
            );
            assert_eq!(
                disconnect_causes(&mut hub.rx),
                [DisconnectCause::Disabled],
                "the disable reached the routes, once"
            );
        }
    }

    /// A disable that lands while the hub backs off reaches the routes too,
    /// and ends the backoff: the next cycle parks disabled, where a subscribe
    /// answers disabled, and a re-enable connects again.
    #[tokio::test(start_paused = true)]
    async fn a_backing_off_hub_notices_a_disable() {
        let (_hub, mut hub) = ConnectedHub::with_later_connects(vec![
            Err(Refused(StreamFailure::Transient)),
            Ok(FakeUpstream::idle()),
        ])
        .await;
        hub.change_settings().await;
        assert_eq!(hub.connects(), 2, "the reconnect failed; backing off");
        control_kinds(&mut hub.rx);

        hub.settings_tx.send(None).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            disconnect_causes(&mut hub.rx),
            [DisconnectCause::Disabled],
            "the disable reaches the routes of a backing-off hub"
        );
        assert!(
            matches!(
                hub.another_subscribe().await,
                Err(Error::ProviderDisabled { .. })
            ),
            "a subscribe answers disabled, not reconnecting"
        );
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert_eq!(hub.connects(), 2, "disabled: no connect");

        hub.change_settings().await;
        assert_eq!(hub.connects(), 3, "re-enabled: connects again");
    }

    /// A session that closes while the hub backs off takes its pair with it:
    /// the route is dropped and told, the next connect does not subscribe the
    /// pair for nobody, and the backoff runs out as it would have.
    #[tokio::test(start_paused = true)]
    async fn an_unsubscribe_during_backoff_drops_the_pair_and_keeps_the_backoff() {
        let third = FakeUpstream::idle();
        let adds = third.add_count();
        let (_hub, mut hub) = ConnectedHub::with_later_connects(vec![
            Err(Refused(StreamFailure::Transient)),
            Ok(third),
        ])
        .await;
        hub.change_settings().await;
        assert_eq!(hub.connects(), 2, "the reconnect failed; backing off");
        control_kinds(&mut hub.rx);

        let ConnectedHub {
            handle,
            mut rx,
            connects,
            ..
        } = hub;
        // Well into the one-second backoff, so a sleep the command cut short
        // or restarted would end outside the window checked below.
        tokio::time::sleep(Duration::from_millis(600)).await;
        handle
            .close()
            .await
            .expect("an unsubscribe while backing off is applied");
        assert!(
            matches!(
                control_kinds(&mut rx).as_slice(),
                [ControlKind::SubscriptionChanged { active: false, .. }]
            ),
            "the route is told its subscription ended"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            2,
            "the command did not cut the backoff short"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            3,
            "the backoff ran out when it would have"
        );
        assert_eq!(
            adds.load(Ordering::SeqCst),
            0,
            "nothing left to re-subscribe"
        );
    }

    /// A session that closes while the hub is parked takes its pair with it
    /// too, so the connect after a rotation subscribes nothing for it.
    #[tokio::test(start_paused = true)]
    async fn an_unsubscribe_during_a_park_drops_the_pair() {
        let third = FakeUpstream::idle();
        let adds = third.add_count();
        let (_hub, hub) = ConnectedHub::with_later_connects(vec![
            Err(Refused(StreamFailure::Unauthenticated)),
            Ok(third),
        ])
        .await;
        hub.rotate_credentials().await;
        assert_eq!(hub.connects(), 2, "the reconnect was refused; parked");

        let ConnectedHub {
            handle,
            mut rx,
            creds_tx,
            connects,
            ..
        } = hub;
        control_kinds(&mut rx);
        handle
            .close()
            .await
            .expect("an unsubscribe while parked is applied");
        assert!(
            matches!(
                control_kinds(&mut rx).as_slice(),
                [ControlKind::SubscriptionChanged { active: false, .. }]
            ),
            "the route is told its subscription ended"
        );
        creds_tx.send(Some(test_credentials())).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            connects.load(Ordering::SeqCst),
            3,
            "a rotation reconnects it"
        );
        assert_eq!(
            adds.load(Ordering::SeqCst),
            0,
            "nothing left to re-subscribe"
        );
    }

    /// A spawn that panics while the slot is locked (here: outside a tokio
    /// runtime) poisons the lock but leaves the state consistent, so the
    /// slot still serves: the next `ensure` inside a runtime spawns the hub.
    #[test]
    fn a_panicking_spawn_does_not_poison_the_slot() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let slot = HubSlot::new(FakeConnector::never_connects(), static_sources());
        let outside = catch_unwind(AssertUnwindSafe(|| slot.ensure()));
        assert!(outside.is_err(), "a spawn outside a runtime panics");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let cmd_tx = slot.ensure();
            assert!(!cmd_tx.is_closed());
            assert_eq!(slot.spawns(), 1);
        });
    }

    /// A hub whose task has ended, here by a panic inside its connector, is
    /// not handed out again: the next `ensure` spawns a fresh hub, so a slot
    /// is either idle or holds a hub that takes commands.
    #[tokio::test]
    async fn a_dead_hub_is_respawned_by_the_next_ensure() {
        let slot = HubSlot::new(FakeConnector::panicking_once(), static_sources());
        let dead = slot.ensure();
        while !dead.is_closed() {
            tokio::task::yield_now().await;
        }
        let live = slot.ensure();
        assert!(!live.same_channel(&dead), "a fresh hub, not the dead one");
        assert!(!live.is_closed());
        assert_eq!(slot.spawns(), 2);
    }

    #[test]
    fn an_idle_slot_spawns_once_and_reuses_its_hub() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let slot = HubSlot::new(FakeConnector::never_connects(), static_sources());
            let a = slot.ensure();
            let b = slot.ensure();
            assert!(a.same_channel(&b), "a running hub is reused");
            assert_eq!(slot.spawns(), 1);
        });
    }

    /// Data routes to the one sink owning its pair and nowhere else; a pair
    /// nobody subscribed to is dropped rather than misrouted; controls fan
    /// out to every sink.
    #[tokio::test]
    async fn dispatch_routes_data_by_pair_and_fans_out_controls() {
        let now = Timestamp(1);
        let trade = |symbol: &str| {
            MarketEvent::Trade(Trade {
                instrument: instrument(symbol),
                source_ts: now,
                rx_ts: now,
                seq: Seq(0),
                price: Price::from_f64_round(1.0),
                size: Quantity::from_units(1),
            })
        };
        let mut routes = HashMap::new();
        let (aapl_tx, mut aapl_rx) = mpsc::channel(8);
        let (spy_tx, mut spy_rx) = mpsc::channel(8);
        routes.insert((instrument("AAPL"), EventKind::Trade), aapl_tx);
        routes.insert((instrument("SPY"), EventKind::Quote), spy_tx);

        dispatch_event(&routes, trade("AAPL")).await;
        assert!(matches!(
            aapl_rx.try_recv(),
            Ok(MarketEvent::Trade(t)) if t.instrument.symbol() == "AAPL"
        ));
        assert!(
            spy_rx.try_recv().is_err(),
            "SPY quote sink must not see an AAPL trade"
        );

        // SPY trades have no subscriber (only SPY quotes do): dropped.
        dispatch_event(&routes, trade("SPY")).await;
        assert!(aapl_rx.try_recv().is_err());
        assert!(spy_rx.try_recv().is_err());

        broadcast_control(
            &routes,
            ControlKind::ProviderConnected {
                provider: FakeConnector::PROVIDER_ID.to_string(),
            },
        )
        .await;
        for rx in [&mut aapl_rx, &mut spy_rx] {
            assert!(matches!(
                rx.try_recv(),
                Ok(MarketEvent::Control(Control {
                    kind: ControlKind::ProviderConnected { .. },
                    ..
                }))
            ));
        }
    }

    /// The fan-out ids of the `ProviderError` controls queued on `rx`.
    pub(in crate::providers) fn fanout_ids(rx: &mut mpsc::Receiver<MarketEvent>) -> Vec<FanoutId> {
        let mut ids = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            match ev {
                MarketEvent::Control(Control {
                    kind:
                        ControlKind::ProviderError {
                            origin: ErrorOrigin::Fanout(id),
                            ..
                        },
                    ..
                }) => ids.push(id),
                other => panic!("expected a fan-out ProviderError, got {other:?}"),
            }
        }
        ids
    }

    /// Two routes on distinct pairs, and the receivers of their sinks.
    pub(in crate::providers) fn two_routes() -> (
        Routes,
        mpsc::Receiver<MarketEvent>,
        mpsc::Receiver<MarketEvent>,
    ) {
        let (a_tx, a_rx) = mpsc::channel(8);
        let (b_tx, b_rx) = mpsc::channel(8);
        let mut routes = HashMap::new();
        routes.insert((instrument("AAPL"), EventKind::Trade), a_tx);
        routes.insert((instrument("SPY"), EventKind::Quote), b_tx);
        (routes, a_rx, b_rx)
    }

    #[tokio::test]
    async fn every_copy_of_a_broadcast_error_shares_one_distinct_id() {
        let (routes, mut a_rx, mut b_rx) = two_routes();
        broadcast_error::<FakeConnector>(&routes, "first".to_string()).await;
        broadcast_error::<FakeConnector>(&routes, "second".to_string()).await;
        let a = fanout_ids(&mut a_rx);
        assert_eq!(a, fanout_ids(&mut b_rx));
        assert_eq!(a.len(), 2);
        assert_ne!(a[0], a[1]);
    }

    #[test]
    fn event_route_key_matches_pair() {
        let now = Timestamp(1);
        let bar = MarketEvent::Bar(Bar {
            instrument: instrument("AAPL"),
            interval: BarInterval::OneDay,
            source_ts: now,
            rx_ts: now,
            seq: Seq(0),
            open: Price::ZERO,
            high: Price::ZERO,
            low: Price::ZERO,
            close: Price::ZERO,
            volume: Quantity::ZERO,
        });
        assert_eq!(
            event_route_key(&bar),
            Some((instrument("AAPL"), EventKind::Bar(BarInterval::OneDay)))
        );
        assert_eq!(
            event_route_key(&control_event(ControlKind::ProviderConnected {
                provider: FakeConnector::PROVIDER_ID.to_string(),
            })),
            None
        );
    }

    /// A connected hub's route state, served one command at a time.
    struct FakeHub {
        cmd_tx: mpsc::Sender<HubCommand>,
        cmd_rx: mpsc::Receiver<HubCommand>,
        routes: HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
        subs: PairSet,
        upstream: FakeUpstream,
    }

    impl FakeHub {
        fn new() -> Self {
            let (cmd_tx, cmd_rx) = mpsc::channel(8);
            Self {
                cmd_tx,
                cmd_rx,
                routes: HashMap::new(),
                subs: PairSet::empty(),
                upstream: FakeUpstream::idle(),
            }
        }

        /// A session's handle onto this hub and the receiver of its sink.
        fn handle(&self) -> (Box<SharedLiveHandle>, mpsc::Receiver<MarketEvent>) {
            let (sink, rx) = mpsc::channel(8);
            let handle = SharedLiveHandle::new(self.cmd_tx.clone(), sink);
            (Box::new(handle), rx)
        }

        async fn serve_one(&mut self) {
            let cmd = self.cmd_rx.recv().await.expect("a command");
            handle_route_command::<FakeConnector>(
                &mut self.routes,
                &mut self.subs,
                &mut self.upstream,
                cmd,
            )
            .await;
        }
    }

    /// A subscribe the upstream refuses also clears a route an earlier
    /// session left behind, so nothing re-applies the pair on a reconnect.
    #[tokio::test]
    async fn a_refused_subscribe_clears_a_route_left_behind() {
        let instrument = aapl();
        let mut hub = FakeHub::new();
        let (left_behind, _left_rx) = hub.handle();
        let (successor, _successor_rx) = hub.handle();
        let (subscribed, ()) = tokio::join!(
            left_behind.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        subscribed.expect("the first session subscribes");

        hub.upstream.refuse_add = Some(Refused(StreamFailure::Transient));
        let (refused, ()) = tokio::join!(
            successor.subscribe(instrument, EventKind::Trade),
            hub.serve_one()
        );
        assert!(refused.is_err(), "the upstream refused the successor");
        assert!(hub.routes.is_empty());
        assert_eq!(hub.subs.trades(), None);
    }

    /// A first subscribe the upstream refuses leaves no route, and no pair to
    /// re-apply on the next connect.
    #[tokio::test]
    async fn a_refused_first_subscribe_leaves_nothing_behind() {
        let instrument = aapl();
        let mut hub = FakeHub::new();
        let (session, _rx) = hub.handle();
        hub.upstream.refuse_add = Some(Refused(StreamFailure::Transient));
        let (refused, ()) = tokio::join!(
            session.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        assert!(refused.is_err(), "the upstream refused the subscribe");
        assert!(hub.routes.is_empty());
        assert_eq!(hub.subs.trades(), None);
    }

    /// A subscribe the upstream rejects as a bad credential is answered as
    /// unauthenticated, like a subscribe to a parked hub.
    #[tokio::test]
    async fn a_subscribe_the_upstream_rejects_as_unauthenticated_says_so() {
        let mut hub = FakeHub::new();
        hub.upstream.refuse_add = Some(Refused(StreamFailure::Unauthenticated));
        let (handle, _rx) = hub.handle();
        let (res, ()) = tokio::join!(handle.subscribe(aapl(), EventKind::Trade), hub.serve_one());
        assert!(
            matches!(res, Err(Error::Unauthenticated { ref provider, ref message })
                if provider == FakeConnector::PROVIDER_ID
                    && message.starts_with("add_subscriptions:")),
            "{res:?}"
        );
    }

    /// Any other refusal stays a provider error.
    #[tokio::test]
    async fn a_subscribe_the_upstream_refuses_otherwise_is_a_provider_error() {
        let mut hub = FakeHub::new();
        hub.upstream.refuse_add = Some(Refused(StreamFailure::Transient));
        let (handle, _rx) = hub.handle();
        let (res, ()) = tokio::join!(handle.subscribe(aapl(), EventKind::Trade), hub.serve_one());
        assert!(matches!(res, Err(Error::Provider { .. })));
    }
}
