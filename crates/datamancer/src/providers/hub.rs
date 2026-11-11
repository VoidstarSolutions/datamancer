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
use std::marker::PhantomData;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datamancer_core::{
    Control, ControlKind, DisconnectCause, Error, EventKind, Instrument, LiveHandle, MarketEvent,
    Result, Seq, Timestamp,
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::credentials::{AlpacaCredentials, CredentialsSource, Resolved};
use super::runtime::{Park, SettingsSource};
use crate::session::ReconnectPolicy;

/// How a streaming failure is handled. A rejected credential pair from a
/// watched source parks the hub until a rotation (retrying cannot help,
/// provisioning can); one from a static source, and everything else — a
/// dropped socket, `406 connection limit exceeded`, `407 slow client` —
/// backs off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamFailure {
    Unauthenticated,
    Transient,
}

/// The credentials the hub resolved for one connect attempt. A source with
/// no credentials yet never reaches [`Connector::connect`]: the hub parks.
pub(super) enum PresentCredentials {
    /// Read the environment pair for the configured account type.
    Env,
    /// Use these.
    Creds(AlpacaCredentials),
}

/// A provider's aggregate subscription set, as its upstream takes it.
pub(super) trait SubscriptionSet: Clone + Send + Sync + 'static {
    /// The set holding no pair.
    fn empty() -> Self;
    /// Whether the set holds no pair.
    fn is_empty(&self) -> bool;
    /// Add (`add`) or remove one `(instrument, kind)` pair.
    fn apply(&mut self, instrument: &Instrument, kind: EventKind, add: bool);
}

/// One connected upstream: the calls the hub makes on it.
pub(super) trait Upstream: Send {
    /// The subscription set this upstream takes.
    type Subs: SubscriptionSet;
    /// What a failed call returns, classified by [`Connector::classify`].
    type Failure: std::error::Error + Send + Sync + 'static;
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

/// What a provider gives the hub: its sources, its reconnect policy, and how
/// to open and classify its upstream.
pub(super) trait Connector: Send + Sync + 'static {
    /// The provider id stamped on every control and error the hub emits.
    const PROVIDER_ID: &'static str;
    /// The runtime settings a connect reads.
    type Settings: Clone + Send + Sync + 'static;
    /// What a failed connect or upstream call returns.
    type Failure: std::error::Error + Send + Sync + 'static;
    /// The connected upstream.
    type Upstream: Upstream<Failure = Self::Failure>;
    /// Where the settings come from; `None` parks the hub disabled.
    fn settings(&self) -> &SettingsSource<Self::Settings>;
    /// Where the credentials come from; none yet parks the hub.
    fn credentials(&self) -> &CredentialsSource;
    /// The backoff policy between failed connects.
    fn reconnect(&self) -> &ReconnectPolicy;
    /// Open the upstream with `settings` and `credentials`.
    fn connect(
        &self,
        settings: &Self::Settings,
        credentials: PresentCredentials,
    ) -> impl Future<Output = std::result::Result<Self::Upstream, Self::Failure>> + Send;
    /// Whether `failure` parks the hub or backs off.
    fn classify(failure: &Self::Failure) -> StreamFailure;
}

// ---------------------------------------------------------------------------
// Hub slot
// ---------------------------------------------------------------------------

/// A provider's hub task: none until the first live session, then one.
/// The slot guards its own state, so a provider holds it plainly and every
/// session's `start_live` shares it.
pub(super) struct HubSlot<C: Connector> {
    state: std::sync::Mutex<SlotState>,
    connector: PhantomData<fn() -> C>,
}

struct SlotState {
    active: Option<ActiveHub>,
    #[cfg(test)]
    spawns: usize,
}

struct ActiveHub {
    cmd_tx: mpsc::Sender<HubCommand>,
    /// The hub exits on its own once the provider and every handle are
    /// gone (its command channel closes); the join handle is kept so a
    /// test can observe that.
    #[allow(dead_code, reason = "observed by the lifecycle test only")]
    task: JoinHandle<()>,
}

impl<C: Connector> HubSlot<C> {
    /// A slot with no hub running.
    pub(super) const fn idle() -> Self {
        Self {
            state: std::sync::Mutex::new(SlotState {
                active: None,
                #[cfg(test)]
                spawns: 0,
            }),
            connector: PhantomData,
        }
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

    /// The hub's command sender, spawning the hub with `make()` if none runs.
    pub(super) fn ensure(&self, make: impl FnOnce() -> C) -> mpsc::Sender<HubCommand> {
        let mut state = self.state();
        if let Some(active) = &state.active {
            return active.cmd_tx.clone();
        }
        let (cmd_tx, cmd_rx) = mpsc::channel::<HubCommand>(64);
        let task = tokio::spawn(run_hub(make(), cmd_rx));
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
pub(super) enum HubCommand {
    Subscribe {
        instrument: Instrument,
        kind: EventKind,
        sink: mpsc::Sender<MarketEvent>,
        ack: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        instrument: Instrument,
        kind: EventKind,
        /// The asking session's sink: only the route it owns is removed.
        sink: mpsc::Sender<MarketEvent>,
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
    pub(super) fn new(cmd_tx: mpsc::Sender<HubCommand>, sink: mpsc::Sender<MarketEvent>) -> Self {
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
                sink: self.sink.clone(),
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
                    sink: self.sink.clone(),
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

#[allow(
    clippy::too_many_lines,
    reason = "single-pass connect / authenticate / subscribe / dispatch / reconnect state machine; extraction would obscure the linear lifecycle"
)]
async fn run_hub<C: Connector>(connector: C, mut cmd_rx: mpsc::Receiver<HubCommand>) {
    // Per-session sinks keyed by the pair they own; the session registry's
    // at-most-one-live-session-per-pair rule keeps this a function.
    let mut routes: HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>> = HashMap::new();
    // Authoritative aggregate subscription set, applied to every fresh socket.
    let mut subs = <C::Upstream as Upstream>::Subs::empty();
    let mut backoff = connector.reconnect().initial_backoff_ms;

    'outer: loop {
        let mut settings_rx = connector.settings().watch();
        let Some(settings) = connector.settings().current() else {
            // Disabled: park until the settings watch delivers a value. Only a
            // Watch source can resolve to None, so the receiver is always
            // present here; exit defensively if it isn't (never busy-loop).
            let Some(rx) = settings_rx.as_mut() else {
                return;
            };
            if !wait_for_provisioning::<C, _>(rx, &mut cmd_rx, Park::Disabled).await {
                return;
            }
            continue 'outer;
        };

        // Fresh receiver per connect attempt: `watch()` marks the current
        // value as seen on the clone (tokio's `Receiver::clone` would
        // otherwise inherit the stored receiver's stale seen-version), so
        // the hot-reconnect arm below only fires on rotations that land
        // *after* this resolution.
        let mut cred_rx = connector.credentials().watch();
        let connect_result = match connector.credentials().current() {
            Resolved::Env => connector.connect(&settings, PresentCredentials::Env).await,
            Resolved::Creds(c) => {
                connector
                    .connect(&settings, PresentCredentials::Creds(c))
                    .await
            }
            Resolved::Missing => {
                // No credentials yet: wait for provisioning instead of
                // hammering bad auth, then retry the outer loop. Only a
                // Watch source can resolve to Missing, so the receiver is
                // always present here; exit defensively if it isn't (never
                // busy-loop).
                let Some(rx) = cred_rx.as_mut() else { return };
                if !wait_for_provisioning::<C, _>(rx, &mut cmd_rx, Park::MissingCredentials).await {
                    return;
                }
                continue 'outer;
            }
        };

        let mut upstream = match connect_result {
            Ok(upstream) => {
                backoff = connector.reconnect().initial_backoff_ms;
                broadcast_control(
                    &routes,
                    ControlKind::ProviderConnected {
                        provider: C::PROVIDER_ID.to_string(),
                    },
                )
                .await;
                upstream
            }
            Err(err) => {
                let failure = C::classify(&err);
                let unauthenticated = failure == StreamFailure::Unauthenticated;
                tracing::warn!(
                    provider = C::PROVIDER_ID,
                    ?failure,
                    error = %error_chain(&err),
                    "market-data stream connect failed"
                );
                broadcast_control(
                    &routes,
                    ControlKind::ProviderDisconnected {
                        provider: C::PROVIDER_ID.to_string(),
                        reason: format!("connect failed: {}", error_chain(&err)),
                        cause: if unauthenticated {
                            DisconnectCause::Unauthenticated
                        } else {
                            DisconnectCause::Error
                        },
                    },
                )
                .await;
                if unauthenticated && let Some(rx) = cred_rx.as_mut() {
                    // Rejected credentials: retrying cannot help. Park until
                    // a rotation (set-credentials hot-apply) or disable —
                    // mirrors the Missing-credentials park above. Static
                    // sources can't rotate, so they fall through to backoff.
                    if !wait_for_provisioning::<C, _>(rx, &mut cmd_rx, Park::RejectedCredentials)
                        .await
                    {
                        return;
                    }
                    continue 'outer;
                }
                if !sleep_with_jitter::<C>(&mut backoff, connector.reconnect(), &mut cmd_rx).await {
                    return;
                }
                continue 'outer;
            }
        };

        // Re-apply the aggregate subscription set on a fresh socket.
        if !subs.is_empty()
            && let Err(err) = upstream.add(&subs).await
        {
            broadcast_error::<C>(
                &routes,
                format!("re-subscribe failed: {}", error_chain(&err)),
            )
            .await;
        }

        // Event loop: select between commands, incoming messages, and the
        // credential/settings watches.
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        // The provider and every handle are gone; no command
                        // can ever arrive again. Close the websocket and exit.
                        upstream.shut_down().await;
                        return;
                    };
                    handle_route_command::<C>(&mut routes, &mut subs, &mut upstream, cmd).await;
                }
                next = upstream.next_events() => {
                    match next {
                        Ok(events) => {
                            for ev in events {
                                dispatch_event(&routes, ev).await;
                            }
                        }
                        Err(err) => {
                            // oxidized_alpaca 0.0.10: `Error::StreamingError`
                            // carries the server's envelope mid-stream, so a
                            // `402` parks and a `406`/`407` backs off; a
                            // mid-stream `StreamingAuth` is connect-only
                            // upstream today and handled the same way.
                            let failure = C::classify(&err);
                            let unauthenticated = failure == StreamFailure::Unauthenticated;
                            tracing::warn!(
                                provider = C::PROVIDER_ID,
                                ?failure,
                                error = %error_chain(&err),
                                "market-data stream failed"
                            );
                            broadcast_control(
                                &routes,
                                ControlKind::ProviderDisconnected {
                                    provider: C::PROVIDER_ID.to_string(),
                                    reason: format!("websocket: {}", error_chain(&err)),
                                    cause: if unauthenticated {
                                        DisconnectCause::Unauthenticated
                                    } else {
                                        DisconnectCause::Error
                                    },
                                },
                            )
                            .await;
                            // Drop the upstream and reconnect.
                            drop(upstream);
                            if unauthenticated && let Some(rx) = cred_rx.as_mut() {
                                // Rejected credentials: retrying cannot help.
                                // Park until a rotation or disable, exactly
                                // as the connect-time arm does. Static
                                // sources can't rotate, so they fall through
                                // to backoff.
                                if !wait_for_provisioning::<C, _>(rx, &mut cmd_rx, Park::RejectedCredentials)
                                    .await
                                {
                                    return;
                                }
                                continue 'outer;
                            }
                            if !sleep_with_jitter::<C>(&mut backoff, connector.reconnect(), &mut cmd_rx).await {
                                return;
                            }
                            continue 'outer;
                        }
                    }
                }
                changed = async {
                    match cred_rx.as_mut() {
                        Some(rx) => rx.changed().await,
                        // Unreachable: the arm is guarded on `is_some()`.
                        None => std::future::pending().await,
                    }
                }, if cred_rx.is_some() => {
                    if changed.is_ok() {
                        tracing::info!(
                            provider = C::PROVIDER_ID,
                            "credentials changed; reconnecting"
                        );
                        // Same in-band control the websocket error path
                        // broadcasts, then reconnect immediately with the
                        // new credentials — reset the backoff, this is a
                        // deliberate rotation, not a failure.
                        broadcast_control(
                            &routes,
                            ControlKind::ProviderDisconnected {
                                provider: C::PROVIDER_ID.to_string(),
                                reason: "credentials rotated".to_string(),
                                cause: DisconnectCause::Error,
                            },
                        )
                        .await;
                        upstream.shut_down().await;
                        backoff = connector.reconnect().initial_backoff_ms;
                        continue 'outer;
                    }
                    // Watch sender dropped: no further rotations can
                    // arrive. Disable this arm (a closed receiver would
                    // otherwise resolve immediately and spin the select).
                    cred_rx = None;
                }
                changed = async {
                    match settings_rx.as_mut() {
                        Some(rx) => rx.changed().await,
                        // Unreachable: the arm is guarded on `is_some()`.
                        None => std::future::pending().await,
                    }
                }, if settings_rx.is_some() => {
                    if changed.is_ok() {
                        // A disable is deliberate, not a fault: classify it so
                        // health does not read the dropped streams as Stale.
                        let (reason, cause) = if connector.settings().current().is_none() {
                            ("provider disabled", DisconnectCause::Disabled)
                        } else {
                            ("settings changed", DisconnectCause::Error)
                        };
                        tracing::info!(provider = C::PROVIDER_ID, reason, "settings changed; reconnecting");
                        broadcast_control(
                            &routes,
                            ControlKind::ProviderDisconnected {
                                provider: C::PROVIDER_ID.to_string(),
                                reason: reason.to_string(),
                                cause,
                            },
                        )
                        .await;
                        upstream.shut_down().await;
                        backoff = connector.reconnect().initial_backoff_ms;
                        continue 'outer;
                    }
                    settings_rx = None;
                }
            }
        }
    }
}

/// Waits for a `Watch` source (settings or credentials) to deliver a new
/// value, servicing the command channel meanwhile (drop of the provider and
/// every handle exits; subscribe/unsubscribe fail fast with `park`'s error).
/// Returns `false` if the hub task should exit.
async fn wait_for_provisioning<C: Connector, T>(
    rx: &mut watch::Receiver<T>,
    cmd_rx: &mut mpsc::Receiver<HubCommand>,
    park: Park,
) -> bool {
    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_ok() {
                    // The outer loop re-resolves; if the new value is still
                    // disabled/missing it lands back here rather than spinning.
                    return true;
                }
                // Sender dropped with no value: it can never arrive. Keep
                // servicing commands so hub shutdown (all handles dropped)
                // still works.
                loop {
                    match cmd_rx.recv().await {
                        Some(
                            HubCommand::Subscribe { ack, .. }
                            | HubCommand::Unsubscribe { ack, .. },
                        ) => {
                            let _ = ack.send(Err(park.error(C::PROVIDER_ID)));
                        }
                        None => return false,
                    }
                }
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(
                        HubCommand::Subscribe { ack, .. }
                        | HubCommand::Unsubscribe { ack, .. },
                    ) => {
                        let _ = ack.send(Err(park.error(C::PROVIDER_ID)));
                    }
                    None => return false,
                }
            }
        }
    }
}

/// Sleeps with exponential backoff, servicing the command channel meanwhile.
/// Returns `false` if the hub task should exit (provider and handles gone).
async fn sleep_with_jitter<C: Connector>(
    backoff_ms: &mut u64,
    policy: &ReconnectPolicy,
    cmd_rx: &mut mpsc::Receiver<HubCommand>,
) -> bool {
    // Full jitter: pick a sleep uniformly in [0, backoff_ms]. With many
    // providers reconnecting after a shared upstream blip this avoids
    // synchronized retry storms; without it every one retries on the same
    // exponential ladder.
    let delay_ms = if policy.jitter {
        fastrand::u64(0..=*backoff_ms)
    } else {
        *backoff_ms
    };
    let delay = Duration::from_millis(delay_ms);
    *backoff_ms = (*backoff_ms * 2).min(policy.max_backoff_ms);

    tokio::select! {
        () = tokio::time::sleep(delay) => true,
        cmd = cmd_rx.recv() => {
            match cmd {
                // While disconnected, fail subscribe/unsubscribe immediately so
                // sessions see a clear error rather than stalling.
                Some(HubCommand::Subscribe { ack, .. } | HubCommand::Unsubscribe { ack, .. }) => {
                    let _ = ack.send(Err(Error::Provider {
                        provider: C::PROVIDER_ID.to_string(),
                        message: "provider is reconnecting".to_string(),
                    }));
                    true
                }
                // The provider and every handle are gone: exit.
                None => false,
            }
        }
    }
}

/// Route one translated event to the sink owning its `(instrument, kind)`;
/// a control (no pair) fans out to every active sink so each session sees
/// provider-level state changes.
pub(super) async fn dispatch_event(
    routes: &HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    ev: MarketEvent,
) {
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
fn ack_error<C: Connector>(what: &str, failure: &C::Failure) -> Error {
    Error::Provider {
        provider: C::PROVIDER_ID.to_string(),
        message: format!("{what}: {}", error_chain(failure)),
    }
}

/// Apply one session's subscribe or unsubscribe on the connected socket and
/// answer it.
async fn handle_route_command<C: Connector>(
    routes: &mut HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    subs: &mut <C::Upstream as Upstream>::Subs,
    upstream: &mut C::Upstream,
    cmd: HubCommand,
) {
    match cmd {
        HubCommand::Subscribe {
            instrument,
            kind,
            sink,
            ack,
        } => {
            let res = subscribe_route::<C>(routes, subs, upstream, instrument, kind, sink).await;
            let _ = ack.send(res);
        }
        HubCommand::Unsubscribe {
            instrument,
            kind,
            sink,
            ack,
        } => {
            let res = unsubscribe_route::<C>(routes, subs, upstream, instrument, kind, &sink).await;
            let _ = ack.send(res);
        }
    }
}

/// Route `(instrument, kind)` to `sink` and subscribe it upstream. If the
/// upstream refuses, a route this one replaced (a session torn down whose
/// unsubscribe has not arrived yet) is restored, its pair still streaming,
/// so that session's unsubscribe still removes it; with no such route the
/// pair is dropped again, so a reconnect does not apply a subscription the
/// server rejected.
async fn subscribe_route<C: Connector>(
    routes: &mut HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    subs: &mut <C::Upstream as Upstream>::Subs,
    upstream: &mut C::Upstream,
    instrument: Instrument,
    kind: EventKind,
    sink: mpsc::Sender<MarketEvent>,
) -> Result<()> {
    let key = (instrument, kind);
    let replaced = routes.insert(key.clone(), sink);
    subs.apply(&key.0, kind, true);
    if let Err(failure) = upstream.add(subs).await {
        if let Some(previous) = replaced {
            routes.insert(key, previous);
        } else {
            routes.remove(&key);
            subs.apply(&key.0, kind, false);
        }
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

/// Remove `sink`'s route for `(instrument, kind)`: drop the pair from
/// `subs`, remove it upstream, then drop the route and tell its sink the
/// subscription ended. A route that another session's sink holds, or no
/// route at all, is left alone, upstream included, and the call answers
/// `Ok`: a session torn down after a successor took its pair cannot cancel
/// the successor. A failed upstream removal keeps the pair in `subs`, so a
/// reconnect applies it again.
async fn unsubscribe_route<C: Connector>(
    routes: &mut HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    subs: &mut <C::Upstream as Upstream>::Subs,
    upstream: &mut C::Upstream,
    instrument: Instrument,
    kind: EventKind,
    sink: &mpsc::Sender<MarketEvent>,
) -> Result<()> {
    let key = (instrument, kind);
    if !routes
        .get(&key)
        .is_some_and(|route| route.same_channel(sink))
    {
        return Ok(());
    }
    subs.apply(&key.0, kind, false);
    let mut removal = <C::Upstream as Upstream>::Subs::empty();
    removal.apply(&key.0, kind, true);
    match upstream.remove(&removal).await {
        Ok(()) => {
            // Drop the route after the upstream ack so any tail events still
            // in flight are routable.
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
            Ok(())
        }
        Err(failure) => {
            // The pair is still streaming upstream, so a reconnect must apply it.
            subs.apply(&key.0, kind, true);
            Err(ack_error::<C>("remove_subscriptions", &failure))
        }
    }
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
async fn broadcast_control(
    routes: &HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    kind: ControlKind,
) {
    let ev = control_event(kind);
    for sink in routes.values() {
        let _ = sink.send(ev.clone()).await;
    }
}

/// Fan one provider error out to every active sink, every copy carrying the
/// same fresh fan-out id.
async fn broadcast_error<C: Connector>(
    routes: &HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>,
    message: String,
) {
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

    use datamancer_core::{
        AssetClass, Bar, BarInterval, ErrorOrigin, FanoutId, Price, ProviderId, Quantity, Trade,
    };

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

        fn apply(&mut self, instrument: &Instrument, kind: EventKind, add: bool) {
            let pair = (instrument.clone(), kind);
            if add {
                self.0.insert(pair);
            } else {
                self.0.remove(&pair);
            }
        }
    }

    /// Records the upstream calls the hub makes; refuses `add` while
    /// `refuse_add` is set, and yields `events` in order, then pends.
    #[derive(Default)]
    struct FakeUpstream {
        adds: usize,
        removals: Vec<PairSet>,
        refuse_add: bool,
        events: VecDeque<std::result::Result<Vec<MarketEvent>, Refused>>,
    }

    impl FakeUpstream {
        /// An upstream whose `next_events` pends forever.
        fn idle() -> Self {
            Self::default()
        }
    }

    impl Upstream for FakeUpstream {
        type Subs = PairSet;
        type Failure = Refused;

        async fn add(&mut self, _subs: &PairSet) -> std::result::Result<(), Refused> {
            self.adds += 1;
            if self.refuse_add {
                return Err(Refused(StreamFailure::Transient));
            }
            Ok(())
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

        async fn shut_down(self) {}
    }

    /// A connector that answers each connect from a script.
    struct FakeConnector {
        connects: std::sync::Mutex<VecDeque<std::result::Result<FakeUpstream, Refused>>>,
        settings: SettingsSource<()>,
        credentials: CredentialsSource,
        reconnect: ReconnectPolicy,
    }

    impl FakeConnector {
        /// Static settings, `Env` credentials; once the script runs out,
        /// `connect` pends forever.
        fn scripted(connects: Vec<std::result::Result<FakeUpstream, Refused>>) -> Self {
            Self {
                connects: std::sync::Mutex::new(connects.into()),
                settings: SettingsSource::Static(()),
                credentials: CredentialsSource::Env,
                // No jitter, so a backoff sleep is never zero.
                reconnect: ReconnectPolicy {
                    initial_backoff_ms: 1_000,
                    max_backoff_ms: 30_000,
                    jitter: false,
                },
            }
        }

        fn never_connects() -> Self {
            Self::scripted(vec![])
        }
    }

    impl Connector for FakeConnector {
        const PROVIDER_ID: &'static str = "fake";
        type Settings = ();
        type Failure = Refused;
        type Upstream = FakeUpstream;

        fn settings(&self) -> &SettingsSource<()> {
            &self.settings
        }

        fn credentials(&self) -> &CredentialsSource {
            &self.credentials
        }

        fn reconnect(&self) -> &ReconnectPolicy {
            &self.reconnect
        }

        fn connect(
            &self,
            _settings: &(),
            _credentials: PresentCredentials,
        ) -> impl Future<Output = std::result::Result<FakeUpstream, Refused>> + Send {
            let next = self
                .connects
                .lock()
                .expect("connect script poisoned")
                .pop_front();
            async move {
                match next {
                    Some(result) => result,
                    None => std::future::pending().await,
                }
            }
        }

        fn classify(failure: &Refused) -> StreamFailure {
            failure.0
        }
    }

    /// Spawn a hub on `connector`: its task and its command sender.
    fn spawn_hub(connector: FakeConnector) -> (JoinHandle<()>, mpsc::Sender<HubCommand>) {
        let slot = HubSlot::idle();
        let cmd_tx = slot.ensure(|| connector);
        let task = slot.take_task().expect("the hub was spawned");
        (task, cmd_tx)
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

    /// A subscribe that lands while the hub sleeps out a backoff is refused
    /// at once as reconnecting rather than queued behind the next connect.
    #[tokio::test(start_paused = true)]
    async fn a_subscribe_during_backoff_is_refused_as_reconnecting() {
        let (hub, cmd_tx) = spawn_hub(FakeConnector::scripted(vec![Err(Refused(
            StreamFailure::Transient,
        ))]));
        let (sink, _rx) = mpsc::channel(8);
        let handle = SharedLiveHandle::new(cmd_tx, sink);
        tokio::task::yield_now().await; // the hub reaches its backoff sleep
        let err = handle
            .subscribe(aapl(), EventKind::Trade)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Provider { ref message, .. } if message == "provider is reconnecting")
        );
        drop(hub);
    }

    #[test]
    fn an_idle_slot_spawns_once_and_reuses_its_hub() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let slot = HubSlot::<FakeConnector>::idle();
            let a = slot.ensure(FakeConnector::never_connects);
            let b = slot.ensure(|| unreachable!("a running hub is reused"));
            assert!(a.same_channel(&b));
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
        assert!(
            matches!(aapl_rx.try_recv(), Ok(MarketEvent::Trade(t)) if t.instrument.symbol() == "AAPL")
        );
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

    pub(in crate::providers) type Routes =
        HashMap<(Instrument, EventKind), mpsc::Sender<MarketEvent>>;

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

        /// Route one trade for `instrument` as the socket would.
        async fn deliver_trade(&self, instrument: &Instrument) {
            let now = Timestamp(1);
            dispatch_event(
                &self.routes,
                MarketEvent::Trade(Trade {
                    instrument: instrument.clone(),
                    source_ts: now,
                    rx_ts: now,
                    seq: Seq(0),
                    price: Price::from_f64_round(1.0),
                    size: Quantity::from_units(1),
                }),
            )
            .await;
        }
    }

    /// Whether `rx` holds a trade, past any controls.
    fn has_trade(rx: &mut mpsc::Receiver<MarketEvent>) -> bool {
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, MarketEvent::Trade(_)) {
                return true;
            }
        }
        false
    }

    /// A session torn down after another session took its pair: its late
    /// unsubscribe leaves the successor's route, its data and the upstream
    /// subscription alone, and the successor's own unsubscribe still removes
    /// all three.
    #[tokio::test]
    async fn a_late_unsubscribe_keeps_the_successors_route() {
        let instrument = aapl();
        let mut hub = FakeHub::new();
        let (s1, _s1_rx) = hub.handle();
        let (s2, mut s2_rx) = hub.handle();
        let (subscribed, ()) = tokio::join!(
            s1.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        subscribed.expect("s1 subscribes");
        let (subscribed, ()) = tokio::join!(
            s2.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        subscribed.expect("s2 subscribes");

        let (closed, ()) = tokio::join!(s1.close(), hub.serve_one());
        closed.expect("s1 closes");
        assert!(
            hub.upstream.removals.is_empty(),
            "the upstream subscription was removed"
        );
        assert_eq!(hub.subs.trades(), Some(vec!["AAPL".to_string()]));
        hub.deliver_trade(&instrument).await;
        assert!(has_trade(&mut s2_rx), "the successor lost its route");

        let (unsubscribed, ()) = tokio::join!(
            s2.unsubscribe(instrument, EventKind::Trade),
            hub.serve_one()
        );
        unsubscribed.expect("s2 unsubscribes");
        assert_eq!(hub.upstream.removals.len(), 1);
        assert!(hub.routes.is_empty());
        assert_eq!(hub.subs.trades(), None);
        assert!(matches!(
            s2_rx.try_recv(),
            Ok(MarketEvent::Control(Control {
                kind: ControlKind::SubscriptionChanged { active: false, .. },
                ..
            }))
        ));
    }

    /// A successor's subscribe that the upstream refuses restores the route
    /// it replaced: the old session keeps its data, and its late unsubscribe
    /// still removes the pair upstream.
    #[tokio::test]
    async fn a_refused_successor_restores_the_route_it_replaced() {
        let instrument = aapl();
        let mut hub = FakeHub::new();
        let (s1, mut s1_rx) = hub.handle();
        let (s2, _s2_rx) = hub.handle();
        let (subscribed, ()) = tokio::join!(
            s1.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        subscribed.expect("s1 subscribes");

        hub.upstream.refuse_add = true;
        let (refused, ()) = tokio::join!(
            s2.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        assert!(refused.is_err(), "the upstream refused s2");
        assert_eq!(hub.subs.trades(), Some(vec!["AAPL".to_string()]));
        hub.deliver_trade(&instrument).await;
        assert!(has_trade(&mut s1_rx), "s1 lost its route");

        let (closed, ()) = tokio::join!(s1.close(), hub.serve_one());
        closed.expect("s1 closes");
        assert_eq!(
            hub.upstream.removals.len(),
            1,
            "the pair was left subscribed upstream"
        );
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
        hub.upstream.refuse_add = true;
        let (refused, ()) = tokio::join!(
            session.subscribe(instrument.clone(), EventKind::Trade),
            hub.serve_one()
        );
        assert!(refused.is_err(), "the upstream refused the subscribe");
        assert!(hub.routes.is_empty());
        assert_eq!(hub.subs.trades(), None);
    }
}
