//! Read-only (`GET`-only) HTTP handlers.
//!
//! Every handler is a thin projection over a pre-assembled snapshot held in
//! [`WebState`]: load the relevant swap, project a sub-object, serialize. No
//! handler blocks the runtime, calls the on-demand `Datamancer::snapshot()`, or
//! mutates any state. The JSON shape **is** the Phase-3 `SystemSnapshot`
//! `Serialize` output (the same wire form the Phase-4 diagnostics plane
//! carries); the section endpoints are pure projections of it.
//!
//! Per CLAUDE.md, every ordered quantity is presented **per-`(instrument, kind)`**:
//! the payloads expose `seq` only inside `authoritative_sessions` keyed by
//! instrument+kind and never a global/merged sequence field. `latency_ns` is
//! observability only.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use datamancer::{
    AuthoritativeSessionSnapshot, CacheSnapshot, ClientSessionSnapshot, ProviderSnapshot,
    SystemSnapshot,
};
use futures::Stream;
use futures::StreamExt as _;
use serde::Serialize;
use tokio_stream::wrappers::WatchStream;

use crate::web::config_api::ConfigState;
use crate::web::state::WebState;

/// `GET /api/snapshot` — the entire live-state [`SystemSnapshot`] as JSON.
pub(crate) async fn snapshot(State(state): State<WebState>) -> Json<Arc<SystemSnapshot>> {
    Json(state.live_snapshot())
}

/// `GET /api/cache` — the cache catalog projection (from the slow swap).
pub(crate) async fn cache(State(state): State<WebState>) -> Json<CacheSnapshot> {
    Json(state.cache_snapshot().cache.clone())
}

/// `GET /api/providers` — provider accounting projection.
pub(crate) async fn providers(State(state): State<WebState>) -> Json<Vec<ProviderSnapshot>> {
    Json(state.live_snapshot().providers.clone())
}

/// The `/api/sessions` projection: the two per-symbol session views, side by
/// side. Authoritative units are shared singletons (with their subscriber
/// refcount); client sessions are the primary consumer handles with their
/// subscription sets.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionsView {
    pub authoritative_sessions: Vec<AuthoritativeSessionSnapshot>,
    pub client_sessions: Vec<ClientSessionSnapshot>,
}

impl SessionsView {
    pub(crate) fn from_snapshot(snap: &SystemSnapshot) -> Self {
        Self {
            authoritative_sessions: snap.authoritative_sessions.clone(),
            client_sessions: snap.client_sessions.clone(),
        }
    }
}

/// `GET /api/sessions` — live per-symbol session state.
pub(crate) async fn sessions(State(state): State<WebState>) -> Json<SessionsView> {
    Json(SessionsView::from_snapshot(&state.live_snapshot()))
}

/// The `/api/health` envelope: a cheap readiness boolean over the full
/// app-facing [`datamancer::HealthView`] ("one type, one reduction" — the
/// web surface is another consumer of the core reduction, not a fork).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct HealthEnvelope {
    /// `true` once every *enabled* provider reports `Connected` (and at
    /// least one is enabled). Disabled providers are deliberate and do not
    /// block readiness.
    pub ready: bool,
    pub health: datamancer::HealthView,
}

impl HealthEnvelope {
    pub(crate) fn from_snapshot(snap: &SystemSnapshot, credential_backend: &str) -> Self {
        let mut health = datamancer::HealthView::from_snapshot(
            snap,
            datamancer::HealthView::DEFAULT_STALE_AFTER_NS,
        );
        health.daemon.version = Some(env!("CARGO_PKG_VERSION").to_string());
        health.daemon.credential_backend = Some(credential_backend.to_string());
        let enabled: Vec<_> = health
            .providers
            .iter()
            .filter(|p| p.state != datamancer::ProviderState::Disabled)
            .collect();
        let ready = !enabled.is_empty()
            && enabled
                .iter()
                .all(|p| p.state == datamancer::ProviderState::Connected);
        Self { ready, health }
    }
}

/// `GET /api/health` — readiness + the full app-facing health view.
pub(crate) async fn health(State(state): State<WebState>) -> Json<HealthEnvelope> {
    Json(HealthEnvelope::from_snapshot(
        &state.live_snapshot(),
        state.credential_backend(),
    ))
}

/// The SSE event payload: the live snapshot plus the config restart flag, so
/// the banner updates without a page reload.
#[derive(Serialize)]
struct StreamEvent<'a> {
    snapshot: &'a SystemSnapshot,
    restart_required: bool,
}

/// Build the underlying stream of serialized live-state envelopes. Emits the
/// current snapshot immediately, then again on every live-refresh publish.
/// Factored out of the SSE handler so it can be unit-tested without HTTP.
pub(crate) fn live_json_stream(
    state: &WebState,
    config: ConfigState,
) -> impl Stream<Item = String> + use<> {
    let state = state.clone();
    WatchStream::new(state.live_version()).map(move |_version| {
        let snap = state.live_snapshot();
        let event = StreamEvent {
            snapshot: &snap,
            restart_required: config.restart_required(),
        };
        serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string())
    })
}

/// `GET /api/stream` — SSE of the live-state envelope, one event per refresh.
pub(crate) async fn stream(
    State(state): State<WebState>,
    State(config): State<ConfigState>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let events = live_json_stream(&state, config).map(|json| Ok(Event::default().data(json)));
    Sse::new(events).keep_alive(KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web::config_api::ConfigState;
    use crate::web::testdata;
    use arc_swap::ArcSwap;
    use datamancer::{ConnectionState, ProviderId, ProviderState, Timestamp};
    use serde_json::Value;
    use tokio::sync::watch;

    #[test]
    fn health_envelope_ignores_disabled_providers_for_readiness() {
        let snap = SystemSnapshot::new(
            Timestamp(1_000),
            vec![
                ProviderSnapshot::new(
                    ProviderId::from_static("on"),
                    ConnectionState::Connected,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                ),
                ProviderSnapshot::new(
                    ProviderId::from_static("off"),
                    ConnectionState::Unknown,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                )
                .with_enabled(false),
            ],
            CacheSnapshot::new(vec![], None),
            vec![],
            vec![],
        );
        let env = HealthEnvelope::from_snapshot(&snap, "keychain");
        assert!(env.ready); // the disabled provider does not block readiness
        assert_eq!(env.health.schema_version, 2);
        assert_eq!(
            env.health.daemon.credential_backend.as_deref(),
            Some("keychain")
        );
        assert_eq!(env.health.providers[1].state, ProviderState::Disabled);
    }

    #[test]
    fn health_envelope_not_ready_when_no_enabled_provider() {
        let snap = SystemSnapshot::new(
            Timestamp(1_000),
            vec![
                ProviderSnapshot::new(
                    ProviderId::from_static("off"),
                    ConnectionState::Unknown,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                )
                .with_enabled(false),
            ],
            CacheSnapshot::new(vec![], None),
            vec![],
            vec![],
        );
        assert!(!HealthEnvelope::from_snapshot(&snap, "file").ready);
    }

    fn config_state() -> ConfigState {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let boot = crate::config::Config::parse("[provider.alpaca]\naccount_type = \"paper\"\n")
            .expect("parse");
        boot.save(&path).expect("seed config");
        // Leak the tempdir so the file outlives the helper (test-only).
        std::mem::forget(dir);
        let (hub, _sources) = crate::config_hub::ConfigHub::bootstrap(boot.clone(), path.clone());
        ConfigState::new(path, boot, hub)
    }

    #[tokio::test]
    async fn sse_stream_emits_initial_then_on_change() {
        let live = Arc::new(ArcSwap::from_pointee(testdata::snapshot()));
        let cache = Arc::new(ArcSwap::from_pointee(testdata::snapshot()));
        let (tx, rx) = watch::channel(0u64);
        let state = WebState::new(live.clone(), cache, rx, "keychain");
        let config = config_state();

        let mut stream = Box::pin(live_json_stream(&state, config));

        // Initial element: reflects the warmed snapshot, wrapped in the envelope.
        let first = stream.next().await.expect("initial SSE sample");
        let first: Value = serde_json::from_str(&first).unwrap();
        let first_snap: SystemSnapshot = serde_json::from_value(first["snapshot"].clone()).unwrap();
        assert_eq!(first_snap, testdata::snapshot());
        assert_eq!(first["restart_required"], Value::Bool(false));

        // Publish a changed snapshot and bump the version: SSE emits again.
        let mut changed = testdata::snapshot();
        changed.captured_at = Timestamp(42);
        live.store(Arc::new(changed.clone()));
        tx.send_modify(|v| *v = v.wrapping_add(1));

        let second = stream.next().await.expect("post-change SSE sample");
        let second: Value = serde_json::from_str(&second).unwrap();
        let second_snap: SystemSnapshot =
            serde_json::from_value(second["snapshot"].clone()).unwrap();
        assert_eq!(second_snap, changed);
        assert_eq!(second["restart_required"], Value::Bool(false));
        assert_ne!(first, second);
    }
}
