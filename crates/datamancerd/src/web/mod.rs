//! The embedded read-only introspection web surface (Phase 6).
//!
//! An `axum` HTTP server, hosted on the daemon's **shared** tokio runtime, that
//! renders the Phase-3 [`datamancer::SystemSnapshot`] for a single same-host
//! operator. It adds **no** domain state, ordering, or transport semantics — it
//! is a pure read-only consumer of a pre-assembled snapshot.
//!
//! # Security posture
//!
//! - **Loopback bind only** (`127.0.0.1`); auth is deferred (single operator,
//!   no network exposure).
//! - **One mutating route**: `PUT /api/config` (validated, atomic, loopback +
//!   same-origin + JSON-content-type guarded) writes the config *file*;
//!   apply-on-restart, the running daemon is never mutated. Everything else is
//!   `GET`-only (guarded by `web_router_single_mutating_route`).
//! - **Single-origin, no CORS**: the UI and JSON API share one loopback origin,
//!   so no CORS layer is added — never a permissive `Any` origin (guarded by
//!   `web_no_permissive_cors`).
//! - Basic response hardening headers (`X-Content-Type-Options`, a `Content-Security-Policy`)
//!   even same-host.

pub mod config_api;
pub mod handlers;
pub mod refresh;
mod settings;
pub mod state;
mod ui;

#[cfg(feature = "metrics")]
pub mod metrics;

#[cfg(test)]
mod testdata;

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;

use axum::Router;
use axum::extract::FromRef;
use axum::http::HeaderValue;
use axum::http::header::{CONTENT_SECURITY_POLICY, X_CONTENT_TYPE_OPTIONS};
use axum::routing::get;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;

pub use config_api::ConfigState;
pub use state::WebState;

/// Combined router state: snapshot reads plus the config-file handle.
#[derive(Clone)]
pub struct AppState {
    pub snapshots: WebState,
    pub config: ConfigState,
}

impl FromRef<AppState> for WebState {
    fn from_ref(state: &AppState) -> Self {
        state.snapshots.clone()
    }
}

impl FromRef<AppState> for ConfigState {
    fn from_ref(state: &AppState) -> Self {
        state.config.clone()
    }
}

/// `Content-Security-Policy` for the same-host operator UI. Self-origin only;
/// inline script/style are permitted because the server-rendered page carries a
/// small inline SSE/chart bootstrap (no external CDN).
const CSP: &str = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; \
     connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'";

/// Build the read-only router over the given [`WebState`].
///
/// Registers **only `GET`** routes (the read-only invariant), adds the security
/// headers and request-trace layers, and — when `assets_dir` resolves to an
/// existing directory — mounts static assets under `/assets`.
pub fn router(state: AppState, assets_dir: Option<&Path>) -> Router {
    let mut app = Router::new()
        .route("/", get(ui::index))
        .route("/config", get(settings::page))
        .route("/api/snapshot", get(handlers::snapshot))
        .route("/api/cache", get(handlers::cache))
        .route("/api/providers", get(handlers::providers))
        .route("/api/sessions", get(handlers::sessions))
        .route("/api/health", get(handlers::health))
        .route("/api/stream", get(handlers::stream))
        .route(
            "/api/config",
            get(config_api::get_config).put(config_api::put_config),
        );

    #[cfg(feature = "metrics")]
    {
        app = app.route("/metrics", get(metrics::render_handler));
    }

    // Static assets: on-disk, rooted at the configured dir. A missing dir is a
    // warning, not a hard error — the dynamic routes still serve.
    if let Some(dir) = assets_dir {
        if dir.is_dir() {
            let index = dir.join("index.html");
            let serve = ServeDir::new(dir).not_found_service(ServeFile::new(index));
            app = app.nest_service("/assets", serve);
        } else {
            tracing::warn!(dir = %dir.display(), "web-ui assets_dir does not exist; serving dynamic routes only");
        }
    }

    app.layer(SetResponseHeaderLayer::overriding(
        X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    ))
    .layer(SetResponseHeaderLayer::overriding(
        CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    ))
    .layer(TraceLayer::new_for_http())
    .with_state(state)
}

/// Bind a `TcpListener` for the web UI, enforcing the loopback-only invariant
/// here (not just at the caller): the introspection surface is unauthenticated
/// and same-host only, so a non-loopback bind is rejected.
///
/// # Errors
///
/// Returns `InvalidInput` for a non-loopback address; otherwise propagates the
/// bind I/O error. Binding here (rather than inside the spawned serve task) lets
/// a bind failure surface to the caller instead of being lost in a detached task.
pub async fn bind(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    if !addr.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("web bind {addr} is not a loopback address; the web UI is same-host only"),
        ));
    }
    tokio::net::TcpListener::bind(addr).await
}

/// Serve `router` on a pre-bound `listener` until `shutdown` resolves, draining
/// in-flight requests.
///
/// # Errors
///
/// Propagates the serve I/O error.
pub async fn serve(
    state: AppState,
    listener: tokio::net::TcpListener,
    assets_dir: Option<std::path::PathBuf>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let app = router(state, assets_dir.as_deref());
    if let Ok(addr) = listener.local_addr() {
        // Emit the literal IPv4/IPv6 URL, not a `localhost` form: the bind is
        // to one address family only, and `localhost` resolves to both on a
        // dual-stack host, so a browser preferring the other family (Happy
        // Eyeballs) can silently land on an unrelated service sharing the port.
        tracing::info!(
            url = %format_args!("http://{addr}/"),
            "datamancerd web UI listening (loopback; single mutating route /api/config) — open this exact URL, not localhost"
        );
    }
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use datamancer::{CacheSnapshot, ProviderSnapshot, SystemSnapshot};
    use http_body_util::BodyExt as _;
    use serde_json::Value;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tower::ServiceExt as _;

    fn state() -> AppState {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let boot = crate::config::Config::parse("[provider.alpaca]\naccount_type = \"paper\"\n")
            .expect("parse");
        boot.save(&path).expect("seed config");
        // Leak the tempdir so the file outlives the helper (test-only).
        std::mem::forget(dir);
        AppState {
            snapshots: WebState::fixed(testdata::snapshot(), testdata::snapshot()),
            config: ConfigState::new(path, boot),
        }
    }

    async fn send(method: Method, uri: &str) -> axum::response::Response {
        let app = router(state(), None);
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn body_bytes(resp: axum::response::Response) -> Vec<u8> {
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec()
    }

    async fn send_json(
        method: Method,
        uri: &str,
        body: &str,
        origin: Option<&str>,
    ) -> axum::response::Response {
        let app = router(state(), None);
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header("host", "127.0.0.1:8080");
        if let Some(o) = origin {
            req = req.header("origin", o);
        }
        app.oneshot(req.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap()
    }

    const ROUTES: &[&str] = &[
        "/",
        "/api/snapshot",
        "/api/cache",
        "/api/providers",
        "/api/sessions",
        "/api/health",
        "/api/stream",
    ];

    #[tokio::test]
    async fn web_router_single_mutating_route() {
        for route in ROUTES {
            for method in [Method::POST, Method::PUT, Method::DELETE, Method::PATCH] {
                let resp = send(method.clone(), route).await;
                assert_eq!(
                    resp.status(),
                    StatusCode::METHOD_NOT_ALLOWED,
                    "{method} {route} must be rejected"
                );
            }
        }
        // /api/config: PUT is allowed (guarded), all other mutations rejected.
        for method in [Method::POST, Method::DELETE, Method::PATCH] {
            let resp = send(method.clone(), "/api/config").await;
            assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        }
    }

    #[tokio::test]
    async fn config_get_returns_config_and_flag() {
        let resp = send(Method::GET, "/api/config").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let v: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(v["restart_required"], Value::Bool(false));
        assert!(v["config"]["provider"]["alpaca"].is_object());
        assert!(v["path"].as_str().unwrap().ends_with("config.toml"));
    }

    #[tokio::test]
    async fn config_put_writes_and_flags_restart() {
        let app = router(state(), None);
        let body = serde_json::json!({
            "provider": {"alpaca": {"account_type": "live"}},
            "session": {"resume_buffer_events": 128, "adjustment": "all"}
        })
        .to_string();
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/api/config")
                    .header("content-type", "application/json")
                    .header("host", "127.0.0.1:8080")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(v["restart_required"], Value::Bool(true));
        assert_eq!(v["config"]["provider"]["alpaca"]["account_type"], "live");
    }

    #[tokio::test]
    async fn config_put_invalid_writes_nothing() {
        // No provider at all -> validation failure with the stable `config` code.
        let resp = send_json(Method::PUT, "/api/config", r#"{"provider": {}}"#, None).await;
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let v: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(v["code"], "config");
    }

    #[tokio::test]
    async fn config_put_malformed_json_is_bad_request() {
        let resp = send_json(Method::PUT, "/api/config", "{not json", None).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let v: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(v["code"], "bad_request");
    }

    #[tokio::test]
    async fn config_put_cross_origin_is_rejected() {
        let body = r#"{"provider": {"alpaca": {"account_type": "paper"}}}"#;
        let resp = send_json(
            Method::PUT,
            "/api/config",
            body,
            Some("http://evil.example"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn config_put_wrong_content_type_is_rejected() {
        let app = router(state(), None);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/api/config")
                    .header("content-type", "text/plain")
                    .header("host", "127.0.0.1:8080")
                    .body(Body::from("x=1"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn web_no_permissive_cors() {
        let resp = send(Method::GET, "/api/snapshot").await;
        assert!(
            resp.headers().get("access-control-allow-origin").is_none(),
            "no CORS allow-origin header should be present (single-origin posture)"
        );
    }

    #[tokio::test]
    async fn web_security_headers_present() {
        let resp = send(Method::GET, "/api/snapshot").await;
        assert_eq!(
            resp.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert!(resp.headers().get("content-security-policy").is_some());
    }

    #[tokio::test]
    async fn web_snapshot_endpoint_serializes() {
        let resp = send(Method::GET, "/api/snapshot").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/json"
        );
        let bytes = body_bytes(resp).await;
        let back: SystemSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, testdata::snapshot());
    }

    #[tokio::test]
    async fn web_section_endpoints_match_snapshot() {
        let snap = testdata::snapshot();

        let cache: CacheSnapshot =
            serde_json::from_slice(&body_bytes(send(Method::GET, "/api/cache").await).await)
                .unwrap();
        assert_eq!(cache, snap.cache);

        let providers: Vec<ProviderSnapshot> =
            serde_json::from_slice(&body_bytes(send(Method::GET, "/api/providers").await).await)
                .unwrap();
        assert_eq!(providers, snap.providers);

        let sessions: Value =
            serde_json::from_slice(&body_bytes(send(Method::GET, "/api/sessions").await).await)
                .unwrap();
        let expected = serde_json::json!({
            "authoritative_sessions": snap.authoritative_sessions,
            "client_sessions": snap.client_sessions,
        });
        assert_eq!(sessions, expected);
    }

    #[tokio::test]
    async fn web_seq_is_per_symbol_in_payload() {
        let sessions: Value =
            serde_json::from_slice(&body_bytes(send(Method::GET, "/api/sessions").await).await)
                .unwrap();
        let auth = sessions["authoritative_sessions"].as_array().unwrap();
        assert_eq!(auth.len(), 2, "two distinct per-symbol units");

        // seq lives only inside each per-symbol unit, keyed by instrument+kind.
        let mut by_symbol = std::collections::BTreeMap::new();
        for entry in auth {
            assert!(entry.get("seq_position").is_some(), "per-unit seq present");
            let symbol = entry["instrument"]["symbol"].as_str().unwrap().to_string();
            by_symbol.insert(symbol, entry["seq_position"].clone());
        }
        // Distinct symbols carry distinct, independent seq positions.
        assert_ne!(by_symbol["AAPL"], by_symbol["MSFT"]);

        // No global/merged sequence field anywhere at the top level.
        assert!(sessions.get("seq").is_none());
        assert!(sessions.get("seq_position").is_none());
        assert!(sessions.get("global_seq").is_none());
    }

    #[tokio::test]
    async fn web_handler_does_not_block_runtime() {
        // `WebState` carries no `Datamancer`, so a handler cannot reach the
        // on-demand (potentially-blocking) snapshot accessor: it can only read
        // the pre-warmed swap. A successful response proves the read path.
        let resp = send(Method::GET, "/api/snapshot").await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn web_graceful_shutdown_drains() {
        use std::sync::Arc;
        use tokio::sync::Notify;

        // A handler that signals when it has started and blocks until released,
        // so we can prove a request is genuinely in flight when shutdown fires.
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let app = {
            let started = started.clone();
            let release = release.clone();
            axum::Router::new().route(
                "/slow",
                axum::routing::get(move || {
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        started.notify_one();
                        release.notified().await;
                        "ok"
                    }
                }),
            )
        };

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await
        });

        // Issue a real request and wait until the handler is actually running.
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        started.notified().await; // request is now in flight

        // Trigger shutdown while the request is mid-flight, then let it finish.
        tx.send(()).unwrap();
        release.notify_one();

        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        assert!(
            text.starts_with("HTTP/1.1 200"),
            "in-flight request must complete with 200 during graceful shutdown, got: {text}"
        );

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("serve task must resolve after shutdown");
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn settings_page_serves_form_shell() {
        let resp = send(Method::GET, "/config").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = String::from_utf8(body_bytes(resp).await).unwrap();
        assert!(body.contains("id=\"settings\""), "form container present");
        assert!(body.contains("/api/config"), "wired to the config API");
    }
}
