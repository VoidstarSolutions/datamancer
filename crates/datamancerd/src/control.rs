//! The control-surface protocol: newline-delimited JSON over a Unix-domain
//! socket.
//!
//! One long-lived connection per client. Each line is one [`Request`]; the
//! daemon replies with one [`Reply`] line. Library errors map to **stable JSON
//! error codes** (the [`codes`] table) — an operator-facing contract guarded by
//! regression tests.
//!
//! Access control is filesystem permissions on the socket path only. This is
//! **not** a network-safe surface.

use datamancer::SystemSnapshot;
use serde::{Deserialize, Serialize};

use crate::config::{AssetClassCfg, EventKindCfg, PersistenceCfg, ScopeCfg};

/// Stable JSON error codes returned in [`Reply::code`]. These are an
/// operator-facing contract; changing a string is a breaking change and is
/// regression-guarded in tests.
pub mod codes {
    /// A live session for the pair is already active and cannot be shared as
    /// requested.
    pub const LIVE_SESSION_CONFLICT: &str = "live_session_conflict";
    /// No registered provider supports the requested `(instrument, kind)`.
    pub const UNSUPPORTED_EVENT_KIND: &str = "unsupported_event_kind";
    /// The requested persistence preset requires a backend that is not
    /// configured.
    pub const PERSISTENCE_REQUIRED: &str = "persistence_required";
    /// A client subscription requested an unsupported (non pure-live) scope.
    pub const UNSUPPORTED_CLIENT_SCOPE: &str = "unsupported_client_scope";
    /// The client already holds a subscription for this pair.
    pub const DUPLICATE_SUBSCRIPTION: &str = "duplicate_subscription";
    /// The client is not subscribed to this pair.
    pub const NOT_SUBSCRIBED: &str = "not_subscribed";
    /// A referenced provider id is not registered.
    pub const UNKNOWN_PROVIDER: &str = "unknown_provider";
    /// The underlying session has shut down.
    pub const SESSION_CLOSED: &str = "session_closed";
    /// The event stream is already held.
    pub const EVENTS_ALREADY_TAKEN: &str = "events_already_taken";
    /// A storage-layer error.
    pub const STORAGE: &str = "storage";
    /// A library configuration error at session construction.
    pub const CONFIG: &str = "config";
    /// An I/O or provider-level library error.
    pub const PROVIDER: &str = "provider";
    /// The named client is not connected/registered.
    pub const UNKNOWN_CLIENT: &str = "unknown_client";
    /// A client tried to `open-client` a name already in use.
    pub const DUPLICATE_CLIENT: &str = "duplicate_client";
    /// The iceoryx2 service cap would be exceeded by this subscribe.
    pub const SERVICE_CAP_EXCEEDED: &str = "service_cap_exceeded";
    /// The request was malformed or named an unsupported op.
    pub const BAD_REQUEST: &str = "bad_request";
    /// The daemon is shutting down and is no longer accepting requests.
    pub const SHUTTING_DOWN: &str = "shutting_down";
    /// An unexpected internal error.
    pub const INTERNAL: &str = "internal";
}

/// Map a library [`datamancer::Error`] to a stable JSON error code.
#[must_use]
pub fn error_code(err: &datamancer::Error) -> &'static str {
    use datamancer::Error;
    match err {
        Error::LiveSessionConflict { .. } => codes::LIVE_SESSION_CONFLICT,
        Error::UnsupportedEventKind { .. } => codes::UNSUPPORTED_EVENT_KIND,
        Error::PersistenceRequired => codes::PERSISTENCE_REQUIRED,
        Error::UnsupportedClientScope => codes::UNSUPPORTED_CLIENT_SCOPE,
        Error::DuplicateSubscription { .. } => codes::DUPLICATE_SUBSCRIPTION,
        Error::NotSubscribed { .. } => codes::NOT_SUBSCRIBED,
        Error::UnknownProvider(_) => codes::UNKNOWN_PROVIDER,
        Error::SessionClosed => codes::SESSION_CLOSED,
        Error::EventsAlreadyTaken => codes::EVENTS_ALREADY_TAKEN,
        Error::Storage(_) => codes::STORAGE,
        Error::Config(_) => codes::CONFIG,
        Error::Provider { .. } | Error::Io(_) => codes::PROVIDER,
        // `Error` is `#[non_exhaustive]`; any future variant maps to internal.
        _ => codes::INTERNAL,
    }
}

/// One target `(instrument, kind)` plus per-request scope/persistence
/// preferences. Used by both `subscribe` and the `open-client` seed list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionSpec {
    pub provider: String,
    pub asset_class: AssetClassCfg,
    pub symbol: String,
    pub kind: EventKindCfg,
    /// Scope preference. On conflict with an existing authoritative scope the
    /// reply returns the *actual* scope rather than erroring (handled server
    /// side); client subscriptions are pure-live today.
    #[serde(default)]
    pub scope: ScopeCfg,
    #[serde(default)]
    pub persistence: PersistenceCfg,
}

/// A control request (one per line).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum Request {
    /// Register a client and create its per-client data-plane service.
    OpenClient {
        client: String,
        #[serde(default)]
        subscriptions: Vec<SubscriptionSpec>,
    },
    /// Add a subscription to an open client.
    Subscribe {
        client: String,
        #[serde(flatten)]
        spec: SubscriptionSpec,
    },
    /// Remove a subscription from an open client.
    Unsubscribe {
        client: String,
        provider: String,
        asset_class: AssetClassCfg,
        symbol: String,
        kind: EventKindCfg,
    },
    /// Tear down a client (graceful).
    CloseClient { client: String },
    /// List currently-connected client names.
    ListClients,
    /// Return the current diagnostics snapshot as JSON.
    Snapshot,
}

/// A control reply (one per line). `ok` discriminates success from error; the
/// remaining fields are populated per op.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reply {
    pub ok: bool,
    /// The per-client data-plane service name (on `open-client`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// Connected client names (on `list-clients`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients: Option<Vec<String>>,
    /// The diagnostics snapshot (on `snapshot`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SystemSnapshot>,
    /// Stable error code (on failure).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Human-readable error detail (on failure).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl Reply {
    /// A bare success.
    #[must_use]
    pub fn ok() -> Self {
        Self {
            ok: true,
            service: None,
            clients: None,
            snapshot: None,
            code: None,
            message: None,
        }
    }

    /// Success carrying a created service name.
    #[must_use]
    pub fn service(name: impl Into<String>) -> Self {
        Self {
            service: Some(name.into()),
            ..Self::ok()
        }
    }

    /// Success carrying the client list.
    #[must_use]
    pub fn clients(names: Vec<String>) -> Self {
        Self {
            clients: Some(names),
            ..Self::ok()
        }
    }

    /// Success carrying a diagnostics snapshot.
    #[must_use]
    pub fn snapshot(snapshot: SystemSnapshot) -> Self {
        Self {
            snapshot: Some(snapshot),
            ..Self::ok()
        }
    }

    /// An error reply with a stable code and a message.
    #[must_use]
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            ok: false,
            service: None,
            clients: None,
            snapshot: None,
            code: Some(code.into()),
            message: Some(message.into()),
        }
    }

    /// An error reply derived from a library error (stable code + display).
    #[must_use]
    pub fn from_library_error(err: &datamancer::Error) -> Self {
        Self::error(error_code(err), err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_protocol_roundtrip_open_client() {
        let req = Request::OpenClient {
            client: "exec-1".to_string(),
            subscriptions: vec![SubscriptionSpec {
                provider: "alpaca-crypto".to_string(),
                asset_class: AssetClassCfg::Crypto,
                symbol: "BTC/USD".to_string(),
                kind: EventKindCfg::Trade,
                scope: ScopeCfg::Live,
                persistence: PersistenceCfg::CachedWithTap,
            }],
        };
        let line = serde_json::to_string(&req).expect("ser");
        let back: Request = serde_json::from_str(&line).expect("de");
        assert_eq!(req, back);
    }

    #[test]
    fn control_protocol_parses_documented_subscribe() {
        let line = r#"{"op":"subscribe","client":"exec-1","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade","scope":"live","persistence":"cached_with_tap"}"#;
        let req: Request = serde_json::from_str(line).expect("de");
        match req {
            Request::Subscribe { client, spec } => {
                assert_eq!(client, "exec-1");
                assert_eq!(spec.symbol, "BTC/USD");
                assert_eq!(spec.kind, EventKindCfg::Trade);
                assert_eq!(spec.persistence, PersistenceCfg::CachedWithTap);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn control_protocol_parses_documented_unsubscribe_and_lifecycle() {
        let u: Request = serde_json::from_str(
            r#"{"op":"unsubscribe","client":"exec-1","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .expect("de");
        assert!(matches!(u, Request::Unsubscribe { .. }));

        let c: Request =
            serde_json::from_str(r#"{"op":"close-client","client":"exec-1"}"#).expect("de");
        assert!(matches!(c, Request::CloseClient { .. }));

        let l: Request = serde_json::from_str(r#"{"op":"list-clients"}"#).expect("de");
        assert!(matches!(l, Request::ListClients));

        let s: Request = serde_json::from_str(r#"{"op":"snapshot"}"#).expect("de");
        assert!(matches!(s, Request::Snapshot));
    }

    #[test]
    fn unknown_op_is_a_parse_error() {
        let err = serde_json::from_str::<Request>(r#"{"op":"frobnicate"}"#);
        assert!(err.is_err());
    }

    #[test]
    fn reply_serialization_omits_empty_fields() {
        let ok = serde_json::to_value(Reply::service("datamancerd/data/3")).expect("ser");
        assert_eq!(ok["ok"], serde_json::Value::Bool(true));
        assert_eq!(ok["service"], "datamancerd/data/3");
        assert!(ok.get("code").is_none());
        assert!(ok.get("snapshot").is_none());

        let err = serde_json::to_value(Reply::error(codes::BAD_REQUEST, "nope")).expect("ser");
        assert_eq!(err["ok"], serde_json::Value::Bool(false));
        assert_eq!(err["code"], "bad_request");
        assert!(err.get("service").is_none());
    }

    #[test]
    fn subscribe_error_maps_to_json_code() {
        use datamancer::{AssetClass, Error, EventKind, Instrument, ProviderId};
        let inst = Instrument::new(
            ProviderId::from_static("alpaca-crypto"),
            AssetClass::Crypto,
            "BTC/USD",
        );

        assert_eq!(
            error_code(&Error::LiveSessionConflict {
                instrument: inst.clone(),
                kind: EventKind::Trade,
            }),
            codes::LIVE_SESSION_CONFLICT
        );
        assert_eq!(
            error_code(&Error::UnsupportedEventKind {
                instrument: inst.clone(),
                kind: EventKind::Trade,
            }),
            codes::UNSUPPORTED_EVENT_KIND
        );
        assert_eq!(
            error_code(&Error::PersistenceRequired),
            codes::PERSISTENCE_REQUIRED
        );
        assert_eq!(
            error_code(&Error::UnsupportedClientScope),
            codes::UNSUPPORTED_CLIENT_SCOPE
        );
        assert_eq!(
            error_code(&Error::DuplicateSubscription {
                instrument: inst.clone(),
                kind: EventKind::Trade,
            }),
            codes::DUPLICATE_SUBSCRIPTION
        );
        assert_eq!(
            error_code(&Error::NotSubscribed {
                instrument: inst,
                kind: EventKind::Trade,
            }),
            codes::NOT_SUBSCRIBED
        );
        assert_eq!(
            error_code(&Error::UnknownProvider("x".to_string())),
            codes::UNKNOWN_PROVIDER
        );
        assert_eq!(error_code(&Error::SessionClosed), codes::SESSION_CLOSED);
        assert_eq!(
            error_code(&Error::EventsAlreadyTaken),
            codes::EVENTS_ALREADY_TAKEN
        );
        assert_eq!(error_code(&Error::Storage("s".into())), codes::STORAGE);
        assert_eq!(error_code(&Error::Config("c".into())), codes::CONFIG);
        assert_eq!(
            error_code(&Error::Provider {
                provider: "p".into(),
                message: "m".into()
            }),
            codes::PROVIDER
        );
    }
}
