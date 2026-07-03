//! The control-surface protocol: newline-delimited JSON over a Unix-domain
//! socket.
//!
//! One long-lived connection per client. Each line is one [`Request`]; the
//! daemon replies with one [`Reply`] line. Library errors map to **stable JSON
//! error codes** (the [`codes`](crate::codes) table) — an operator-facing
//! contract guarded by regression tests.
//!
//! Access control is filesystem permissions on the socket path only. This is
//! **not** a network-safe surface.

use datamancer_core::SystemSnapshot;
use serde::{Deserialize, Serialize};

use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

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
        #[serde(flatten)]
        spec: UnsubscribeSpec,
    },
    /// Tear down a client (graceful).
    CloseClient { client: String },
    /// List currently-connected client names.
    ListClients,
    /// Return the current diagnostics snapshot as JSON.
    Snapshot,
    /// Enumerate available instruments and their supported kinds, optionally
    /// restricted to one provider (a full equities catalog is ~10k rows —
    /// prefer the filter).
    Instruments {
        #[serde(default)]
        provider: Option<String>,
    },
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
    /// The instrument catalog (on `instruments`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruments: Option<Vec<datamancer_core::InstrumentInfo>>,
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
            instruments: None,
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

    /// Success carrying the instrument catalog.
    #[must_use]
    pub fn instruments(catalog: Vec<datamancer_core::InstrumentInfo>) -> Self {
        Self {
            instruments: Some(catalog),
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
            instruments: None,
            code: Some(code.into()),
            message: Some(message.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{AssetClassCfg, EventKindCfg, PersistenceCfg, ScopeCfg};

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

        let err =
            serde_json::to_value(Reply::error(crate::codes::BAD_REQUEST, "nope")).expect("ser");
        assert_eq!(err["ok"], serde_json::Value::Bool(false));
        assert_eq!(err["code"], "bad_request");
        assert!(err.get("service").is_none());
    }

    #[test]
    fn instruments_request_parses_with_and_without_filter() {
        let filtered: Request =
            serde_json::from_str(r#"{"op":"instruments","provider":"alpaca-crypto"}"#).unwrap();
        assert!(
            matches!(filtered, Request::Instruments { provider: Some(p) } if p == "alpaca-crypto")
        );
        let all: Request = serde_json::from_str(r#"{"op":"instruments"}"#).unwrap();
        assert!(matches!(all, Request::Instruments { provider: None }));
    }

    #[test]
    fn instruments_reply_round_trips() {
        use datamancer_core::{AssetClass, EventKind, Instrument, InstrumentInfo, ProviderId};
        let reply = Reply::instruments(vec![InstrumentInfo {
            instrument: Instrument::new(
                ProviderId::from_static("alpaca-crypto"),
                AssetClass::Crypto,
                "BTC/USD",
            ),
            kinds: vec![EventKind::Trade],
        }]);
        let line = serde_json::to_string(&reply).unwrap();
        let back: Reply = serde_json::from_str(&line).unwrap();
        assert_eq!(reply, back);
        assert!(back.ok);
        assert_eq!(back.instruments.unwrap().len(), 1);
    }
}
