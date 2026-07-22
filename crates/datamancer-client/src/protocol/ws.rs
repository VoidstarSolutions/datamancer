//! The WebSocket control protocol: JSON frames over the one WS connection.
//!
//! Reuses the UDS control vocabulary — [`SubscriptionSpec`](crate::spec::SubscriptionSpec)
//! and the stable [`codes`](crate::codes) table — but drops the
//! per-request `client` field (the connection identifies the client) and adds a
//! correlation `id` echoed on the reply, because event frames interleave with
//! replies on the shared socket. `open-client` is implicit on connect and has no
//! request.

use datamancer_core::SystemSnapshot;
use serde::{Deserialize, Serialize};

use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

/// A WS control request (one JSON text frame).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum WsRequest {
    /// Add a subscription to this connection's client.
    Subscribe {
        id: u64,
        #[serde(flatten)]
        spec: SubscriptionSpec,
    },
    /// Remove a subscription.
    Unsubscribe {
        id: u64,
        #[serde(flatten)]
        spec: UnsubscribeSpec,
    },
    /// Return the current diagnostics snapshot.
    Snapshot { id: u64 },
    /// Gracefully close this connection's client.
    CloseClient { id: u64 },
    /// Enumerate available instruments and their supported kinds, optionally
    /// restricted to one provider.
    Instruments {
        id: u64,
        #[serde(default)]
        provider: Option<String>,
    },
    /// On-demand per-instrument capabilities for a named provider's symbols.
    Capabilities {
        id: u64,
        provider: String,
        #[serde(default)]
        symbols: Vec<String>,
    },
    /// Subscribe to the daemon's periodic `HealthView` push (the Windows
    /// same-host health plane — the iceoryx2 health service is not available on
    /// Windows). The daemon acks with `ok` then pushes `Health` frames on its
    /// diagnostics cadence until the connection closes.
    WatchHealth { id: u64 },
}

impl WsRequest {
    /// The correlation id carried by every request.
    #[must_use]
    pub fn id(&self) -> u64 {
        match self {
            Self::Subscribe { id, .. }
            | Self::Unsubscribe { id, .. }
            | Self::Snapshot { id }
            | Self::CloseClient { id }
            | Self::Instruments { id, .. }
            | Self::Capabilities { id, .. }
            | Self::WatchHealth { id } => *id,
        }
    }
}

/// A WS control reply (one JSON text frame), echoing the request `id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WsReply {
    pub id: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SystemSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruments: Option<Vec<datamancer_core::InstrumentInfo>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<datamancer_core::InstrumentEntry>>,
}

/// A server-initiated health push (the Windows same-host health plane — the
/// iceoryx2 health service is unavailable on Windows). Sent unsolicited after a
/// [`WsRequest::WatchHealth`] subscribe, on the daemon's diagnostics cadence,
/// until the connection closes. Its shape (`{"view": …}`) is disjoint from
/// [`WsReply`] (no `id`/`ok`) and the transport crate's `EventFrame` (no
/// `"type"` tag), so the client's inbound demux is unambiguous — it is **not**
/// an `EventFrame` (a health push is not a `MarketEvent`, and `transport-ws`
/// requires all `EventFrame`s to route through `to_wire`/`from_wire`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WsHealthPush {
    pub view: datamancer_core::HealthView,
}

impl WsReply {
    #[must_use]
    pub fn ok(id: u64) -> Self {
        Self {
            id,
            ok: true,
            code: None,
            message: None,
            snapshot: None,
            instruments: None,
            capabilities: None,
        }
    }

    #[must_use]
    pub fn error(id: u64, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            id,
            ok: false,
            code: Some(code.into()),
            message: Some(message.into()),
            snapshot: None,
            instruments: None,
            capabilities: None,
        }
    }

    #[must_use]
    pub fn snapshot(id: u64, snapshot: SystemSnapshot) -> Self {
        Self {
            id,
            ok: true,
            code: None,
            message: None,
            snapshot: Some(snapshot),
            instruments: None,
            capabilities: None,
        }
    }

    #[must_use]
    pub fn instruments(id: u64, catalog: Vec<datamancer_core::InstrumentInfo>) -> Self {
        Self {
            id,
            ok: true,
            code: None,
            message: None,
            snapshot: None,
            instruments: Some(catalog),
            capabilities: None,
        }
    }

    #[must_use]
    pub fn capabilities(id: u64, entries: Vec<datamancer_core::InstrumentEntry>) -> Self {
        Self {
            id,
            ok: true,
            code: None,
            message: None,
            snapshot: None,
            instruments: None,
            capabilities: Some(entries),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{WsHealthPush, WsReply, WsRequest};
    use crate::spec::EventKindCfg;

    #[test]
    fn ws_subscribe_parses_with_id_and_shared_spec() {
        let line = r#"{"id":7,"op":"subscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#;
        let req: WsRequest = serde_json::from_str(line).expect("de");
        match req {
            WsRequest::Subscribe { id, spec } => {
                assert_eq!(id, 7);
                assert_eq!(spec.symbol, "BTC/USD");
                assert_eq!(spec.kind, EventKindCfg::Trade);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn ws_snapshot_and_close_and_unsubscribe_parse() {
        assert!(matches!(
            serde_json::from_str::<WsRequest>(r#"{"id":1,"op":"snapshot"}"#).unwrap(),
            WsRequest::Snapshot { id: 1 }
        ));
        assert!(matches!(
            serde_json::from_str::<WsRequest>(r#"{"id":2,"op":"close-client"}"#).unwrap(),
            WsRequest::CloseClient { id: 2 }
        ));
        let u = serde_json::from_str::<WsRequest>(
            r#"{"id":3,"op":"unsubscribe","provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
        )
        .unwrap();
        assert!(matches!(u, WsRequest::Unsubscribe { id: 3, .. }));
    }

    #[test]
    fn ws_reply_serialization_omits_empty_fields_and_carries_id() {
        let ok = serde_json::to_value(WsReply::ok(5)).unwrap();
        assert_eq!(ok["id"], 5);
        assert_eq!(ok["ok"], serde_json::Value::Bool(true));
        assert!(ok.get("code").is_none());
        assert!(ok.get("snapshot").is_none());

        let err = serde_json::to_value(WsReply::error(6, "bad_request", "nope")).unwrap();
        assert_eq!(err["id"], 6);
        assert_eq!(err["ok"], serde_json::Value::Bool(false));
        assert_eq!(err["code"], "bad_request");
    }

    #[test]
    fn ws_instruments_parses_and_carries_id() {
        let req: WsRequest =
            serde_json::from_str(r#"{"id":4,"op":"instruments","provider":"alpaca-crypto"}"#)
                .unwrap();
        assert!(
            matches!(&req, WsRequest::Instruments { id: 4, provider: Some(p) } if p == "alpaca-crypto")
        );
        let all: WsRequest = serde_json::from_str(r#"{"id":5,"op":"instruments"}"#).unwrap();
        assert!(matches!(
            all,
            WsRequest::Instruments {
                id: 5,
                provider: None
            }
        ));
    }

    #[test]
    fn ws_capabilities_parses_and_carries_id() {
        let req: WsRequest = serde_json::from_str(
            r#"{"id":9,"op":"capabilities","provider":"alpaca","symbols":["AAPL","MSFT"]}"#,
        )
        .unwrap();
        assert!(matches!(
            &req,
            WsRequest::Capabilities { id: 9, provider, symbols }
                if provider == "alpaca" && symbols == &["AAPL", "MSFT"]
        ));
        assert_eq!(req.id(), 9);
    }

    #[test]
    fn ws_watch_health_parses_and_carries_id() {
        let req: WsRequest = serde_json::from_str(r#"{"id":7,"op":"watch-health"}"#).unwrap();
        assert!(matches!(&req, WsRequest::WatchHealth { id: 7 }));
        assert_eq!(req.id(), 7);
    }

    #[test]
    fn ws_health_push_round_trips_and_is_disjoint_from_reply() {
        let view: datamancer_core::HealthView = serde_json::from_str(
            r#"{"schema_version":2,"daemon":{"version":"1.0.0","credential_backend":null,"captured_at":0},"providers":[],"streams":[]}"#,
        )
        .expect("HealthView fixture");
        let push = WsHealthPush { view: view.clone() };
        let line = serde_json::to_string(&push).unwrap();
        let back: WsHealthPush = serde_json::from_str(&line).unwrap();
        assert_eq!(back.view, view);
        // No `id`/`ok`, so a health push can never be mistaken for a reply by
        // the client's inbound demux (locks the disjointness invariant).
        assert!(serde_json::from_str::<WsReply>(&line).is_err());
    }

    #[test]
    fn ws_capabilities_reply_round_trips() {
        use datamancer_core::{AssetClass, Instrument, InstrumentEntry, ProviderId};
        let entries = vec![InstrumentEntry::bare(Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "AAPL",
        ))];
        let reply = WsReply::capabilities(9, entries.clone());
        let back: WsReply = serde_json::from_str(&serde_json::to_string(&reply).unwrap()).unwrap();
        assert_eq!(back.capabilities, Some(entries));
        assert!(back.ok);
        assert_eq!(back.id, 9);
    }

    #[test]
    fn ws_control_vocabulary_shares_subscription_spec_with_uds() {
        // The same subscribe spec body parses under both control surfaces,
        // guarding the "one control vocabulary" claim.
        let spec_json = r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#;
        let uds: crate::spec::SubscriptionSpec = serde_json::from_str(spec_json).unwrap();
        let ws_line = format!(
            r#"{{"id":1,"op":"subscribe",{}}}"#,
            &spec_json[1..spec_json.len() - 1]
        );
        let ws: WsRequest = serde_json::from_str(&ws_line).unwrap();
        match ws {
            WsRequest::Subscribe { spec, .. } => assert_eq!(spec, uds),
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
