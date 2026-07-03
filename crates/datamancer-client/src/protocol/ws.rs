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
}

impl WsRequest {
    /// The correlation id carried by every request.
    #[must_use]
    pub fn id(&self) -> u64 {
        match self {
            Self::Subscribe { id, .. }
            | Self::Unsubscribe { id, .. }
            | Self::Snapshot { id }
            | Self::CloseClient { id } => *id,
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
}

impl WsReply {
    #[must_use]
    pub fn ok(id: u64) -> Self {
        Self { id, ok: true, code: None, message: None, snapshot: None }
    }

    #[must_use]
    pub fn error(id: u64, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { id, ok: false, code: Some(code.into()), message: Some(message.into()), snapshot: None }
    }

    #[must_use]
    pub fn snapshot(id: u64, snapshot: SystemSnapshot) -> Self {
        Self { id, ok: true, code: None, message: None, snapshot: Some(snapshot) }
    }
}

#[cfg(test)]
mod tests {
    use super::{WsReply, WsRequest};
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
    fn ws_control_vocabulary_shares_subscription_spec_with_uds() {
        // The same subscribe spec body parses under both control surfaces,
        // guarding the "one control vocabulary" claim.
        let spec_json = r#"{"provider":"alpaca-crypto","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#;
        let uds: crate::spec::SubscriptionSpec = serde_json::from_str(spec_json).unwrap();
        let ws_line = format!(r#"{{"id":1,"op":"subscribe",{}}}"#, &spec_json[1..spec_json.len() - 1]);
        let ws: WsRequest = serde_json::from_str(&ws_line).unwrap();
        match ws {
            WsRequest::Subscribe { spec, .. } => assert_eq!(spec, uds),
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
