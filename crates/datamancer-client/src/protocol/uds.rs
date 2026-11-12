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

use datamancer_core::{HealthView, Subscribed, SystemSnapshot};
use serde::{Deserialize, Serialize};

use super::SubscribedFields;
use crate::spec::{QueryId, QuerySpec, SubscriptionSpec, UnsubscribeSpec};

/// A control request (one per line).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum Request {
    /// Register a client and create its per-client data-plane service.
    OpenClient {
        client: String,
        /// Live-only subscriptions, made before the reply.
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
    /// Tear down a client (graceful). The reply comes when the teardown starts;
    /// the client counts toward `max_clients` until it ends.
    CloseClient { client: String },
    /// Open a bounded historical query. Ungated (a read, like `snapshot`).
    /// Not a subscription: it needs no prior `open-client`, opens its own
    /// short-lived data service, and ends with a terminal `SessionClosing`.
    OpenQuery {
        #[serde(flatten)]
        spec: QuerySpec,
    },
    /// Cancel an in-flight query: its fetch stops, its data stream ends, and
    /// the daemon releases its data service and `max_queries` slot at once.
    /// Answering `unknown_query` means the query already finished or was
    /// already cancelled.
    CancelQuery { query: QueryId },
    /// All in-flight query ids, daemon-wide (the `list-clients` analogue).
    ListQueries,
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
    /// On-demand per-instrument capabilities for a named provider's symbols.
    Capabilities {
        provider: String,
        #[serde(default)]
        symbols: Vec<String>,
    },
    /// Liveness/version probe. Answerable before `open-client`; used by the
    /// app facade for spawn-readiness and version-skew detection.
    Ping,
    /// Store (create or rotate) credentials for a configured provider.
    /// UDS-only, peer-cred gated; a configured provider hot-applies.
    SetCredentials {
        provider: String,
        credentials: datamancer_core::ProviderCredentials,
    },
    /// Read the stored credentials (the trade app reuses the same keys for
    /// its own trading connections — the daemon is the one copy).
    GetCredentials { provider: String },
    /// Remove stored credentials. The running provider keeps its last
    /// applied credentials until restart (there is no un-apply).
    ClearCredentials { provider: String },
    /// Return the daemon's current config (TOML as JSON) plus whether it
    /// takes a restart to fully apply: a cold-classified field differs from
    /// the boot-applied config, or an `always_on` startup session skipped at
    /// boot has its provider enabled and stays unopened.
    GetConfig,
    /// Enable (or re-configure) a compiled-in provider. `settings` is the
    /// provider's config-section shape (e.g. `{"account_type":"live"}`);
    /// unknown fields are rejected with `unknown_config_field`. UDS-only,
    /// peer-cred gated; applies live and persists atomically.
    ConfigureProvider {
        provider: String,
        #[serde(default)]
        settings: serde_json::Value,
    },
    /// Disable a compiled-in provider (its section is removed from the
    /// persisted config; stored credentials are untouched). UDS-only,
    /// peer-cred gated; applies live.
    RemoveProvider { provider: String },
    /// Graceful, deliberate daemon stop (the full drain path). UDS-only,
    /// peer-cred gated.
    Shutdown,
    /// The app-facing `HealthView`, reduced and stamped daemon-side; ungated
    /// read-only op.
    Health,
}

/// A control reply (one per line). `ok` discriminates success from error; the
/// remaining fields are populated per op.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Reply {
    pub ok: bool,
    /// The per-client data-plane service name (on `open-client`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// Connected client names (on `list-clients`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients: Option<Vec<String>>,
    /// The id of a newly opened query (on `open-query`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<QueryId>,
    /// In-flight query ids (on `list-queries`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queries: Option<Vec<QueryId>>,
    /// The diagnostics snapshot (on `snapshot`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SystemSnapshot>,
    /// The instrument catalog (on `instruments`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruments: Option<Vec<datamancer_core::InstrumentInfo>>,
    /// The per-instrument capabilities (on `capabilities`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<datamancer_core::InstrumentEntry>>,
    /// The daemon's crate version (on `ping`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Stored credentials (on `get-credentials`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<datamancer_core::ProviderCredentials>,
    /// The daemon's active credential-store backend (on `ping`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_backend: Option<String>,
    /// Stable error code (on failure).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Human-readable error detail (on failure).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The daemon config (on `get-config`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
    /// Whether the daemon's config takes a restart to fully apply: a
    /// cold-classified field differs from the boot config, or an `always_on`
    /// startup session skipped at boot has its provider enabled and stays
    /// unopened (on `get-config`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_required: Option<bool>,
    /// Applied scope for a mutating config op (on config mutations).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied: Option<String>,
    /// The app-facing health view (on `health`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<HealthView>,
    /// What the subscription opened (on `subscribe`).
    #[serde(flatten)]
    pub(crate) subscribed: SubscribedFields,
}

impl Reply {
    /// A bare success.
    #[must_use]
    pub fn ok() -> Self {
        Self {
            ok: true,
            service: None,
            clients: None,
            query: None,
            queries: None,
            snapshot: None,
            instruments: None,
            capabilities: None,
            version: None,
            credentials: None,
            credential_backend: None,
            code: None,
            message: None,
            config: None,
            restart_required: None,
            applied: None,
            health: None,
            subscribed: SubscribedFields::default(),
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

    /// Success carrying a newly opened query's id and its data-service name.
    #[must_use]
    pub fn query(id: QueryId, service: impl Into<String>) -> Self {
        Self {
            query: Some(id),
            service: Some(service.into()),
            ..Self::ok()
        }
    }

    /// Success carrying the in-flight query list.
    #[must_use]
    pub fn queries(ids: Vec<QueryId>) -> Self {
        Self {
            queries: Some(ids),
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

    /// Success carrying per-instrument capabilities.
    #[must_use]
    pub fn capabilities(entries: Vec<datamancer_core::InstrumentEntry>) -> Self {
        Self {
            capabilities: Some(entries),
            ..Self::ok()
        }
    }

    /// Success carrying stored credentials (on `get-credentials`).
    #[must_use]
    pub fn credentials(creds: datamancer_core::ProviderCredentials) -> Self {
        Self {
            credentials: Some(creds),
            ..Self::ok()
        }
    }

    /// Success carrying the daemon version and active credential backend
    /// (on `ping`).
    #[must_use]
    pub fn pong(version: impl Into<String>, credential_backend: impl Into<String>) -> Self {
        Self {
            version: Some(version.into()),
            credential_backend: Some(credential_backend.into()),
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
            query: None,
            queries: None,
            snapshot: None,
            instruments: None,
            capabilities: None,
            version: None,
            credentials: None,
            credential_backend: None,
            code: Some(code.into()),
            message: Some(message.into()),
            config: None,
            restart_required: None,
            applied: None,
            health: None,
            subscribed: SubscribedFields::default(),
        }
    }

    /// Success carrying the daemon config and whether it takes a restart to
    /// fully apply (on `get-config`).
    #[must_use]
    pub fn config(config: serde_json::Value, restart_required: bool) -> Self {
        Self {
            config: Some(config),
            restart_required: Some(restart_required),
            ..Self::ok()
        }
    }

    /// Success for a mutating config op that applied to the running daemon.
    #[must_use]
    pub fn applied_live() -> Self {
        Self {
            applied: Some("live".to_string()),
            ..Self::ok()
        }
    }

    /// Success for a mutating config op that applied to the running daemon,
    /// where the resulting config takes a restart to fully apply.
    #[must_use]
    pub fn applied_restart_required() -> Self {
        Self {
            applied: Some("restart_required".to_string()),
            ..Self::ok()
        }
    }

    /// A successful `health` reply.
    #[must_use]
    pub fn health(view: HealthView) -> Self {
        Self {
            health: Some(view),
            ..Self::ok()
        }
    }

    /// A successful `subscribe` reply.
    #[must_use]
    pub fn from_subscribed(subscribed: Subscribed) -> Self {
        Self {
            subscribed: subscribed.into(),
            ..Self::ok()
        }
    }

    /// What a successful `subscribe` reply says the subscription opened. Check
    /// `ok` first: on an error reply, or a reply to another op, this reads as a
    /// created pure-live session and means nothing.
    #[must_use]
    pub fn subscribed(&self) -> Subscribed {
        Subscribed::from(&self.subscribed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{AssetClassCfg, EventKindCfg, PersistenceCfg};
    use datamancer_core::{LiveScope, Timestamp};

    #[test]
    fn control_protocol_roundtrip_open_client() {
        let req = Request::OpenClient {
            client: "exec-1".to_string(),
            subscriptions: vec![SubscriptionSpec {
                provider: "alpaca-crypto".to_string(),
                asset_class: AssetClassCfg::Crypto,
                symbol: "BTC/USD".to_string(),
                kind: EventKindCfg::Trade,
                scope: LiveScope::default(),
                persistence: PersistenceCfg::CachedWithTap,
            }],
        };
        let line = serde_json::to_string(&req).expect("ser");
        let back: Request = serde_json::from_str(&line).expect("de");
        assert_eq!(req, back);
    }

    /// The documented backfill subscribe parses with its timestamp, and the
    /// reply that answers it round-trips the actual scope.
    #[test]
    fn control_protocol_parses_backfill_subscribe_and_reply_carries_scope() {
        let line = r#"{"op":"subscribe","client":"exec-1","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1m","scope":"live_backfill","backfill_from":1704067200000000000,"persistence":"cached"}"#;
        let req: Request = serde_json::from_str(line).expect("de");
        let Request::Subscribe { spec, .. } = req else {
            panic!("wrong variant");
        };
        let from = Timestamp(1_704_067_200_000_000_000);
        let backfill = LiveScope::backfill(from);
        assert_eq!(spec.scope, backfill);

        let reply = Reply::from_subscribed(Subscribed::created(backfill));
        let json = serde_json::to_string(&reply).expect("ser");
        assert_eq!(
            json,
            r#"{"ok":true,"scope":"live_backfill","backfill_from":1704067200000000000}"#
        );
        let back: Reply = serde_json::from_str(&json).expect("de");
        assert_eq!(back.subscribed(), Subscribed::created(backfill));
        assert_eq!(
            serde_json::to_string(&Reply::from_subscribed(Subscribed::created(
                LiveScope::default()
            )))
            .expect("ser"),
            r#"{"ok":true,"scope":"live"}"#
        );
        let joined = Subscribed::joined(LiveScope::default());
        let json = serde_json::to_string(&Reply::from_subscribed(joined)).expect("ser");
        assert_eq!(json, r#"{"ok":true,"scope":"live","joined":true}"#);
        let back: Reply = serde_json::from_str(&json).expect("de");
        assert_eq!(back.subscribed(), joined);
        let unseeded = Subscribed::created(LiveScope::unseeded());
        let json = serde_json::to_string(&Reply::from_subscribed(unseeded)).expect("ser");
        assert_eq!(json, r#"{"ok":true,"scope":"live","seed":false}"#);
        let back: Reply = serde_json::from_str(&json).expect("de");
        assert_eq!(back.subscribed(), unseeded);
    }

    fn aapl_subscribe(spec: impl FnOnce(SubscriptionSpec) -> SubscriptionSpec) -> String {
        use datamancer_core::{AssetClass, EventKind, Instrument, ProviderId};
        let aapl = Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "AAPL",
        );
        let spec = spec(SubscriptionSpec::new(&aapl, EventKind::Trade).unwrap());
        serde_json::to_string(&Request::Subscribe {
            client: "exec-1".to_string(),
            spec,
        })
        .unwrap()
    }

    fn reply_for(scope: LiveScope) -> String {
        serde_json::to_string(&Reply::from_subscribed(Subscribed::created(scope))).unwrap()
    }

    #[test]
    fn a_seeded_live_scope_writes_no_seed_key() {
        let request = aapl_subscribe(|spec| spec);
        assert!(
            request.contains(r#""scope":"live","persistence""#),
            "{request}"
        );
        assert!(!request.contains("seed"), "{request}");
        assert_eq!(
            reply_for(LiveScope::default()),
            r#"{"ok":true,"scope":"live"}"#
        );
    }

    /// One rule on both sides of the wire: an absent `scope` means live,
    /// whatever `seed` says.
    #[test]
    fn a_seed_with_no_scope_reads_as_live_both_ways() {
        let unseeded = {
            let line = aapl_subscribe(SubscriptionSpec::without_seed);
            let Request::Subscribe { spec, .. } = serde_json::from_str(&line).unwrap() else {
                panic!("wrong variant");
            };
            spec.scope
        };
        for (seed, scope) in [("false", unseeded), ("true", LiveScope::default())] {
            let request = format!(
                r#"{{"op":"subscribe","client":"exec-1","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"trade","seed":{seed}}}"#
            );
            let Request::Subscribe { spec, .. } = serde_json::from_str(&request).unwrap() else {
                panic!("wrong variant");
            };
            assert_eq!(spec.scope, scope, "{request}");
            let reply = format!(r#"{{"ok":true,"seed":{seed}}}"#);
            let reply: Reply = serde_json::from_str(&reply).unwrap();
            assert_eq!(reply.subscribed(), Subscribed::created(scope));
        }
    }

    #[test]
    fn a_joiner_reply_carries_a_pending_seed() {
        let pending = Subscribed::joined(LiveScope::default()).with_seed_pending();
        let json = serde_json::to_string(&Reply::from_subscribed(pending)).unwrap();
        assert!(json.contains(r#""seed_pending":true"#), "{json}");
        let back: Reply = serde_json::from_str(&json).unwrap();
        assert_eq!(back.subscribed(), pending);
    }

    #[test]
    fn a_joiner_reply_carries_a_backfill_in_flight() {
        let in_flight = Subscribed::joined(LiveScope::default()).with_backfill_in_flight();
        let json = serde_json::to_string(&Reply::from_subscribed(in_flight)).unwrap();
        assert!(json.contains(r#""backfill_in_flight":true"#), "{json}");
        let back: Reply = serde_json::from_str(&json).unwrap();
        assert_eq!(back.subscribed(), in_flight);
    }

    #[test]
    fn a_subscribe_reply_with_no_scope_reads_as_created_live() {
        let reply: Reply = serde_json::from_str(r#"{"ok":true}"#).expect("de");
        assert_eq!(
            reply.subscribed(),
            Subscribed::created(LiveScope::default())
        );
        assert_eq!(
            serde_json::to_string(&Reply::ok()).expect("ser"),
            r#"{"ok":true}"#
        );
    }

    #[test]
    fn a_malformed_subscribe_reply_fails_to_parse() {
        for line in [
            r#"{"ok":true,"scope":"live_backfill"}"#,
            r#"{"ok":true,"scope":"live","backfill_from":5}"#,
            r#"{"ok":true,"backfill_from":5}"#,
            r#"{"ok":true,"scope":"live_backfill","backfill_from":5,"seed":true}"#,
            r#"{"ok":true,"scope":"warp"}"#,
        ] {
            assert!(serde_json::from_str::<Reply>(line).is_err(), "{line}");
        }
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
        let ok = serde_json::to_value(Reply::service("datamancerd/v3/data/3")).expect("ser");
        assert_eq!(ok["ok"], serde_json::Value::Bool(true));
        assert_eq!(ok["service"], "datamancerd/v3/data/3");
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
    fn ping_round_trips_and_reply_carries_version() {
        let req: Request = serde_json::from_str(r#"{"op":"ping"}"#).expect("de");
        assert!(matches!(req, Request::Ping));
        assert_eq!(
            serde_json::to_string(&Request::Ping).unwrap(),
            r#"{"op":"ping"}"#
        );

        let reply = serde_json::to_value(Reply::pong("0.1.0", "keychain")).expect("ser");
        assert_eq!(reply["ok"], serde_json::Value::Bool(true));
        assert_eq!(reply["version"], "0.1.0");
        assert!(reply.get("code").is_none());
    }

    #[test]
    fn credential_ops_round_trip_documented_wire_shapes() {
        use datamancer_core::ProviderCredentials;
        let set: Request = serde_json::from_str(
            r#"{"op":"set-credentials","provider":"alpaca","credentials":{"type":"api_key_pair","key_id":"AKID","secret":"s"}}"#,
        )
        .expect("de");
        match &set {
            Request::SetCredentials {
                provider,
                credentials,
            } => {
                assert_eq!(provider, "alpaca");
                assert!(matches!(
                    credentials,
                    ProviderCredentials::ApiKeyPair { .. }
                ));
            }
            other => panic!("wrong variant: {other:?}"),
        }
        assert_eq!(
            serde_json::to_string(&set).unwrap(),
            r#"{"op":"set-credentials","provider":"alpaca","credentials":{"type":"api_key_pair","key_id":"AKID","secret":"s"}}"#
        );
        let get: Request =
            serde_json::from_str(r#"{"op":"get-credentials","provider":"alpaca"}"#).unwrap();
        assert!(matches!(get, Request::GetCredentials { .. }));
        let clear: Request =
            serde_json::from_str(r#"{"op":"clear-credentials","provider":"alpaca"}"#).unwrap();
        assert!(matches!(clear, Request::ClearCredentials { .. }));
    }

    #[test]
    fn credentials_reply_and_backend_carrying_pong() {
        use datamancer_core::ProviderCredentials;
        let reply = serde_json::to_value(Reply::credentials(ProviderCredentials::ApiKeyPair {
            key_id: "AKID".to_string(),
            secret: "s".to_string(),
        }))
        .unwrap();
        assert_eq!(reply["ok"], serde_json::Value::Bool(true));
        assert_eq!(reply["credentials"]["type"], "api_key_pair");
        assert!(reply.get("version").is_none());

        let pong = serde_json::to_value(Reply::pong("0.3.0", "keychain")).unwrap();
        assert_eq!(pong["version"], "0.3.0");
        assert_eq!(pong["credential_backend"], "keychain");
        assert!(pong.get("credentials").is_none());
    }

    #[test]
    fn new_credential_codes_are_stable() {
        assert_eq!(crate::codes::CREDENTIALS_MISSING, "credentials_missing");
        assert_eq!(
            crate::codes::CREDENTIAL_BACKEND_UNAVAILABLE,
            "credential_backend_unavailable"
        );
        assert_eq!(crate::codes::PERMISSION_DENIED, "permission_denied");
    }

    #[test]
    fn instruments_reply_round_trips() {
        use datamancer_core::{AssetClass, EventKind, Instrument, InstrumentInfo, ProviderId};
        let reply = Reply::instruments(vec![InstrumentInfo::new(
            Instrument::new(
                ProviderId::from_static("alpaca-crypto"),
                AssetClass::Crypto,
                "BTC/USD",
            ),
            vec![EventKind::Trade],
            vec![],
        )]);
        let line = serde_json::to_string(&reply).unwrap();
        let back: Reply = serde_json::from_str(&line).unwrap();
        assert_eq!(reply, back);
        assert!(back.ok);
        assert_eq!(back.instruments.unwrap().len(), 1);
    }

    #[test]
    fn capabilities_request_parses() {
        let req: Request = serde_json::from_str(
            r#"{"op":"capabilities","provider":"alpaca","symbols":["AAPL","MSFT"]}"#,
        )
        .unwrap();
        assert!(matches!(
            &req,
            Request::Capabilities { provider, symbols }
                if provider == "alpaca" && symbols == &["AAPL", "MSFT"]
        ));
    }

    #[test]
    fn capabilities_reply_round_trips() {
        use datamancer_core::{AssetClass, Instrument, InstrumentEntry, ProviderId};
        let entries = vec![InstrumentEntry::bare(Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "AAPL",
        ))];
        let reply = Reply::capabilities(entries.clone());
        let back: Reply = serde_json::from_str(&serde_json::to_string(&reply).unwrap()).unwrap();
        assert_eq!(back.capabilities, Some(entries));
        assert!(back.ok);
    }

    #[test]
    fn config_ops_round_trip_documented_wire_shapes() {
        let get: Request = serde_json::from_str(r#"{"op":"get-config"}"#).expect("de");
        assert!(matches!(get, Request::GetConfig));

        let cfg: Request = serde_json::from_str(
            r#"{"op":"configure-provider","provider":"alpaca","settings":{"account_type":"live"}}"#,
        )
        .expect("de");
        match &cfg {
            Request::ConfigureProvider { provider, settings } => {
                assert_eq!(provider, "alpaca");
                assert_eq!(settings["account_type"], "live");
            }
            other => panic!("wrong variant: {other:?}"),
        }
        assert_eq!(
            serde_json::to_string(&cfg).unwrap(),
            r#"{"op":"configure-provider","provider":"alpaca","settings":{"account_type":"live"}}"#
        );

        let rm: Request =
            serde_json::from_str(r#"{"op":"remove-provider","provider":"alpaca"}"#).unwrap();
        assert!(matches!(rm, Request::RemoveProvider { .. }));

        let sd: Request = serde_json::from_str(r#"{"op":"shutdown"}"#).unwrap();
        assert!(matches!(sd, Request::Shutdown));
    }

    #[test]
    fn config_replies_carry_payloads_and_omit_when_absent() {
        let reply =
            serde_json::to_value(Reply::config(serde_json::json!({"provider": {}}), true)).unwrap();
        assert_eq!(reply["ok"], serde_json::Value::Bool(true));
        assert_eq!(reply["restart_required"], serde_json::Value::Bool(true));
        assert!(reply.get("applied").is_none());

        let applied = serde_json::to_value(Reply::applied_live()).unwrap();
        assert_eq!(applied["applied"], "live");
        assert!(applied.get("config").is_none());

        let restart = serde_json::to_value(Reply::applied_restart_required()).unwrap();
        assert_eq!(restart["applied"], "restart_required");

        let plain = serde_json::to_value(Reply::ok()).unwrap();
        assert!(plain.get("config").is_none());
        assert!(plain.get("restart_required").is_none());
    }

    #[test]
    fn new_config_codes_are_stable() {
        assert_eq!(crate::codes::RESTART_REQUIRED, "restart_required");
        assert_eq!(crate::codes::PROVIDER_DISABLED, "provider_disabled");
        assert_eq!(crate::codes::UNKNOWN_CONFIG_FIELD, "unknown_config_field");
        assert_eq!(crate::codes::BACKFILL_NOT_IN_PAST, "backfill_not_in_past");
        assert_eq!(crate::codes::BACKFILL_TOO_OLD, "backfill_too_old");
    }

    #[test]
    fn health_round_trips_and_reply_carries_view() {
        use datamancer_core::{CacheSnapshot, HealthView, Timestamp};

        let req: Request = serde_json::from_str(r#"{"op":"health"}"#).unwrap();
        assert!(matches!(req, Request::Health));
        let snap = SystemSnapshot::new(
            Timestamp(1),
            vec![],
            CacheSnapshot::new(vec![], None),
            vec![],
            vec![],
        );
        let view = HealthView::from_snapshot(&snap, HealthView::DEFAULT_QUIET_AFTER_NS);
        let line = serde_json::to_string(&Reply::health(view.clone())).unwrap();
        let back: Reply = serde_json::from_str(&line).unwrap();
        assert!(back.ok);
        assert_eq!(back.health, Some(view));
    }

    #[test]
    fn integrity_code_is_stable() {
        assert_eq!(crate::codes::INTEGRITY_REJECTED, "integrity_rejected");
    }

    #[test]
    fn unsupported_on_windows_code_is_stable() {
        assert_eq!(
            crate::codes::UNSUPPORTED_ON_WINDOWS,
            "unsupported_on_windows"
        );
    }

    #[test]
    fn open_query_parses_with_flattened_spec_fields() {
        let line = r#"{"op":"open-query","provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1d","from":100,"to":200}"#;
        let request: Request = serde_json::from_str(line).expect("parse");
        match request {
            Request::OpenQuery { spec } => {
                assert_eq!(spec.symbol, "AAPL");
                assert_eq!(spec.from, datamancer_core::Timestamp(100));
                assert_eq!(spec.to, datamancer_core::Timestamp(200));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn cancel_and_list_queries_parse() {
        let cancel: Request =
            serde_json::from_str(r#"{"op":"cancel-query","query":7}"#).expect("parse cancel");
        assert_eq!(cancel, Request::CancelQuery { query: 7 });
        let list: Request = serde_json::from_str(r#"{"op":"list-queries"}"#).expect("parse list");
        assert_eq!(list, Request::ListQueries);
    }

    #[test]
    fn query_reply_carries_id_and_service_and_omits_absent_fields() {
        let reply = Reply::query(7, "datamancerd/v3/data/7".to_string());
        let json = serde_json::to_string(&reply).expect("serialize");
        assert!(json.contains(r#""query":7"#), "got {json}");
        assert!(
            json.contains(r#""service":"datamancerd/v3/data/7""#),
            "got {json}"
        );
        assert!(
            !json.contains("queries"),
            "absent fields must not serialize: {json}"
        );
    }

    #[test]
    fn queries_reply_lists_ids() {
        let json = serde_json::to_string(&Reply::queries(vec![1, 4])).expect("serialize");
        assert!(json.contains(r#""queries":[1,4]"#), "got {json}");
    }

    #[test]
    fn a_bare_ok_reply_still_omits_the_query_fields() {
        let json = serde_json::to_string(&Reply::ok()).expect("serialize");
        assert!(!json.contains("query"), "got {json}");
    }
}
