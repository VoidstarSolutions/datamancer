//! The control-surface protocol: newline-delimited JSON over a Unix-domain
//! socket.
//!
//! The wire vocabulary (`Request`/`Reply`, `SubscriptionSpec`, the stable
//! error `codes` table) lives in `datamancer-client` so other client
//! libraries can share it; this module re-exports it under its historical
//! path plus the daemon-side glue that needs the orchestrator's `datamancer`
//! crate (mapping a library error to a stable code, building an error
//! `Reply`).

pub use datamancer_client::codes;
pub use datamancer_client::protocol::uds::{Reply, Request};
pub use datamancer_client::spec::SubscriptionSpec;

use datamancer::{Scope, SubscribeOutcome, Timestamp};
use datamancer_client::spec::{ScopeCfg, Subscribed};

/// The wall clock as a [`Timestamp`]: the live edge a backfill would end at
/// if it started now. The `[[startup_session]]` check at boot compares a
/// configured `backfill_from` against it; a subscribe is checked by the
/// library itself ([`datamancer::Error::BackfillFromNotInPast`]).
#[must_use]
pub fn live_edge_now() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
    Timestamp(nanos)
}

/// The live [`Scope`] a subscription spec asks for. The `scope` /
/// `backfill_from` pair must agree, the same rule `[[startup_session]]`
/// enforces at boot: `live_backfill` needs a `backfill_from`, and a
/// `backfill_from` under `live` is refused rather than ignored, so a request
/// can never be silently downgraded to pure-live (#89). Whether the start
/// lies before the live edge is the library's rule, not the daemon's: the
/// subscribe answers [`datamancer::Error::BackfillFromNotInPast`], which
/// [`error_code`] maps to `bad_request`. The error here is the `bad_request`
/// message for a disagreeing pair.
///
/// # Errors
///
/// A `bad_request` message when the pair disagrees.
pub fn requested_scope(spec: &SubscriptionSpec) -> Result<Scope, String> {
    match (spec.scope, spec.backfill_from) {
        (ScopeCfg::Live, None) => Ok(Scope::Live {
            backfill_from: None,
        }),
        (ScopeCfg::LiveBackfill, Some(from)) => Ok(Scope::Live {
            backfill_from: Some(from),
        }),
        (ScopeCfg::LiveBackfill, None) => {
            Err("scope live_backfill requires backfill_from".to_string())
        }
        (ScopeCfg::Live, Some(_)) => Err("backfill_from requires scope live_backfill".to_string()),
    }
}

/// The wire form of what a subscription actually opened, for the `subscribe`
/// reply. A client subscription is never historical
/// (`ClientSession::subscribe` rejects that scope before it reaches here);
/// the arm exists only because `Scope` is exhaustive.
#[must_use]
pub fn actual_scope(outcome: SubscribeOutcome) -> Subscribed {
    let actual = match outcome.scope {
        Scope::Live {
            backfill_from: Some(from),
        } => Subscribed::live_backfill(from),
        Scope::Live {
            backfill_from: None,
        } => Subscribed::live(),
        Scope::Historical { .. } => {
            debug_assert!(false, "a client subscription is never historical");
            Subscribed::live()
        }
    };
    actual.with_joined(outcome.joined)
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
        // The request itself is malformed (a backfill with nothing to fetch),
        // the same answer the daemon gives a disagreeing scope pair.
        Error::BackfillFromNotInPast { .. } => codes::BAD_REQUEST,
        Error::DuplicateSubscription { .. } => codes::DUPLICATE_SUBSCRIPTION,
        Error::NotSubscribed { .. } => codes::NOT_SUBSCRIBED,
        Error::UnknownProvider(_) => codes::UNKNOWN_PROVIDER,
        Error::SessionClosed => codes::SESSION_CLOSED,
        Error::EventsAlreadyTaken => codes::EVENTS_ALREADY_TAKEN,
        Error::Storage(_) => codes::STORAGE,
        Error::Config(_) => codes::CONFIG,
        Error::Provider { .. } | Error::Unauthenticated { .. } | Error::Io(_) => codes::PROVIDER,
        // `Error` is `#[non_exhaustive]`; any future variant maps to internal.
        _ => codes::INTERNAL,
    }
}

/// An error reply derived from a library error (stable code + display).
#[must_use]
pub fn reply_from_library_error(err: &datamancer::Error) -> Reply {
    Reply::error(error_code(err), err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(scope: ScopeCfg, backfill_from: Option<datamancer::Timestamp>) -> SubscriptionSpec {
        SubscriptionSpec {
            provider: "alpaca".to_string(),
            asset_class: datamancer_client::spec::AssetClassCfg::Equity,
            symbol: "AAPL".to_string(),
            kind: datamancer_client::spec::EventKindCfg::Trade,
            scope,
            backfill_from,
            persistence: datamancer_client::spec::PersistenceCfg::None,
        }
    }

    fn outcome(scope: Scope, joined: bool) -> SubscribeOutcome {
        SubscribeOutcome { scope, joined }
    }

    /// The `scope`/`backfill_from` pair must agree; a disagreeing pair is
    /// refused rather than quietly downgraded to pure-live (#89).
    #[test]
    fn requested_scope_requires_the_pair_to_agree() {
        assert_eq!(
            requested_scope(&spec(ScopeCfg::Live, None)),
            Ok(Scope::Live {
                backfill_from: None
            })
        );
        assert_eq!(
            requested_scope(&spec(ScopeCfg::LiveBackfill, Some(Timestamp(7)))),
            Ok(Scope::Live {
                backfill_from: Some(Timestamp(7))
            })
        );
        let missing = requested_scope(&spec(ScopeCfg::LiveBackfill, None)).unwrap_err();
        assert!(missing.contains("requires backfill_from"), "{missing}");
        let stray = requested_scope(&spec(ScopeCfg::Live, Some(Timestamp(7)))).unwrap_err();
        assert!(stray.contains("requires scope live_backfill"), "{stray}");
    }

    #[test]
    fn actual_scope_maps_both_live_shapes_and_joined_to_the_wire() {
        assert_eq!(
            actual_scope(outcome(
                Scope::Live {
                    backfill_from: None
                },
                false
            )),
            Subscribed::live()
        );
        assert_eq!(
            actual_scope(outcome(
                Scope::Live {
                    backfill_from: Some(Timestamp(7))
                },
                true
            )),
            Subscribed::live_backfill(Timestamp(7)).with_joined(true)
        );
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
                surface: datamancer::Surface::History,
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
            error_code(&Error::BackfillFromNotInPast {
                from: datamancer::Timestamp(2),
                edge: datamancer::Timestamp(1),
            }),
            codes::BAD_REQUEST,
            "a backfill with nothing to fetch is a malformed request"
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
