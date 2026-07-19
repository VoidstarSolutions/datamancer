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

/// An error reply derived from a library error (stable code + display).
#[must_use]
pub fn reply_from_library_error(err: &datamancer::Error) -> Reply {
    Reply::error(error_code(err), err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

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
