//! Transport-crate error type.
//!
//! iceoryx2's builder/port errors are many small enums; this crate funnels them
//! into one stringly-typed [`TransportError`] at the crate boundary. The
//! [`EventSink`](datamancer_core::EventSink) `flush` contract returns the core
//! [`datamancer_core::Error`]; [`TransportError`] converts into it via
//! [`std::io::Error`] so no `datamancer-core` change is needed.

/// An error originating in the iceoryx2 transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// A service name was not a valid iceoryx2 `ServiceName`.
    BadServiceName(String),
    /// Creating or opening an iceoryx2 service/port failed.
    Service(String),
    /// The named service exists on the host but is marked for destruction and
    /// no live node references it: a previous owner was killed while removing
    /// it, and iceoryx2's dead-node cleanup never visits such a service.
    /// Opening it fails until the stale resources are removed (see the daemon
    /// README, "Recovering from a crashed predecessor").
    StaleService(String),
    /// Loaning or sending a sample failed.
    Send(String),
    /// An instrument could not be interned for transport (tuple too long).
    Interning(String),
    /// A `MarketEvent` variant this transport build cannot encode reached the
    /// sink (the core event model gained a data variant newer than this
    /// transport). Surfaced rather than silently acknowledged as delivered.
    Unsupported(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadServiceName(m) => write!(f, "invalid iceoryx2 service name: {m}"),
            Self::Service(m) => write!(f, "iceoryx2 service error: {m}"),
            Self::StaleService(name) => write!(
                f,
                "iceoryx2 service {name} is marked for destruction with no live owner \
                 (a previous owner was killed while removing it); it cannot be opened \
                 until its stale resources are removed"
            ),
            Self::Send(m) => write!(f, "iceoryx2 send error: {m}"),
            Self::Interning(m) => write!(f, "symbol interning error: {m}"),
            Self::Unsupported(m) => write!(f, "unsupported event for transport: {m}"),
        }
    }
}

impl std::error::Error for TransportError {}

/// Classify a publish-subscribe `open_or_create` failure on service `name`.
///
/// `IsMarkedForDestruction` and `SystemInFlux` are the two timeout exits of
/// the same retry loop in iceoryx2 0.10 (`open_or_create` waits for a service
/// marked for destruction to disappear); both mean a stale service with no
/// live owner is in the way and become [`TransportError::StaleService`].
/// Everything else keeps the debug text as [`TransportError::Service`].
pub(crate) fn open_or_create_error(
    name: &str,
    e: iceoryx2::service::builder::publish_subscribe::PublishSubscribeOpenOrCreateError,
) -> TransportError {
    use iceoryx2::service::builder::publish_subscribe::{
        PublishSubscribeOpenError, PublishSubscribeOpenOrCreateError,
    };
    match e {
        PublishSubscribeOpenOrCreateError::SystemInFlux
        | PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(
            PublishSubscribeOpenError::IsMarkedForDestruction,
        ) => TransportError::StaleService(name.to_string()),
        other => TransportError::Service(format!("{other:?}")),
    }
}

impl From<TransportError> for datamancer_core::Error {
    fn from(e: TransportError) -> Self {
        datamancer_core::Error::Io(std::io::Error::other(e.to_string()))
    }
}

/// Convenience alias for transport-crate results.
pub type Result<T, E = TransportError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::{TransportError, open_or_create_error};
    use iceoryx2::service::builder::publish_subscribe::{
        PublishSubscribeOpenError, PublishSubscribeOpenOrCreateError,
    };

    #[test]
    fn both_timeout_exits_of_open_or_create_classify_as_a_stale_service() {
        let stale = TransportError::StaleService("p/v3/data/0".to_string());
        assert_eq!(
            open_or_create_error(
                "p/v3/data/0",
                PublishSubscribeOpenOrCreateError::SystemInFlux
            ),
            stale
        );
        assert_eq!(
            open_or_create_error(
                "p/v3/data/0",
                PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(
                    PublishSubscribeOpenError::IsMarkedForDestruction
                )
            ),
            stale
        );
        assert!(stale.to_string().contains("p/v3/data/0"));
    }

    #[test]
    fn other_open_failures_keep_the_debug_text() {
        let e = open_or_create_error(
            "p/v3/data/0",
            PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(
                PublishSubscribeOpenError::IncompatibleTypes,
            ),
        );
        assert!(matches!(&e, TransportError::Service(m) if m.contains("IncompatibleTypes")));
    }
}
