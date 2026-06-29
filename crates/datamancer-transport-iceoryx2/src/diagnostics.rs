//! Diagnostics plane: publish the Phase-3 [`SystemSnapshot`] to consumer
//! processes.
//!
//! Deliberately **not** the zero-copy POD hot path: the snapshot is serialized
//! (JSON) into a fixed-capacity byte-slice payload (`publish_subscribe::<[u8]>`)
//! at a low cadence. Periodic publish with `history_size(1)` so a late joiner
//! immediately reads the current snapshot.
//!
//! Provider connectivity and last-error surface here (via
//! [`ProviderSnapshot`](datamancer_core::ProviderSnapshot)) because the
//! connection-scoped controls are suppressed on the data plane.
//!
//! The reconciliation pass splits sizing between a bounded live-state snapshot
//! and a heavier cache catalog; this phase serializes the whole
//! [`SystemSnapshot`] and returns a clean [`DiagnosticsError::Oversize`] if it
//! exceeds the payload cap (chunking is deferred — see the plan's open
//! questions), so the cap is never silently exceeded.

use datamancer_core::SystemSnapshot;

/// Maximum serialized snapshot size, in bytes, carried in one diagnostics
/// sample. Generous fixed cap; oversize is a clean error pending the deferred
/// chunking scheme.
pub const DIAGNOSTICS_PAYLOAD_CAPACITY: usize = 1024 * 1024;

/// Error encoding/decoding a diagnostics snapshot.
#[derive(Debug)]
pub enum DiagnosticsError {
    /// `serde` serialization or deserialization failed.
    Codec(serde_json::Error),
    /// The serialized snapshot exceeded [`DIAGNOSTICS_PAYLOAD_CAPACITY`].
    Oversize {
        /// The serialized length that exceeded the cap.
        len: usize,
    },
}

impl std::fmt::Display for DiagnosticsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Codec(e) => write!(f, "diagnostics codec error: {e}"),
            Self::Oversize { len } => write!(
                f,
                "serialized snapshot is {len} bytes, exceeds cap {DIAGNOSTICS_PAYLOAD_CAPACITY}"
            ),
        }
    }
}

impl std::error::Error for DiagnosticsError {}

/// Serialize a snapshot to the diagnostics wire bytes, enforcing the payload
/// cap.
///
/// # Errors
///
/// Returns [`DiagnosticsError::Codec`] on a serialization failure or
/// [`DiagnosticsError::Oversize`] if the result exceeds
/// [`DIAGNOSTICS_PAYLOAD_CAPACITY`].
pub fn encode_snapshot(snapshot: &SystemSnapshot) -> Result<Vec<u8>, DiagnosticsError> {
    let bytes = serde_json::to_vec(snapshot).map_err(DiagnosticsError::Codec)?;
    if bytes.len() > DIAGNOSTICS_PAYLOAD_CAPACITY {
        return Err(DiagnosticsError::Oversize { len: bytes.len() });
    }
    Ok(bytes)
}

/// Reconstruct a snapshot from diagnostics wire bytes.
///
/// # Errors
///
/// Returns [`DiagnosticsError::Codec`] if the bytes are not a valid serialized
/// snapshot.
pub fn decode_snapshot(bytes: &[u8]) -> Result<SystemSnapshot, DiagnosticsError> {
    serde_json::from_slice(bytes).map_err(DiagnosticsError::Codec)
}

pub use runtime::{Iceoryx2DiagnosticsPublisher, Iceoryx2DiagnosticsSubscriber};

mod runtime {
    use super::{DIAGNOSTICS_PAYLOAD_CAPACITY, decode_snapshot, encode_snapshot};
    use crate::error::{Result, TransportError};
    use datamancer_core::SystemSnapshot;
    use iceoryx2::port::publisher::Publisher;
    use iceoryx2::port::subscriber::Subscriber;
    use iceoryx2::prelude::{Node, ServiceName, ipc_threadsafe};

    const DIAGNOSTICS_SERVICE: &str = "datamancer/diagnostics";

    fn diagnostics_name() -> Result<ServiceName> {
        DIAGNOSTICS_SERVICE
            .try_into()
            .map_err(|e| TransportError::BadServiceName(format!("{e:?}")))
    }

    /// Single-instance diagnostics publisher (not per client). The daemon
    /// (Phase 5) drives its cadence from the existing tokio runtime — never a
    /// second runtime.
    pub struct Iceoryx2DiagnosticsPublisher {
        publisher: Publisher<ipc_threadsafe::Service, [u8], ()>,
    }

    impl Iceoryx2DiagnosticsPublisher {
        /// Create the diagnostics service and its byte-slice publisher on
        /// `node`, with `history_size(1)` for immediate late-joiner delivery.
        ///
        /// # Errors
        ///
        /// Returns [`TransportError`] if the service or publisher cannot be
        /// created.
        pub fn new(node: &Node<ipc_threadsafe::Service>) -> Result<Self> {
            let service = node
                .service_builder(&diagnostics_name()?)
                .publish_subscribe::<[u8]>()
                .history_size(1)
                .open_or_create()
                .map_err(|e| TransportError::Service(format!("{e:?}")))?;
            let publisher = service
                .publisher_builder()
                .initial_max_slice_len(DIAGNOSTICS_PAYLOAD_CAPACITY)
                .create()
                .map_err(|e| TransportError::Service(format!("{e:?}")))?;
            Ok(Self { publisher })
        }

        /// Serialize and publish one snapshot sample.
        ///
        /// # Errors
        ///
        /// Returns [`TransportError`] if encoding fails (including oversize) or
        /// the iceoryx2 loan/send fails.
        pub fn publish(&self, snapshot: &SystemSnapshot) -> Result<()> {
            let bytes =
                encode_snapshot(snapshot).map_err(|e| TransportError::Send(e.to_string()))?;
            let sample = self
                .publisher
                .loan_slice_uninit(bytes.len())
                .map_err(|e| TransportError::Send(format!("{e:?}")))?;
            let sample = sample.write_from_slice(&bytes);
            sample
                .send()
                .map_err(|e| TransportError::Send(format!("{e:?}")))?;
            Ok(())
        }
    }

    /// Subscriber-side diagnostics helper (test + future operator tooling).
    pub struct Iceoryx2DiagnosticsSubscriber {
        subscriber: Subscriber<ipc_threadsafe::Service, [u8], ()>,
    }

    impl Iceoryx2DiagnosticsSubscriber {
        /// Open the diagnostics service and a subscriber port on `node`.
        ///
        /// # Errors
        ///
        /// Returns [`TransportError`] if the service or subscriber cannot be
        /// created.
        pub fn open(node: &Node<ipc_threadsafe::Service>) -> Result<Self> {
            let service = node
                .service_builder(&diagnostics_name()?)
                .publish_subscribe::<[u8]>()
                .history_size(1)
                .open_or_create()
                .map_err(|e| TransportError::Service(format!("{e:?}")))?;
            let subscriber = service
                .subscriber_builder()
                .create()
                .map_err(|e| TransportError::Service(format!("{e:?}")))?;
            Ok(Self { subscriber })
        }

        /// Receive and decode the most recent snapshot, if one is available.
        ///
        /// # Errors
        ///
        /// Returns [`TransportError`] on a receive or decode failure.
        pub fn receive(&self) -> Result<Option<SystemSnapshot>> {
            let mut latest = None;
            while let Some(sample) = self
                .subscriber
                .receive()
                .map_err(|e| TransportError::Send(format!("{e:?}")))?
            {
                latest = Some(
                    decode_snapshot(&sample).map_err(|e| TransportError::Send(e.to_string()))?,
                );
            }
            Ok(latest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DIAGNOSTICS_PAYLOAD_CAPACITY, DiagnosticsError, decode_snapshot, encode_snapshot};
    use datamancer_core::{
        CacheSnapshot, ConnectionState, ProviderId, ProviderSnapshot, SystemSnapshot, Timestamp,
    };

    fn sample_snapshot(provider_count: usize) -> SystemSnapshot {
        let providers = (0..provider_count)
            .map(|i| {
                ProviderSnapshot::new(
                    ProviderId::new(format!("alpaca-{i}")),
                    ConnectionState::Connected,
                    1,
                    0,
                    1,
                    2,
                    0,
                    0,
                    99,
                    0,
                    Some("last error".to_string()),
                )
            })
            .collect();
        SystemSnapshot::new(
            Timestamp(1_700_000_000),
            providers,
            CacheSnapshot::new(vec![], None),
            vec![],
            vec![],
        )
    }

    #[test]
    fn diagnostics_snapshot_serde_round_trips() {
        let snapshot = sample_snapshot(3);
        let bytes = encode_snapshot(&snapshot).unwrap();
        assert!(bytes.len() <= DIAGNOSTICS_PAYLOAD_CAPACITY);
        let back = decode_snapshot(&bytes).unwrap();
        assert_eq!(snapshot, back);
    }

    #[test]
    fn provider_connectivity_survives_diagnostics_codec() {
        // The data plane suppresses connection controls; remote consumers learn
        // connectivity + last-error here instead.
        let snapshot = sample_snapshot(1);
        let back = decode_snapshot(&encode_snapshot(&snapshot).unwrap()).unwrap();
        assert_eq!(
            back.providers[0].connection_state,
            ConnectionState::Connected
        );
        assert_eq!(back.providers[0].last_error.as_deref(), Some("last error"));
    }

    #[test]
    fn oversize_snapshot_is_clean_error_not_panic() {
        // Force the payload past the cap with many providers, then assert the
        // documented error (chunking deferred).
        let snapshot = sample_snapshot(200_000);
        match encode_snapshot(&snapshot) {
            Err(DiagnosticsError::Oversize { len }) => {
                assert!(len > DIAGNOSTICS_PAYLOAD_CAPACITY);
            }
            other => panic!("expected Oversize, got {other:?}"),
        }
    }
}
