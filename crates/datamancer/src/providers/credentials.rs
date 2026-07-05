//! Injectable credential sources for the Alpaca providers (spec 2026-07-05,
//! cycle 2). This is the hot-apply seam: the daemon (or an embedder) hands a
//! provider a `Watch` source and rotates credentials by sending on the
//! channel — the provider reconnects with the new value. `Env` preserves the
//! legacy env-var path (`ALPACA_{PAPER,LIVE}_API_*`), which is deprecated
//! for daemon use but remains the embedder default.

use oxidized_alpaca::ApiKey;

/// An Alpaca key-id/secret pair.
#[derive(Clone, PartialEq, Eq)]
pub struct AlpacaCredentials {
    /// The Alpaca API key id (`APCA-API-KEY-ID`).
    pub key_id: String,
    /// The Alpaca API secret key (`APCA-API-SECRET-KEY`). Redacted from
    /// `Debug` output.
    pub secret: String,
}

impl AlpacaCredentials {
    pub(crate) fn to_api_key(&self) -> ApiKey {
        ApiKey::new(self.key_id.clone(), self.secret.clone())
    }
}

impl std::fmt::Debug for AlpacaCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaCredentials")
            .field("key_id", &self.key_id)
            .field("secret", &"********")
            .finish()
    }
}

/// Where a provider gets its credentials, resolved fresh at every
/// (re)connect.
#[derive(Clone, Debug, Default)]
pub enum CredentialsSource {
    /// Legacy: `oxidized_alpaca` loads from the `ALPACA_*` env vars selected
    /// by `account_type`. The embedder default; deprecated in `datamancerd`
    /// (the daemon warns and prefers the credential store).
    #[default]
    Env,
    /// Fixed explicit credentials.
    Static(AlpacaCredentials),
    /// Live-updatable source (the daemon's broker path). `None` = no
    /// credentials provisioned yet: REST calls fail unavailable and the
    /// streaming task waits for `Some` instead of hammering bad auth.
    Watch(tokio::sync::watch::Receiver<Option<AlpacaCredentials>>),
}

/// One resolution of a [`CredentialsSource`].
#[derive(Debug, Clone)]
pub(crate) enum Resolved {
    /// Use the env-var constructor path.
    Env,
    /// Use `new_with_credentials` with these.
    Creds(AlpacaCredentials),
    /// A `Watch` source with no value yet.
    Missing,
}

impl CredentialsSource {
    pub(crate) fn current(&self) -> Resolved {
        match self {
            Self::Env => Resolved::Env,
            Self::Static(c) => Resolved::Creds(c.clone()),
            Self::Watch(rx) => match rx.borrow().as_ref() {
                Some(c) => Resolved::Creds(c.clone()),
                None => Resolved::Missing,
            },
        }
    }

    /// The watch receiver, when this source is watchable (the streaming
    /// loops select on it for hot reconnect).
    ///
    /// The clone is returned with the current value marked seen: tokio's
    /// `Receiver::clone` copies the *original* receiver's seen version, and
    /// the stored receiver never advances (`current()` uses `borrow`), so
    /// without `mark_unchanged` every clone handed out after the first
    /// rotation would report `has_changed` immediately — turning the
    /// per-connect clones in the streaming loops into an unbounded
    /// reconnect storm.
    pub(crate) fn watch(&self) -> Option<tokio::sync::watch::Receiver<Option<AlpacaCredentials>>> {
        match self {
            Self::Watch(rx) => {
                let mut rx = rx.clone();
                rx.mark_unchanged();
                Some(rx)
            }
            _ => None,
        }
    }
}

/// Whether the cached REST-side receiver has an unseen rotation, consuming
/// the change marker. Shared by both providers' rebuild-on-use guards.
///
/// On a closed channel (sender dropped) tokio's `has_changed` returns `Err`
/// even when a final unseen rotation is pending, so `Err` counts as changed
/// — the caller rebuilds once with the last value — and the receiver is
/// dropped so subsequent calls return `false` instead of rebuilding forever.
pub(crate) fn rest_credentials_changed(
    cred_rx: &mut Option<tokio::sync::watch::Receiver<Option<AlpacaCredentials>>>,
) -> bool {
    let Some(rx) = cred_rx.as_mut() else {
        return false;
    };
    match rx.has_changed() {
        Ok(true) => {
            let _ = rx.borrow_and_update();
            true
        }
        Ok(false) => false,
        Err(_) => {
            *cred_rx = None;
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AlpacaCredentials, CredentialsSource, Resolved, rest_credentials_changed};

    fn creds(key: &str) -> AlpacaCredentials {
        AlpacaCredentials {
            key_id: key.to_string(),
            secret: "s3cret".to_string(),
        }
    }

    #[test]
    fn debug_redacts_the_secret() {
        let debug = format!("{:?}", creds("AKID"));
        assert!(!debug.contains("s3cret"), "secret leaked: {debug}");
        assert!(debug.contains("AKID"));
    }

    #[test]
    fn env_source_resolves_to_env() {
        assert!(matches!(CredentialsSource::Env.current(), Resolved::Env));
    }

    #[test]
    fn static_source_resolves_to_its_credentials() {
        match CredentialsSource::Static(creds("A")).current() {
            Resolved::Creds(c) => assert_eq!(c.key_id, "A"),
            other => panic!("expected Creds, got {other:?}"),
        }
    }

    #[test]
    fn watch_clone_does_not_see_pre_clone_sends() {
        // tokio's `Receiver::clone` copies the *original* receiver's seen
        // version, and `current()` never advances it (it uses `borrow`).
        // Without `mark_unchanged` on the clone, every receiver handed out
        // after the first rotation would report `has_changed` immediately —
        // the reconnect-storm bug.
        let (tx, rx) = tokio::sync::watch::channel(None);
        let source = CredentialsSource::Watch(rx);
        tx.send(Some(creds("A"))).unwrap();
        let fresh = source.watch().expect("watchable");
        assert_eq!(fresh.has_changed().ok(), Some(false));
    }

    #[test]
    fn watch_clone_sees_post_clone_sends() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let source = CredentialsSource::Watch(rx);
        let fresh = source.watch().expect("watchable");
        tx.send(Some(creds("A"))).unwrap();
        assert_eq!(fresh.has_changed().ok(), Some(true));
    }

    #[test]
    fn rest_change_detection_consumes_the_marker() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let source = CredentialsSource::Watch(rx);
        let mut cached = source.watch();
        assert!(!rest_credentials_changed(&mut cached));
        tx.send(Some(creds("A"))).unwrap();
        assert!(rest_credentials_changed(&mut cached));
        // Marker consumed: the same rotation must not rebuild twice.
        assert!(!rest_credentials_changed(&mut cached));
    }

    #[test]
    fn rest_change_detection_syncs_once_on_closed_channel() {
        // A final rotation delivered just before the sender drops must
        // still be picked up (tokio reports `Err` from `has_changed` on a
        // closed channel even when an unseen value is pending) — but
        // exactly once, not on every subsequent call.
        let (tx, rx) = tokio::sync::watch::channel(None);
        let source = CredentialsSource::Watch(rx);
        let mut cached = source.watch();
        tx.send(Some(creds("FINAL"))).unwrap();
        drop(tx);
        assert!(rest_credentials_changed(&mut cached));
        assert!(!rest_credentials_changed(&mut cached));
        assert!(!rest_credentials_changed(&mut cached));
    }

    #[test]
    fn rest_change_detection_ignores_non_watch_sources() {
        let mut cached = CredentialsSource::Env.watch();
        assert!(!rest_credentials_changed(&mut cached));
    }

    #[test]
    fn watch_source_tracks_updates_and_none_is_missing() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let source = CredentialsSource::Watch(rx);
        assert!(matches!(source.current(), Resolved::Missing));
        tx.send(Some(creds("B"))).unwrap();
        match source.current() {
            Resolved::Creds(c) => assert_eq!(c.key_id, "B"),
            other => panic!("expected Creds, got {other:?}"),
        }
    }
}
