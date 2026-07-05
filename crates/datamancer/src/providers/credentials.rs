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
    pub(crate) fn watch(&self) -> Option<tokio::sync::watch::Receiver<Option<AlpacaCredentials>>> {
        match self {
            Self::Watch(rx) => Some(rx.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AlpacaCredentials, CredentialsSource, Resolved};

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
