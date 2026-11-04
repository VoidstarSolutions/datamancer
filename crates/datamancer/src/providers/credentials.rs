//! Injectable credential sources for the Alpaca providers (spec 2026-07-05,
//! cycle 2). This is the hot-apply seam: the daemon (or an embedder) hands a
//! provider a `Watch` source and rotates credentials by sending on the
//! channel — the provider reconnects with the new value. `Env` preserves the
//! legacy env-var path (`ALPACA_{PAPER,LIVE}_API_*`), which is deprecated
//! for daemon use but remains the embedder default.

use oxidized_alpaca::{AccountType, ApiKey};

/// The `ALPACA_*` environment pair the legacy [`CredentialsSource::Env`]
/// path reads for an account type — the same names `oxidized_alpaca`'s own
/// env-var constructors use, so `Env` keeps its documented contract.
pub(crate) fn env_var_names(account_type: AccountType) -> (&'static str, &'static str) {
    match account_type {
        AccountType::Paper => ("ALPACA_PAPER_API_KEY_ID", "ALPACA_PAPER_API_SECRET_KEY"),
        AccountType::Live => ("ALPACA_LIVE_API_KEY_ID", "ALPACA_LIVE_API_SECRET_KEY"),
    }
}

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

    /// Read the pair from the environment for `account_type` (see
    /// [`env_var_names`]). The streaming path needs the credentials in hand
    /// rather than `oxidized_alpaca`'s env-var constructor, because that
    /// constructor couples the credential pair *and* the stream host to one
    /// account type (see `alpaca::STREAM_ACCOUNT_TYPE`). A missing variable
    /// surfaces as the same [`oxidized_alpaca::Error`] the upstream
    /// constructor raises, so callers classify it identically.
    ///
    /// # Errors
    ///
    /// [`oxidized_alpaca::Error::MissingEnvironmentVariable`] naming the first
    /// variable that is unset.
    pub(crate) fn from_env(
        account_type: AccountType,
    ) -> std::result::Result<Self, oxidized_alpaca::Error> {
        let (key_var, secret_var) = env_var_names(account_type);
        let read = |variable: &'static str| {
            std::env::var(variable).map_err(|source| {
                oxidized_alpaca::Error::MissingEnvironmentVariable {
                    variable_name: variable.to_string(),
                    source,
                }
            })
        };
        Ok(Self {
            key_id: read(key_var)?,
            secret: read(secret_var)?,
        })
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
    /// Read the `ALPACA_*` pair for the account type (REST clients through
    /// `oxidized_alpaca`'s env constructors, the stream through
    /// [`AlpacaCredentials::from_env`]).
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

    /// The `Env` source's contract is the `ALPACA_{PAPER,LIVE}_API_*` pair
    /// keyed on the account type; the names must stay in lockstep with
    /// `oxidized_alpaca`'s own env constructors.
    #[test]
    fn env_var_names_follow_the_account_type() {
        use oxidized_alpaca::AccountType;
        assert_eq!(
            super::env_var_names(AccountType::Paper),
            ("ALPACA_PAPER_API_KEY_ID", "ALPACA_PAPER_API_SECRET_KEY")
        );
        assert_eq!(
            super::env_var_names(AccountType::Live),
            ("ALPACA_LIVE_API_KEY_ID", "ALPACA_LIVE_API_SECRET_KEY")
        );
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
