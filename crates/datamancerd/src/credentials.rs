//! The daemon-side credential broker: one store, one watch channel per
//! configured provider, stable-coded replies. Ops run off-actor (blocking
//! store I/O behind `spawn_blocking`) and are peer-cred gated same-uid.

// Native Windows port (#29): the credential-broker ops here are reached only via
// the Unix control socket, so they are transitionally dead on Windows until the
// named-pipe transport revives them in Phase 3. Scoped allow — Unix/macOS stay
// lint-strict; remove when Phase 3 lands.
#![cfg_attr(windows, allow(dead_code, unused_imports))]

use std::collections::HashMap;
use std::sync::Arc;

use datamancer::providers::AccountType;
use datamancer::{AlpacaCredentials, CredentialsSource};
use datamancer_core::ProviderCredentials;
use datamancer_credentials::{CredentialError, CredentialStore};
use tokio::sync::watch;

use crate::control::{Reply, codes};
use crate::error::{DaemonError, Result};

/// Same-uid gate for credential and config-mutation ops (and `shutdown`).
/// Unreadable peer credentials are denied, not defaulted.
pub(crate) fn privileged_op_permitted(peer_uid: Option<u32>, own_euid: u32) -> bool {
    peer_uid == Some(own_euid)
}

/// The broker: the daemon-owned [`CredentialStore`] plus one watch sender per
/// configured provider (the hot-apply seam — providers hold the receivers).
pub(crate) struct CredentialHub {
    store: Arc<CredentialStore>,
    senders: HashMap<String, watch::Sender<Option<AlpacaCredentials>>>,
    /// Per-provider op serialization (same keys as `senders`). `set` and
    /// `clear` hold the provider's lock across the store write **and** the
    /// watch `send_replace`, so concurrent mutations for one provider can't
    /// interleave persist and apply (store ending on B while the live watch
    /// ends on A). `get` stays lock-free: it is a single atomic-enough store
    /// read and never touches the watch.
    locks: HashMap<String, tokio::sync::Mutex<()>>,
}

/// The Alpaca shape of a wire credential; `None` for any other shape (both
/// built-in providers take an `ApiKeyPair`).
fn to_alpaca(creds: &ProviderCredentials) -> Option<AlpacaCredentials> {
    match creds {
        ProviderCredentials::ApiKeyPair { key_id, secret } => Some(AlpacaCredentials {
            key_id: key_id.clone(),
            secret: secret.clone(),
        }),
        // `ProviderCredentials` is `#[non_exhaustive]`; no future shape maps
        // onto the Alpaca pair either.
        _ => None,
    }
}

/// A store failure as a stable-coded reply. `CredentialError` messages carry
/// no secret material (a crate-level contract).
fn backend_error(e: &CredentialError) -> Reply {
    Reply::error(codes::CREDENTIAL_BACKEND_UNAVAILABLE, e.to_string())
}

/// The deprecated env-var fallback pair for an account type. Reads values;
/// logs only the provider and variable *names*, never values.
fn env_credentials(account_type: AccountType) -> Option<AlpacaCredentials> {
    let (key_var, secret_var) = match account_type {
        AccountType::Paper => ("ALPACA_PAPER_API_KEY_ID", "ALPACA_PAPER_API_SECRET_KEY"),
        AccountType::Live => ("ALPACA_LIVE_API_KEY_ID", "ALPACA_LIVE_API_SECRET_KEY"),
    };
    match (std::env::var(key_var), std::env::var(secret_var)) {
        (Ok(key_id), Ok(secret)) => Some(AlpacaCredentials { key_id, secret }),
        _ => None,
    }
}

impl CredentialHub {
    /// The env-free constructor: one watch channel per provider id, seeded
    /// from the store (`Some` when stored, else `None`). Returns the hub plus
    /// the per-provider [`CredentialsSource`] map for `build_runtime`.
    ///
    /// Store reads here are blocking; this runs at boot (and in tests),
    /// before the async control surface exists.
    pub(crate) fn with_store(
        store: CredentialStore,
        provider_ids: &[&str],
    ) -> (Self, HashMap<String, CredentialsSource>) {
        let mut senders = HashMap::new();
        let mut locks = HashMap::new();
        let mut sources = HashMap::new();
        for &id in provider_ids {
            locks.insert(id.to_string(), tokio::sync::Mutex::new(()));
            let seed = match store.get(id) {
                Ok(stored) => stored.as_ref().and_then(to_alpaca),
                Err(e) => {
                    tracing::warn!(provider = id, error = %e, "credential store read failed at bootstrap; provider starts unprovisioned");
                    None
                }
            };
            let (tx, rx) = watch::channel(seed);
            senders.insert(id.to_string(), tx);
            sources.insert(id.to_string(), CredentialsSource::Watch(rx));
        }
        (
            Self {
                store: Arc::new(store),
                senders,
                locks,
            },
            sources,
        )
    }

    /// Open the platform-default store and seed one watch channel per
    /// **compiled-in** provider (`all_ids`) — so `set-credentials` works
    /// before a provider is enabled — then apply the deprecated env-var
    /// fallback only to providers with a config section (`env_fallback`):
    /// stored credentials win; else the provider's env-var pair (deprecated —
    /// warns); else `None` (the provider parks until `set-credentials`).
    ///
    /// # Errors
    ///
    /// [`DaemonError::CredentialStore`] when no backend is available.
    pub(crate) fn bootstrap(
        all_ids: &[&str],
        env_fallback: &[(&str, AccountType)],
    ) -> Result<(Arc<Self>, HashMap<String, CredentialsSource>)> {
        let store = CredentialStore::open_default().map_err(DaemonError::CredentialStore)?;
        tracing::info!(backend = store.backend_name(), "credential store opened");
        let (hub, sources) = Self::with_store(store, all_ids);
        for &(id, account_type) in env_fallback {
            let Some(sender) = hub.senders.get(id) else {
                continue;
            };
            if sender.borrow().is_some() {
                tracing::info!(provider = id, "credentials loaded from the store");
                continue;
            }
            if let Some(env) = env_credentials(account_type) {
                tracing::warn!(
                    provider = id,
                    "credentials loaded from environment variables (deprecated); provision via \
                     set-credentials — env fallback will be removed once the broker is proven"
                );
                sender.send_replace(Some(env));
            } else {
                tracing::warn!(
                    provider = id,
                    "no credentials at start (store empty, env unset); provider parks until \
                     set-credentials — subscribes will fail until then"
                );
            }
        }
        Ok((Arc::new(hub), sources))
    }

    /// The active store backend's name (threaded into the actor for `ping`).
    pub(crate) fn backend_name(&self) -> &'static str {
        self.store.backend_name()
    }

    /// Store (create or rotate) credentials for `provider`, then hot-apply
    /// them on the provider's watch. Persist-then-apply: a store failure
    /// leaves the running provider untouched.
    pub(crate) async fn set(&self, provider: &str, creds: ProviderCredentials) -> Reply {
        let Some(sender) = self.senders.get(provider) else {
            return unknown_provider(provider);
        };
        let Some(alpaca) = to_alpaca(&creds) else {
            return Reply::error(
                codes::BAD_REQUEST,
                format!("provider {provider:?} takes an api_key_pair credential"),
            );
        };
        // Serialize persist+apply per provider: without this, concurrent
        // set/set (or set/clear) could leave the store on one value and the
        // live watch on another.
        let _op_guard = self.locks[provider].lock().await;
        let store = Arc::clone(&self.store);
        let id = provider.to_string();
        match tokio::task::spawn_blocking(move || store.set(&id, &creds)).await {
            Ok(Ok(())) => {
                sender.send_replace(Some(alpaca));
                tracing::info!(provider, "credentials stored and hot-applied");
                Reply::ok()
            }
            Ok(Err(e)) => backend_error(&e),
            Err(e) => Reply::error(codes::INTERNAL, format!("credential task failed: {e}")),
        }
    }

    /// Read the stored credentials for `provider` (from the store — the
    /// single source of truth — never the watch). Deliberately lock-free
    /// (see `locks`): a single store read is atomic enough, and a get racing
    /// a set legitimately observes either side of it.
    pub(crate) async fn get(&self, provider: &str) -> Reply {
        if !self.senders.contains_key(provider) {
            return unknown_provider(provider);
        }
        let store = Arc::clone(&self.store);
        let id = provider.to_string();
        match tokio::task::spawn_blocking(move || store.get(&id)).await {
            Ok(Ok(Some(creds))) => Reply::credentials(creds),
            Ok(Ok(None)) => Reply::error(
                codes::CREDENTIALS_MISSING,
                format!("no stored credentials for provider {provider:?}"),
            ),
            Ok(Err(e)) => backend_error(&e),
            Err(e) => Reply::error(codes::INTERNAL, format!("credential task failed: {e}")),
        }
    }

    /// Remove stored credentials for `provider`. Deliberately does **not**
    /// touch the watch: a running provider keeps its last applied credentials
    /// until restart (there is no un-apply).
    pub(crate) async fn clear(&self, provider: &str) -> Reply {
        if !self.senders.contains_key(provider) {
            return unknown_provider(provider);
        }
        // Same per-provider serialization as `set`: a clear racing a set must
        // not interleave with its persist+apply pair.
        let _op_guard = self.locks[provider].lock().await;
        let store = Arc::clone(&self.store);
        let id = provider.to_string();
        match tokio::task::spawn_blocking(move || store.clear(&id)).await {
            Ok(Ok(())) => {
                tracing::info!(
                    provider,
                    "stored credentials cleared (running provider keeps its last applied \
                     credentials until restart)"
                );
                Reply::ok()
            }
            Ok(Err(e)) => backend_error(&e),
            Err(e) => Reply::error(codes::INTERNAL, format!("credential task failed: {e}")),
        }
    }
}

fn unknown_provider(provider: &str) -> Reply {
    Reply::error(
        codes::UNKNOWN_PROVIDER,
        format!("no configured provider {provider:?}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datamancer_core::ProviderCredentials;

    #[test]
    fn gate_requires_exact_uid_match() {
        assert!(privileged_op_permitted(Some(501), 501));
        assert!(!privileged_op_permitted(Some(502), 501));
        assert!(!privileged_op_permitted(None, 501)); // unreadable peer = denied
    }

    #[tokio::test]
    async fn hub_set_hot_applies_and_get_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = datamancer_credentials::CredentialStore::with_backend(Box::new(
            datamancer_credentials::FileBackend::new(dir.path().join("c.json")),
        ));
        let (hub, sources) = CredentialHub::with_store(store, &["alpaca"]);
        // Watch seeded None (no stored creds; `with_store` never reads env).
        let src = sources.get("alpaca").unwrap().clone();
        let rx = match &src {
            datamancer::CredentialsSource::Watch(rx) => rx.clone(),
            other => panic!("expected Watch source, got {other:?}"),
        };
        assert!(rx.borrow().is_none());

        let creds = ProviderCredentials::ApiKeyPair {
            key_id: "AKID".to_string(),
            secret: "s".to_string(),
        };
        let reply = hub.set("alpaca", creds.clone()).await;
        assert!(reply.ok, "set failed: {reply:?}");
        // Hot-apply: the provider-side watch sees the new value.
        assert_eq!(
            rx.borrow().as_ref().map(|c| c.key_id.clone()),
            Some("AKID".to_string())
        );
        // get round-trips from the STORE (single source of truth).
        let got = hub.get("alpaca").await;
        assert_eq!(got.credentials, Some(creds));
        // clear: store emptied, running provider unaffected (watch unchanged).
        assert!(hub.clear("alpaca").await.ok);
        assert_eq!(
            hub.get("alpaca").await.code.as_deref(),
            Some("credentials_missing")
        );
        assert!(
            rx.borrow().is_some(),
            "clear must not un-apply live credentials"
        );
    }

    #[tokio::test]
    async fn hub_rejects_unknown_provider_and_wrong_shape() {
        let dir = tempfile::tempdir().unwrap();
        let store = datamancer_credentials::CredentialStore::with_backend(Box::new(
            datamancer_credentials::FileBackend::new(dir.path().join("c.json")),
        ));
        let (hub, _sources) = CredentialHub::with_store(store, &["alpaca"]);
        let creds = ProviderCredentials::ApiKeyPair {
            key_id: "k".to_string(),
            secret: "s".to_string(),
        };
        assert_eq!(
            hub.set("nope", creds).await.code.as_deref(),
            Some("unknown_provider")
        );
        let gateway = ProviderCredentials::Gateway {
            host: "h".to_string(),
            port: 1,
            client_id: 1,
        };
        assert_eq!(
            hub.set("alpaca", gateway).await.code.as_deref(),
            Some("bad_request")
        );
    }
}
