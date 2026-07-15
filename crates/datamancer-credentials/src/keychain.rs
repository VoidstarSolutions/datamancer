//! The OS-keychain backend: macOS Keychain Services / Linux Secret Service
//! (D-Bus), via the `keyring` crate's classic API. Windows Credential
//! Manager is additive later through the same seam.
//!
//! Entries live under service `"datamancer"`, username = provider id,
//! password = the serde-JSON of the credential shape.

use std::sync::Once;

use datamancer_core::ProviderCredentials;

use crate::{CredentialBackend, CredentialError};

const SERVICE: &str = "datamancer";

pub struct KeychainBackend {
    _probe: (),
}

/// Install the platform-native store as `keyring-core`'s default, once.
///
/// Deviation from the brief: `keyring` 4.1.3's `v1::Entry::new` is documented
/// to lazily initialize the default store on first use, but its internal
/// gate is `AtomicBool::compare_exchange(false, true, ..)` checked against
/// `== Ok(true)`. A successful first swap returns `Ok(false)` (the *prior*
/// value), so that branch never runs and no default store is ever installed
/// — every `Entry::new`/`get_password` call fails with
/// `keyring::Error::NoDefaultStore("No default store has been set, ...")`,
/// confirmed empirically (`cargo test -p datamancer-credentials -- --ignored`
/// failed with exactly that message before this workaround). We replicate
/// the upstream `set_credential_store` logic ourselves — same platform-store
/// crates the `v1` feature already depends on, installed via the public
/// `keyring_core::set_default_store` — guarded by a correct `std::sync::Once`.
fn init_default_store() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        #[cfg(target_os = "macos")]
        let store = apple_native_keyring_store::keychain::Store::new();
        #[cfg(all(unix, not(target_os = "macos")))]
        let store = zbus_secret_service_keyring_store::Store::new();
        #[cfg(windows)]
        let store = windows_native_keyring_store::Store::new();
        if let Ok(store) = store {
            keyring_core::set_default_store(store);
        }
        // If the platform store fails to construct, leave no default store
        // set; subsequent `Entry::new` calls surface `NoDefaultStore` and
        // `KeychainBackend::new`'s probe below reports it as unavailable.
    });
}

impl KeychainBackend {
    /// The platform's backend name (health-surface contract).
    pub const NAME: &'static str = if cfg!(target_os = "macos") {
        "keychain"
    } else if cfg!(windows) {
        // Windows Credential Manager (via windows-native-keyring-store) — not
        // the Linux D-Bus Secret Service the `else` arm names.
        "credential-manager"
    } else {
        "secret-service"
    };

    /// Construct, probing that the platform store initializes (a headless
    /// host without a Secret Service daemon fails here, triggering the file
    /// fallback in [`crate::CredentialStore::open_default`]).
    ///
    /// # Errors
    ///
    /// [`CredentialError::Backend`] when the platform store is unavailable.
    pub fn new() -> Result<Self, CredentialError> {
        init_default_store();
        // Entry::new initializes the default store on first use; a probe
        // get exercises the store connection itself. NoEntry is success
        // (store reachable, entry absent).
        let entry = keyring::Entry::new(SERVICE, "datamancer-availability-probe")
            .map_err(|e| CredentialError::Backend(e.to_string()))?;
        match entry.get_password() {
            Ok(_) | Err(keyring::Error::NoEntry) => Ok(Self { _probe: () }),
            Err(e) => Err(CredentialError::Backend(e.to_string())),
        }
    }

    fn entry(provider: &str) -> Result<keyring::Entry, CredentialError> {
        keyring::Entry::new(SERVICE, provider).map_err(|e| CredentialError::Backend(e.to_string()))
    }
}

impl CredentialBackend for KeychainBackend {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn get(&self, provider: &str) -> Result<Option<ProviderCredentials>, CredentialError> {
        match Self::entry(provider)?.get_password() {
            Ok(json) => Ok(Some(serde_json::from_str(&json)?)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(CredentialError::Backend(e.to_string())),
        }
    }

    fn set(&self, provider: &str, creds: &ProviderCredentials) -> Result<(), CredentialError> {
        let json = serde_json::to_string(creds)?;
        Self::entry(provider)?
            .set_password(&json)
            .map_err(|e| CredentialError::Backend(e.to_string()))
    }

    fn clear(&self, provider: &str) -> Result<(), CredentialError> {
        match Self::entry(provider)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(CredentialError::Backend(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KeychainBackend;

    /// Real OS keychain — mutates the developer's keyring under the
    /// "datamancer" service (test-prefixed provider ids only) and may prompt.
    /// CI runs the file backend instead.
    #[test]
    #[ignore = "touches the real OS keychain; run on a dev machine"]
    fn keychain_backend_satisfies_the_contract() {
        let backend = KeychainBackend::new().expect("keychain available on dev machine");
        crate::contract_tests(&backend);
    }

    #[test]
    fn backend_name_is_platform_stable() {
        // Name is a health-surface contract even when the backend can't
        // construct (asserts the constant, not availability).
        assert!(
            ["keychain", "secret-service", "credential-manager"].contains(&KeychainBackend::NAME)
        );
    }
}
