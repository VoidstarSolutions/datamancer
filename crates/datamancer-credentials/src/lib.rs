//! Credential storage for datamancer providers (spec 2026-07-05, cycle 2).
//!
//! One store, two consumers: `datamancerd` wraps it with control-surface ops
//! (the broker), and embedders use it in-process (library parity, spec
//! decision 9). The backend is chosen at runtime — OS keychain where
//! available, a permissions-locked file elsewhere — and the choice is always
//! visible (`backend_name`, surfaced through `HealthView`).
//!
//! The API is deliberately **blocking** (OS keychain APIs are); async
//! callers wrap calls in `tokio::task::spawn_blocking`.
#![forbid(unsafe_code)]

mod file;
mod keychain;

use std::path::PathBuf;

use datamancer_core::ProviderCredentials;
pub use file::FileBackend;
pub use keychain::KeychainBackend;

/// A credential-store failure. Messages never carry secret material.
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    /// The platform backend failed (keychain locked, service unavailable…).
    #[error("credential backend: {0}")]
    Backend(String),
    /// Stored payload did not (de)serialize.
    #[error("credential encoding: {0}")]
    Serde(#[from] serde_json::Error),
    /// File-backend I/O.
    #[error("credential file i/o: {0}")]
    Io(#[from] std::io::Error),
}

/// One credential storage mechanism. Keyed by provider id; values are the
/// serde form of [`ProviderCredentials`].
pub trait CredentialBackend: Send + Sync {
    /// Stable, human-readable backend name (`"keychain"`, `"secret-service"`,
    /// `"file"`) — surfaced in health so a surprising fallback is visible.
    fn name(&self) -> &'static str;
    /// The stored credentials for `provider`, `None` if absent.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialError`] on backend failure or if the stored
    /// payload fails to decode.
    fn get(&self, provider: &str) -> Result<Option<ProviderCredentials>, CredentialError>;
    /// Store (create or replace) credentials for `provider`.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialError`] on backend failure.
    fn set(&self, provider: &str, creds: &ProviderCredentials) -> Result<(), CredentialError>;
    /// Remove credentials for `provider`. Removing an absent entry is Ok.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialError`] on backend failure.
    fn clear(&self, provider: &str) -> Result<(), CredentialError>;
}

/// The store handle both the daemon and embedders hold.
pub struct CredentialStore {
    backend: Box<dyn CredentialBackend>,
}

impl CredentialStore {
    /// A store on an explicit backend (tests, embedders with opinions).
    #[must_use]
    pub fn with_backend(backend: Box<dyn CredentialBackend>) -> Self {
        Self { backend }
    }

    /// The active backend's name.
    #[must_use]
    pub fn backend_name(&self) -> &'static str {
        self.backend.name()
    }

    /// See [`CredentialBackend::get`].
    ///
    /// # Errors
    ///
    /// Propagates the backend failure.
    pub fn get(&self, provider: &str) -> Result<Option<ProviderCredentials>, CredentialError> {
        self.backend.get(provider)
    }

    /// See [`CredentialBackend::set`].
    ///
    /// # Errors
    ///
    /// Propagates the backend failure.
    pub fn set(&self, provider: &str, creds: &ProviderCredentials) -> Result<(), CredentialError> {
        self.backend.set(provider, creds)
    }

    /// See [`CredentialBackend::clear`].
    ///
    /// # Errors
    ///
    /// Propagates the backend failure.
    pub fn clear(&self, provider: &str) -> Result<(), CredentialError> {
        self.backend.clear(provider)
    }
}

impl CredentialStore {
    /// The platform-default store: OS keychain when it initializes, else the
    /// file backend at [`default_file_path`]. The choice is never silent —
    /// read it back via [`Self::backend_name`].
    ///
    /// # Errors
    ///
    /// [`CredentialError::Backend`] when neither backend is possible (no
    /// keychain and no derivable home directory for the file path).
    pub fn open_default() -> Result<Self, CredentialError> {
        if let Ok(backend) = KeychainBackend::new() {
            return Ok(Self::with_backend(Box::new(backend)));
        }
        let path = default_file_path().ok_or_else(|| {
            CredentialError::Backend(
                "no keychain and no home directory for the file fallback".to_string(),
            )
        })?;
        Ok(Self::with_backend(Box::new(FileBackend::new(path))))
    }
}

/// Default file-backend location: `<data dir>/credentials.json` (macOS
/// `~/Library/Application Support/datamancer`, Linux `~/.local/share/datamancer`).
#[must_use]
pub fn default_file_path() -> Option<PathBuf> {
    let dirs = directories::ProjectDirs::from("", "", "datamancer")?;
    Some(dirs.data_dir().join("credentials.json"))
}

/// The behavior every backend must satisfy. `pub` so each backend's test
/// module (including the ignored keychain tests) runs the same suite.
///
/// # Panics
///
/// Panics on any contract violation (it is a test helper).
pub fn contract_tests(backend: &dyn CredentialBackend) {
    let provider = "contract-test-provider";
    // Fresh state: absent reads as None; clearing absent is Ok.
    backend.clear(provider).expect("clear absent is ok");
    assert!(backend.get(provider).expect("get").is_none());
    // Set then get round-trips.
    let creds = ProviderCredentials::ApiKeyPair {
        key_id: "AKID".to_string(),
        secret: "s3cret".to_string(),
    };
    backend.set(provider, &creds).expect("set");
    assert_eq!(backend.get(provider).expect("get"), Some(creds));
    // Replace overwrites.
    let rotated = ProviderCredentials::ApiKeyPair {
        key_id: "AKID2".to_string(),
        secret: "n3w".to_string(),
    };
    backend.set(provider, &rotated).expect("replace");
    assert_eq!(backend.get(provider).expect("get"), Some(rotated));
    // Distinct providers are independent.
    let other = ProviderCredentials::Gateway {
        host: "127.0.0.1".to_string(),
        port: 4001,
        client_id: 1,
    };
    backend
        .set("contract-test-other", &other)
        .expect("set other");
    assert_eq!(
        backend.get("contract-test-other").expect("get other"),
        Some(other)
    );
    // Clear removes only the named provider.
    backend.clear(provider).expect("clear");
    assert!(backend.get(provider).expect("get after clear").is_none());
    assert!(
        backend
            .get("contract-test-other")
            .expect("other survives")
            .is_some()
    );
    backend.clear("contract-test-other").expect("cleanup");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_backend_satisfies_the_contract() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FileBackend::new(dir.path().join("credentials.json"));
        contract_tests(&backend);
    }

    #[cfg(unix)]
    #[test]
    fn file_backend_creates_the_file_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        let backend = FileBackend::new(path.clone());
        backend
            .set(
                "p",
                &datamancer_core::ProviderCredentials::ApiKeyPair {
                    key_id: "k".to_string(),
                    secret: "s".to_string(),
                },
            )
            .unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "credentials file must be owner-only");
    }

    #[test]
    fn store_reports_backend_name() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            CredentialStore::with_backend(Box::new(FileBackend::new(dir.path().join("c.json"))));
        assert_eq!(store.backend_name(), "file");
    }

    #[test]
    fn open_default_always_selects_some_backend() {
        // On any host with a home dir this must succeed — keychain if the
        // platform store is up, else the file fallback. Never silently: the
        // name says which.
        let store = CredentialStore::open_default().expect("some backend");
        assert!(["keychain", "secret-service", "file"].contains(&store.backend_name()));
    }
}
