//! The fallback backend: one JSON file, owner-only permissions, atomic
//! writes (tmp + rename, the same pattern as datamancerd's config writes).
//! For headless hosts with no keychain — and for CI, where it carries the
//! backend contract tests.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Mutex;

use datamancer_core::ProviderCredentials;

use crate::{CredentialBackend, CredentialError};

/// See module docs. The mutex serializes read-modify-write cycles within
/// this process; cross-process writers are out of scope (the daemon is the
/// only writer in the brokered deployment).
pub struct FileBackend {
    path: PathBuf,
    lock: Mutex<()>,
}

impl FileBackend {
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            lock: Mutex::new(()),
        }
    }

    fn load(&self) -> Result<BTreeMap<String, ProviderCredentials>, CredentialError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
            Err(e) => Err(e.into()),
        }
    }

    fn save(&self, map: &BTreeMap<String, ProviderCredentials>) -> Result<(), CredentialError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut opts = std::fs::OpenOptions::new();
            opts.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                opts.mode(0o600);
            }
            let mut f = opts.open(&tmp)?;
            f.write_all(&serde_json::to_vec_pretty(map)?)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

impl CredentialBackend for FileBackend {
    fn name(&self) -> &'static str {
        "file"
    }

    fn get(&self, provider: &str) -> Result<Option<ProviderCredentials>, CredentialError> {
        let _guard = self.lock.lock().expect("file backend lock poisoned");
        Ok(self.load()?.remove(provider))
    }

    fn set(&self, provider: &str, creds: &ProviderCredentials) -> Result<(), CredentialError> {
        let _guard = self.lock.lock().expect("file backend lock poisoned");
        let mut map = self.load()?;
        map.insert(provider.to_string(), creds.clone());
        self.save(&map)
    }

    fn clear(&self, provider: &str) -> Result<(), CredentialError> {
        let _guard = self.lock.lock().expect("file backend lock poisoned");
        let mut map = self.load()?;
        if map.remove(provider).is_some() {
            self.save(&map)?;
        }
        Ok(())
    }
}
