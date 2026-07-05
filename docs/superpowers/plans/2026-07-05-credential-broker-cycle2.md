# Credential Broker (Cycle 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The daemon becomes the single owner of provider credentials — keychain-backed storage, `set/get/clear-credentials` UDS ops behind a same-uid peer check, and hot-apply (provider reconnects with new credentials, no restart) — per cycle 2 of `docs/superpowers/specs/2026-07-05-app-facing-daemon-design.md`.

**Architecture:** A pure `ProviderCredentials` type lands in `datamancer-core` (so the client crate never pulls keychain deps). A new `datamancer-credentials` crate owns the I/O: a `CredentialBackend` trait with a keychain backend (`keyring` v4, macOS Keychain / Linux secret-service, Windows-additive) and a 0600-file fallback, selected at runtime. Providers gain an injectable `CredentialsSource` (`Env` legacy / `Static` / `Watch`) — the `Watch` variant makes hot-apply a `tokio::watch` send: the streaming loop selects on credential changes and reconnects, REST clients rebuild lazily. The daemon seeds one watch channel per provider at bootstrap (store → env fallback with deprecation warning → none), serves the credential ops off-actor gated by `UnixStream::peer_cred()`, and reports the active backend via `ping` → `HealthView.daemon.credential_backend`. Library parity (spec decision 9): embedders use the same `CredentialsSource` on `AlpacaProviderConfig` and the same store crate in-process.

**Tech Stack:** Rust edition 2024, tokio (`watch` via existing `sync` feature), `keyring = "4"` (feature `v1`), `rustix` (already a datamancerd dep, for `geteuid`), serde/serde_json/thiserror.

## Global Constraints

- `clippy::pedantic = deny` workspace-wide; **every crate `#![forbid(unsafe_code)]`** (this is why env-var injection is impossible: `std::env::set_var` is unsafe in edition 2024 — the injectable source is the sanctioned mechanism).
- `datamancer-core` stays pure types + traits: no I/O, no tokio, no keyring.
- `datamancer-client` depends on `datamancer-core` + transport crates only — it must NOT depend on `datamancer-credentials` (keychain deps stay out of the client).
- Credential ops are UDS-only — the WS surface (`protocol/ws.rs`, `datamancerd/src/ws/`) is NEVER touched by this plan.
- Stable codes/wire shapes are an operator contract: additions only, each regression-tested. New codes this cycle: `credentials_missing`, `credential_backend_unavailable`, `permission_denied`.
- **No secret material in error messages, Debug output, or logs.** Every type holding a secret implements a redacting `Debug`.
- `datamancer-client` and `datamancerd` versions bump **in lockstep** (ping version gate; regression test `daemon_and_client_versions_stay_in_lockstep` in datamancerd enforces it).
- **Before opening the PR, run the CI gates locally** (workspace CLAUDE.md): `cargo deny check` (the keyring/zbus tree is new — licenses must clear the allowlist) and `.github/scripts/semver-checks.sh origin/main`, bumping crate versions as the tool directs (Task 9).
- Branch: `feature/credential-broker`, stacked on `design/app-facing-daemon` — rebase onto main after PR #17 merges; do not merge cycle-1 commits into this PR's story.
- Commit style: `type(scope): summary`.

---

### Task 1: Update the `oxidized_alpaca` pin (explicit-credentials API)

`oxidized-alpaca` main (its PR #30, commit `5588b3d`+) already ships the explicit-credentials API this plan needs: `ApiKey::new(key_id, secret_key)` (redacting Debug) and `new_with_credentials` constructors on every client. datamancer's lockfile pins an older rev (env-var-only `Env`). This task updates the pin and proves the API is available.

**Files:**
- Modify: `Cargo.lock` (via `cargo update`)

**Interfaces:**
- Produces (later tasks rely on exactly these, from `oxidized_alpaca`):
  - `ApiKey::new(key_id: impl Into<String>, secret_key: impl Into<String>) -> ApiKey` (Clone, Debug-redacted)
  - `MarketDataClient::new_with_credentials(account_type: AccountType, api_key: ApiKey) -> Result<Self>`
  - `TradingClient::new_with_credentials(account_type: AccountType, api_key: ApiKey) -> Result<Self>`
  - `StreamingStockClient::new_with_credentials(account_type: AccountType, feed: StreamingFeed, api_key: ApiKey) -> Result<Self, Error>` *(async; verify the stock client's exact arg list against the updated source — the crypto client's is `(account_type, feed, api_key)`)*
  - `StreamingCryptoClient::new_with_credentials(account_type: AccountType, feed: CryptoFeed, api_key: ApiKey) -> Result<Self, Error>` (async)

- [ ] **Step 1: Update the pin**

Run: `cargo update -p oxidized_alpaca`
Expected: lockfile moves to the current `main` rev of `https://github.com/VoidstarSolutions/oxidized-alpaca`.

- [ ] **Step 2: Verify the API landed and nothing broke**

Run: `cargo build && cargo test 2>&1 | grep -E "test result|error\[" | head -30`
Expected: workspace builds; all suites green (the old public constructors `MarketDataClient::new`/`StreamingCryptoClient::new` are unchanged upstream).
Then confirm the new API is visible: `grep -rn "new_with_credentials\|pub struct ApiKey" ~/.cargo/git/checkouts/oxidized-alpaca-*/*/src/ | head -8` — expect hits in `env.rs`, `restful/*`, `streaming/*`. If the checkout dir is ambiguous, `cargo build` first (it materializes the rev in the lockfile).

- [ ] **Step 3: Run clippy + fmt gates**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add Cargo.lock
git commit -m "chore(deps): update oxidized_alpaca to main (explicit-credentials ApiKey API)"
```

---

### Task 2: `ProviderCredentials` in `datamancer-core` + `DaemonHealth.credential_backend`

**Files:**
- Create: `crates/datamancer-core/src/credentials.rs`
- Modify: `crates/datamancer-core/src/lib.rs` (module + re-export)
- Modify: `crates/datamancer-core/src/health.rs` (new `DaemonHealth` field)

**Interfaces:**
- Produces:
  - `ProviderCredentials::{ApiKeyPair { key_id: String, secret: String }, Gateway { host: String, port: u16, client_id: u32 }}` — `#[non_exhaustive]`, serde `tag = "type", rename_all = "snake_case"`, Clone, PartialEq, **manual Debug redacting `secret`**.
  - `DaemonHealth.credential_backend: Option<String>` — pub field, left `None` by the pure reduction (same contract as `version`: the caller stamps it).

- [ ] **Step 1: Write the failing tests**

Create `crates/datamancer-core/src/credentials.rs` with only the test module:

```rust
//! Provider credential shapes (spec 2026-07-05, cycle 2).
//!
//! Pure serde types — storage and transport live elsewhere
//! (`datamancer-credentials` for the store, the UDS control surface for the
//! wire). Tagged per provider *shape*, not a universal key/secret pair:
//! IBKR-style `Gateway` credentials contain no secret at all.

#[cfg(test)]
mod tests {
    use super::ProviderCredentials;

    #[test]
    fn api_key_pair_round_trips_with_stable_wire_tags() {
        let creds = ProviderCredentials::ApiKeyPair {
            key_id: "AKID".to_string(),
            secret: "s3cret".to_string(),
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert_eq!(
            json,
            r#"{"type":"api_key_pair","key_id":"AKID","secret":"s3cret"}"#
        );
        let back: ProviderCredentials = serde_json::from_str(&json).unwrap();
        assert_eq!(back, creds);
    }

    #[test]
    fn gateway_round_trips_and_carries_no_secret() {
        let creds = ProviderCredentials::Gateway {
            host: "127.0.0.1".to_string(),
            port: 4001,
            client_id: 7,
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert_eq!(
            json,
            r#"{"type":"gateway","host":"127.0.0.1","port":4001,"client_id":7}"#
        );
        assert_eq!(serde_json::from_str::<ProviderCredentials>(&json).unwrap(), creds);
    }

    #[test]
    fn debug_never_reveals_the_secret() {
        let creds = ProviderCredentials::ApiKeyPair {
            key_id: "AKID".to_string(),
            secret: "s3cret".to_string(),
        };
        let debug = format!("{creds:?}");
        assert!(!debug.contains("s3cret"), "secret leaked into Debug: {debug}");
        assert!(debug.contains("AKID"), "key id is not secret and aids diagnosis");
    }
}
```

And in `crates/datamancer-core/src/health.rs`, extend the existing tests: in `provider_states_map_from_connection_state` (or a new small test) assert `view.daemon.credential_backend.is_none()` after `from_snapshot`.

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p datamancer-core 2>&1 | tail -5`
Expected: COMPILE ERROR — `ProviderCredentials` and the field don't exist.

- [ ] **Step 3: Implement**

Above the test module in `credentials.rs`:

```rust
use serde::{Deserialize, Serialize};

/// Credentials for one provider, tagged by shape.
///
/// `Gateway` is the IBKR-style shape reserved by the spec appendix: an
/// attach-to-local-companion "credential" that contains no secret. Nothing
/// consumes it yet; the wire tag is stable now so shipped consumers already
/// parse it.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderCredentials {
    /// A key-id + secret pair (Alpaca-style).
    ApiKeyPair { key_id: String, secret: String },
    /// A local companion-process endpoint (IBKR-style; reserved).
    Gateway { host: String, port: u16, client_id: u32 },
}

impl std::fmt::Debug for ProviderCredentials {
    /// Redacts secret material; key ids and endpoints are diagnostic, not
    /// secret.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKeyPair { key_id, .. } => f
                .debug_struct("ApiKeyPair")
                .field("key_id", key_id)
                .field("secret", &"********")
                .finish(),
            Self::Gateway {
                host,
                port,
                client_id,
            } => f
                .debug_struct("Gateway")
                .field("host", host)
                .field("port", port)
                .field("client_id", client_id)
                .finish(),
        }
    }
}
```

In `lib.rs`: `pub mod credentials;` + add `credentials::ProviderCredentials` to the re-export list. In `health.rs`, add to `DaemonHealth` (after `version`):

```rust
    /// The daemon's active credential-store backend (`"keychain"`,
    /// `"secret-service"`, `"file"`). `None` out of the pure reduction — the
    /// caller stamps it (facade, from the `ping` handshake), and `None` also
    /// means "daemon predates the credential broker". A surprising `"file"`
    /// on a desktop host is visible here rather than silent.
    pub credential_backend: Option<String>,
```

and set `credential_backend: None` in `from_snapshot`'s `DaemonHealth` literal.

- [ ] **Step 4: Run tests + gates**

Run: `cargo test -p datamancer-core && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: green (note: `datamancer-client`'s `fill_health` and tests still compile — they assign fields, never construct `DaemonHealth` literally).

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-core/src/credentials.rs crates/datamancer-core/src/lib.rs crates/datamancer-core/src/health.rs
git commit -m "feat(core): ProviderCredentials shapes and DaemonHealth credential-backend visibility"
```

---

### Task 3: `datamancer-credentials` crate — trait, file backend, store

**Files:**
- Create: `crates/datamancer-credentials/Cargo.toml`
- Create: `crates/datamancer-credentials/src/lib.rs`
- Create: `crates/datamancer-credentials/src/file.rs`
- Create: `crates/datamancer-credentials/CLAUDE.md`, `crates/datamancer-credentials/README.md`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: `datamancer_core::ProviderCredentials` (Task 2).
- Produces (Tasks 4, 7, and embedders rely on exactly these):
  - `CredentialError` — thiserror enum: `Backend(String)` (backend failure, message pre-scrubbed of secrets), `Serde(#[from] serde_json::Error)`, `Io(#[from] std::io::Error)`.
  - `pub trait CredentialBackend: Send + Sync { fn name(&self) -> &'static str; fn get(&self, provider: &str) -> Result<Option<ProviderCredentials>, CredentialError>; fn set(&self, provider: &str, creds: &ProviderCredentials) -> Result<(), CredentialError>; fn clear(&self, provider: &str) -> Result<(), CredentialError>; }` — **synchronous, blocking** (keychain APIs are blocking; async callers wrap in `spawn_blocking`).
  - `FileBackend::new(path: PathBuf) -> FileBackend` (`name() == "file"`; single JSON object file `{provider: ProviderCredentials}`, `0o600`, atomic tmp+rename writes).
  - `CredentialStore { … }` with `CredentialStore::with_backend(backend: Box<dyn CredentialBackend>) -> Self`, `backend_name(&self) -> &'static str`, and delegating `get`/`set`/`clear`. (`open_default()` arrives in Task 4 with the keychain backend.)
  - `pub fn default_file_path() -> Option<PathBuf>` — `<data dir>/credentials.json` via `ProjectDirs::from("", "", "datamancer")` (same convention as `datamancer-client/src/paths.rs`).
  - `pub fn contract_tests(backend: &dyn CredentialBackend)` — the shared backend contract suite, `pub` so Task 4's keychain tests reuse it.

Crate skeleton — `Cargo.toml`:

```toml
[package]
name = "datamancer-credentials"
version = "0.1.0"
edition = "2024"
license = "MIT OR Apache-2.0"
description = "Credential storage for datamancer providers: OS keychain with a locked-down file fallback"

[dependencies]
datamancer-core = { path = "../datamancer-core" }
serde_json = { workspace = true }
thiserror = { workspace = true }
directories = "6"

[dev-dependencies]
tempfile = "3"

[lints]
workspace = true
```

(`keyring` joins in Task 4.) Add `"crates/datamancer-credentials"` to the workspace `members` list in the root `Cargo.toml`.

- [ ] **Step 1: Write the failing tests**

`src/lib.rs` — module docs, error, trait, store, the shared contract suite, and a test module driving it against `FileBackend`:

```rust
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

use std::path::PathBuf;

use datamancer_core::ProviderCredentials;
pub use file::FileBackend;

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
    fn get(&self, provider: &str) -> Result<Option<ProviderCredentials>, CredentialError>;
    /// Store (create or replace) credentials for `provider`.
    fn set(&self, provider: &str, creds: &ProviderCredentials) -> Result<(), CredentialError>;
    /// Remove credentials for `provider`. Removing an absent entry is Ok.
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
    pub fn set(
        &self,
        provider: &str,
        creds: &ProviderCredentials,
    ) -> Result<(), CredentialError> {
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
    backend.set("contract-test-other", &other).expect("set other");
    assert_eq!(
        backend.get("contract-test-other").expect("get other"),
        Some(other)
    );
    // Clear removes only the named provider.
    backend.clear(provider).expect("clear");
    assert!(backend.get(provider).expect("get after clear").is_none());
    assert!(backend.get("contract-test-other").expect("other survives").is_some());
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
        let store = CredentialStore::with_backend(Box::new(FileBackend::new(
            dir.path().join("c.json"),
        )));
        assert_eq!(store.backend_name(), "file");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p datamancer-credentials 2>&1 | tail -5`
Expected: COMPILE ERROR — `FileBackend` not defined.

- [ ] **Step 3: Implement `src/file.rs`**

```rust
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
```

Note: a pre-existing file keeps its original mode (`mode(0o600)` applies at creation; rename preserves the tmp file's mode, so every `save` re-establishes 0600 — which is why the permissions test asserts after `set`).

- [ ] **Step 4: Write `CLAUDE.md` and `README.md`**

`CLAUDE.md` (match the workspace's terse crate-CLAUDE.md voice): depends on `datamancer-core` only — never the orchestrator; blocking API by design (`spawn_blocking` at async call sites); `name()` strings are a health-surface contract (`"keychain"`, `"secret-service"`, `"file"`); no secret material in errors/Debug/logs; `contract_tests` is the shared behavior gate every backend must pass. `README.md`: what it is, the backend selection order (keychain → file, Task 4), the two consumers (daemon broker / in-process embedder), default file path, and that Windows (Credential Manager) is additive later via the same trait.

- [ ] **Step 5: Run tests + gates**

Run: `cargo test -p datamancer-credentials && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: 3 tests (4 on unix) pass; workspace clippy clean.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates/datamancer-credentials
git commit -m "feat(credentials): datamancer-credentials crate — backend trait, file backend, store"
```

---

### Task 4: Keychain backend + `open_default` selection + `cargo deny`

**Files:**
- Create: `crates/datamancer-credentials/src/keychain.rs`
- Modify: `crates/datamancer-credentials/src/lib.rs` (module, `open_default`)
- Modify: `crates/datamancer-credentials/Cargo.toml` (keyring dep)

**Interfaces:**
- Produces:
  - `KeychainBackend::new() -> Result<KeychainBackend, CredentialError>` — probes availability at construction; `name()` is `"keychain"` on macOS, `"secret-service"` on other unix, `"keychain"` on Windows (future).
  - `CredentialStore::open_default() -> Result<CredentialStore, CredentialError>` — keychain if it constructs, else `FileBackend` at `default_file_path()` (error only if neither is possible, e.g. no home dir → `CredentialError::Backend`).

Dependency (verified against keyring 4.1.3 source): the `v1` feature exposes `keyring::Entry::new(service, user) -> keyring::Result<Entry>` with `set_password`/`get_password`/`delete_credential`, auto-selecting macOS Keychain / Windows Credential Manager / zbus secret-service. In `Cargo.toml`:

```toml
keyring = { version = "4", default-features = false, features = ["v1"] }
```

Storage convention: service `"datamancer"`, username = provider id, password = the serde-JSON of `ProviderCredentials`.

- [ ] **Step 1: Write the failing tests**

In `src/keychain.rs`, tests only first:

```rust
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
        assert!(["keychain", "secret-service"].contains(&KeychainBackend::NAME));
    }
}
```

And in `lib.rs`'s test module:

```rust
    #[test]
    fn open_default_always_selects_some_backend() {
        // On any host with a home dir this must succeed — keychain if the
        // platform store is up, else the file fallback. Never silently: the
        // name says which.
        let store = CredentialStore::open_default().expect("some backend");
        assert!(["keychain", "secret-service", "file"].contains(&store.backend_name()));
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-credentials 2>&1 | tail -5`
Expected: COMPILE ERROR.

- [ ] **Step 3: Implement `src/keychain.rs`**

```rust
//! The OS-keychain backend: macOS Keychain Services / Linux Secret Service
//! (D-Bus), via the `keyring` crate's classic API. Windows Credential
//! Manager is additive later through the same seam.
//!
//! Entries live under service `"datamancer"`, username = provider id,
//! password = the serde-JSON of the credential shape.

use datamancer_core::ProviderCredentials;

use crate::{CredentialBackend, CredentialError};

const SERVICE: &str = "datamancer";

pub struct KeychainBackend {
    _probe: (),
}

impl KeychainBackend {
    /// The platform's backend name (health-surface contract).
    pub const NAME: &'static str = if cfg!(target_os = "macos") {
        "keychain"
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
        keyring::Entry::new(SERVICE, provider)
            .map_err(|e| CredentialError::Backend(e.to_string()))
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
```

Note: `keyring::Error` is non-exhaustive — the wildcard arms are load-bearing, and error strings from the keychain never contain the stored password (they describe store/entry state). If clippy pedantic asks, `#[allow(clippy::match_same_arms)]` is acceptable on the `Ok(_) | Err(NoEntry)` probe match if it fires.

In `lib.rs`: `mod keychain;`, `pub use keychain::KeychainBackend;`, and:

```rust
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
```

- [ ] **Step 4: Run tests, the ignored keychain contract test, and `cargo deny`**

Run: `cargo test -p datamancer-credentials` → default tests pass.
Run: `cargo test -p datamancer-credentials -- --ignored` → the keychain contract test passes on the dev Mac (may prompt once for keychain access).
Run: `cargo deny check 2>&1 | tail -3` → **must** pass: the keyring/zbus subtree is all-new; if any transitive license falls outside the allowlist, STOP and report the exact crate+license to the controller rather than editing deny.toml yourself (allowlist additions are a project decision per deny.toml's own comment).
Run: `cargo clippy --all-targets -- -D warnings && cargo fmt`

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-credentials Cargo.lock
git commit -m "feat(credentials): OS keychain backend and default-store selection"
```

---

### Task 5: `CredentialsSource` on the Alpaca providers (hot-apply seam)

**Files:**
- Create: `crates/datamancer/src/providers/credentials.rs`
- Modify: `crates/datamancer/src/providers/mod.rs` (or wherever provider modules are declared — follow the existing layout)
- Modify: `crates/datamancer/src/providers/alpaca.rs`
- Modify: `crates/datamancer/src/providers/alpaca_crypto.rs`
- Modify: `crates/datamancer/src/lib.rs` (re-export `AlpacaCredentials`, `CredentialsSource`)

**Interfaces:**
- Consumes: `oxidized_alpaca::ApiKey` + `new_with_credentials` constructors (Task 1).
- Produces:
  - `AlpacaCredentials { pub key_id: String, pub secret: String }` — Clone, PartialEq, **redacting Debug**; `fn to_api_key(&self) -> oxidized_alpaca::ApiKey`.
  - `CredentialsSource` (Clone, Debug):
    - `Env` — legacy: clients constructed via the env-var path (`Client::new(account_type, …)`). Default. The library-embedder status quo.
    - `Static(AlpacaCredentials)` — fixed explicit credentials.
    - `Watch(tokio::sync::watch::Receiver<Option<AlpacaCredentials>>)` — live-updatable: each (re)connect reads the current value; a change forces a streaming reconnect; `None` means "no credentials yet" (REST unavailable, streaming waits instead of hammering bad auth).
  - `AlpacaProviderConfig.credentials: CredentialsSource` and `AlpacaCryptoProviderConfig.credentials: CredentialsSource` (new field, default `Env`; both structs already have `Default` impls — extend them).
  - `CredentialsSource::current(&self) -> Resolved` where `Resolved` is `pub(crate) enum { Env, Creds(AlpacaCredentials), Missing }` — the per-connect resolution helper both providers share.

This task is the **library-parity surface** (spec decision 9): an embedder passes `Static`/`Watch` (or wires the `datamancer-credentials` store to a watch channel) on the provider config. The spec's phrase "the builder gains a credential-source API" lands here — at provider-config level — because the builder consumes already-constructed providers; document that in the field docs.

- [ ] **Step 1: Write the failing tests**

In `crates/datamancer/src/providers/credentials.rs` (tests at bottom):

```rust
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer providers::credentials 2>&1 | tail -5`
Expected: COMPILE ERROR.

- [ ] **Step 3: Implement `providers/credentials.rs`**

```rust
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
    pub key_id: String,
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
```

- [ ] **Step 4: Wire into both providers**

This step adapts existing code, so read the current shapes first (`alpaca.rs`, `alpaca_crypto.rs`). The pattern, applied to both:

1. Add `pub credentials: CredentialsSource` to `AlpacaProviderConfig` (alpaca.rs:~106) and the crypto config, extending their `Default` impls with `credentials: CredentialsSource::Env`. Field doc: "Where this provider's credentials come from. This — not the `DatamancerBuilder` — is the library's credential-source API (spec decision 9); the builder consumes providers already constructed."
2. **REST clients** (constructed in `AlpacaProvider::new`, alpaca.rs:~141, currently `MarketDataClient::new(cfg.account_type).ok()`): replace the once-at-construction fields with a small per-provider helper that rebuilds when the source changes:

```rust
/// REST clients rebuilt whenever the credential source changes (watch
/// bump) — cheap relative to REST call frequency, and `has_changed` makes
/// the common path a no-op.
struct RestClients {
    market_data: Option<MarketDataClient>,
    trading: Option<TradingClient>,
}

fn build_rest(cfg: &AlpacaProviderConfig) -> RestClients {
    match cfg.credentials.current() {
        Resolved::Env => RestClients {
            market_data: MarketDataClient::new(cfg.account_type).ok(),
            trading: TradingClient::new(cfg.account_type).ok(),
        },
        Resolved::Creds(c) => {
            let key = c.to_api_key();
            RestClients {
                market_data: MarketDataClient::new_with_credentials(
                    cfg.account_type,
                    key.clone(),
                )
                .ok(),
                trading: TradingClient::new_with_credentials(cfg.account_type, key).ok(),
            }
        }
        Resolved::Missing => RestClients {
            market_data: None,
            trading: None,
        },
    }
}
```

   Store `RestClients` behind `std::sync::Mutex<RestClients>` plus a cached `watch` receiver; before each use (`fetch_history`, `list_instruments`), if `cfg.credentials.watch()` is `Some(rx)` and `rx.has_changed().unwrap_or(false)`, rebuild via `build_rest` and `rx.borrow_and_update()`. Keep the existing error paths (a `None` client already maps to the provider error today — preserve that mapping, alpaca.rs:191-194).
3. **Streaming** (`run_streaming_task`'s `'outer` reconnect loop, alpaca.rs:289-472; crypto analog at alpaca_crypto.rs:~336): at the top of each `'outer` iteration, resolve:

```rust
let client = match cfg.credentials.current() {
    Resolved::Env => StreamingStockClient::new(cfg.account_type, feed).await,
    Resolved::Creds(c) => {
        StreamingStockClient::new_with_credentials(cfg.account_type, feed, c.to_api_key())
            .await
    }
    Resolved::Missing => {
        // No credentials yet: wait for provisioning instead of hammering
        // bad auth, then retry the outer loop.
        if let Some(mut rx) = cfg.credentials.watch() {
            let _ = rx.changed().await;
            continue 'outer;
        }
        // Static/Env can't be Missing; defensive.
        continue 'outer;
    }
};
```

   (Verify `StreamingStockClient::new_with_credentials`'s exact arg order from Task 1; the crypto client is `(account_type, feed, api_key)`.)
4. **Hot reconnect**: inside the connected message loop, add a `select!` arm on the watch (only when the source is watchable): before the loop, `let mut cred_rx = cfg.credentials.watch();` then

```rust
tokio::select! {
    msg = stream.next() => { /* existing handling */ }
    changed = async { cred_rx.as_mut().expect("guarded").changed().await },
        if cred_rx.is_some() =>
    {
        if changed.is_ok() {
            tracing::info!(provider = %PROVIDER_ID, "credentials changed; reconnecting");
            // Emit the same ProviderDisconnected control the error path
            // emits, then reconnect immediately (skip backoff — this is
            // deliberate, not a failure).
            /* reuse the existing disconnect-control emission here */
            continue 'outer;
        }
    }
}
```

   Adapt names to the real loop (the explorer notes: disconnect control emission + `sleep_with_jitter` backoff live at alpaca.rs:476-514; reset/skip the backoff for credential-triggered reconnects). If the existing loop isn't already `select!`-shaped, restructure minimally — the invariant to preserve: every message-handling path stays byte-identical, the new arm only triggers reconnect.
5. Re-export `AlpacaCredentials` and `CredentialsSource` from `crates/datamancer/src/lib.rs` (next to the existing provider config re-exports).

- [ ] **Step 5: Run tests + gates**

Run: `cargo test -p datamancer && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: 4 new unit tests pass; all existing provider/session tests still green (the `Env` default preserves current behavior exactly).

- [ ] **Step 6: Commit**

```bash
git add crates/datamancer/src
git commit -m "feat(datamancer): injectable CredentialsSource on Alpaca providers with watch-driven hot reconnect"
```

---

### Task 6: Wire vocabulary — credential ops, reply fields, stable codes

**Files:**
- Modify: `crates/datamancer-client/src/protocol/uds.rs`
- Modify: `crates/datamancer-client/src/codes.rs`

**Interfaces:**
- Consumes: `datamancer_core::ProviderCredentials` (Task 2).
- Produces (Tasks 7–8 rely on exactly these):
  - `Request::SetCredentials { provider: String, credentials: ProviderCredentials }` (wire: `{"op":"set-credentials","provider":"alpaca","credentials":{"type":"api_key_pair",…}}`)
  - `Request::GetCredentials { provider: String }`, `Request::ClearCredentials { provider: String }`
  - `Reply.credentials: Option<ProviderCredentials>` + `Reply::credentials(creds: ProviderCredentials) -> Reply`
  - `Reply.credential_backend: Option<String>`; `Reply::pong` gains the backend: `pub fn pong(version: impl Into<String>, credential_backend: impl Into<String>) -> Self` (wire: `{"ok":true,"version":"…","credential_backend":"keychain"}`)
  - codes: `pub const CREDENTIALS_MISSING: &str = "credentials_missing";`, `pub const CREDENTIAL_BACKEND_UNAVAILABLE: &str = "credential_backend_unavailable";`, `pub const PERMISSION_DENIED: &str = "permission_denied";`
- The WS vocabulary (`protocol/ws.rs`) is untouched — credential ops are same-host-trust only.

- [ ] **Step 1: Write the failing tests**

In `uds.rs`'s test module:

```rust
#[test]
fn credential_ops_round_trip_documented_wire_shapes() {
    use datamancer_core::ProviderCredentials;
    let set: Request = serde_json::from_str(
        r#"{"op":"set-credentials","provider":"alpaca","credentials":{"type":"api_key_pair","key_id":"AKID","secret":"s"}}"#,
    )
    .expect("de");
    match &set {
        Request::SetCredentials { provider, credentials } => {
            assert_eq!(provider, "alpaca");
            assert!(matches!(credentials, ProviderCredentials::ApiKeyPair { .. }));
        }
        other => panic!("wrong variant: {other:?}"),
    }
    assert_eq!(
        serde_json::to_string(&set).unwrap(),
        r#"{"op":"set-credentials","provider":"alpaca","credentials":{"type":"api_key_pair","key_id":"AKID","secret":"s"}}"#
    );
    let get: Request =
        serde_json::from_str(r#"{"op":"get-credentials","provider":"alpaca"}"#).unwrap();
    assert!(matches!(get, Request::GetCredentials { .. }));
    let clear: Request =
        serde_json::from_str(r#"{"op":"clear-credentials","provider":"alpaca"}"#).unwrap();
    assert!(matches!(clear, Request::ClearCredentials { .. }));
}

#[test]
fn credentials_reply_and_backend_carrying_pong() {
    use datamancer_core::ProviderCredentials;
    let reply = serde_json::to_value(Reply::credentials(ProviderCredentials::ApiKeyPair {
        key_id: "AKID".to_string(),
        secret: "s".to_string(),
    }))
    .unwrap();
    assert_eq!(reply["ok"], serde_json::Value::Bool(true));
    assert_eq!(reply["credentials"]["type"], "api_key_pair");
    assert!(reply.get("version").is_none());

    let pong = serde_json::to_value(Reply::pong("0.3.0", "keychain")).unwrap();
    assert_eq!(pong["version"], "0.3.0");
    assert_eq!(pong["credential_backend"], "keychain");
    assert!(pong.get("credentials").is_none());
}

#[test]
fn new_credential_codes_are_stable() {
    assert_eq!(crate::codes::CREDENTIALS_MISSING, "credentials_missing");
    assert_eq!(
        crate::codes::CREDENTIAL_BACKEND_UNAVAILABLE,
        "credential_backend_unavailable"
    );
    assert_eq!(crate::codes::PERMISSION_DENIED, "permission_denied");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-client credential 2>&1 | tail -5`
Expected: COMPILE ERROR.

- [ ] **Step 3: Implement**

In `uds.rs` — three `Request` variants (after `Ping`), matching the kebab-case tag convention:

```rust
    /// Store (create or rotate) credentials for a configured provider.
    /// UDS-only, peer-cred gated; a configured provider hot-applies.
    SetCredentials {
        provider: String,
        credentials: datamancer_core::ProviderCredentials,
    },
    /// Read the stored credentials (the trade app reuses the same keys for
    /// its own trading connections — the daemon is the one copy).
    GetCredentials { provider: String },
    /// Remove stored credentials. The running provider keeps its last
    /// applied credentials until restart (there is no un-apply).
    ClearCredentials { provider: String },
```

Two `Reply` fields (after `version`), both `#[serde(default, skip_serializing_if = "Option::is_none")]`:

```rust
    /// Stored credentials (on `get-credentials`).
    pub credentials: Option<datamancer_core::ProviderCredentials>,
    /// The daemon's active credential-store backend (on `ping`).
    pub credential_backend: Option<String>,
```

Update `Reply::ok()`/`Reply::error` full literals with the two `None`s; add the constructor and change `pong`:

```rust
    /// Success carrying stored credentials (on `get-credentials`).
    #[must_use]
    pub fn credentials(creds: datamancer_core::ProviderCredentials) -> Self {
        Self {
            credentials: Some(creds),
            ..Self::ok()
        }
    }

    /// Success carrying the daemon version and active credential backend
    /// (on `ping`).
    #[must_use]
    pub fn pong(version: impl Into<String>, credential_backend: impl Into<String>) -> Self {
        Self {
            version: Some(version.into()),
            credential_backend: Some(credential_backend.into()),
            ..Self::ok()
        }
    }
```

Update the existing `ping_round_trips_and_reply_carries_version` test's `Reply::pong("0.1.0")` call site to the two-arg form. In `codes.rs`, append with doc comments in the file's style:

```rust
/// No credentials are stored for the named provider.
pub const CREDENTIALS_MISSING: &str = "credentials_missing";
/// The credential-store backend failed or is unavailable.
pub const CREDENTIAL_BACKEND_UNAVAILABLE: &str = "credential_backend_unavailable";
/// The connection's peer credentials failed the same-uid check.
pub const PERMISSION_DENIED: &str = "permission_denied";
```

Note: `Reply::pong`'s signature change breaks the daemon's `dispatch` arm and Task-5-cycle-1's `TokioEndpoint` — fix the daemon call site in this task (temporarily `Reply::pong(env!("CARGO_PKG_VERSION"), "unavailable")`; Task 7 wires the real backend) so the workspace stays green; `TokioEndpoint` only reads `version` and compiles unchanged.

- [ ] **Step 4: Run tests + gates**

Run: `cargo test -p datamancer-client && cargo test -p datamancerd && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: green (WS suites untouched and passing — proving the surface separation).

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-client/src crates/datamancerd/src
git commit -m "feat(control): set/get/clear-credentials vocabulary, backend-carrying pong, stable codes"
```

---

### Task 7: Daemon — peer-cred gate, credential hub, hot-apply, env deprecation

**Files:**
- Create: `crates/datamancerd/src/credentials.rs`
- Modify: `crates/datamancerd/src/main.rs` (module decl — follow how other modules are declared)
- Modify: `crates/datamancerd/src/server.rs` (peer-cred capture, off-actor credential ops, ping arm)
- Modify: `crates/datamancerd/src/config.rs` (`build_runtime` provider construction takes sources)
- Modify: `crates/datamancerd/Cargo.toml` (dep `datamancer-credentials`)
- Modify: `crates/datamancerd/README.md` (control-protocol section + a Credentials section)

**Interfaces:**
- Consumes: `CredentialStore`/`open_default` (Task 4), `CredentialsSource::Watch`/`AlpacaCredentials` (Task 5), vocabulary + codes (Task 6).
- Produces: `CredentialHub` — `pub(crate) struct CredentialHub { store: CredentialStore, senders: HashMap<String, watch::Sender<Option<AlpacaCredentials>>> }` with:
  - `CredentialHub::bootstrap(provider_ids: &[&str]) -> Result<(Arc<CredentialHub>, HashMap<String, CredentialsSource>), DaemonError>` — opens the default store; per provider id: stored creds seed `Some`, else env-var pair for that provider's account type seeds `Some` **with a deprecation warning log**, else `None`; returns the sources map for `build_runtime`.
  - `async fn set(&self, provider: &str, creds: ProviderCredentials) -> Reply`, `async fn get(&self, provider: &str) -> Reply`, `async fn clear(&self, provider: &str) -> Reply` — store I/O via `tokio::task::spawn_blocking`, hot-apply via the watch sender, stable-code error mapping (`unknown_provider` for an id with no sender, `bad_request` for a shape mismatch — only `ApiKeyPair` applies to alpaca providers, `credential_backend_unavailable` on store errors, `credentials_missing` on get-none).
  - `fn backend_name(&self) -> &'static str` — for the ping arm.

Design notes locked in:
- **Peer-cred gate**: in `handle_connection` (server.rs:737, the natural slot per the accept path), capture once: `let peer_uid = stream.peer_cred().ok().map(|c| c.uid());` (tokio `net` feature suffices). Credential ops are intercepted **off-actor** in the per-connection loop (exactly like `Instruments`, server.rs:757-762) — they do blocking store I/O and must not stall the actor. Gate: `if peer_uid != Some(rustix::process::geteuid().as_raw())` → `Reply::error(codes::PERMISSION_DENIED, "credential ops require the daemon owner's uid")`. `rustix` is already a datamancerd dependency (single-instance lock); enable its `process` feature if not already on. Extract the gate decision into a pure function for testing: `fn credential_op_permitted(peer_uid: Option<u32>, own_euid: u32) -> bool { peer_uid == Some(own_euid) }`.
- **Env fallback reading**: the daemon READS env vars (safe) — `ALPACA_PAPER_API_KEY_ID`/`ALPACA_PAPER_API_SECRET_KEY` or the `LIVE` pair per the provider's configured `account_type` — to seed the watch when the store is empty, logging: `tracing::warn!(provider, "credentials loaded from environment variables (deprecated); provision via set-credentials — env fallback will be removed once the broker is proven")`. This keeps existing deployments working while flipping authority to the store.
- **Provider ids**: use the exact ids the providers register (`Provider::id()` — verify by grepping `fn id` in `providers/alpaca.rs` / `alpaca_crypto.rs`; the control protocol already uses `"alpaca-crypto"` in its documented examples).
- **`build_runtime`**: gains a `sources: HashMap<String, CredentialsSource>` parameter; each provider config gets `credentials: sources.get(id).cloned().unwrap_or_default()`. `Server::bootstrap` calls `CredentialHub::bootstrap` first, passes sources in, and holds the `Arc<CredentialHub>`, handing a clone to `accept_loop` → `handle_connection`.
- **Ping**: the actor's `Request::Ping` arm becomes `Reply::pong(env!("CARGO_PKG_VERSION"), self.credential_backend_name)` — thread the name into the actor state at construction (a `&'static str` field; the hub itself stays out of the actor).

- [ ] **Step 1: Write the failing tests**

In `credentials.rs` (daemon-side), a test module covering the pure/fast parts:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use datamancer_core::ProviderCredentials;

    #[test]
    fn gate_requires_exact_uid_match() {
        assert!(credential_op_permitted(Some(501), 501));
        assert!(!credential_op_permitted(Some(502), 501));
        assert!(!credential_op_permitted(None, 501)); // unreadable peer = denied
    }

    #[tokio::test]
    async fn hub_set_hot_applies_and_get_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = datamancer_credentials::CredentialStore::with_backend(Box::new(
            datamancer_credentials::FileBackend::new(dir.path().join("c.json")),
        ));
        let (hub, sources) = CredentialHub::with_store(store, &["alpaca"]);
        // Watch seeded None (no stored creds, no env in this test's ids).
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
        assert_eq!(rx.borrow().as_ref().map(|c| c.key_id.clone()), Some("AKID".to_string()));
        // get round-trips from the STORE (single source of truth).
        let got = hub.get("alpaca").await;
        assert_eq!(got.credentials, Some(creds));
        // clear: store emptied, running provider unaffected (watch unchanged).
        assert!(hub.clear("alpaca").await.ok);
        assert_eq!(hub.get("alpaca").await.code.as_deref(), Some("credentials_missing"));
        assert!(rx.borrow().is_some(), "clear must not un-apply live credentials");
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
```

(`CredentialHub::with_store(store, ids)` is the env-free test constructor; `bootstrap` wraps it with `open_default()` + env seeding. Add `tempfile` to datamancerd's dev-deps if absent — it is already there per Task 7 of cycle 1.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd credential 2>&1 | tail -5`
Expected: COMPILE ERROR.

- [ ] **Step 3: Implement `credentials.rs`, then wire `server.rs`/`config.rs`**

`credentials.rs` core (fill in from the interfaces above; the load-bearing pieces):

```rust
//! The daemon-side credential broker: one store, one watch channel per
//! configured provider, stable-coded replies. Ops run off-actor (blocking
//! store I/O behind spawn_blocking) and are peer-cred gated same-uid.

use std::collections::HashMap;

use datamancer::{AlpacaCredentials, CredentialsSource};
use datamancer_core::ProviderCredentials;
use datamancer_credentials::{CredentialError, CredentialStore};
use tokio::sync::watch;

use crate::control::{Reply, codes};

/// Same-uid gate for credential ops. Unreadable peer credentials are denied,
/// not defaulted.
pub(crate) fn credential_op_permitted(peer_uid: Option<u32>, own_euid: u32) -> bool {
    peer_uid == Some(own_euid)
}

pub(crate) struct CredentialHub {
    store: CredentialStore,
    senders: HashMap<String, watch::Sender<Option<AlpacaCredentials>>>,
}

fn to_alpaca(creds: &ProviderCredentials) -> Option<AlpacaCredentials> {
    match creds {
        ProviderCredentials::ApiKeyPair { key_id, secret } => Some(AlpacaCredentials {
            key_id: key_id.clone(),
            secret: secret.clone(),
        }),
        _ => None,
    }
}

fn backend_error(e: &CredentialError) -> Reply {
    Reply::error(codes::CREDENTIAL_BACKEND_UNAVAILABLE, e.to_string())
}
```

with `with_store` seeding a `watch::channel(stored.and_then(|c| to_alpaca(&c)))` per id and returning `CredentialsSource::Watch(rx)` per provider; `bootstrap(provider_account_types: &[(String, AccountType)])` layering `open_default()` + the env fallback (read the pair per account type via `std::env::var`, seed, warn) — signature adapted to what `config.rs` can provide; `set`/`get`/`clear` as `spawn_blocking` closures over a cloned... note `CredentialStore` is not `Clone` — wrap it in `std::sync::Arc` inside the hub and clone the `Arc` into each `spawn_blocking` (the trait is `Send + Sync`). `set` maps: unknown id → `unknown_provider`; non-`ApiKeyPair` for alpaca ids → `bad_request`; store error → `credential_backend_unavailable`; success → persist **then** `sender.send_replace(Some(alpaca))` → `Reply::ok()`. `get`: store miss → `credentials_missing`, hit → `Reply::credentials(...)`. `clear`: store clear only (no un-apply) → `Reply::ok()`.

`server.rs` wiring:
- `handle_connection` signature gains `hub: Arc<CredentialHub>` and `own_euid: u32` (compute once in `Server::run` via `rustix::process::geteuid().as_raw()`); capture `peer_uid` before `into_split()`.
- In the per-connection request match (next to the `Instruments` intercept), intercept the three credential variants: gate first (`permission_denied` reply on failure), then `hub.set/get/clear(...).await`.
- `reject_unknown_keys` (server.rs:682) — confirm the new variants pass through it like existing ops (it validates against the enum; no change expected, but the wire tests in Task 6 + an e2e in Task 8 cover it).
- Actor `Ping` arm: `Reply::pong(env!("CARGO_PKG_VERSION"), self.credential_backend)` where `credential_backend: &'static str` is set at bootstrap from `hub.backend_name()` (replacing Task 6's temporary `"unavailable"`).

`config.rs`: `build_runtime(sources: HashMap<String, CredentialsSource>)` — thread each provider's source into its config struct; `Server::bootstrap` builds the hub first (needs the configured provider ids + account types from `Config` before `build_runtime` — read them off the same `ProviderConfig` sections).

`README.md`: add the three ops to the control-protocol example block; a short **Credentials** section: daemon-owned store (keychain → file fallback, visible via `ping`/health), UDS-only + same-uid gated, never on WS; hot-apply semantics (set applies live via provider reconnect; clear does not un-apply); env vars deprecated (warning at boot, removal after the broker is proven); `get-credentials` exists because apps reuse the keys for trading connections.

- [ ] **Step 4: Run tests + gates**

Run: `cargo test -p datamancerd && cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: 3 new tests + all existing green (68+ in datamancerd).

- [ ] **Step 5: Commit**

```bash
git add crates/datamancerd Cargo.lock
git commit -m "feat(datamancerd): credential broker — peer-cred gate, hub, hot-apply, env deprecation"
```

---

### Task 8: Facade methods + health backend fill

**Files:**
- Modify: `crates/datamancer-client/src/app/mod.rs`
- Modify: `crates/datamancer-client/src/app/platform.rs` (`TokioEndpoint` returns backend too)
- Modify: `crates/datamancer-client/src/app/lifecycle.rs` (`ensure_daemon` return carries it)
- Modify: `crates/datamancer-client/README.md` (app-facade section: credential methods)

**Interfaces:**
- Consumes: vocabulary (Task 6), `ProviderCredentials` (Task 2), existing `AppHandle`/`fill_health`.
- Produces:
  - `AppHandle::set_credentials(&mut self, provider: &str, credentials: ProviderCredentials) -> Result<(), ClientError<Iceoryx2ClientError>>`
  - `AppHandle::get_credentials(&mut self, provider: &str) -> Result<ProviderCredentials, ClientError<Iceoryx2ClientError>>` (a `credentials_missing` rejection surfaces as `ClientError::Control` with that code — the two-layer error model, no new variant)
  - `AppHandle::clear_credentials(&mut self, provider: &str) -> Result<(), ClientError<Iceoryx2ClientError>>`
  - `AppHandle::health()` now stamps `daemon.credential_backend` from the ping handshake (alongside `version`).

Mechanics: the `Client` trait has no credential methods (they're facade/app-level, UDS-only — deliberately NOT added to the transport-generic trait, which the WS client also implements). `Iceoryx2Client`'s control connection is private — add `pub(crate)` passthrough(s) on `Iceoryx2Client` for a raw `Request` → `Reply` round-trip (`pub(crate) async fn control_request(&mut self, req: &Request) -> Result<Reply, ClientError<Iceoryx2ClientError>>` using the existing `ControlConn::request` + `check`), and build the three facade methods on it. `ensure_daemon`/`TokioEndpoint::ping` return type changes from `String` to a small `pub(crate) struct DaemonHello { version: String, credential_backend: Option<String> }` (older daemons' pongs lack the field — `Option` keeps the probe compatible); `AppHandle` stores both and `fill_health` gains the backend parameter.

- [ ] **Step 1: Write the failing tests**

In `app/mod.rs` tests:

```rust
#[test]
fn health_fill_sets_backend_alongside_version() {
    use datamancer_core::{CacheSnapshot, SystemSnapshot, Timestamp};
    let snap = SystemSnapshot::new(
        Timestamp(1),
        vec![],
        CacheSnapshot::new(vec![], None),
        vec![],
        vec![],
    );
    let view = fill_health(&snap, "0.3.0", Some("keychain"));
    assert_eq!(view.daemon.version.as_deref(), Some("0.3.0"));
    assert_eq!(view.daemon.credential_backend.as_deref(), Some("keychain"));
    let older = fill_health(&snap, "0.3.0", None);
    assert!(older.daemon.credential_backend.is_none());
}
```

In `app/platform.rs` tests, extend the fake-daemon ping test: a reply `{"ok":true,"version":"9.9.9","credential_backend":"file"}` yields `DaemonHello { version: "9.9.9", credential_backend: Some("file") }`, and the existing version-only reply yields `credential_backend: None`. Update the scripted-endpoint fakes in `lifecycle.rs` tests mechanically (`Ok("0.1.0".to_string())` → `Ok(DaemonHello { version: "0.1.0".to_string(), credential_backend: None })` — a small helper `hello("0.1.0")` keeps it readable).

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-client --features app 2>&1 | tail -5`
Expected: COMPILE ERROR.

- [ ] **Step 3: Implement**

- `lifecycle.rs`: define `DaemonHello`; `ControlEndpoint::ping` and `ensure_daemon` return `Result<DaemonHello, …>`; the version gate in `mod.rs` reads `hello.version`.
- `platform.rs`: parse both fields from the pong `Reply`.
- `iceoryx2.rs`: the `pub(crate) control_request` passthrough.
- `app/mod.rs`: store `daemon_hello: DaemonHello`; `fill_health(snapshot, version, backend)`; the three methods:

```rust
    /// Store (create or rotate) provider credentials in the daemon's broker.
    /// Applies live: a configured provider reconnects with the new
    /// credentials.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with the stable codes (`unknown_provider`,
    /// `bad_request`, `credential_backend_unavailable`, `permission_denied`)
    /// or a transport failure.
    pub async fn set_credentials(
        &mut self,
        provider: &str,
        credentials: ProviderCredentials,
    ) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client
            .control_request(&Request::SetCredentials {
                provider: provider.to_string(),
                credentials,
            })
            .await
            .map(|_| ())
    }
```

  (`get_credentials` maps a missing `credentials` field on an ok reply to a protocol error, mirroring the `open-client` missing-service pattern; `clear_credentials` is the `map(|_| ())` shape.)
- README: document the three methods and that they are same-host/UDS-only and peer-cred gated; note `health().daemon.credential_backend`.

- [ ] **Step 4: Run tests + gates**

Run: `cargo test -p datamancer-client --features app && cargo test && cargo clippy --all-targets -- -D warnings && cargo clippy -p datamancer-client --all-targets --features app -- -D warnings && cargo fmt`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-client
git commit -m "feat(client): AppHandle credential methods and credential-backend health fill"
```

---

### Task 9: E2E, version bumps via local gates, docs sweep

**Files:**
- Create: `crates/datamancerd/tests/credential_broker_e2e.rs`
- Modify: crate versions as the semver gate directs (expected: `datamancer-core` patch/minor, `datamancer` breaking→`0.3.0`, `datamancer-client` + `datamancerd` lockstep `0.3.0`)
- Modify: root `CLAUDE.md` (crate count + new crate bullet), `crates/datamancer/README.md` if it documents provider config, spec status note

**Interfaces:**
- Consumes: everything above; `daemon_and_client_versions_stay_in_lockstep` (existing regression test) enforces the paired bump.

- [ ] **Step 1: Write the e2e (`#[ignore]`d, mirrors `app_facade_e2e.rs` conventions — temp config/socket, `--test-threads=1`, lockfile-pid `stop_daemon`)**

The strongest possible proof: the daemon is spawned **without** Alpaca env vars (scrub them from the child env — the facade's `ProcessSpawner` inherits the parent env, so scrub via a config… no: spawn the daemon directly with `Command::env_remove`, like `daemon_e2e.rs` spawns, rather than through the facade), then credentials arrive **only** through the broker:

```rust
//! End-to-end: the credential broker against the real binary, with NO
//! ALPACA_* env vars in the daemon's environment — credentials arrive only
//! via set-credentials. #[ignore]d: needs live iceoryx2 + paper credentials
//! in the TEST's env. Run:
//! `cargo test -p datamancerd --test credential_broker_e2e -- --ignored --test-threads=1`

#[tokio::test]
#[ignore = "needs live iceoryx2 runtime, paper credentials, host-global lock"]
async fn broker_provisions_credentials_and_provider_connects() {
    // 1. Spawn datamancerd with Command::env_remove for all four ALPACA_*
    //    vars (config: alpaca_crypto paper + [server] admin_socket in a
    //    tempdir — copy app_facade_e2e.rs's fixture).
    // 2. Connect AppHandle (facade), assert health: provider present, and
    //    health.daemon.credential_backend is Some (broker active).
    // 3. get-credentials -> ClientError::Control(credentials_missing).
    // 4. set_credentials with the TEST env's paper key pair
    //    (std::env::var("ALPACA_PAPER_API_KEY_ID") etc. — read in the test,
    //    sent over the wire).
    // 5. Subscribe BTC/USD trades; within a bounded wait, snapshot shows the
    //    provider Connected (the watch seeded None, set hot-applied, the
    //    streaming task connected with brokered creds).
    // 6. get_credentials round-trips the pair; clear_credentials ok; a
    //    second get is credentials_missing again while the stream stays up
    //    (clear does not un-apply).
    // 7. close + stop_daemon.
}
```

Write it as real code following those numbered steps (the comment block above is the test's skeleton contract — every numbered step becomes real assertions; reuse `app_facade_e2e.rs`'s `write_config`/`stop_daemon` helpers by extracting them into a small shared `tests/util/mod.rs` if that's cleaner than duplication).

- [ ] **Step 2: Verify compile + ignored-by-default; then run live**

Run: `cargo test -p datamancerd --test credential_broker_e2e` → `1 ignored`.
Run: `cargo test -p datamancerd --test credential_broker_e2e -- --ignored --test-threads=1` with paper creds in the test env → 1 passed. If the environment lacks credentials, report DONE_WITH_CONCERNS with the captured failure — do not claim a pass.

- [ ] **Step 3: Version bumps via the local gates**

Run: `git fetch origin main && cargo deny check && .github/scripts/semver-checks.sh origin/main`
Expected: deny passes (keyring tree already vetted in Task 4); semver FAILS naming the crates needing bumps. Apply exactly what it demands: expected `datamancer-core` → `0.1.1` (additive: new enum + non_exhaustive field), `datamancer` → `0.3.0` (new pub config fields on constructible structs), `datamancer-client` → `0.3.0` and `datamancerd` → `0.3.0` **together** (Request/Reply additions; the lockstep test fails any solo bump). Update the README ping example version string (`crates/datamancerd/README.md`). Re-run the semver script until clean. Also fix the skew-gate expectation if needed (it derives from `CARGO_PKG_VERSION` — no change expected).

- [ ] **Step 4: Docs sweep**

- Root `CLAUDE.md`: "six crates" → "seven"; add the `datamancer-credentials` bullet (one store, two consumers, keychain→file, blocking API, no orchestrator dep); note the daemon env-var deprecation in the datamancerd bullet's credential sentence (README.md:37-40's env-var statement also needs the deprecation note — that's `crates/datamancerd/README.md`, already touched in Task 7).
- Spec: append a status line to the cycle-2 section: implemented on `feature/credential-broker`, noting the two recorded deviations — credential-source API lands on provider configs (not the builder), and `clear` does not un-apply live credentials.

- [ ] **Step 5: Full gates + commit**

Run: `cargo test && cargo test -p datamancer-client --features app && cargo clippy --all-targets -- -D warnings && cargo clippy -p datamancer-client --all-targets --features app -- -D warnings && cargo fmt --check && cargo deny check && .github/scripts/semver-checks.sh origin/main`
Expected: everything clean.

```bash
git add -A
git commit -m "test(datamancerd): credential-broker e2e; version bumps and docs for cycle 2"
```

---

## Self-review notes (already applied)

- **Spec coverage** (cycle-2 section): tagged per-provider shapes (T2), `CredentialBackend` keychain/secret-service/file chosen at runtime + visible in health (T3/T4/T2), set/get/clear ops UDS-only peer-cred gated (T6/T7), hot-apply via provider reconnect riding existing `Control` connectivity events (T5/T7), `get-credentials` for app key reuse (T6/T8), env deprecation everywhere-with-parity (daemon warns now, T7; library `Env` variant documented deprecated-for-daemon, removal deferred until "the store is proven" per spec), `[ws].auth_token` migration explicitly deferred (spec says "can migrate later" — out of scope, recorded here).
- **Recorded deviations**: (1) the spec's "the `Datamancer` builder gains a credential-source API" is delivered as `CredentialsSource` on the provider configs — the builder consumes constructed providers; this is the honest surface and is documented in the field docs + spec status note. (2) `clear-credentials` does not un-apply from a running provider (no un-auth primitive exists); documented in op docs + README. (3) `Reply::pong` signature change is internal-API-breaking for the client crate — covered by the 0.3.0 lockstep bump.
- **Placeholder scan**: Task 7's `credentials.rs` shows the load-bearing pieces with the remainder specified by exact interface signatures and behavior tables (mapping rules per op enumerated); Task 9's e2e skeleton enumerates every assertion as a numbered contract. No TBDs.
- **Type consistency**: `DaemonHello` produced in T8-lifecycle and consumed by the gate/facade in the same task; `Reply::pong(version, backend)` two-arg form used consistently in T6 (temporary `"unavailable"` daemon arm) and T7 (real backend); `CredentialsSource::Watch(Receiver<Option<AlpacaCredentials>>)` identical in T5 (provider), T7 (hub seeding); `contract_tests` public in T3, reused in T4.
- **Cross-cutting risk named for reviewers**: the streaming-loop restructure in T5 step 4 is the highest-risk edit (touches the reconnect loop's control-event emission) — the task pins the invariant (message-handling paths byte-identical; new arm only triggers reconnect) and existing provider tests plus the T9 e2e cover it.
