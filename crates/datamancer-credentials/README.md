# datamancer-credentials

Credential storage for datamancer providers: an OS keychain backend where
available, a permissions-locked file elsewhere — one [`CredentialBackend`]
trait, chosen at runtime, with the choice always visible.

This crate depends on `datamancer-core` only; it never depends on the
`datamancer` orchestrator. `datamancerd` and in-process embedders both build
on it directly (see below).

## The `CredentialBackend` trait

```rust,ignore
pub trait CredentialBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn get(&self, provider: &str) -> Result<Option<ProviderCredentials>, CredentialError>;
    fn set(&self, provider: &str, creds: &ProviderCredentials) -> Result<(), CredentialError>;
    fn clear(&self, provider: &str) -> Result<(), CredentialError>;
}
```

Keyed by provider id (`"alpaca-crypto"`, …); values are
[`ProviderCredentials`] (from `datamancer-core`), tagged per provider shape
rather than a universal key/secret pair.

The trait is **synchronous and blocking** — OS keychain APIs are blocking by
nature. Async callers wrap calls in `tokio::task::spawn_blocking`.

## Backend selection

- **This task:** `FileBackend` only — one JSON file
  (`{provider: ProviderCredentials}`), created with owner-only permissions
  (`0o600` on Unix) and written atomically (tmp file + rename), the same
  pattern `datamancerd` uses for its own config writes.
- **Task 4** adds a keychain backend (via the `keyring` crate) and
  `CredentialStore::open_default()`, which prefers the keychain and falls
  back to `FileBackend` on headless hosts where no keychain/secret-service is
  reachable. The active choice is never silent — `CredentialStore::backend_name()`
  surfaces it, and `datamancerd` threads it through `HealthView` so an
  unexpected fallback is visible to an operator.
- **Windows** (Credential Manager) is additive later, as another
  implementation of the same trait — not a widened enum or a new code path
  in the store.

## Default file path

```rust,ignore
pub fn default_file_path() -> Option<PathBuf>;
```

`<data dir>/credentials.json` via `ProjectDirs::from("", "", "datamancer")`
— macOS `~/Library/Application Support/datamancer/credentials.json`, Linux
`~/.local/share/datamancer/credentials.json` — the same convention
`datamancer-client::paths` uses for the control socket and daemon log.

## Two consumers

- **`datamancerd` (the broker).** The daemon owns the store and exposes
  credential ops over its control surface; provider processes never see raw
  secrets, only the daemon that holds the backend.
- **In-process embedders.** A library consumer that doesn't run
  `datamancerd` can construct a `CredentialStore` directly — same trait, same
  contract, no daemon required (library parity).

## Contract tests

```rust,ignore
pub fn contract_tests(backend: &dyn CredentialBackend);
```

The shared behavior every backend must satisfy — round-trip, overwrite,
per-provider isolation, and clear semantics (absent is `Ok`, and only removes
the named provider). `pub` so Task 4's keychain backend runs the identical
suite (including its `#[ignore]`d, platform-only tests) instead of
duplicating assertions.

[`CredentialBackend`]: ./src/lib.rs
[`ProviderCredentials`]: ../datamancer-core/src/credentials.rs
