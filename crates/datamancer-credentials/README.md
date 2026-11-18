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

- **`KeychainBackend`** — the OS store via the `keyring` crate: macOS
  Keychain (`"keychain"`), Linux Secret Service over D-Bus
  (`"secret-service"`), Windows Credential Manager (`"credential-manager"`).
  Entries live under service `"datamancer"`, username = provider id.
- **`FileBackend`** — one JSON file (`{provider: ProviderCredentials}`),
  created with owner-only permissions (`0o600` on Unix) and written
  atomically (tmp file + rename), the same pattern `datamancerd` uses for its
  own config writes.
- **`CredentialStore::open_default()`** prefers the keychain and falls back
  to `FileBackend` at `default_file_path()` on headless hosts where no
  keychain/secret-service is reachable; with neither (no keychain and no home
  directory) it fails. The active choice is never silent —
  `CredentialStore::backend_name()` surfaces it, and `datamancerd` threads it
  through `ping` and `HealthView` so an unexpected fallback is visible to an
  operator.

### Testing/ops override: `DATAMANCER_CREDENTIALS_FILE`

When the `DATAMANCER_CREDENTIALS_FILE` env var is set (non-empty),
`CredentialStore::open_default()` uses the `FileBackend` at that path
unconditionally — the keychain is skipped, and the backend name stays
`"file"`. This is an escape hatch for tests and operators (e.g. the daemon
e2e suite points a spawned `datamancerd`'s broker at a tempdir so it never
touches the developer's real keychain). It is **not** a supported
configuration surface; provision real deployments through the default
selection.

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
the named provider). `pub` so the keychain backend runs the identical suite
(in its `#[ignore]`d test, which touches the real OS keychain) instead of
duplicating assertions.

[`CredentialBackend`]: ./src/lib.rs
[`ProviderCredentials`]: ../datamancer-core/src/credentials.rs
