# datamancer-credentials

Credential storage for datamancer providers: OS keychain (Task 4) with a
locked-down file fallback.

## Invariants / stance

- **`#![forbid(unsafe_code)]`**, `[lints] workspace = true`.
- **Depends on `datamancer-core` only — never the `datamancer` orchestrator.**
  This crate is consumed by `datamancerd` (the broker) and by embedders
  in-process; it never depends back on either.
- **The API is synchronous and blocking, by design.** OS keychain APIs are
  blocking; async callers (`datamancerd`, embedders on tokio) wrap calls in
  `tokio::task::spawn_blocking`. Do not add an async variant here.
- **`name()` strings are a health-surface contract.** `"keychain"`,
  `"secret-service"`, `"credential-manager"` (Windows), `"file"` — whatever a
  backend returns is surfaced
  through `HealthView` so a silent fallback to the file backend is never
  invisible to an operator. Treat existing name strings as stable; adding a
  new backend adds a new name, it doesn't rename an old one.
- **No secret material in errors, `Debug`, or logs — anywhere in this
  crate.** `CredentialError::Backend` messages must be pre-scrubbed by the
  caller before construction; this crate never logs a `ProviderCredentials`
  value itself (its `Debug` impl in `datamancer-core` already redacts
  secrets, but don't rely on that as the only guard).
- **`contract_tests` is the shared behavior gate.** Every backend
  (`FileBackend` here; the keychain backend in Task 4, including its
  `#[ignore]`d platform tests) runs the identical suite in `lib.rs`. Changing
  the contract is a breaking change to every backend at once — do it
  deliberately.
- **`FileBackend` writes are atomic (tmp + rename) and owner-only
  (`0o600`).** A pre-existing file's mode is not trusted; every `save`
  re-creates the tmp file at `0o600` before the rename, so mode is
  re-established on every write, not just at first creation.
- **`open_default()` arrives in Task 4** once the keychain backend exists to
  pick between; this task only exposes `default_file_path()` and
  `CredentialStore::with_backend`.
