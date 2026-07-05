# Config Service (Cycle 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Runtime enable/disable of compiled-in providers plus the daemon config service: `get-config` / `configure-provider` / `remove-provider` / `shutdown` UDS ops with atomic TOML persistence and a hot/cold field-classification table shared with the web UI.

**Architecture:** Generalize cycle 2's credentials `Watch(None)` parking into a per-provider *settings* watch (`None` = disabled, `Some(settings)` = enabled with those settings). Every compiled-in provider is constructed and registered at boot — the immutable `Datamancer` registry is untouched — and parks unless enabled. A daemon-side `ConfigHub` (sibling of `CredentialHub`) owns the watch senders and the config file: validate → atomically persist → `send_replace` (persist-then-apply, per-hub-lock serialized). The web UI's config writer routes through the same hub so the daemon has exactly one hot-path writer.

**Tech Stack:** Rust edition 2024, tokio (`sync::watch`, `spawn_blocking`), serde/serde_json/toml, existing datamancer workspace crates.

## Global Constraints

- `clippy::pedantic = deny` workspace-wide; `#![forbid(unsafe_code)]` in all seven crates.
- **No secret material in logs, errors, or `Debug` output anywhere.** Credentials never appear in the config file.
- Mutating config ops and `shutdown` are **UDS-only, peer-cred gated same-uid** (same gate as credential ops); they must never be reachable on the WS surface.
- Stable JSON error codes are an operator contract; new codes this cycle: `restart_required`, `unknown_config_field` (spec §Error handling, verbatim).
- Mutating-op flow, verbatim from spec: "validate → apply live if hot-classified → atomically persist TOML → reply `{"applied":"live"}` or `{"applied":"restart_required"}`". Persist-then-apply ordering within that flow (a store failure leaves the running provider untouched), matching `CredentialHub`.
- "Every config field is classified hot or cold in one table … A new field without a classification fails the build" (enforced via an exhaustiveness test over a fully-populated config).
- Concurrent config writers serialize: "last-write-wins per op, never a torn file. The daemon is the sole hot-path writer; operator hand-edits are read at boot only." (Recorded deviation: serialization happens through the `ConfigHub`'s single lock rather than the control actor — the ops run off-actor like the credential ops; the guarantee is identical.)
- Provider lifecycle (cycle-3 revision, verbatim): "the provider set is **fixed at build time** … runtime configuration only flips compiled-in providers between disabled and enabled. Every compiled-in provider is constructed and registered at boot … but starts **disabled (parked)** unless the persisted config enables it."
- Library parity (spec decision 9): every new capability must surface through the library API too — the settings source lands on each provider's config struct (`Static`/`Watch`), like `CredentialsSource`. Embedder default is `Static` (= always enabled), preserving current embedder behavior.
- `datamancer-client` and `datamancerd` versions bump **in lockstep** (ping version gate; pinned by `daemon_and_client_versions_stay_in_lockstep`). This cycle: both 0.3.0 → **0.4.0**; `datamancer` 0.3.0 → **0.4.0** (breaking provider-config change).
- tokio watch discipline (cycle-2 lessons): fresh receiver clones get `mark_unchanged()`; `has_changed() == Err` (closed channel) counts as one final change; capture receivers **before** building clients (receiver-before-build ordering).
- Windows CI builds only the ws-portable subset; any path-shape test assertions must be cfg'd per-OS.
- Before the PR: `git fetch origin main && cargo deny check && .github/scripts/semver-checks.sh origin/main`.

## File Structure

- `crates/datamancer/src/providers/runtime.rs` — **new**: generic `SettingsSource<T>` + `watch_changed<T>` (the watch plumbing both providers share).
- `crates/datamancer/src/providers/alpaca.rs`, `alpaca_crypto.rs` — settings-driven connect/park/reconnect; REST rebuild on settings *or* credential change.
- `crates/datamancer/src/providers/mod.rs`, `crates/datamancer/src/lib.rs` — re-exports.
- `crates/datamancerd/src/config.rs` — zero-provider configs become valid; `build_runtime` constructs **all** compiled-in providers with watch sources.
- `crates/datamancerd/src/config_class.rs` — **new**: hot/cold classification table + cold-divergence diff.
- `crates/datamancerd/src/config_hub.rs` — **new**: the config service (watch senders, persist-then-apply, get/configure/remove/apply_full).
- `crates/datamancerd/src/credentials.rs` — hub seeds all compiled-in provider ids; gate renamed `privileged_op_permitted`.
- `crates/datamancerd/src/server.rs` — op dispatch, shutdown control-flow, wiring.
- `crates/datamancerd/src/web/config_api.rs` — `ConfigState::write` delegates to `ConfigHub` (single writer).
- `crates/datamancerd/src/paths.rs` — scaffold template: providers commented out (disabled by default).
- `crates/datamancer-client/src/protocol/uds.rs`, `codes.rs` — wire vocabulary.
- `crates/datamancer-client/src/app/mod.rs` — facade methods.
- `crates/datamancerd/tests/config_service_e2e.rs` — **new** `#[ignore]`d e2e.

---

### Task 1: Generic settings source (`SettingsSource<T>` + `watch_changed<T>`)

**Files:**
- Create: `crates/datamancer/src/providers/runtime.rs`
- Modify: `crates/datamancer/src/providers/mod.rs` (module + re-export)
- Modify: `crates/datamancer/src/providers/credentials.rs` (delegate `rest_credentials_changed` to the generic; move its behavior tests)

**Interfaces:**
- Produces: `pub enum SettingsSource<T> { Static(T), Watch(watch::Receiver<Option<T>>) }` with `pub fn current(&self) -> Option<T>` (T: Clone), `pub(crate) fn watch(&self) -> Option<watch::Receiver<Option<T>>>` (mark_unchanged'd clone), and `pub(crate) fn watch_changed<T>(rx: &mut Option<watch::Receiver<T>>) -> bool`. Tasks 2–3 consume all three; Task 7 constructs `SettingsSource::Watch` values.

- [ ] **Step 1: Write the failing tests**

Create `crates/datamancer/src/providers/runtime.rs` with the tests first (module body empty apart from imports so the file compiles standalone is not possible in Rust — write tests together with stubs that `todo!()`, or simply write tests and implementation in one commit; for this task use the test-first *content* below and verify red by stubbing `current` with `todo!()`):

```rust
//! Injectable runtime-settings sources for providers (spec 2026-07-05,
//! cycle-3 revision). This is the enable/disable + hot-settings seam: the
//! daemon hands a provider a `Watch` source; `None` = disabled (the provider
//! parks), `Some(settings)` = enabled with those settings. `Static` is the
//! embedder default: always enabled, settings fixed at construction.

use tokio::sync::watch;

/// Where a provider gets its runtime settings, resolved fresh at every
/// (re)connect. `None` from a `Watch` source means the provider is disabled.
#[derive(Clone, Debug)]
pub enum SettingsSource<T> {
    /// Fixed settings; the provider is always enabled. The embedder default.
    Static(T),
    /// Live-updatable source (the daemon's config service). `None` =
    /// disabled: the streaming task parks and REST calls fail unavailable.
    Watch(watch::Receiver<Option<T>>),
}

impl<T: Clone> SettingsSource<T> {
    /// The current settings, or `None` when a `Watch` source is disabled.
    pub fn current(&self) -> Option<T> {
        match self {
            Self::Static(s) => Some(s.clone()),
            Self::Watch(rx) => rx.borrow().clone(),
        }
    }

    /// The watch receiver, when this source is watchable. The clone is
    /// returned with the current value marked seen (tokio's
    /// `Receiver::clone` copies the *original* receiver's seen version;
    /// without `mark_unchanged` every clone handed out after the first
    /// change would report `has_changed` immediately — the reconnect-storm
    /// bug from cycle 2).
    pub(crate) fn watch(&self) -> Option<watch::Receiver<Option<T>>> {
        match self {
            Self::Watch(rx) => {
                let mut rx = rx.clone();
                rx.mark_unchanged();
                Some(rx)
            }
            Self::Static(_) => None,
        }
    }
}

/// Whether a cached watch receiver has an unseen change, consuming the
/// marker. Shared by the providers' REST rebuild-on-use guards for both the
/// credential and the settings receivers.
///
/// On a closed channel (sender dropped) tokio's `has_changed` returns `Err`
/// even when a final unseen value is pending, so `Err` counts as changed —
/// the caller rebuilds once with the last value — and the receiver is
/// dropped so subsequent calls return `false` instead of rebuilding forever.
pub(crate) fn watch_changed<T>(rx_slot: &mut Option<watch::Receiver<T>>) -> bool {
    let Some(rx) = rx_slot.as_mut() else {
        return false;
    };
    match rx.has_changed() {
        Ok(true) => {
            let _ = rx.borrow_and_update();
            true
        }
        Ok(false) => false,
        Err(_) => {
            *rx_slot = None;
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SettingsSource, watch_changed};

    #[test]
    fn static_source_is_always_enabled() {
        let src = SettingsSource::Static(7_u32);
        assert_eq!(src.current(), Some(7));
        assert!(src.watch().is_none());
    }

    #[test]
    fn watch_source_none_is_disabled_and_tracks_updates() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        assert_eq!(src.current(), None);
        tx.send(Some(7_u32)).unwrap();
        assert_eq!(src.current(), Some(7));
    }

    #[test]
    fn watch_clone_does_not_see_pre_clone_sends() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        tx.send(Some(1_u32)).unwrap();
        let fresh = src.watch().expect("watchable");
        assert_eq!(fresh.has_changed().ok(), Some(false));
    }

    #[test]
    fn watch_clone_sees_post_clone_sends() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        let fresh = src.watch().expect("watchable");
        tx.send(Some(1_u32)).unwrap();
        assert_eq!(fresh.has_changed().ok(), Some(true));
    }

    #[test]
    fn watch_changed_consumes_the_marker() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        let mut cached = src.watch();
        assert!(!watch_changed(&mut cached));
        tx.send(Some(1_u32)).unwrap();
        assert!(watch_changed(&mut cached));
        assert!(!watch_changed(&mut cached));
    }

    #[test]
    fn watch_changed_syncs_once_on_closed_channel() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        let mut cached = src.watch();
        tx.send(Some(1_u32)).unwrap();
        drop(tx);
        assert!(watch_changed(&mut cached));
        assert!(!watch_changed(&mut cached));
        assert!(!watch_changed(&mut cached));
    }

    #[test]
    fn watch_changed_ignores_static_sources() {
        let mut cached = SettingsSource::Static(1_u32).watch();
        assert!(!watch_changed(&mut cached));
    }
}
```

- [ ] **Step 2: Wire the module and re-export**

In `crates/datamancer/src/providers/mod.rs`, alongside the existing `pub mod credentials;`-style declarations (this module is `#[cfg(feature = "provider-alpaca")]`-shaped — match the surrounding gating exactly): add `pub mod runtime;` and re-export `pub use runtime::SettingsSource;` next to the existing `CredentialsSource` re-export. Mirror in `crates/datamancer/src/lib.rs` where `CredentialsSource` is re-exported (`pub use providers::…`).

- [ ] **Step 3: Delegate `rest_credentials_changed`**

In `crates/datamancer/src/providers/credentials.rs`, replace the body of `rest_credentials_changed` (keep the function and its doc comment — Tasks 2–3 remove it once call sites migrate):

```rust
pub(crate) fn rest_credentials_changed(
    cred_rx: &mut Option<tokio::sync::watch::Receiver<Option<AlpacaCredentials>>>,
) -> bool {
    super::runtime::watch_changed(cred_rx)
}
```

Leave the existing `rest_change_detection_*` tests in `credentials.rs` untouched — they now pin the delegation and stay green.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p datamancer providers::runtime providers::credentials`
Expected: PASS (all new `runtime` tests plus the existing credentials tests).

Run: `cargo clippy -p datamancer --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer/src/providers/runtime.rs crates/datamancer/src/providers/mod.rs crates/datamancer/src/providers/credentials.rs crates/datamancer/src/lib.rs
git commit -m "feat(datamancer): generic SettingsSource<T> and watch_changed for provider runtime settings"
```

---

### Task 2: Alpaca equities provider — settings-driven enable/disable

**Files:**
- Modify: `crates/datamancer/src/providers/alpaca.rs`
- Modify: `crates/datamancer/src/lib.rs` (re-export `AlpacaSettings`)
- Modify: `crates/datamancerd/src/config.rs:669-679` (compile shim only — proper wiring is Task 7)

**Interfaces:**
- Consumes: `SettingsSource<T>`, `watch_changed` (Task 1).
- Produces: `pub struct AlpacaSettings { pub account_type: AccountType }` (Clone, Copy, Debug, PartialEq, Eq); `AlpacaProviderConfig.settings: SettingsSource<AlpacaSettings>` replacing `account_type` (breaking); parked-provider behavior: subscribe on a disabled provider fails fast with provider error message `"provider disabled"`. Task 7 constructs `SettingsSource::Watch(rx)` for this type.

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `alpaca.rs`:

```rust
#[tokio::test]
async fn disabled_provider_parks_and_fails_subscribes_fast() {
    use datamancer_core::LiveHandle as _;
    let (_tx, rx) = tokio::sync::watch::channel(None);
    let p = AlpacaProvider::new(AlpacaProviderConfig {
        settings: SettingsSource::Watch(rx),
        ..Default::default()
    });
    let (sink, _events) = tokio::sync::mpsc::channel(8);
    let handle = p.start_live(sink).await.expect("start_live");
    let err = handle
        .subscribe(provider_instrument("AAPL"), EventKind::Trade)
        .await
        .expect_err("disabled provider must fail subscribes fast");
    let msg = format!("{err}");
    assert!(msg.contains("provider disabled"), "msg={msg:?}");
    // `start_live` already returns `Box<dyn LiveHandle>`; `close` takes
    // `self: Box<Self>`, so call it directly on the box.
    handle.close().await.expect("close while parked");
}

#[tokio::test]
async fn settings_source_default_is_enabled_paper() {
    let cfg = AlpacaProviderConfig::default();
    assert_eq!(
        cfg.settings.current(),
        Some(AlpacaSettings { account_type: AccountType::Paper })
    );
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer disabled_provider_parks -- --nocapture`
Expected: FAIL to compile — `AlpacaSettings` / `settings` field don't exist yet.

- [ ] **Step 3: Implement the settings type and config change**

In `alpaca.rs`, add `use super::runtime::SettingsSource;` and define above `AlpacaProviderConfig`:

```rust
/// Runtime settings for [`AlpacaProvider`] — the hot-reconfigurable subset
/// of its configuration, delivered through a [`SettingsSource`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlpacaSettings {
    /// Paper or live account; selects endpoints and, for the legacy `Env`
    /// credential source, which env credential pair is loaded.
    pub account_type: AccountType,
}
```

In `AlpacaProviderConfig`, replace the `account_type: AccountType` field with:

```rust
/// Runtime settings source. `Static` (the default) is always enabled;
/// `Watch` is the daemon's enable/disable + hot-settings seam (`None` =
/// disabled: the streaming task parks and REST calls fail unavailable).
pub settings: SettingsSource<AlpacaSettings>,
```

and in `Default`, replace `account_type: AccountType::Paper,` with:

```rust
settings: SettingsSource::Static(AlpacaSettings {
    account_type: AccountType::Paper,
}),
```

- [ ] **Step 4: REST side — rebuild on settings or credential change**

`build_rest` takes the resolved settings (`None` = disabled → no clients):

```rust
fn build_rest(cfg: &AlpacaProviderConfig) -> RestClients {
    let Some(settings) = cfg.settings.current() else {
        return RestClients { market_data: None, trading: None };
    };
    match cfg.credentials.current() {
        Resolved::Env => RestClients {
            market_data: MarketDataClient::new(settings.account_type).ok(),
            trading: TradingClient::new(settings.account_type).ok(),
        },
        Resolved::Creds(c) => {
            let key = c.to_api_key();
            RestClients {
                market_data: MarketDataClient::new_with_credentials(settings.account_type, key.clone()).ok(),
                trading: TradingClient::new_with_credentials(settings.account_type, key).ok(),
            }
        }
        Resolved::Missing => RestClients { market_data: None, trading: None },
    }
}
```

`RestState` gains a settings receiver (same rebuild-trigger pattern as `cred_rx`):

```rust
struct RestState {
    clients: RestClients,
    cred_rx: Option<watch::Receiver<Option<AlpacaCredentials>>>,
    settings_rx: Option<watch::Receiver<Option<AlpacaSettings>>>,
}
```

In `AlpacaProvider::new` and `with_rest`, capture **both** receivers before `build_rest` (receiver-before-build ordering invariant — keep the existing comment and extend it to cover settings):

```rust
let cred_rx = cfg.credentials.watch();
let settings_rx = cfg.settings.watch();
```

`rest_clients()` must consume **both** change markers — use `|`, not `||` (short-circuiting would leave the second marker unconsumed and trigger a spurious rebuild later):

```rust
fn rest_clients(&self) -> RestClients {
    let mut state = self.rest.lock().expect("REST client state poisoned");
    let changed = super::runtime::watch_changed(&mut state.cred_rx)
        | super::runtime::watch_changed(&mut state.settings_rx);
    if changed {
        state.clients = build_rest(&self.cfg);
    }
    state.clients.clone()
}
```

- [ ] **Step 5: Streaming loop — resolve settings per iteration, park when disabled**

Generalize `wait_for_credentials` into `wait_for_provisioning` (same body; two changes — generic receiver type and a `reason` used in the fail-fast messages):

```rust
/// Waits for a `Watch` source (settings or credentials) to deliver a new
/// value, servicing the command channel meanwhile (close exits,
/// subscribe/unsubscribe fail fast with `reason`). Returns `false` if the
/// task should exit.
async fn wait_for_provisioning<T>(
    rx: &mut watch::Receiver<T>,
    cmd_rx: &mut mpsc::Receiver<LiveCommand>,
    reason: &'static str,
) -> bool {
```

Inside, replace both fail-fast message literals (`"no credentials provisioned"` and `"waiting for credentials"`) with `reason.to_string()`. Everything else is unchanged.

At the top of `'outer:` in `run_streaming_task`, before the `feed` match, resolve settings (fresh receiver first, same seen-version discipline as the credentials receiver):

```rust
let mut settings_rx = cfg.settings.watch();
let Some(settings) = cfg.settings.current() else {
    // Disabled: park until the settings watch delivers a value. Only a
    // Watch source can resolve to None, so the receiver is always
    // present here; exit defensively if it isn't (never busy-loop).
    let Some(rx) = settings_rx.as_mut() else { return };
    if !wait_for_provisioning(rx, &mut cmd_rx, "provider disabled").await {
        return;
    }
    continue 'outer;
};
```

Replace the two `cfg.account_type` uses in the connect match with `settings.account_type`, and the `Resolved::Missing` arm's call becomes `wait_for_provisioning(rx, &mut cmd_rx, "waiting for credentials")`.

In the connected `select!`, add a settings arm directly after the existing credentials arm (mirroring its structure, including the closed-channel disable):

```rust
changed = async {
    match settings_rx.as_mut() {
        Some(rx) => rx.changed().await,
        // Unreachable: the arm is guarded on `is_some()`.
        None => std::future::pending().await,
    }
}, if settings_rx.is_some() => {
    if changed.is_ok() {
        let reason = if cfg.settings.current().is_none() {
            "provider disabled"
        } else {
            "settings changed"
        };
        tracing::info!(provider = PROVIDER_ID, reason, "settings changed; reconnecting");
        emit_control(
            &sink,
            ControlKind::ProviderDisconnected {
                provider: PROVIDER_ID.to_string(),
                reason: reason.to_string(),
            },
        )
        .await;
        let _ = client.shut_down().await;
        backoff = cfg.reconnect.initial_backoff_ms;
        continue 'outer;
    }
    settings_rx = None;
}
```

(`continue 'outer` re-resolves: a `None` lands in the park branch, new settings reconnect with them.)

- [ ] **Step 6: Compile shim in `datamancerd`**

`crates/datamancerd/src/config.rs` `build_runtime` no longer compiles (`account_type` field gone). Minimal shim — replace the `AlpacaProvider::new(...)` construction with:

```rust
let provider = AlpacaProvider::new(AlpacaProviderConfig {
    settings: datamancer::providers::SettingsSource::Static(
        datamancer::providers::AlpacaSettings {
            account_type: alpaca_cfg.account_type.into(),
        },
    ),
    credentials: sources
        .get(alpaca::PROVIDER_ID)
        .cloned()
        .unwrap_or_default(),
    ..Default::default()
});
```

(Task 7 replaces this with `SettingsSource::Watch` from the `ConfigHub`.) Add `AlpacaSettings`/`SettingsSource` to the `datamancer::providers` re-exports in `crates/datamancer/src/providers/mod.rs` and `lib.rs` if not already done in Task 1.

- [ ] **Step 7: Run tests + clippy, fix any other broken call sites**

Run: `cargo test -p datamancer && cargo build --workspace && cargo clippy --all-targets -- -D warnings`
Expected: PASS. `provider_supports_kinds` and the two new tests pass; any test/example constructing `AlpacaProviderConfig { account_type, .. }` is updated to the `settings` field (check `crates/datamancer/examples/` and `crates/datamancer/tests/`).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(datamancer)!: AlpacaProvider runtime settings source — enable/disable and hot account_type"
```

---

### Task 3: Alpaca crypto provider — same treatment

**Files:**
- Modify: `crates/datamancer/src/providers/alpaca_crypto.rs`
- Modify: `crates/datamancer/src/providers/credentials.rs` (delete the now-unused `rest_credentials_changed`)
- Modify: `crates/datamancer/src/lib.rs` / `providers/mod.rs` (re-export `AlpacaCryptoSettings`)
- Modify: `crates/datamancerd/src/config.rs:680-691` (compile shim, as Task 2)

**Interfaces:**
- Consumes: `SettingsSource<T>`, `watch_changed` (Task 1).
- Produces: `pub struct AlpacaCryptoSettings { pub account_type: AccountType, pub venue: AlpacaCryptoVenue }` (Clone, Copy, Debug, PartialEq, Eq); `AlpacaCryptoProviderConfig.settings: SettingsSource<AlpacaCryptoSettings>` replacing `account_type` **and** `venue` (breaking). Default: `Static(AlpacaCryptoSettings { account_type: Paper, venue: Us })`.

- [ ] **Step 1: Write the failing tests** — mirror Task 2 Step 1 exactly, in `alpaca_crypto.rs`'s tests module, using this provider's `provider_instrument("BTC/USD")`, `AlpacaCryptoProviderConfig`, and asserting the default is `Some(AlpacaCryptoSettings { account_type: AccountType::Paper, venue: AlpacaCryptoVenue::Us })`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer --lib alpaca_crypto -- --nocapture`
Expected: FAIL to compile.

- [ ] **Step 3: Implement** — apply the Task 2 pattern to `alpaca_crypto.rs`:
  - `AlpacaCryptoSettings` struct as above; config field swap + `Default`.
  - `build_trading(cfg)` gains the `let Some(settings) = cfg.settings.current() else { return None; };` guard and uses `settings.account_type`.
  - `RestState` gains `settings_rx: Option<watch::Receiver<Option<AlpacaCryptoSettings>>>`; `AlpacaCryptoProvider::new` captures both receivers before `build_trading`; `trading_client()` uses the `watch_changed(a) | watch_changed(b)` non-short-circuiting pattern from Task 2 Step 4.
  - In this file's streaming loop (`run_hub_task`'s connect loop, structured like alpaca's `run_streaming_task`): settings resolution + park at the `'outer` top (the `feed = match cfg.venue` at line ~378 becomes `match settings.venue`, and `cfg.account_type` uses become `settings.account_type`); generalize this file's `wait_for_credentials` to `wait_for_provisioning<T>` with the `reason` parameter; add the settings select arm after the credentials arm — all verbatim from Task 2 Step 5 with `PROVIDER_ID` = `"alpaca-crypto"`.
  - Delete `rest_credentials_changed` from `credentials.rs` (both providers now call `runtime::watch_changed` directly) **and move its four `rest_change_detection_*` tests' coverage**: they already exist generically in `runtime.rs` (Task 1), so simply delete the delegator and its tests, keeping `credentials.rs`'s remaining tests.

- [ ] **Step 4: Compile shim in `datamancerd`** — replace the `AlpacaCryptoProvider::new(...)` construction in `build_runtime`:

```rust
let provider = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig {
    settings: datamancer::providers::SettingsSource::Static(
        datamancer::providers::AlpacaCryptoSettings {
            account_type: crypto.account_type.into(),
            venue: crypto.venue.into(),
        },
    ),
    credentials: sources
        .get(alpaca_crypto::PROVIDER_ID)
        .cloned()
        .unwrap_or_default(),
    ..Default::default()
});
```

- [ ] **Step 5: Run tests + clippy**

Run: `cargo test --workspace && cargo clippy --all-targets -- -D warnings`
Expected: PASS (fix any remaining `AlpacaCryptoProviderConfig` construction sites in tests/examples).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(datamancer)!: AlpacaCryptoProvider runtime settings source; retire rest_credentials_changed"
```

---

### Task 4: Daemon config groundwork — zero-provider boot, all-provider credential seeding

**Files:**
- Modify: `crates/datamancerd/src/config.rs` (`validate`, new `compiled_provider_ids`)
- Modify: `crates/datamancerd/src/paths.rs` (scaffold template)
- Modify: `crates/datamancerd/src/credentials.rs` (`bootstrap` signature), `crates/datamancerd/src/server.rs:184-185` (call site)

**Interfaces:**
- Produces: `pub fn compiled_provider_ids() -> Vec<&'static str>` in `config.rs` (feature-gated list: `alpaca::PROVIDER_ID`, `alpaca_crypto::PROVIDER_ID`); `CredentialHub::bootstrap(all_ids: &[&str], env_fallback: &[(&str, AccountType)])` — watch channels for **every** compiled-in provider (so `set-credentials` works before a provider is enabled), env fallback applied only to providers with a config section (unchanged deprecation posture). Tasks 7–8 consume both.

- [ ] **Step 1: Write the failing tests**

In `config.rs` tests:

```rust
#[test]
fn config_with_no_providers_is_valid() {
    // Cycle 3: compiled-in providers start disabled; an empty [provider]
    // block (or none at all) is a valid boot state — the app enables
    // providers at runtime via configure-provider.
    let config = Config::parse("[provider]\n").expect("parse");
    config.validate().expect("zero providers must validate");
    let config = Config::parse("").expect("parse empty");
    config.validate().expect("empty config must validate");
}

#[test]
fn compiled_provider_ids_lists_both_alpaca_providers() {
    let ids = compiled_provider_ids();
    assert!(ids.contains(&alpaca::PROVIDER_ID));
    assert!(ids.contains(&alpaca_crypto::PROVIDER_ID));
}
```

Note: `Config.provider` is `ProviderConfig` (non-optional struct field) — empty-string TOML parses only if `provider` gets `#[serde(default)]`. Add that attribute to the `provider` field on `Config`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancerd config_with_no_providers -- --nocapture`
Expected: FAIL — `validate` rejects "no provider configured"; `compiled_provider_ids` undefined; the old `config_rejects_no_provider` test now asserts the opposite (delete it in Step 3).

- [ ] **Step 3: Implement**

- In `Config::validate`, delete the leading `if self.provider.alpaca.is_none() && self.provider.alpaca_crypto.is_none() { return Err(...) }` block. Delete the `config_rejects_no_provider` test.
- Add `#[serde(default)]` to `pub provider: ProviderConfig` on `Config`.
- Add to `config.rs`:

```rust
/// Every provider compiled into this binary, whether or not it is enabled
/// in the config. The credential hub and config hub seed one watch channel
/// per entry (cycle-3 revision: fixed compiled-in set, runtime
/// enable/disable).
#[must_use]
pub fn compiled_provider_ids() -> Vec<&'static str> {
    vec![alpaca::PROVIDER_ID, alpaca_crypto::PROVIDER_ID]
}
```

(Both providers live behind the crate's default `provider-alpaca` feature which `datamancerd` always enables; no extra cfg needed unless the imports already carry one — match the file's existing gating.)

- In `credentials.rs`, change `bootstrap`'s signature and seeding split:

```rust
pub(crate) fn bootstrap(
    all_ids: &[&str],
    env_fallback: &[(&str, AccountType)],
) -> Result<(Arc<Self>, HashMap<String, CredentialsSource>)> {
    let store = CredentialStore::open_default().map_err(DaemonError::CredentialStore)?;
    tracing::info!(backend = store.backend_name(), "credential store opened");
    let (hub, sources) = Self::with_store(store, all_ids);
    for &(id, account_type) in env_fallback {
        let Some(sender) = hub.senders.get(id) else { continue };
        if sender.borrow().is_some() {
            tracing::info!(provider = id, "credentials loaded from the store");
            continue;
        }
        // … existing env_credentials fallback + warnings, unchanged …
    }
    Ok((Arc::new(hub), sources))
}
```

- In `server.rs::bootstrap`, replace lines 184-185:

```rust
let env_fallback = config.configured_providers();
let all_ids = crate::config::compiled_provider_ids();
let (hub, sources) = CredentialHub::bootstrap(&all_ids, &env_fallback)?;
```

- In `paths.rs`'s `default_config_toml()` template: comment out the provider section(s) it scaffolds (keep them as documentation), and update the surrounding doc text to say providers start disabled until enabled via `configure-provider` (or by uncommenting). The scaffolded first-run config must parse and validate with zero providers.

- [ ] **Step 4: Run tests**

Run: `cargo test -p datamancerd && cargo clippy -p datamancerd --all-targets -- -D warnings`
Expected: PASS, including existing credential-hub tests (update any that call the old `bootstrap(providers)` shape).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(datamancerd): zero-provider boot, compiled_provider_ids, credential seeding for all compiled-in providers"
```

---

### Task 5: Hot/cold classification table

**Files:**
- Create: `crates/datamancerd/src/config_class.rs`
- Modify: `crates/datamancerd/src/main.rs` (module declaration — match how `credentials`/`config` are declared)

**Interfaces:**
- Produces: `pub(crate) enum FieldClass { Hot, Cold }`; `pub(crate) fn classify(path: &str) -> Option<FieldClass>` (dotted TOML path, e.g. `"provider.alpaca.account_type"`); `pub(crate) fn cold_divergence(baseline: &Config, current: &Config) -> Vec<String>` (dotted paths of cold-classified leaves that differ). Tasks 7 and 9 consume `cold_divergence`; the exhaustiveness test is the spec's "new field without a classification fails the build" gate.

- [ ] **Step 1: Write the module with tests**

```rust
//! The hot/cold classification of every daemon config field — one table
//! shared by the control surface and the web UI (spec cycle 3). "Hot"
//! fields apply to the running daemon when changed through the config
//! service; "cold" fields persist but take effect at the next boot
//! (`restart_required`). The exhaustiveness test below fails when a config
//! field is added without a classification.

use crate::config::Config;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldClass {
    /// Applies live via the config hub's provider watches.
    Hot,
    /// Persisted; applied at next boot.
    Cold,
}

/// Classification by longest matching dotted-path prefix. Entries end with
/// `.` to classify a whole section; exact entries classify one leaf.
const CLASSIFICATION: &[(&str, FieldClass)] = &[
    // Provider sections: presence (enable/disable) and every setting apply
    // live through the per-provider settings watch.
    ("provider.", FieldClass::Hot),
    // Everything else is boot-time composition: storage backends, session
    // knobs, sockets/listeners, transport caps, cadences, anchors.
    ("cache.", FieldClass::Cold),
    ("tap_log.", FieldClass::Cold),
    ("session.", FieldClass::Cold),
    ("server.", FieldClass::Cold),
    ("diagnostics.", FieldClass::Cold),
    ("iceoryx2.", FieldClass::Cold),
    ("web_ui.", FieldClass::Cold),
    ("ws.", FieldClass::Cold),
    ("startup_session.", FieldClass::Cold),
];

/// The class for a dotted config path, or `None` for an unknown path.
pub(crate) fn classify(path: &str) -> Option<FieldClass> {
    CLASSIFICATION
        .iter()
        .filter(|(prefix, _)| path.starts_with(prefix) || path == prefix.trim_end_matches('.'))
        .max_by_key(|(prefix, _)| prefix.len())
        .map(|&(_, class)| class)
}

/// Every leaf path (dotted) in a config's JSON form. Arrays are treated as
/// one leaf under their section path (element-level diffing adds noise
/// without changing any classification decision).
fn leaf_paths(value: &serde_json::Value, prefix: &str, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                leaf_paths(v, &path, out);
            }
        }
        _ => out.push(prefix.to_string()),
    }
}

/// Cold-classified leaves that differ between `baseline` (the boot-applied
/// config) and `current`. Non-empty ⇒ a restart is required for `current`
/// to fully apply.
pub(crate) fn cold_divergence(baseline: &Config, current: &Config) -> Vec<String> {
    let a = serde_json::to_value(baseline).expect("Config serializes");
    let b = serde_json::to_value(current).expect("Config serializes");
    let mut paths = Vec::new();
    leaf_paths(&a, "", &mut paths);
    let mut more = Vec::new();
    leaf_paths(&b, "", &mut more);
    paths.extend(more);
    paths.sort();
    paths.dedup();
    paths
        .into_iter()
        .filter(|p| {
            let av = lookup(&a, p);
            let bv = lookup(&b, p);
            av != bv && classify(p) == Some(FieldClass::Cold)
        })
        .collect()
}

fn lookup<'v>(value: &'v serde_json::Value, path: &str) -> Option<&'v serde_json::Value> {
    let mut v = value;
    for seg in path.split('.') {
        v = v.get(seg)?;
    }
    Some(v)
}
```

Simplify `config_leaves` away if unused — `cold_divergence` + `classify` + the test below are the required surface. Tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Keep in sync with config.rs's FULL fixture: every section populated,
    // so every serializable field appears in the exhaustiveness walk.
    const FULL: &str = r#"
[provider.alpaca]
account_type = "paper"

[provider.alpaca_crypto]
account_type = "live"
venue = "us_kraken"

[cache]
backend = "embedded"
path = "/tmp/dmc-cache"

[tap_log]
backend = "memory"

[session]
resume_buffer_events = 1024
adjustment = "split"

[server]
admin_socket = "/tmp/dmc/admin.sock"
service_prefix = "dmc"
shutdown_timeout_secs = 5

[diagnostics]
publish_interval_ms = 500
cache_catalog_interval_ms = 10000

[iceoryx2]
max_clients = 8

[web_ui]
enabled = true

[ws]
enabled = true
auth_token = "t"

[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
scope = "live"
persistence = "none"
"#;

    /// Spec cycle 3: "a new field without a classification fails the
    /// build" — every leaf of a fully-populated config must classify.
    #[test]
    fn every_config_field_is_classified() {
        let config = Config::parse(FULL).expect("parse");
        let value = serde_json::to_value(&config).expect("serialize");
        let mut paths = Vec::new();
        leaf_paths(&value, "", &mut paths);
        assert!(!paths.is_empty());
        let unclassified: Vec<_> = paths
            .into_iter()
            .filter(|p| classify(p).is_none())
            .collect();
        assert!(
            unclassified.is_empty(),
            "config fields missing a hot/cold classification: {unclassified:?} — add them to CLASSIFICATION"
        );
    }

    #[test]
    fn provider_fields_are_hot_everything_else_cold() {
        assert_eq!(classify("provider.alpaca.account_type"), Some(FieldClass::Hot));
        assert_eq!(classify("provider.alpaca_crypto.venue"), Some(FieldClass::Hot));
        assert_eq!(classify("server.admin_socket"), Some(FieldClass::Cold));
        assert_eq!(classify("session.resume_buffer_events"), Some(FieldClass::Cold));
        assert_eq!(classify("nonexistent.field"), None);
    }

    #[test]
    fn cold_divergence_ignores_hot_changes_and_flags_cold_ones() {
        let boot = Config::parse(FULL).expect("parse");
        let mut hot_changed = boot.clone();
        hot_changed.provider.alpaca = None; // hot: enable/disable
        assert!(cold_divergence(&boot, &hot_changed).is_empty());

        let mut cold_changed = boot.clone();
        cold_changed.session.resume_buffer_events = 42;
        let diverged = cold_divergence(&boot, &cold_changed);
        assert_eq!(diverged, vec!["session.resume_buffer_events".to_string()]);
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p datamancerd config_class && cargo clippy -p datamancerd --all-targets -- -D warnings`
Expected: PASS. If the exhaustiveness test flags the `ws.auth_token` leaf or others, that means the FULL fixture found a real gap — extend `CLASSIFICATION`, never the filter.

- [ ] **Step 3: Commit**

```bash
git add crates/datamancerd/src/config_class.rs crates/datamancerd/src/main.rs
git commit -m "feat(datamancerd): hot/cold config field classification table with exhaustiveness gate"
```

---

### Task 6: Wire vocabulary — new ops, reply fields, codes

**Files:**
- Modify: `crates/datamancer-client/src/protocol/uds.rs`
- Modify: `crates/datamancer-client/src/codes.rs`

**Interfaces:**
- Produces (Tasks 8–10 consume):
  - `Request::{GetConfig, ConfigureProvider { provider: String, settings: serde_json::Value }, RemoveProvider { provider: String }, Shutdown}` (kebab-case ops: `get-config`, `configure-provider`, `remove-provider`, `shutdown`). **`Request` drops its `Eq` derive** (`serde_json::Value` is only `PartialEq`) — breaking, covered by the 0.4.0 lockstep bump.
  - `Reply` fields: `config: Option<serde_json::Value>`, `restart_required: Option<bool>`, `applied: Option<String>` (all `skip_serializing_if`), plus constructors `Reply::config(value, restart_required: bool)` and `Reply::applied_live()` (sets `applied: Some("live".into())`).
  - `codes::RESTART_REQUIRED = "restart_required"`, `codes::UNKNOWN_CONFIG_FIELD = "unknown_config_field"`.

- [ ] **Step 1: Write the failing wire tests** (append to `uds.rs` tests)

```rust
#[test]
fn config_ops_round_trip_documented_wire_shapes() {
    let get: Request = serde_json::from_str(r#"{"op":"get-config"}"#).expect("de");
    assert!(matches!(get, Request::GetConfig));

    let cfg: Request = serde_json::from_str(
        r#"{"op":"configure-provider","provider":"alpaca","settings":{"account_type":"live"}}"#,
    )
    .expect("de");
    match &cfg {
        Request::ConfigureProvider { provider, settings } => {
            assert_eq!(provider, "alpaca");
            assert_eq!(settings["account_type"], "live");
        }
        other => panic!("wrong variant: {other:?}"),
    }
    assert_eq!(
        serde_json::to_string(&cfg).unwrap(),
        r#"{"op":"configure-provider","provider":"alpaca","settings":{"account_type":"live"}}"#
    );

    let rm: Request =
        serde_json::from_str(r#"{"op":"remove-provider","provider":"alpaca"}"#).unwrap();
    assert!(matches!(rm, Request::RemoveProvider { .. }));

    let sd: Request = serde_json::from_str(r#"{"op":"shutdown"}"#).unwrap();
    assert!(matches!(sd, Request::Shutdown));
}

#[test]
fn config_replies_carry_payloads_and_omit_when_absent() {
    let reply = serde_json::to_value(Reply::config(serde_json::json!({"provider": {}}), true)).unwrap();
    assert_eq!(reply["ok"], serde_json::Value::Bool(true));
    assert_eq!(reply["restart_required"], serde_json::Value::Bool(true));
    assert!(reply.get("applied").is_none());

    let applied = serde_json::to_value(Reply::applied_live()).unwrap();
    assert_eq!(applied["applied"], "live");
    assert!(applied.get("config").is_none());

    let plain = serde_json::to_value(Reply::ok()).unwrap();
    assert!(plain.get("config").is_none());
    assert!(plain.get("restart_required").is_none());
}

#[test]
fn new_config_codes_are_stable() {
    assert_eq!(crate::codes::RESTART_REQUIRED, "restart_required");
    assert_eq!(crate::codes::UNKNOWN_CONFIG_FIELD, "unknown_config_field");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p datamancer-client config_ops -- --nocapture`
Expected: FAIL to compile.

- [ ] **Step 3: Implement**

- `Request`: change the derive to `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]` (drop `Eq`) and append variants:

```rust
/// Return the daemon's current config (TOML as JSON) plus whether any
/// cold-classified field diverges from the boot-applied config.
GetConfig,
/// Enable (or re-configure) a compiled-in provider. `settings` is the
/// provider's config-section shape (e.g. `{"account_type":"live"}`);
/// unknown fields are rejected with `unknown_config_field`. UDS-only,
/// peer-cred gated; applies live and persists atomically.
ConfigureProvider {
    provider: String,
    #[serde(default)]
    settings: serde_json::Value,
},
/// Disable a compiled-in provider (its section is removed from the
/// persisted config; stored credentials are untouched). UDS-only,
/// peer-cred gated; applies live.
RemoveProvider { provider: String },
/// Graceful, deliberate daemon stop (the full drain path). UDS-only,
/// peer-cred gated.
Shutdown,
```

- `Reply`: add the three fields (with `#[serde(default, skip_serializing_if = "Option::is_none")]`), extend `Reply::ok()`/`Reply::error()` field lists, and add:

```rust
/// Success carrying the daemon config and its cold-field divergence flag
/// (on `get-config`).
#[must_use]
pub fn config(config: serde_json::Value, restart_required: bool) -> Self {
    Self {
        config: Some(config),
        restart_required: Some(restart_required),
        ..Self::ok()
    }
}

/// Success for a mutating config op that applied to the running daemon.
#[must_use]
pub fn applied_live() -> Self {
    Self {
        applied: Some("live".to_string()),
        ..Self::ok()
    }
}
```

- `codes.rs`:

```rust
/// The op was persisted but a cold-classified field needs a daemon restart
/// to take effect.
pub const RESTART_REQUIRED: &str = "restart_required";
/// A configure-provider payload carried a field the provider's config
/// section does not define.
pub const UNKNOWN_CONFIG_FIELD: &str = "unknown_config_field";
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p datamancer-client && cargo clippy -p datamancer-client --all-targets -- -D warnings`
Expected: PASS. (If anything relied on `Request: Eq`, switch it to `PartialEq` comparison.)

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-client/src/protocol/uds.rs crates/datamancer-client/src/codes.rs
git commit -m "feat(datamancer-client): config-service wire vocabulary — get-config/configure-provider/remove-provider/shutdown"
```

---

### Task 7: `ConfigHub` — settings watches, persist-then-apply, runtime wiring

**Files:**
- Create: `crates/datamancerd/src/config_hub.rs`
- Modify: `crates/datamancerd/src/main.rs` (module declaration)
- Modify: `crates/datamancerd/src/config.rs` (`build_runtime` takes settings sources; **always** constructs both providers)
- Modify: `crates/datamancerd/src/server.rs::bootstrap` (create hub, thread sources)

**Interfaces:**
- Consumes: `SettingsSource`/`AlpacaSettings`/`AlpacaCryptoSettings` (Tasks 2–3), `compiled_provider_ids` (Task 4), `cold_divergence` (Task 5), `Reply::config`/`applied_live` + codes (Task 6).
- Produces (Tasks 8–9 consume):
  - `ConfigHub::bootstrap(config: Config, path: PathBuf) -> (Arc<Self>, ProviderSettingsSources)`
  - `pub(crate) struct ProviderSettingsSources { pub alpaca: SettingsSource<AlpacaSettings>, pub alpaca_crypto: SettingsSource<AlpacaCryptoSettings> }`
  - `async fn get_config(&self) -> Reply`, `async fn configure_provider(&self, provider: &str, settings: serde_json::Value) -> Reply`, `async fn remove_provider(&self, provider: &str) -> Reply`
  - `async fn apply_full(&self, new: Config) -> Result<bool>` (web-UI path; returns restart_required)
  - `Config::build_runtime(self, sources: &HashMap<String, CredentialsSource>, settings: ProviderSettingsSources) -> Result<BuiltRuntime>`

- [ ] **Step 1: Write the failing hub tests** (in `config_hub.rs`'s tests module)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn hub_with(toml: &str) -> (std::sync::Arc<ConfigHub>, ProviderSettingsSources, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let config = Config::parse(toml).expect("parse");
        config.save(&path).expect("seed file");
        let (hub, sources) = ConfigHub::bootstrap(config, path);
        (hub, sources, dir)
    }

    #[tokio::test]
    async fn boot_seeds_watches_from_sections() {
        let (_hub, sources, _dir) = hub_with("[provider.alpaca]\naccount_type = \"live\"\n");
        assert_eq!(
            sources.alpaca.current(),
            Some(datamancer::providers::AlpacaSettings {
                account_type: datamancer::providers::AccountType::Live
            })
        );
        // Unconfigured provider: constructed but disabled.
        assert_eq!(sources.alpaca_crypto.current(), None);
    }

    #[tokio::test]
    async fn configure_provider_persists_then_applies() {
        let (hub, sources, dir) = hub_with("[provider]\n");
        assert_eq!(sources.alpaca.current(), None);
        let reply = hub
            .configure_provider("alpaca", serde_json::json!({"account_type": "live"}))
            .await;
        assert!(reply.ok, "{reply:?}");
        assert_eq!(reply.applied.as_deref(), Some("live"));
        // Applied live on the watch:
        assert_eq!(
            sources.alpaca.current().map(|s| s.account_type),
            Some(datamancer::providers::AccountType::Live)
        );
        // Persisted atomically:
        let on_disk = Config::load(dir.path().join("config.toml")).expect("reload");
        assert!(on_disk.provider.alpaca.is_some());
    }

    #[tokio::test]
    async fn configure_provider_rejects_unknown_field_without_applying() {
        let (hub, sources, dir) = hub_with("[provider]\n");
        let reply = hub
            .configure_provider("alpaca", serde_json::json!({"account_type": "live", "bogus": 1}))
            .await;
        assert!(!reply.ok);
        assert_eq!(reply.code.as_deref(), Some("unknown_config_field"));
        assert_eq!(sources.alpaca.current(), None, "must not apply");
        let on_disk = Config::load(dir.path().join("config.toml")).expect("reload");
        assert!(on_disk.provider.alpaca.is_none(), "must not persist");
    }

    #[tokio::test]
    async fn configure_provider_rejects_unknown_provider() {
        let (hub, _sources, _dir) = hub_with("[provider]\n");
        let reply = hub.configure_provider("nope", serde_json::json!({})).await;
        assert_eq!(reply.code.as_deref(), Some("unknown_provider"));
    }

    #[tokio::test]
    async fn remove_provider_disables_and_persists() {
        let (hub, sources, dir) = hub_with("[provider.alpaca]\naccount_type = \"paper\"\n");
        assert!(sources.alpaca.current().is_some());
        let reply = hub.remove_provider("alpaca").await;
        assert!(reply.ok, "{reply:?}");
        assert_eq!(reply.applied.as_deref(), Some("live"));
        assert_eq!(sources.alpaca.current(), None);
        let on_disk = Config::load(dir.path().join("config.toml")).expect("reload");
        assert!(on_disk.provider.alpaca.is_none());
    }

    #[tokio::test]
    async fn get_config_reports_no_divergence_at_boot_and_after_hot_ops() {
        let (hub, _sources, _dir) = hub_with("[provider]\n");
        let reply = hub.get_config().await;
        assert!(reply.ok);
        assert_eq!(reply.restart_required, Some(false));
        hub.configure_provider("alpaca", serde_json::json!({"account_type": "paper"}))
            .await;
        let reply = hub.get_config().await;
        assert_eq!(reply.restart_required, Some(false), "hot ops never require restart");
        assert!(reply.config.unwrap()["provider"]["alpaca"].is_object());
    }
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p datamancerd config_hub` → FAIL to compile.

- [ ] **Step 3: Implement the hub**

```rust
//! The daemon-side config service: the authoritative in-memory [`Config`],
//! the config file, and one settings watch per compiled-in provider.
//! Mutating ops follow the credential hub's discipline — one hub lock
//! serializes validate → **persist** → **apply**, so a store failure leaves
//! the running daemon untouched and concurrent writers can never tear the
//! file or leave the file and the live watches on different values.

use std::path::PathBuf;
use std::sync::Arc;

use datamancer::providers::{
    AccountType, AlpacaCryptoSettings, AlpacaSettings, SettingsSource, alpaca, alpaca_crypto,
};
use tokio::sync::watch;

use crate::config::{AlpacaCryptoSection, AlpacaSection, Config};
use crate::config_class::cold_divergence;
use crate::control::{Reply, codes};
use crate::error::Result;

/// The per-provider settings sources handed to `build_runtime`.
pub(crate) struct ProviderSettingsSources {
    pub alpaca: SettingsSource<AlpacaSettings>,
    pub alpaca_crypto: SettingsSource<AlpacaCryptoSettings>,
}

struct HubState {
    /// The authoritative current config (starts as the boot config; every
    /// accepted mutation updates it in the same critical section as the
    /// persist + apply).
    current: Config,
}

pub(crate) struct ConfigHub {
    path: PathBuf,
    /// Boot-applied config: `cold_divergence(&boot, &current)` non-empty ⇒
    /// restart required.
    boot: Config,
    state: tokio::sync::Mutex<HubState>,
    alpaca_tx: watch::Sender<Option<AlpacaSettings>>,
    alpaca_crypto_tx: watch::Sender<Option<AlpacaCryptoSettings>>,
}

fn alpaca_settings(section: &AlpacaSection) -> AlpacaSettings {
    AlpacaSettings { account_type: section.account_type.into() }
}

fn alpaca_crypto_settings(section: &AlpacaCryptoSection) -> AlpacaCryptoSettings {
    AlpacaCryptoSettings {
        account_type: section.account_type.into(),
        venue: section.venue.into(),
    }
}

impl ConfigHub {
    /// Seed one settings watch per compiled-in provider from the boot
    /// config (section present ⇒ enabled, absent ⇒ disabled) and return the
    /// sources for `build_runtime`.
    pub(crate) fn bootstrap(config: Config, path: PathBuf) -> (Arc<Self>, ProviderSettingsSources) {
        let (alpaca_tx, alpaca_rx) =
            watch::channel(config.provider.alpaca.as_ref().map(alpaca_settings));
        let (crypto_tx, crypto_rx) = watch::channel(
            config.provider.alpaca_crypto.as_ref().map(alpaca_crypto_settings),
        );
        let hub = Self {
            path,
            boot: config.clone(),
            state: tokio::sync::Mutex::new(HubState { current: config }),
            alpaca_tx,
            alpaca_crypto_tx: crypto_tx,
        };
        (
            Arc::new(hub),
            ProviderSettingsSources {
                alpaca: SettingsSource::Watch(alpaca_rx),
                alpaca_crypto: SettingsSource::Watch(crypto_rx),
            },
        )
    }

    /// The current config as JSON plus the cold-field divergence flag.
    pub(crate) async fn get_config(&self) -> Reply {
        let state = self.state.lock().await;
        let restart_required = !cold_divergence(&self.boot, &state.current).is_empty();
        match serde_json::to_value(&state.current) {
            Ok(value) => Reply::config(value, restart_required),
            Err(e) => Reply::error(codes::INTERNAL, format!("config serialize: {e}")),
        }
    }

    /// Enable or re-configure a provider: validate → persist → apply.
    pub(crate) async fn configure_provider(
        &self,
        provider: &str,
        settings: serde_json::Value,
    ) -> Reply {
        // An omitted/null settings payload means "defaults".
        let settings = if settings.is_null() {
            serde_json::Value::Object(serde_json::Map::new())
        } else {
            settings
        };
        let mut state = self.state.lock().await;
        let mut candidate = state.current.clone();
        match provider {
            alpaca::PROVIDER_ID => match serde_json::from_value::<AlpacaSection>(settings) {
                Ok(section) => candidate.provider.alpaca = Some(section),
                Err(e) => return settings_error(&e),
            },
            alpaca_crypto::PROVIDER_ID => {
                match serde_json::from_value::<AlpacaCryptoSection>(settings) {
                    Ok(section) => candidate.provider.alpaca_crypto = Some(section),
                    Err(e) => return settings_error(&e),
                }
            }
            _ => return unknown_provider(provider),
        }
        match self.persist(&mut state, candidate).await {
            Ok(()) => {
                self.apply(&state.current);
                tracing::info!(provider, "provider configured and hot-applied");
                Reply::applied_live()
            }
            Err(reply) => reply,
        }
    }

    /// Disable a provider (remove its section; stored credentials are
    /// untouched — re-enabling reuses them).
    pub(crate) async fn remove_provider(&self, provider: &str) -> Reply {
        let mut state = self.state.lock().await;
        let mut candidate = state.current.clone();
        match provider {
            alpaca::PROVIDER_ID => candidate.provider.alpaca = None,
            alpaca_crypto::PROVIDER_ID => candidate.provider.alpaca_crypto = None,
            _ => return unknown_provider(provider),
        }
        match self.persist(&mut state, candidate).await {
            Ok(()) => {
                self.apply(&state.current);
                tracing::info!(provider, "provider disabled");
                Reply::applied_live()
            }
            Err(reply) => reply,
        }
    }

    /// Replace the whole config (the web UI's PUT path). Hot provider
    /// changes apply live; returns whether cold fields now diverge from the
    /// boot config.
    pub(crate) async fn apply_full(&self, new: Config) -> Result<bool> {
        let mut state = self.state.lock().await;
        let path = self.path.clone();
        let candidate = new.clone();
        tokio::task::spawn_blocking(move || candidate.save(&path))
            .await
            .map_err(|e| crate::error::DaemonError::ConfigInvalid(format!("config task failed: {e}")))??;
        state.current = new;
        self.apply(&state.current);
        Ok(!cold_divergence(&self.boot, &state.current).is_empty())
    }

    /// Validate + atomically persist `candidate`; commit it to `state`
    /// only on success. Persist-then-apply: callers apply after this
    /// returns `Ok`.
    async fn persist(
        &self,
        state: &mut tokio::sync::MutexGuard<'_, HubState>,
        candidate: Config,
    ) -> std::result::Result<(), Reply> {
        let path = self.path.clone();
        let to_write = candidate.clone();
        match tokio::task::spawn_blocking(move || to_write.save(&path)).await {
            Ok(Ok(())) => {
                state.current = candidate;
                Ok(())
            }
            Ok(Err(e)) => Err(Reply::error(codes::BAD_REQUEST, format!("config rejected: {e}"))),
            Err(e) => Err(Reply::error(codes::INTERNAL, format!("config task failed: {e}"))),
        }
    }

    /// Push the current provider sections onto the settings watches.
    fn apply(&self, current: &Config) {
        self.alpaca_tx
            .send_replace(current.provider.alpaca.as_ref().map(alpaca_settings));
        self.alpaca_crypto_tx.send_replace(
            current.provider.alpaca_crypto.as_ref().map(alpaca_crypto_settings),
        );
    }
}

/// Map a settings-payload deserialization failure to a stable code:
/// unknown keys are the operator-contract `unknown_config_field`; anything
/// else (bad enum value, wrong type) is `bad_request`.
fn settings_error(e: &serde_json::Error) -> Reply {
    let msg = e.to_string();
    if msg.contains("unknown field") {
        Reply::error(codes::UNKNOWN_CONFIG_FIELD, msg)
    } else {
        Reply::error(codes::BAD_REQUEST, format!("invalid settings: {msg}"))
    }
}

fn unknown_provider(provider: &str) -> Reply {
    Reply::error(
        codes::UNKNOWN_PROVIDER,
        format!("no compiled-in provider {provider:?}"),
    )
}
```

Note on `apply`: `watch::Sender::send_replace` on an unchanged value still wakes receivers, forcing a needless reconnect. Guard each with a comparison:

```rust
fn apply(&self, current: &Config) {
    let alpaca = current.provider.alpaca.as_ref().map(alpaca_settings);
    if *self.alpaca_tx.borrow() != alpaca {
        self.alpaca_tx.send_replace(alpaca);
    }
    let crypto = current.provider.alpaca_crypto.as_ref().map(alpaca_crypto_settings);
    if *self.alpaca_crypto_tx.borrow() != crypto {
        self.alpaca_crypto_tx.send_replace(crypto);
    }
}
```

(Use this guarded version, and add a test: configuring the same settings twice does not bump the watch — `sources.alpaca.watch()`-style receivers must report `has_changed == false` after a no-op reconfigure. Accessing the receiver in the test: keep a clone of the `SettingsSource` from bootstrap and use `SettingsSource::watch()`… that method is `pub(crate)` in `datamancer` — instead assert via a raw receiver: `let rx = match &sources.alpaca { SettingsSource::Watch(rx) => rx.clone(), _ => unreachable!() };` then `rx.has_changed()`.)

- [ ] **Step 4: Rewire `build_runtime` to construct all compiled-in providers**

Change the signature and body in `config.rs`:

```rust
pub async fn build_runtime(
    self,
    sources: &HashMap<String, CredentialsSource>,
    settings: crate::config_hub::ProviderSettingsSources,
) -> Result<BuiltRuntime> {
    let mut builder = Datamancer::builder()
        .resume_buffer_events(self.session.resume_buffer_events)
        .adjustment(self.session.adjustment.into());

    // Cycle 3: every compiled-in provider is constructed and registered,
    // parked unless its settings watch carries a value. Presence of a
    // `[provider.*]` section is enablement, applied through the watch —
    // not through conditional construction.
    let provider = AlpacaProvider::new(AlpacaProviderConfig {
        settings: settings.alpaca,
        credentials: sources.get(alpaca::PROVIDER_ID).cloned().unwrap_or_default(),
        ..Default::default()
    });
    builder = builder.provider(Box::new(provider));
    let provider = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig {
        settings: settings.alpaca_crypto,
        credentials: sources
            .get(alpaca_crypto::PROVIDER_ID)
            .cloned()
            .unwrap_or_default(),
        ..Default::default()
    });
    builder = builder.provider(Box::new(provider));

    // … cache/tap-log wiring unchanged …
}
```

- [ ] **Step 5: Wire bootstrap in `server.rs`**

In `Server::bootstrap`, after the credential hub block and before `build_runtime`:

```rust
let (config_hub, provider_settings) =
    crate::config_hub::ConfigHub::bootstrap(config.clone(), config_path.clone());
let built = config.build_runtime(&sources, provider_settings).await?;
```

Store `config_hub: Arc<crate::config_hub::ConfigHub>` as a new `Server` field (next to `hub`), and thread a clone through `accept_loop` → `handle_connection` in Task 8. Note `config_path` was previously consumed by `ConfigState::new` under `web-ui` — clone as needed and keep the `#[cfg(not(feature = "web-ui"))] let _ = config_path;` line removed (the config hub always uses it now).

- [ ] **Step 6: Run tests**

Run: `cargo test -p datamancerd && cargo clippy -p datamancerd --all-targets -- -D warnings`
Expected: PASS (hub tests green; `daemon_e2e`/other `#[ignore]`d suites still compile).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(datamancerd): ConfigHub — per-provider settings watches, persist-then-apply, all providers constructed at boot"
```

---

### Task 8: Server dispatch — config ops, gated shutdown

**Files:**
- Modify: `crates/datamancerd/src/server.rs`
- Modify: `crates/datamancerd/src/credentials.rs` (rename `credential_op_permitted` → `privileged_op_permitted`, update doc + tests)
- Modify: `crates/datamancerd/src/control.rs` only if it re-exports touched items (check; likely no change)

**Interfaces:**
- Consumes: Task 6 vocabulary, Task 7 `ConfigHub`.
- Produces: dispatch behavior Tasks 10–11 test against —
  - `get-config`: off-actor, ungated (the config never contains secrets).
  - `configure-provider` / `remove-provider`: off-actor via `ConfigHub`, **same-uid gated**.
  - `shutdown`: same-uid gated in `handle_connection`, forwarded to the actor; the actor replies `Reply::ok()` and breaks its run loop into the existing drain path (identical to SIGTERM).

- [ ] **Step 1: Write the failing unit tests**

In `server.rs` tests (alongside the lockstep test), pure gate + dispatch-shape tests:

```rust
#[test]
fn privileged_gate_requires_exact_uid_match() {
    use crate::credentials::privileged_op_permitted;
    assert!(privileged_op_permitted(Some(501), 501));
    assert!(!privileged_op_permitted(Some(502), 501));
    assert!(!privileged_op_permitted(None, 501));
}
```

(The op-routing behavior itself is exercised end-to-end in Task 11; the actor's shutdown control flow is verified by the compile-time `ControlFlow` plumbing below plus the e2e.)

- [ ] **Step 2: Rename the gate**

In `credentials.rs`: rename `credential_op_permitted` → `privileged_op_permitted`, update its doc to "Same-uid gate for credential and config-mutation ops (and `shutdown`)…", update the existing gate test name/uses and the `server.rs` import + call site.

- [ ] **Step 3: Off-actor dispatch in `handle_connection`**

Give `handle_connection` (and `accept_loop`, and their spawn sites) one more parameter: `config_hub: Arc<crate::config_hub::ConfigHub>`. In the request-routing chain, after the credential-ops block, add:

```rust
} else if matches!(&request, Request::GetConfig) {
    config_hub.get_config().await
} else if matches!(
    &request,
    Request::ConfigureProvider { .. } | Request::RemoveProvider { .. } | Request::Shutdown
) {
    if privileged_op_permitted(peer_uid, own_euid) {
        match request {
            Request::ConfigureProvider { provider, settings } => {
                config_hub.configure_provider(&provider, settings).await
            }
            Request::RemoveProvider { provider } => {
                config_hub.remove_provider(&provider).await
            }
            Request::Shutdown => {
                // Forward to the actor: shutdown is a run-loop decision,
                // not hub state.
                let (tx, rx) = oneshot::channel();
                if cmd_tx
                    .send(ServerCommand::Request { request: Request::Shutdown, reply: tx })
                    .await
                    .is_err()
                {
                    break;
                }
                match rx.await {
                    Ok(reply) => reply,
                    Err(_) => break,
                }
            }
            _ => Reply::error(codes::INTERNAL, "unreachable config dispatch"),
        }
    } else {
        Reply::error(
            codes::PERMISSION_DENIED,
            "config mutation and shutdown ops require the daemon owner's uid",
        )
    }
} else {
```

- [ ] **Step 4: Actor shutdown control flow**

Change `handle` to return `std::ops::ControlFlow<()>`:

```rust
async fn handle(&mut self, cmd: ServerCommand) -> std::ops::ControlFlow<()> {
    match cmd {
        ServerCommand::Request { request, reply } => {
            if matches!(request, Request::Shutdown) && !self.draining {
                tracing::info!("shutdown requested via control op");
                let _ = reply.send(Reply::ok());
                return std::ops::ControlFlow::Break(());
            }
            let response = self.dispatch(request).await;
            let _ = reply.send(response);
        }
        ServerCommand::Disconnect { client } => {
            self.teardown_client(&client).await;
        }
    }
    std::ops::ControlFlow::Continue(())
}
```

Run-loop arm becomes:

```rust
maybe = cmd_rx.recv() => {
    match maybe {
        Some(cmd) => {
            if self.handle(cmd).await.is_break() {
                break;
            }
        }
        None => break,
    }
}
```

In `dispatch`, add defense-in-depth arms for exhaustiveness (mirroring the credential-op comment style):

```rust
// Dispatched off-actor in `handle_connection` (config-hub state; the
// mutating ops and shutdown are peer-cred gated there). These arms only
// exist for match exhaustiveness / defense in depth.
Request::GetConfig
| Request::ConfigureProvider { .. }
| Request::RemoveProvider { .. } => Reply::error(
    codes::INTERNAL,
    "config ops are dispatched off-actor; this arm is unreachable",
),
// `handle` intercepts Shutdown before dispatch; reaching here means the
// daemon is already draining.
Request::Shutdown => Reply::error(codes::SHUTTING_DOWN, "daemon is shutting down"),
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p datamancerd && cargo clippy -p datamancerd --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(datamancerd): dispatch config-service ops; peer-cred-gated shutdown breaks the run loop into the drain path"
```

---

### Task 9: Single hot-path writer — web UI `ConfigState` delegates to `ConfigHub`

**Files:**
- Modify: `crates/datamancerd/src/web/config_api.rs`
- Modify: `crates/datamancerd/src/server.rs` (`start_web` threading)

**Interfaces:**
- Consumes: `ConfigHub::apply_full` (Task 7), `cold_divergence` (Task 5).
- Produces: `ConfigState::write` routes through the hub — hot provider edits from the web UI now apply live, and `restart_required` generalizes from "disk ≠ boot" to "cold fields diverge from boot".

- [ ] **Step 1: Adapt the existing tests first**

`config_api.rs`'s tests (`restart_required_tracks_disk_vs_boot`, etc.) construct `ConfigState::new(path, boot)`. Give `ConfigState` a hub: change the constructor to `ConfigState::new(path: PathBuf, boot: Config, hub: Arc<crate::config_hub::ConfigHub>)` and update tests to build a hub via `ConfigHub::bootstrap(boot.clone(), path.clone())`. Update/extend the behavior tests:

```rust
#[tokio::test]
async fn hot_only_web_edit_does_not_require_restart_and_applies_live() {
    // boot: no providers
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let boot = Config::parse("[provider]\n").unwrap();
    boot.save(&path).unwrap();
    let (hub, sources) = crate::config_hub::ConfigHub::bootstrap(boot.clone(), path.clone());
    let state = ConfigState::new(path, boot.clone(), hub);

    let mut edited = boot.clone();
    edited.provider.alpaca = Some(crate::config::AlpacaSection {
        account_type: crate::config::AccountTypeCfg::Live,
    });
    state.write(&edited).await.expect("write");
    assert!(!state.restart_required(), "hot-only edit must not require restart");
    assert!(sources.alpaca.current().is_some(), "hot edit applies live");
}

#[tokio::test]
async fn cold_web_edit_requires_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let boot = Config::parse("[provider]\n").unwrap();
    boot.save(&path).unwrap();
    let (hub, _sources) = crate::config_hub::ConfigHub::bootstrap(boot.clone(), path.clone());
    let state = ConfigState::new(path, boot.clone(), hub);

    let mut edited = boot.clone();
    edited.session.resume_buffer_events = 42;
    state.write(&edited).await.expect("write");
    assert!(state.restart_required());
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p datamancerd --features web-ui config_api` → FAIL.

- [ ] **Step 3: Implement**

In `ConfigState::write` (config_api.rs:101-138): replace the direct `spawn_blocking(config.save)` + disk-vs-boot flag with delegation:

```rust
let restart_required = self.inner.hub.apply_full(config.clone()).await?;
self.inner
    .restart_required
    .store(restart_required, Ordering::Relaxed);
```

(Keep the existing secret-preservation (`REDACTED_SECRET`) handling *before* the hub call — that logic stays in the web layer.) Also update `read_disk`-driven restart_required computation (the "external edit shows up" test path around config_api.rs:140-144 and 323): compute the flag with `cold_divergence(state.boot(), &disk_config)` instead of plain inequality — external hot-only hand edits are still *not* applied (hand edits are boot-time only, per spec), so for the read-disk path keep flagging any divergence but split the message… **Decision (keep it honest and simple):** for `read_disk` comparisons, `restart_required = disk != boot-config-as-currently-known-by-hub` is replaced by `!cold_divergence(&boot, &disk).is_empty() || disk != hub_current` — a hand-edited hot field that the hub hasn't applied still requires a restart to take effect. Implement exactly that: expose `ConfigHub::current(&self) -> Config` (clone under the lock) and compute both terms.

In `server.rs::bootstrap`, construct `ConfigState::new(config_path.clone(), config.clone(), config_hub.clone())` after the hub exists.

- [ ] **Step 4: Run tests**

Run: `cargo test -p datamancerd --all-features && cargo clippy -p datamancerd --all-features --all-targets -- -D warnings`
Expected: PASS (all pre-existing config_api tests updated and green).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(datamancerd): web config writes route through ConfigHub — one hot-path writer, classified restart_required"
```

---

### Task 10: `AppHandle` facade methods

**Files:**
- Modify: `crates/datamancer-client/src/app/mod.rs`

**Interfaces:**
- Consumes: Task 6 vocabulary.
- Produces (app-facing contract):

```rust
pub struct DaemonConfig { pub config: serde_json::Value, pub restart_required: bool }
pub enum Applied { Live, RestartRequired }
pub async fn get_config(&mut self) -> Result<DaemonConfig, ClientError<Iceoryx2ClientError>>
pub async fn configure_provider(&mut self, provider: &str, settings: serde_json::Value) -> Result<Applied, ClientError<Iceoryx2ClientError>>
pub async fn remove_provider(&mut self, provider: &str) -> Result<Applied, ClientError<Iceoryx2ClientError>>
pub async fn shutdown_daemon(self) -> Result<(), ClientError<Iceoryx2ClientError>>
```

- [ ] **Step 1: Write the failing tests**

The facade tests run against the fake `ControlEndpoint` used by the cycle-1/2 tests in `app/` (see existing `set_credentials`-style coverage — mirror its fixture). Add:

```rust
#[tokio::test]
async fn configure_provider_maps_applied_live() { /* fake endpoint replies {"ok":true,"applied":"live"} → Ok(Applied::Live) */ }

#[tokio::test]
async fn get_config_requires_payload() { /* fake ok reply WITHOUT config → Err(transport/protocol error), mirroring get_credentials' missing-payload handling */ }
```

If the existing facade tests exercise methods only through a fake `Iceoryx2Client` seam that doesn't accommodate raw control replies, follow whatever pattern `get_credentials`' missing-payload path is tested with; if it is untested at that layer, cover the mapping functions directly (extract `fn applied_from(reply: &Reply) -> Result<Applied, …>` and unit-test it).

- [ ] **Step 2: Run to verify failure** — `cargo test -p datamancer-client --features app` → FAIL to compile.

- [ ] **Step 3: Implement** (place after `clear_credentials`, matching its doc style):

```rust
/// The daemon's current configuration (TOML as JSON) and whether any
/// cold-classified field awaits a restart.
#[derive(Debug, Clone, PartialEq)]
pub struct DaemonConfig {
    pub config: serde_json::Value,
    pub restart_required: bool,
}

/// How a mutating config op took effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// Applied to the running daemon.
    Live,
    /// Persisted; takes effect at the next daemon start.
    RestartRequired,
}

impl AppHandle {
    /// Fetch the daemon's current config.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with stable codes, or a transport failure
    /// (including a malformed ok reply missing the `config` payload).
    pub async fn get_config(&mut self) -> Result<DaemonConfig, ClientError<Iceoryx2ClientError>> {
        let reply = self.client.control_request(&Request::GetConfig).await?;
        let config = reply.config.ok_or_else(|| {
            ClientError::Transport(Iceoryx2ClientError::Protocol(
                "ok get-config reply missing config payload".to_string(),
            ))
        })?;
        Ok(DaemonConfig {
            config,
            restart_required: reply.restart_required.unwrap_or(false),
        })
    }

    /// Enable (or re-configure) a compiled-in provider. `settings` is the
    /// provider's config-section shape, e.g.
    /// `json!({"account_type": "live"})`; pass `json!({})` for defaults.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with `unknown_provider`,
    /// `unknown_config_field`, `bad_request`, or `permission_denied`; or a
    /// transport failure.
    pub async fn configure_provider(
        &mut self,
        provider: &str,
        settings: serde_json::Value,
    ) -> Result<Applied, ClientError<Iceoryx2ClientError>> {
        let reply = self
            .client
            .control_request(&Request::ConfigureProvider {
                provider: provider.to_string(),
                settings,
            })
            .await?;
        Ok(applied_from(&reply))
    }

    /// Disable a compiled-in provider. Stored credentials are untouched;
    /// re-enabling reuses them.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with `unknown_provider` or
    /// `permission_denied`; or a transport failure.
    pub async fn remove_provider(
        &mut self,
        provider: &str,
    ) -> Result<Applied, ClientError<Iceoryx2ClientError>> {
        let reply = self
            .client
            .control_request(&Request::RemoveProvider {
                provider: provider.to_string(),
            })
            .await?;
        Ok(applied_from(&reply))
    }

    /// Deliberately stop the daemon (graceful drain). Consumes the handle:
    /// the connection is gone once the daemon exits.
    ///
    /// # Errors
    ///
    /// `ClientError::Control` with `permission_denied`, or a transport
    /// failure sending the request.
    pub async fn shutdown_daemon(mut self) -> Result<(), ClientError<Iceoryx2ClientError>> {
        self.client.control_request(&Request::Shutdown).await.map(|_| ())
    }
}

fn applied_from(reply: &Reply) -> Applied {
    match reply.applied.as_deref() {
        Some("restart_required") => Applied::RestartRequired,
        _ => Applied::Live,
    }
}
```

Also update `close()`'s doc comment (`app/mod.rs:290-292`): drop "deliberate daemon stop is a cycle-3 capability" in favor of pointing at `shutdown_daemon`. Export `DaemonConfig`/`Applied` from the `app` module the same way `EnsureError` etc. are exported.

- [ ] **Step 4: Run tests**

Run: `cargo test -p datamancer-client --all-features && cargo clippy -p datamancer-client --all-features --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/datamancer-client
git commit -m "feat(datamancer-client): AppHandle get_config/configure_provider/remove_provider/shutdown_daemon"
```

---

### Task 11: End-to-end test, docs, version bumps, CI gates

**Files:**
- Create: `crates/datamancerd/tests/config_service_e2e.rs` (`#[ignore]`d)
- Modify: `crates/datamancerd/README.md` (config-service section: ops, wire shapes, gating, classification, shutdown semantics, disabled-by-default posture)
- Modify: root `CLAUDE.md` + `crates/datamancer/README.md` if the invariants text mentions provider construction (add one line: compiled-in providers, runtime enable/disable via settings watch)
- Modify: `crates/datamancer/Cargo.toml` (0.3.0 → 0.4.0), `crates/datamancer-client/Cargo.toml` (0.3.0 → 0.4.0), `crates/datamancerd/Cargo.toml` (0.3.0 → 0.4.0) — client/daemon in lockstep; bump intra-workspace dependency version requirements to match.

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the e2e** (pattern: `crates/datamancerd/tests/credential_broker_e2e.rs` — copy its daemon-spawn scaffolding, env hygiene (`env_remove` all four `ALPACA_*`, `DATAMANCER_CREDENTIALS_FILE` → tempdir), and UDS request helper verbatim). Scenario:

1. Boot the daemon with a config containing **no** provider sections.
2. `get-config` → ok, `restart_required == false`, `config.provider` empty.
3. `subscribe`-shaped `open-client` against `alpaca-crypto` → provider error (parked/disabled), or skip if the harness subscribes lazily — assert instead that `configure-provider` is required before data flows.
4. `set-credentials` for `alpaca-crypto` (works while disabled — hub seeds all compiled-in ids).
5. `configure-provider alpaca-crypto {"account_type":"paper","venue":"us"}` → `{"ok":true,"applied":"live"}`.
6. `get-config` → section present.
7. Open a client + subscribe BTC/USD trades → events flow (live network; this test is `#[ignore]`d like `daemon_e2e`).
8. `remove-provider alpaca-crypto` → `applied:"live"`; a fresh subscribe eventually fails or the stream sees `ProviderDisconnected { reason: "provider disabled" }`.
9. `{"op":"shutdown"}` → `{"ok":true}` and the daemon process exits 0 within the shutdown timeout (assert on `child.wait()`).

Also include one **non-network** e2e (not `#[ignore]`d if the daemon boots without providers cleanly and needs no iceoryx2 runtime — if the daemon still requires a live iceoryx2 node at boot, keep it `#[ignore]`d too): boot with zero providers → ping → get-config → configure-provider with an unknown field → expect `unknown_config_field` → shutdown op → clean exit.

- [ ] **Step 2: Run the e2e**

Run: `cargo test -p datamancerd --test config_service_e2e -- --ignored --nocapture`
Expected: PASS against the live environment (needs Alpaca credentials in the isolated store — the test sets them via the broker from env vars it reads itself, or document the required env in the test header, matching `credential_broker_e2e.rs`'s approach).

- [ ] **Step 3: Docs**

`crates/datamancerd/README.md` — add a "Config service" section documenting: the four ops with example request/reply lines (copy the wire shapes from Task 6's tests), gating (mutating ops + shutdown same-uid; get-config ungated; none available on WS), the hot/cold table location and current classification (provider sections hot; everything else cold), the disabled-until-configured posture (scaffolded config enables nothing), `remove-provider` leaves credentials stored, `shutdown` runs the full drain path (tap-log flush before sink flush before service drop), and that the daemon is the sole config writer at runtime (hand edits at boot only). Update the credentials section's cross-references if they say providers exist only when configured.

Root `CLAUDE.md`: in the `datamancerd` bullet, add the config service ops and the fixed-set/runtime-enable model; in the `datamancer` bullet mention `SettingsSource` alongside the credential-source API.

- [ ] **Step 4: Version bumps + lockstep check**

Bump the three crate versions to 0.4.0 and any intra-workspace `version = "0.3"` requirements pointing at them. Run: `cargo test -p datamancerd daemon_and_client_versions_stay_in_lockstep` → PASS.

- [ ] **Step 5: Full local CI gates**

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --all-targets --all-features -- -D warnings
git fetch origin main
cargo deny check
.github/scripts/semver-checks.sh origin/main
```

Expected: all green. semver-checks will report the breaking `datamancer`/`datamancer-client` changes as **allowed** because of the minor bumps (pre-1.0); if it still fails, the failure text names the missing bump.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test(datamancerd): config-service e2e; docs and 0.4.0 lockstep version bumps for cycle 3"
```
