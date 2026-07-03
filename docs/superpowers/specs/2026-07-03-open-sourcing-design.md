# Open-Sourcing Datamancer — Program Design

**Date:** 2026-07-03
**Status:** Approved (brainstorm complete; per-sub-project plans to follow)

## Goal

Take the private `VoidstarSolutions/datamancer` workspace public as a
professional open-source project: installable daemon with auto-update,
published library crates, a CI pipeline that prevents breaking API changes,
and a first-run setup flow with secure provider-key storage.

## Decisions (fixed)

| Decision | Choice |
|---|---|
| Distribution model | Both, daemon-first: `datamancerd` binary releases are the headline product; all six crates publish to crates.io (five libraries plus the `datamancerd` binary crate for `cargo install`) |
| License | MIT OR Apache-2.0, copyright Voidstar Solutions |
| History | Rewrite with `git-filter-repo` (keep commit granularity, strip internal paths); full-history `gitleaks` scan first — any hit downgrades to squash |
| Key storage | OS keychain via the `keyring` crate; resolution precedence env var → keychain → absent; env vars remain the headless-server path |
| CI platforms | Full CI on Linux (PRs + main). Windows job covers only the ws-portable subset |
| Release targets | `x86_64-unknown-linux-gnu` + `aarch64-apple-darwin` for the daemon (macOS is arm64-only; no x86_64 mac). Windows ships no daemon — its support boundary is the ws transport + `datamancer-client` (ws feature), enforced by CI, distributed via crates.io |
| Release tooling | `cargo-dist` (installers, artifacts, `axoupdater` auto-update) + `release-plz` (crates.io publishing, changelogs, version bumps, semver gating) |
| Versioning | Stays 0.x; `cargo-semver-checks` enforces compatibility within 0.x minor lines |

## Amendment (2026-07-03, post-SP1-Task-4)

The SP1 license gate found SurrealDB is BUSL-1.1 (source-available) as a
direct default dependency, plus two surrealdb-rooted advisories with no
upstream fix. Decision: replace the storage backend with Turso — see
`2026-07-03-turso-storage-design.md`. That port runs as **SP6**, sequenced
after SP1 and before SP5 (the public repo never ships a BUSL tree).
`deny.toml` carries clearly-marked transitional exceptions until then.
MPL-2.0 is allowed for dependencies (file-level copyleft, unmodified use).

## Sub-projects and ordering

Each is its own spec → plan → implementation cycle. Order is load-bearing at
the bookends: CI first (protects everything after; its config survives the
history rewrite), the rewrite last (so it captures all prior commits and
SHAs break only once). PR #11 merges before any of this begins.

### SP1 — CI pipeline

- `rust-toolchain.toml` pins channel + components (rustfmt, clippy) so local
  and CI formatting agree byte-for-byte (rustfmt drift already bit us once).
- **Core job** (ubuntu, PR + main push): `cargo fmt --check` →
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` →
  `cargo test --workspace --all-features`. Cached (`Swatinem/rust-cache`).
- **Feature matrix**: `cargo hack check --each-feature --workspace` — all
  features are off-by-default, so `--all-features` alone lies about
  combination breaks.
- **Gates job**: `cargo deny check` (licenses, advisories) +
  `cargo semver-checks` for the library crates (vs. crates.io once
  published; vs. baseline git tag until then).
- **Windows job**: build/test only `datamancer-transport-ws` and
  `datamancer-client --features ws` on `windows-latest` — the support
  boundary lives in CI, not just prose.
- **Live e2e job**: the `#[ignore]`d daemon suites (`daemon_e2e`,
  `client_transport_e2e`) run nightly and on a `run-e2e` PR label — off the
  PR critical path (slow, needs live iceoryx2 runtime).

### SP2 — Setup flow + key storage

- `datamancerd setup` subcommand (the binary already owns config loading).
- Config lives in the platform config dir by default; `--config` override
  unchanged.
- Per provider (Alpaca first): resolve keys env var → OS keychain → absent;
  show the source; ask whether to enable the provider; offer to enter keys
  if absent.
- Interactively entered keys are offered keychain storage (`keyring` crate,
  service name `datamancerd`, one entry per provider/key). The config
  records only that a provider is enabled and `credentials = "env" |
  "keychain"` — **never secrets in TOML**.
- `datamancerd setup --check`: non-interactive resolution validation with
  useful diagnostics, for headless hosts (secret-service is often absent on
  servers; env vars are the documented path there).
- Daemon startup uses the same resolution chain as setup, so they cannot
  disagree.

### SP3 — Release engineering

- `release-plz` on main: rolling release PR with version bumps + changelogs
  from conventional commits (history already follows them), semver-checks,
  and on merge publishes to crates.io in dependency order:
  `datamancer-core` → transports → `datamancer-client` → `datamancer` →
  `datamancerd`.
- `cargo-dist` on the resulting tag: daemon binaries for the two targets,
  `curl | sh` installer, Homebrew formula, `axoupdater`-backed self-update
  (functional once releases are public).

### SP4 — Docs & community files

- README rework as the public front door (CLAUDE.md remains but is not the
  de facto public doc); crate READMEs already exist and stay authoritative
  per crate.
- CONTRIBUTING, CODE_OF_CONDUCT, SECURITY.md with a **private**
  vulnerability-reporting channel (the WS listener is a network-reachable
  surface — this file is not optional), issue/PR templates.
- docs.rs metadata + `doc_cfg` so feature-gated modules (`transport`,
  `transport_ws`, `client`) render on docs.rs.

### SP5 — Scrub, legal & flip-public (last)

- `LICENSE-MIT` + `LICENSE-APACHE` at root; `license = "MIT OR Apache-2.0"`
  in workspace `Cargo.toml`, inherited by all six crates (only 3/6 have any
  license metadata today).
- `cargo deny` license-allowlist run as the compatibility receipt.
- crates.io name-availability check for `datamancer*` — do this **early**
  even though the sub-project runs last; a collision reshapes naming.
- `gitleaks` over full history. Any confirmed secret ⇒ abandon rewrite,
  squash to a fresh initial commit instead.
- `git-filter-repo` strip list: `.claude/`, `.superpowers/`, `.omniscient/`,
  `.datamancerd/`, `docs/superpowers/`, `docs/plans/`, `omniscient.toml`,
  plus anything the scan flags. Run on a fresh clone at flip-time; the
  private repo stays the working origin until cutover.

## Out of scope

- Windows daemon support (UDS control socket and iceoryx2 shm are
  POSIX-shaped; porting is a separate future decision).
- x86_64 macOS.
- Encrypted-file secret storage (keychain + env covers v1; revisit if
  headless demand appears).
- 1.0 versioning commitments.

## Success criteria

- A stranger on Linux or an arm64 Mac installs `datamancerd` with one
  command, runs `datamancerd setup`, enables Alpaca with keys stored in
  their OS keychain, and streams events — without reading source.
- A Rust consumer adds `datamancer-client` from crates.io and compiles
  against the `Client` trait.
- A PR that breaks a published library API fails CI before review.
- The public history contains no secrets and no internal working artifacts.
