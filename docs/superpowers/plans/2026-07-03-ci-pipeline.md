# CI Pipeline Implementation Plan (Open-Sourcing SP1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A GitHub Actions CI pipeline that pins the toolchain, gates every PR on fmt/clippy/tests/feature-matrix/licenses/semver, marks the Windows support boundary, and runs the live e2e suites nightly.

**Architecture:** One `ci.yml` with four independent jobs (core, features, gates, windows) triggered on PRs and main pushes, plus a separate `e2e.yml` (nightly cron + `run-e2e` PR label). Toolchain pinned via `rust-toolchain.toml` so local and CI rustfmt/clippy agree byte-for-byte. Semver checking baselines against the PR base branch until crates are published (per the design spec).

**Tech Stack:** GitHub Actions, `dtolnay/rust-toolchain` (respects `rust-toolchain.toml`), `Swatinem/rust-cache`, `taiki-e/install-action` (prebuilt cargo-hack / cargo-deny / cargo-semver-checks), gitleaks-style tools NOT in scope (SP5).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-03-open-sourcing-design.md` (SP1 section is authoritative).
- Toolchain pin: channel `1.96.1`, components `rustfmt`, `clippy` (current local toolchain — CI must match local exactly).
- Full CI on Linux only (`ubuntu-latest`); Windows job covers ONLY `datamancer-transport-ws` + `datamancer-client --features ws`.
- Workspace clippy is `pedantic = deny`; CI must run `--all-targets --all-features -- -D warnings` — identical to the local command in root `CLAUDE.md`.
- The `#[ignore]`d suites (`daemon_e2e`, `client_transport_e2e`) stay OFF the PR critical path: nightly + `run-e2e` label only.
- Semver checks apply to the five library crates only (`datamancer-core`, `datamancer-transport-ws`, `datamancer-transport-iceoryx2`, `datamancer-client`, `datamancer`) — `datamancerd` is a binary.
- Never weaken a lint or skip a test to make CI green: fix the code, or stop and report.
- All commits follow the repo's conventional-commit style (`ci:`, `chore:` prefixes).

---

### Task 1: Pin the toolchain

**Files:**
- Create: `rust-toolchain.toml`

**Interfaces:**
- Produces: pinned toolchain `1.96.1` that every later job and every local build resolves automatically; CI setup steps rely on `dtolnay/rust-toolchain@stable` reading this file.

- [ ] **Step 1: Confirm the current toolchain is 1.96.1 (the pin must match what the repo is developed with)**

Run: `rustc --version`
Expected: `rustc 1.96.1 (...)`. If it differs, use the version actually installed — the point is local/CI agreement.

- [ ] **Step 2: Write `rust-toolchain.toml`**

```toml
# Pins the toolchain for every contributor and CI runner: rustfmt and clippy
# output must agree byte-for-byte between local runs and the pipeline.
# Bump deliberately (a PR that updates this file and fixes any new lints).
[toolchain]
channel = "1.96.1"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 3: Verify the pin resolves and the workspace still builds/formats identically**

Run: `rustup show active-toolchain && cargo fmt --check && cargo clippy --workspace --all-targets --all-features -- -D warnings`
Expected: active toolchain `1.96.1-<host>`, fmt check silent, clippy finishes with no errors. If `cargo fmt --check` fails, the local tree has drift — run `cargo fmt`, inspect the diff (it must touch only files already changed by this branch; if it reflows unrelated files, STOP and report — the pin does not match the version the repo was last formatted with, and the fix is `cargo fmt` in its own commit first).

- [ ] **Step 4: Commit**

```bash
git add rust-toolchain.toml
git commit -m "ci: pin toolchain to 1.96.1 with rustfmt and clippy"
```

---

### Task 2: Core CI workflow (fmt, clippy, tests)

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: workflow `CI` with job `core`; Tasks 3–6 append jobs `features`, `gates`, `windows` to this same file. Trigger block and concurrency group defined here are shared by all of them.

- [ ] **Step 1: Write the workflow with the `core` job**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

# One in-flight run per ref: a force-push cancels the stale run.
concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

env:
  CARGO_TERM_COLOR: always

jobs:
  core:
    name: fmt + clippy + tests (Linux)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      # Reads rust-toolchain.toml; the @stable ref is the action version,
      # not the toolchain choice.
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - name: rustfmt
        run: cargo fmt --check
      - name: clippy (pedantic, all targets, all features)
        run: cargo clippy --workspace --all-targets --all-features -- -D warnings
      - name: tests (all features)
        run: cargo test --workspace --all-features
```

- [ ] **Step 2: Validate the YAML locally**

Run: `command -v actionlint >/dev/null && actionlint .github/workflows/ci.yml || python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"`
Expected: `actionlint` silent (exit 0) or `yaml ok`.

- [ ] **Step 3: Run the job's commands locally as the ground-truth check**

Run: `cargo fmt --check && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace --all-features`
Expected: all green (this is the same gate the job will run).

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: core job - fmt, pedantic clippy, all-features tests on Linux"
```

---

### Task 3: Feature-matrix job (cargo-hack)

**Files:**
- Modify: `.github/workflows/ci.yml` (append job `features` after `core`)

**Interfaces:**
- Consumes: trigger/concurrency block from Task 2.
- Produces: job `features` proving every individual feature of every crate compiles alone (all features here are off-by-default, so `--all-features` alone hides combination breaks).

- [ ] **Step 1: Install cargo-hack locally**

Run: `cargo hack --version || cargo install cargo-hack --locked`
Expected: a version string (e.g. `cargo-hack 0.6.x`).

- [ ] **Step 2: Run the matrix locally FIRST — this is the step most likely to find real breakage**

Run: `cargo hack check --each-feature --no-dev-deps --workspace`
Expected: every `(crate, feature)` pair checks green. **If any pair fails:** that is a real bug this task exists to catch — fix the missing feature gate or `cfg` in the affected crate (typical shape: a module referencing an item from a sibling feature without `#[cfg(feature = ...)]`), re-run until green, and include the fix in this task's commit with its own explanation. Do not narrow the cargo-hack invocation to dodge a failure.

- [ ] **Step 3: Append the job to `ci.yml`**

```yaml
  features:
    name: feature matrix (cargo-hack)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@v2
        with:
          tool: cargo-hack
      - name: check each feature in isolation
        run: cargo hack check --each-feature --no-dev-deps --workspace
```

- [ ] **Step 4: Validate YAML**

Run: `command -v actionlint >/dev/null && actionlint .github/workflows/ci.yml || python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: cargo-hack each-feature matrix job"
```

(If Step 2 required code fixes, commit those first, separately: `fix(<crate>): gate <item> behind <feature> so each-feature builds stand alone`.)

---

### Task 4: License and advisory gate (cargo-deny)

**Files:**
- Create: `deny.toml`
- Modify: `.github/workflows/ci.yml` (append job `gates`; Task 5 adds a second step to this same job)

**Interfaces:**
- Consumes: trigger block from Task 2.
- Produces: `deny.toml` at repo root (also the SP5 "license compatibility receipt"); job `gates` that Task 5 extends.

- [ ] **Step 1: Install cargo-deny locally**

Run: `cargo deny --version || cargo install cargo-deny --locked`
Expected: version string.

- [ ] **Step 2: Write the starting `deny.toml`**

```toml
# Dependency gates: licenses must stay permissive (the workspace itself is
# MIT OR Apache-2.0), advisories must be triaged, sources must be crates.io.

[graph]
all-features = true

[licenses]
# Permissive-only allowlist. Extending it with another PERMISSIVE license
# (BSD-family, ISC, Zlib, Unicode) is routine; adding any copyleft or
# source-available license (GPL/AGPL/LGPL/MPL/BUSL/SSPL) is a project
# decision - stop and raise it, do not just append.
allow = [
    "MIT",
    "Apache-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "Zlib",
    "Unicode-3.0",
]

[advisories]
yanked = "deny"

[bans]
multiple-versions = "warn"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
```

- [ ] **Step 3: Run it and iterate the allowlist to reality**

Run: `cargo deny check`
Expected on first run: likely FAILS with specific unlisted licenses (the tree includes tokio-tungstenite, iceoryx2, surrealdb-sdk, ring-style crates with compound expressions). For each finding: if the license is permissive (BSD-family, ISC, Zlib, Unicode-*, CDLA-Permissive-2.0, OpenSSL notice-style), add it to `allow` with a trailing comment naming the crate that needs it (e.g. `"OpenSSL", # ring`). If anything copyleft or source-available appears, STOP and report it to the user — that is a dependency decision, not a config edit. `[advisories]` findings: report any active advisory to the user rather than adding an `ignore` entry silently. Re-run until `cargo deny check` exits 0 (multiple-versions warnings are non-fatal by config).

- [ ] **Step 4: Append the `gates` job to `ci.yml`**

```yaml
  gates:
    name: licenses + advisories + semver
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0 # semver step (next task) diffs against the PR base
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@v2
        with:
          tool: cargo-deny
      - name: cargo deny (licenses, advisories, sources)
        run: cargo deny check
```

- [ ] **Step 5: Validate YAML, then commit**

Run: `command -v actionlint >/dev/null && actionlint .github/workflows/ci.yml || python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"`
Expected: clean.

```bash
git add deny.toml .github/workflows/ci.yml
git commit -m "ci: cargo-deny license allowlist and advisory gate"
```

---

### Task 5: Semver gate (cargo-semver-checks vs PR base)

**Files:**
- Create: `.github/scripts/semver-checks.sh`
- Modify: `.github/workflows/ci.yml` (append steps to the existing `gates` job from Task 4)

**Interfaces:**
- Consumes: `gates` job (with `fetch-depth: 0`) from Task 4.
- Produces: `semver-checks.sh <baseline-rev>` — also runnable locally as `.github/scripts/semver-checks.sh origin/main`.

- [ ] **Step 1: Install cargo-semver-checks locally**

Run: `cargo semver-checks --version || cargo install cargo-semver-checks --locked`
Expected: version string.

- [ ] **Step 2: Write the script**

```bash
#!/usr/bin/env bash
# Semver gate for the five library crates (datamancerd is a binary).
# Pre-publication baseline is a git rev (normally the PR base); once crates
# are on crates.io, SP3's release tooling takes over the authoritative check.
# A crate that does not exist at the baseline rev is new - nothing to break.
set -euo pipefail

BASELINE_REV="${1:?usage: semver-checks.sh <baseline-rev>}"
CRATES=(
  datamancer-core
  datamancer-transport-ws
  datamancer-transport-iceoryx2
  datamancer-client
  datamancer
)

for crate in "${CRATES[@]}"; do
  if ! git cat-file -e "${BASELINE_REV}:crates/${crate}/Cargo.toml" 2>/dev/null; then
    echo "-- ${crate}: absent at ${BASELINE_REV} (new crate), skipping"
    continue
  fi
  echo "-- ${crate}: checking against ${BASELINE_REV}"
  cargo semver-checks check-release \
    --baseline-rev "${BASELINE_REV}" \
    --package "${crate}" \
    --all-features
done
```

- [ ] **Step 3: Make it executable and run it locally against origin/main**

Run: `chmod +x .github/scripts/semver-checks.sh && git fetch origin main && .github/scripts/semver-checks.sh origin/main`
Expected: five `-- <crate>: checking...` blocks (or `skipping` for crates absent on main), each ending in `Summary no semver update required` or a pass. **If a check FAILS:** the current branch really does break a library API relative to main. Do not suppress; report the finding — on this pre-1.0 codebase the resolution is normally "bump the minor version in that crate's Cargo.toml", which cargo-semver-checks accepts as the declared intent.

- [ ] **Step 4: Append the semver steps to the `gates` job in `ci.yml`**

```yaml
      - uses: taiki-e/install-action@v2
        with:
          tool: cargo-semver-checks
      - name: semver checks vs PR base
        # Only meaningful against a diff target; on main pushes the gate
        # already ran on the PR that merged.
        if: github.event_name == 'pull_request'
        run: .github/scripts/semver-checks.sh "origin/${{ github.base_ref }}"
```

- [ ] **Step 5: Validate YAML, then commit**

Run: `command -v actionlint >/dev/null && actionlint .github/workflows/ci.yml || python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"`
Expected: clean.

```bash
git add .github/scripts/semver-checks.sh .github/workflows/ci.yml
git commit -m "ci: cargo-semver-checks gate against the PR base for library crates"
```

---

### Task 6: Windows boundary job (ws-portable subset)

**Files:**
- Modify: `.github/workflows/ci.yml` (append job `windows`)

**Interfaces:**
- Consumes: trigger block from Task 2.
- Produces: job `windows` — the executable statement of the support boundary from the spec: ws transport + `datamancer-client --features ws`, nothing else.

- [ ] **Step 1: Append the job**

```yaml
  windows:
    # The support boundary, enforced: Windows gets the ws transport and the
    # ws-featured client ONLY. The daemon (UDS control socket) and the
    # iceoryx2 transport are POSIX; do not add them here - that is a porting
    # decision, not a CI edit (see the open-sourcing design spec).
    name: ws-portable subset (Windows)
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - name: build + test the portable subset
        run: cargo test -p datamancer-transport-ws -p datamancer-client --features datamancer-client/ws
```

- [ ] **Step 2: Sanity-check the exact cargo invocation on Linux (flag syntax, feature name)**

Run: `cargo test -p datamancer-transport-ws -p datamancer-client --features datamancer-client/ws`
Expected: compiles and passes (this validates the multi-package `--features pkg/feat` syntax; actual Windows compatibility is proven by the job itself in Task 8).

- [ ] **Step 3: Validate YAML, then commit**

Run: `command -v actionlint >/dev/null && actionlint .github/workflows/ci.yml || python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"`
Expected: clean.

```bash
git add .github/workflows/ci.yml
git commit -m "ci: Windows job pins the ws-portable support boundary"
```

---

### Task 7: Live e2e workflow (nightly + label)

**Files:**
- Create: `.github/workflows/e2e.yml`

**Interfaces:**
- Consumes: nothing from `ci.yml` (independent workflow by design — must not gate PRs).
- Produces: workflow `e2e` running the two `#[ignore]`d suites; requires repo secrets `ALPACA_KEY_ID` / `ALPACA_SECRET_KEY` (the daemon subscribes to the live Alpaca feed).

- [ ] **Step 1: Confirm the env-var names the daemon expects for Alpaca credentials**

Run: `grep -rn "env::var\|std::env" crates/datamancer/src/providers/ crates/datamancerd/src/ | grep -i "alpaca\|key\|secret" | head`
Expected: the exact variable names (e.g. `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY` or project-specific). **Use whatever the code actually reads in the workflow below — do not trust this plan's placeholder names.**

- [ ] **Step 2: Write the workflow (substituting the verified env names)**

```yaml
name: e2e

on:
  schedule:
    - cron: "17 6 * * *" # nightly, off the top-of-hour rush
  pull_request:
    types: [labeled, synchronize]

concurrency:
  group: e2e-${{ github.ref }}
  cancel-in-progress: true

env:
  CARGO_TERM_COLOR: always

jobs:
  live:
    # Nightly always; on PRs only when labeled 'run-e2e'.
    if: >-
      github.event_name == 'schedule' ||
      contains(github.event.pull_request.labels.*.name, 'run-e2e')
    name: live daemon e2e (iceoryx2 + ws)
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      # Live Alpaca credentials; the suites soft-gate on quiet feeds but the
      # daemon cannot start a provider without keys.
      APCA_API_KEY_ID: ${{ secrets.ALPACA_KEY_ID }}
      APCA_API_SECRET_KEY: ${{ secrets.ALPACA_SECRET_KEY }}
    steps:
      - uses: actions/checkout@v4
      - name: fail fast when secrets are absent
        run: |
          if [ -z "$APCA_API_KEY_ID" ]; then
            echo "::error::ALPACA_KEY_ID secret not configured - live e2e cannot run"
            exit 1
          fi
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - name: daemon e2e
        run: cargo test -p datamancerd --all-features --test daemon_e2e -- --ignored
      - name: client transport e2e
        run: cargo test -p datamancerd --all-features --test client_transport_e2e -- --ignored
```

- [ ] **Step 3: Validate YAML**

Run: `command -v actionlint >/dev/null && actionlint .github/workflows/e2e.yml || python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/e2e.yml')); print('yaml ok')"`
Expected: clean.

- [ ] **Step 4: Confirm the suites still pass locally (same commands the job runs)**

Run: `cargo test -p datamancerd --all-features --test client_transport_e2e -- --ignored`
Expected: `test result: ok. 2 passed` (takes ~45s; requires local Alpaca env vars and the iceoryx2 runtime — if absent locally, note it and rely on Task 8's label run).

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/e2e.yml
git commit -m "ci: nightly + on-label live e2e workflow"
```

---

### Task 8: Prove the pipeline on a real PR

**Files:**
- None created; this task exercises everything on GitHub.

**Interfaces:**
- Consumes: all prior tasks, pushed as one branch.

- [ ] **Step 1: Push the branch and open a PR**

```bash
git push -u origin HEAD
gh pr create --base main --title "ci: full pipeline (toolchain pin, gates, Windows boundary, nightly e2e)" --body "Implements SP1 of docs/superpowers/specs/2026-07-03-open-sourcing-design.md: pinned toolchain; Linux core/feature-matrix/license+semver gate jobs; Windows ws-portable boundary job; nightly + on-label live e2e.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

- [ ] **Step 2: Watch all four CI jobs to completion**

Run: `gh pr checks --watch`
Expected: `core`, `features`, `gates`, `windows` all pass. For each failure: read the log (`gh run view <run-id> --log-failed`), fix the root cause locally (a genuinely broken thing this pipeline just caught — never delete the failing step), commit, push, re-watch. The Windows job is first-run territory: a portable-subset compile error there is a real finding — fix it in the affected crate (`cfg(windows)`-safe code or a corrected dependency feature set), and if it cannot be fixed inside the subset boundary, STOP and report (the boundary itself may be drawn wrong, which is a spec question).

- [ ] **Step 3: Add repo secrets, then prove the e2e workflow via label**

```bash
gh secret set ALPACA_KEY_ID    # paste value when prompted; ask the user to run this if the key is not available to you
gh secret set ALPACA_SECRET_KEY
gh pr edit --add-label run-e2e
gh run watch $(gh run list --workflow e2e --limit 1 --json databaseId --jq '.[0].databaseId')
```

Expected: the `live` job runs and passes. If secrets can't be set in this session, ask the user to add them, and verify the workflow instead by confirming the label-trigger fires and the fail-fast step reports the missing secret cleanly (that path is also part of the design).

- [ ] **Step 4: Hand the PR to the user**

Report: PR URL, all check states, and any code fixes the pipeline forced (feature-gate repairs, license allowlist additions, semver bumps). The user merges; merging this PR is the moment `main` becomes protected by the pipeline.

---

## Self-Review Notes

- Spec coverage: toolchain pin (T1), core job (T2), cargo-hack matrix (T3), cargo-deny (T4), semver-checks vs base (T5), Windows boundary (T6), nightly+label e2e (T7), rust-cache used in every job (T2–T7). `docs.rs`/`doc_cfg` is SP4, publishing baselines are SP3 — intentionally absent here.
- The e2e secrets names in Task 7 are explicitly marked "verify against code first" — the one place a placeholder risk exists, converted into a verification step.
- Type/name consistency: job names `core`/`features`/`gates`/`windows` and the script path `.github/scripts/semver-checks.sh` are used identically across tasks.
