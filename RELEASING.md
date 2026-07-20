# Releasing datamancer

Releases are automated with [release-plz](https://release-plz.dev). We produce
**git tags + GitHub Releases + changelogs only** — nothing is published to
crates.io. Config: `release-plz.toml`. Workflow: `.github/workflows/release-plz.yml`.

## How a release happens

1. You merge normal PRs to `main` using [Conventional Commits] — **without
   touching the version**. See "Never bump the version by hand" below.
2. The `release-plz PR` job opens/updates a **"chore: release"** PR that bumps
   the single workspace version (`[workspace.package] version` in the root
   `Cargo.toml`) and updates changelogs. It runs through normal CI.
3. You review and merge that PR.
4. The `release-plz release` job creates the git tag (`vX.Y.Z`) and a GitHub
   Release with the changelog. Done — no crates.io.

### Never bump the version by hand

The version field belongs to release-plz. The `release` job runs on **every**
push to `main` and asks one question: is there a tag for the version in
`Cargo.toml`? If you hand-bump the version in a feature PR, merging that PR
tags and releases it immediately — ahead of any release PR — and the standing
`chore: release` PR then rebases onto that surprise tag and proposes a version
you didn't intend.

That is exactly what happened to `v0.6.0`, `v0.7.0`, and `v0.8.0`: each was
tagged by a feature merge rather than a release PR, so the standing release PR
(#39) was never the thing that released, and **every one of those GitHub
Releases has empty notes** — the release PR is the only step that writes
`CHANGELOG.md`, and it never merged. The tags themselves are fine; the
changelogs are the casualty.

Merge your feature PRs at the *current released version*. The release PR is the
only thing that edits `[workspace.package] version`.

### How the next version is computed (we're pre-1.0 — this is not plain semver)

While the major version is `0`, release-plz ([`next_version`]) demotes every
bump one level. Note especially that a plain `feat:` is **not** a minor bump:

| Commit | 0.x.y (today) | ≥1.0.0 (later) |
| --- | --- | --- |
| `fix:` / non-conventional | patch (0.7.0 → 0.7.1) | patch |
| `feat:` | patch (0.7.0 → 0.7.1) | minor |
| `feat!:` / `BREAKING CHANGE:` | **minor** (0.7.0 → 0.8.0) | major |

So a release cycle only reaches the next minor when it contains a breaking
change. If you want a minor bump for a non-breaking milestone, say so in the
release PR — override it there, not in a feature branch.

[`next_version`]: https://docs.rs/next_version/latest/next_version/

All seven crates share one version, so every release re-tags the whole
workspace together. This keeps `datamancer-client` and `datamancerd` in
lockstep for the ping version gate.

**Known gotcha — the Windows `datamancer-winsec` dep requirement.** `datamancer-winsec`
is pulled in under `[target.'cfg(windows)'.dependencies]` by `datamancer-client`
and `datamancerd`. release-plz does **not** reliably bump the `version = "X.Y.Z"`
requirement inside target-gated dependency tables, so after each workspace bump
that literal must be **hand-aligned** to the new version in both crates'
`Cargo.toml` (else the path dep resolves fine locally but the requirement lags,
and a published build would mismatch). Grep for `datamancer-winsec = {` before
merging a release PR. (History: commits aligning this to `0.7.0`, then `0.8.0`.)

One version and one tag get **one changelog**: the root `CHANGELOG.md`, owned
by the `datamancer` package (`changelog_path` + `changelog_include` in
`release-plz.toml`) exactly as it owns the tag and the Release. The other six
crates have `changelog_update = false` so there is a single writer — releases
before `v0.9.0` were backfilled with git-cliff and carry no per-crate history.

Semver/API-break protection is the `.github/scripts/semver-checks.sh` gate in
`ci.yml` (cargo-semver-checks vs. the PR base). release-plz does not run its own
semver check (`semver_check = false`).

Because release-plz owns the version, every PR is checked at an *unchanged*
version, so `check-release` reports "requires new major/minor" for any API
change — it cannot be a plain pass/fail on the bump. The gate therefore
enforces **declaration** instead: a detected break must be marked by some commit
in the PR range (`type!: subject` or a `BREAKING CHANGE:` footer). That marker
is precisely what release-plz reads to size the bump, so the gate is really
asking "will the release PR pick the right version for this change?" — with an
undeclared break the answer is no. Additive-only changes pass with a note.

Do not silence the gate by adding a marker to a break you didn't intend; make
the change additive instead.

## Workspace manifest constraints (load-bearing for `git_only`)

Even though we never publish, release-plz's `git_only` version diffing runs
`cargo package --allow-dirty --workspace` at the previous release tag (to detect
which crates changed). `cargo package` is strict about manifests, so the whole
workspace must stay packageable or **every** release after the first fails
(release-plz [#2595]). Three rules keep it green:

1. **No git/path dependency without a registry `version`.** Every internal
   path dep carries `version = "0.5.0"` alongside `path = "..."`
   (`cargo package` rewrites `path` → registry and needs a version to write),
   and external deps must resolve on crates.io — `oxidized_alpaca` is a
   published crate (`version = "0.0.9"`), **not** a `git = ` dependency. If you
   add a `git = ` dependency to any workspace member, `cargo package` cannot
   package it and releases break; publish it (or a fork) to crates.io first.
2. **No `publish = false` on a crate that another workspace member depends on.**
   `cargo package --workspace` does *not* skip `publish = false` crates (that is
   `cargo publish` behaviour only), and a `publish = false` crate is absent from
   the temporary registry its dependents resolve against — so its dependents
   fail to package. Only `datamancerd` keeps `publish = false` (nothing depends
   on it; it is the binary and is never published). The library crates rely on
   the release-plz backstops below, not `publish = false`, to stay off crates.io.
3. **Every non-`publish = false` crate needs a `license` field.** `cargo deny`'s
   licenses gate skips `publish = false` crates but requires a license on the
   rest. All library crates declare `license = "MIT OR Apache-2.0"`.

The real "never crates.io" backstops are in `release-plz.toml`
(`git_only = true` + `publish = false` at the workspace level), not the
per-crate `publish = false` — release-plz never runs `cargo publish`.

[#2595]: https://github.com/release-plz/release-plz/issues/2595

## One-time setup: the GitHub App token

release-plz opens a PR that must trigger CI. GitHub's default `GITHUB_TOKEN`
cannot trigger workflows on PRs it creates, so we mint a token from a dedicated
GitHub App instead.

1. Create the App: **GitHub → Settings → Developer settings → GitHub Apps →
   New GitHub App**.
   - GitHub App name: `datamancer-release-plz` (any unique name).
   - Homepage URL: the repo URL.
   - Uncheck **Webhook → Active**.
   - **Repository permissions**: **Contents: Read and write**,
     **Pull requests: Read and write**. Leave everything else "No access".
   - **Where can this GitHub App be installed?**: "Only on this account".
   - Create the app.
2. On the App's page, note the **App ID**. Under **Private keys**, click
   **Generate a private key** — a `.pem` downloads.
3. Install the App: App page → **Install App** → install on
   `VoidstarSolutions/datamancer` (select "Only select repositories" → this repo).
4. Add repo secrets (**repo → Settings → Secrets and variables → Actions →
   New repository secret**):
   - `RELEASE_PLZ_APP_ID` = the App ID from step 2.
   - `RELEASE_PLZ_APP_PRIVATE_KEY` = the **entire contents** of the downloaded
     `.pem` (including the `-----BEGIN…` / `-----END…` lines).

## One-time setup: the baseline tag

`git_only` mode computes "what changed since the last release" from git tags.
Before the first automated run there are no release tags, so `main` needs one
at the current unified version, `v0.5.0`.

**You don't have to do this manually.** The `release` job runs on every push
to `main`, including the merge that lands this automation — with no `v0.5.0`
tag yet, release-plz will likely create the `v0.5.0` tag and a GitHub Release
for it itself on that first merge. This is harmless (no crates.io involved),
but the changelog on that first auto-generated Release will cover the entire
project history up to that point, which may be noisier than you want as a
"baseline".

If you'd rather have a clean, empty baseline, pre-create and push the tag
**before** merging the PR that adds this automation:

```bash
git checkout main && git pull
git tag -a v0.5.0 -m "Baseline release v0.5.0 (workspace version unification)"
git push origin v0.5.0
```

Either way, once `v0.5.0` exists, the first `release-plz PR` run will propose
the next version from commits made after it.

## First release: verify

On the first real release (whether it's the auto-generated `v0.5.0` baseline
or the first `chore: release` PR you merge after it), confirm:

- **Exactly one** `vX.Y.Z` git tag was created — not one per crate.
- **Exactly one** GitHub Release was created.
- **Zero** crates.io / registry network activity in the workflow run logs.

The single-version workspace config produces exactly one tag and one Release,
so this shouldn't happen. If release-plz ever does create multiple Releases
pointing at the same tag (one per crate), add per-package overrides to
`release-plz.toml` — a `[[package]]` block with `git_release_enable = false`
for every crate except the one you want to author the Release.
