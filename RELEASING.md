# Releasing datamancer

Releases are automated with [release-plz](https://release-plz.dev). We produce
**git tags + GitHub Releases + changelogs only** — nothing is published to
crates.io. Config: `release-plz.toml`. Workflow: `.github/workflows/release-plz.yml`.

## How a release happens

1. You merge normal PRs to `main` using [Conventional Commits]
   (`fix:` → patch, `feat:` → minor, `feat!:`/`BREAKING CHANGE:` → major).
2. The `release-plz PR` job opens/updates a **"chore: release"** PR that bumps
   the single workspace version (`[workspace.package] version` in the root
   `Cargo.toml`) and updates changelogs. It runs through normal CI.
3. You review and merge that PR.
4. The `release-plz release` job creates the git tag (`vX.Y.Z`) and a GitHub
   Release with the changelog. Done — no crates.io.

All seven crates share one version, so every release re-tags the whole
workspace together. This keeps `datamancer-client` and `datamancerd` in
lockstep for the ping version gate.

Semver/API-break protection is the existing `.github/scripts/semver-checks.sh`
gate in `ci.yml` (cargo-semver-checks vs. the PR base). release-plz does not run
its own semver check (`semver_check = false`).

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
Before the first automated run there are no release tags, so create one at the
current unified version (see the bootstrap step in the plan / below):

```bash
git checkout main && git pull
git tag -a v0.5.0 -m "Baseline release v0.5.0 (workspace version unification)"
git push origin v0.5.0
```

After this, the first `release-plz PR` run will propose the next version from
commits made after `v0.5.0`.
