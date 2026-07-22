#!/usr/bin/env bash
# Semver gate for the five library crates (datamancerd is a binary).
# Pre-publication baseline is a git rev (normally the PR base); once crates
# are on crates.io, SP3's release tooling takes over the authoritative check.
# A crate that does not exist at the baseline rev is new - nothing to break.
#
# This gate does NOT require the version to be bumped in the PR. release-plz
# owns `[workspace.package] version` and bumps it in the release PR only (see
# RELEASING.md), so every PR is checked at an unchanged version and
# `cargo semver-checks check-release` reports "requires new {major,minor}"
# for any API change. What we enforce instead is that a *breaking* change is
# declared with a Conventional Commits breaking marker - the marker is what
# release-plz reads to compute the bump, so an undeclared break is exactly the
# case that would ship under a too-small version.
set -euo pipefail

BASELINE_REV="${1:?usage: semver-checks.sh <baseline-rev>}"

# A missing baseline must fail the gate, not silently skip every crate below.
git rev-parse --verify --quiet "${BASELINE_REV}^{commit}" >/dev/null \
  || { echo "::error::baseline rev '${BASELINE_REV}' not found"; exit 1; }

CRATES=(
  datamancer-core
  datamancer-transport-ws
  datamancer-transport-iceoryx2
  datamancer-client
  datamancer
)

breaking=()
additive=()

for crate in "${CRATES[@]}"; do
  if ! git cat-file -e "${BASELINE_REV}:crates/${crate}/Cargo.toml" 2>/dev/null; then
    echo "-- ${crate}: absent at ${BASELINE_REV} (new crate), skipping"
    continue
  fi
  echo "-- ${crate}: checking against ${BASELINE_REV}"
  # check-release exits non-zero for both "requires new major" and "requires
  # new minor"; the summary line is what distinguishes them, so capture rather
  # than let `set -e` abort here.
  out=""
  if out=$(cargo semver-checks check-release \
             --baseline-rev "${BASELINE_REV}" \
             --package "${crate}" \
             --all-features 2>&1); then
    echo "${out}"
    continue
  fi
  echo "${out}"
  if grep -q 'requires new major version' <<<"${out}"; then
    breaking+=("${crate}")
  elif grep -q 'requires new minor version' <<<"${out}"; then
    additive+=("${crate}")
  else
    # Not a semver verdict - a genuine tool/build failure. Never swallow it.
    echo "::error::${crate}: cargo semver-checks failed without a semver verdict"
    exit 1
  fi
done

if [[ ${#additive[@]} -gt 0 ]]; then
  echo "-- additive API change (no break): ${additive[*]}"
fi

if [[ ${#breaking[@]} -eq 0 ]]; then
  echo "-- no breaking changes vs ${BASELINE_REV}"
  exit 0
fi

# A break is fine - it just has to be declared, so release-plz bumps for it.
# Conventional Commits spells that either `type(scope)!:` or a BREAKING CHANGE
# footer.
#
# Capture the log ONCE into variables and scan with here-strings. Piping
# `git log` straight into `grep -q` is unsafe under `set -o pipefail`: grep
# short-circuits on the first match and closes the pipe, `git log` then dies of
# SIGPIPE (141), and pipefail promotes that to a pipeline failure - a false
# "undeclared" even though the marker matched. Print the resolved range so this
# is never debugged blind (on a pull_request the checkout is a detached merge
# ref; the range still spans the PR commits).
range="${BASELINE_REV}..HEAD"
echo "-- marker scan: ${range} = $(git rev-parse --short "${BASELINE_REV}")..$(git rev-parse --short HEAD), $(git rev-list --count "${range}") commits"
subjects=$(git log --format='%s' "${range}")
bodies=$(git log --format='%b' "${range}")
if grep -qE '^[a-zA-Z]+(\([^)]*\))?!:' <<<"${subjects}" \
   || grep -qE '^BREAKING[ -]CHANGE:' <<<"${bodies}"; then
  echo "-- breaking change in ${breaking[*]}, declared via a breaking commit marker - OK"
  exit 0
fi

# No marker matched - dump the subjects we scanned so the failure is diagnosable.
echo "-- no marker in these ${range} subjects:" >&2
sed 's/^/     | /' >&2 <<<"${subjects}"

cat >&2 <<EOF
::error::undeclared breaking change in: ${breaking[*]}
cargo-semver-checks found a breaking API change, but no commit in
${BASELINE_REV}..HEAD declares one. release-plz computes the version from these
markers, so this would release under a too-small version.

Fix by declaring the break (do NOT edit the version - release-plz owns it):
  * mark the commit \`type!: subject\` (e.g. \`feat!: split Provider::supports\`), or
  * add a \`BREAKING CHANGE: <what breaks>\` footer to its message.
If the break was unintentional, make the change additive instead.
EOF
exit 1
