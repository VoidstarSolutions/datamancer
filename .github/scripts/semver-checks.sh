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
