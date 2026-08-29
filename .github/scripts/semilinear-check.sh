#!/usr/bin/env bash
# Semi-linear history gate.
#
# GitHub has no native semi-linear merge mode. The merge queue's `MERGE`
# method preserves merge commits (what we want) but never rebases the PR
# head (what breaks us): a branch cut from an older main merges as-is and
# lands a non-semi-linear merge on main. `strict_required_status_checks_policy`
# ("require branches to be up to date") does not close the hole either - it is
# not enforced for PRs entering a merge queue. Three such merges reached main
# before this gate existed (#30, #51, #58), plus one back-merge bubble (#53).
#
# Semi-linear means, for every merge M landing on main with parents (P1, P2):
#   1. P1 is an ancestor of P2 - the branch sits *on top of* the base, and
#   2. P2's chain back to P1 contains no merge commits - no back-merge bubbles.
#
# Two call shapes:
#
#   semilinear-check.sh merge <merge-commit>
#     Authoritative. Used on the `merge_group` event, where HEAD is the exact
#     merge commit the queue will fast-forward onto main. Checking here cannot
#     be raced by main moving, which a PR-event check can.
#
#   semilinear-check.sh branch <base-rev> <head-rev>
#     Advisory pre-flight on the `pull_request` event, so a stale branch is
#     flagged in the PR rather than ejected from the queue later.
set -euo pipefail

fail() { echo "::error::$*"; exit 1; }

# Reject a merge whose second-parent chain contains merges of its own.
# Args: <base> <head> <label-for-errors>
check_no_bubbles() {
  local base="$1" head="$2" what="$3"
  local bubbles
  bubbles="$(git rev-list --merges "$head" --not "$base")"
  [ -z "$bubbles" ] || {
    echo "::error::$what contains merge commit(s) - a back-merge bubble." \
         "Rebase onto the base instead of merging it in:"
    # Not a pipe: `while read` in a pipeline runs in a subshell, so an exit
    # from it would not propagate. The explicit fail below is what exits.
    while read -r c; do
      [ -n "$c" ] && echo "::error::    $(git log -1 --format='%h %s' "$c")"
    done <<< "$bubbles"
    fail "back-merge bubble(s) found; rebase the branch"
  }
}

MODE="${1:?usage: semilinear-check.sh merge <sha> | branch <base> <head>}"

case "$MODE" in
  merge)
    MERGE_SHA="${2:?usage: semilinear-check.sh merge <merge-commit>}"
    git rev-parse --verify --quiet "${MERGE_SHA}^{commit}" >/dev/null \
      || fail "merge commit '${MERGE_SHA}' not found"

    # A queue entry built with a non-MERGE method (or an empty batch) has no
    # second parent. Nothing to enforce - a linear result is trivially fine.
    if ! git rev-parse --verify --quiet "${MERGE_SHA}^2" >/dev/null; then
      echo "queue head ${MERGE_SHA} is not a merge commit; nothing to check"
      exit 0
    fi
    # More than two parents is an octopus merge. The ruleset pins
    # `merge_queue.max_entries_to_build: 1`, so a group never holds more than
    # one PR and this cannot happen - but if that setting is ever raised, fail
    # loudly rather than silently checking only the first two parents.
    # (`max_entries_to_merge` does NOT batch a group; it only caps how many
    # already-built groups fast-forward onto the base at once.)
    git rev-parse --verify --quiet "${MERGE_SHA}^3" >/dev/null \
      && fail "queue head ${MERGE_SHA} is an octopus merge; expected one PR per entry"

    P1="$(git rev-parse "${MERGE_SHA}^1")"
    P2="$(git rev-parse "${MERGE_SHA}^2")"

    if ! git merge-base --is-ancestor "$P1" "$P2"; then
      echo "::error::This merge is not semi-linear: the branch is not rebased" \
           "onto its base, so it would land a merge bubble on main."
      echo "::error::  base (parent 1): $(git log -1 --format='%h %s' "$P1")"
      echo "::error::  head (parent 2): $(git log -1 --format='%h %s' "$P2")"
      echo "::error::  branch cut from: $(git log -1 --format='%h %s' "$(git merge-base "$P1" "$P2")")"
      echo "::error::  base commits the branch is missing:" \
           "$(git rev-list --count "$P1" --not "$P2")"
      fail "rebase the branch onto the base branch and re-queue"
    fi

    check_no_bubbles "$P1" "$P2" "the merged branch"
    echo "OK: semi-linear - $(git rev-list --count "$P2" --not "$P1") commit(s) stacked on $(git log -1 --format='%h' "$P1")"
    ;;

  branch)
    BASE="${2:?usage: semilinear-check.sh branch <base-rev> <head-rev>}"
    HEAD_REV="${3:?usage: semilinear-check.sh branch <base-rev> <head-rev>}"
    for r in "$BASE" "$HEAD_REV"; do
      git rev-parse --verify --quiet "${r}^{commit}" >/dev/null \
        || fail "rev '${r}' not found"
    done

    if ! git merge-base --is-ancestor "$BASE" "$HEAD_REV"; then
      echo "::error::Branch is not rebased onto ${BASE}. The merge queue does" \
           "not rebase for you - merging as-is lands a bubble on main."
      echo "::error::  missing $(git rev-list --count "$BASE" --not "$HEAD_REV") commit(s) from ${BASE}"
      fail "run: git fetch origin && git rebase origin/\${base} && git push --force-with-lease"
    fi

    check_no_bubbles "$BASE" "$HEAD_REV" "this branch"
    echo "OK: $(git rev-list --count "$HEAD_REV" --not "$BASE") commit(s) rebased on top of ${BASE}, no merge bubbles"
    ;;

  *)
    fail "unknown mode '${MODE}' (expected 'merge' or 'branch')"
    ;;
esac
