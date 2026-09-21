#!/usr/bin/env bash
#
# The zero-leg vacuity guard (issue #3002).
#
# `summarise-test-legs.py` carries the rig's only vacuity guard, and it is
# correctly unitised: keyed on `(package, shard)` and summed across every tier,
# so a shard that executed nothing anywhere fails `build-and-test`. But that
# step is conditioned on `leg_count != '0'`, so the configuration in which
# package selection is most badly wrong - selecting NOTHING - is the exact
# configuration in which the guard protecting against wrong selection does not
# run. A selection defect whose limiting case selects zero packages emits zero
# legs, the aggregate is skipped, and no vacuity check runs anywhere.
#
# A zero-leg plan is still a legitimate outcome, so the remedy cannot be "fail
# on zero legs". The two cases are distinguishable, and only by a property the
# `leg_count != '0'` condition does not look at: the CHANGED PATH SET.
#
#   changed paths                                      legitimate zero-leg?
#   -------------------------------------------------  --------------------
#   docs/**, samples/**, benchmark/**, .github/**, *.md  yes
#   a release version bump (*.csproj <Version> only)     yes
#   *.md or *.txt anywhere, including under src//test/   yes
#   test/azure-throughput-silo, .../lattice.explorer.uitests  yes (own lanes)
#   any other file under src/** or test/**               NO - selection is wrong
#
# The last row needs no judgement and no hand-maintained list: a change that
# touches a package's own source or tests and selects zero packages is a
# selection defect by construction, because the selector's documented no-match
# fallback is to test EVERY package. Reaching zero from a seeding path means
# the selection logic, not the change, is at fault.
#
# Both halves of the predicate live here rather than inline in `ci.yml` so that
# one source of truth is driven by one self-test
# (`zero-leg-guard-selftest.py`). Two matchers that must agree will drift, and
# the drift would appear as the plan classifying a path the verdict does not.
#
# Usage:
#   classify   reads changed paths on stdin, prints `true` or `false`
#   verdict    --leg-count N --package-source-changed BOOL --version-only BOOL
#              exits non-zero when the guard fires
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  zero-leg-guard.sh classify < changed-paths
  zero-leg-guard.sh verdict --leg-count N \
                            --package-source-changed true|false \
                            --version-only true|false
EOF
  exit 2
}

# A changed path SEEDS a package, so its presence makes a zero-leg plan a
# selection defect. Mirrors the exclusion set of the `code`/`nonSample` paths
# filters for the entries that can appear under src/ or test/: markdown and
# text never seed, and the two separately-laned test trees run their own lanes
# and are excluded from the library matrix by those same filters.
classify() {
  local seeds
  seeds=$(grep -E '^(src|test)/' \
    | grep -vE '\.md$' \
    | grep -vE '\.txt$' \
    | grep -vE '^test/azure-throughput-silo/' \
    | grep -vE '^test/lattice\.explorer\.uitests/' \
    || true)

  if [ -n "$seeds" ]; then
    echo "true"
  else
    echo "false"
  fi
}

verdict() {
  local leg_count="" package_source_changed="" version_only=""

  while [ $# -gt 0 ]; do
    case "$1" in
      --leg-count) leg_count="${2-}"; shift 2 ;;
      --package-source-changed) package_source_changed="${2-}"; shift 2 ;;
      --version-only) version_only="${2-}"; shift 2 ;;
      *) echo "unknown argument: $1" >&2; usage ;;
    esac
  done

  if [ -z "$leg_count" ]; then
    echo "::error::zero-leg-guard: --leg-count is required." >&2
    exit 2
  fi

  # Only the zero-leg plan is in scope. Every other plan is covered by the
  # aggregate guard, which is strictly better than this one.
  if [ "$leg_count" != "0" ]; then
    echo "zero-leg-guard: ${leg_count} leg(s) planned; the aggregate guard covers this run."
    return 0
  fi

  # A release version bump legitimately reaches zero legs while touching
  # src/**/*.csproj. It is the one carve-out, and `ci.yml` establishes it with
  # a content-aware diff rather than a path rule, so it has to be passed in
  # rather than re-derived here.
  if [ "$version_only" = "true" ]; then
    echo "zero-leg-guard: zero legs, release version bump only; legitimate."
    return 0
  fi

  # Empty (not `false`) means the classifier did not run - it is
  # pull-request-only, exactly like every other change-detection step. A push
  # lane reaching here has no changed-path set to judge, and inventing one
  # would fail runs on a property nothing measured.
  if [ -z "$package_source_changed" ]; then
    echo "zero-leg-guard: zero legs, no changed-path classification (not a pull request); nothing to judge."
    return 0
  fi

  if [ "$package_source_changed" != "true" ]; then
    echo "zero-leg-guard: zero legs, and no package source or test file changed; legitimate."
    return 0
  fi

  echo "::error::Test selection chose ZERO packages, but this change touches files under src/** or test/** that seed a package. The selector's documented no-match fallback is to test EVERY package, so reaching zero from a seeding path is a selection defect, not a legitimate empty plan. Because no legs ran, the aggregate vacuity guard in summarise-test-legs.py was skipped, and without this check the run would report a clean green having executed no package's tests at all. Inspect the 'Discover changed packages' and 'Decide whether to run tests' steps in the plan job." >&2
  return 1
}

[ $# -ge 1 ] || usage

mode="$1"
shift

case "$mode" in
  classify) classify ;;
  verdict) verdict "$@" ;;
  *) usage ;;
esac
