#!/usr/bin/env bash
#
# Self-test for select-test-packages.sh.
#
# This is the SECOND of the two controls issue #2330 distinguishes, and the two
# are not interchangeable:
#
#   * A planted needle - break an assertion in a package the change does not
#     touch by name, and watch CI go red - proves the selection REACHES that
#     one package.
#   * These assertions run over the EMITTED PACKAGE LIST, so they can speak
#     about every package at once: what is selected, what is not, and why the
#     exclusions are sound.
#
# A needle cannot establish the second and this cannot establish the first, so
# both are carried.
#
# Runs in CI on every pull request (it is pure bash - no restore, no build) and
# locally with:
#
#   .github/workflows/select-test-packages-selftest.sh
#
# Run from the repository root. SELECT_TEST_PACKAGES overrides the script under
# test, which is what lets the negative control run a deliberately broken copy
# and confirm these checks can fail.

set -euo pipefail

SELECTOR="${SELECT_TEST_PACKAGES:-.github/workflows/select-test-packages.sh}"

if [ ! -x "$SELECTOR" ] && [ ! -f "$SELECTOR" ]; then
  echo "self-test: $SELECTOR not found; run from the repository root." >&2
  exit 2
fi

failures=0
checks=0

fail() {
  failures=$((failures + 1))
  echo "::error::select-test-packages self-test: $*"
  echo "FAIL: $*" >&2
}

pass() {
  echo "  ok: $*"
}

check() {
  checks=$((checks + 1))
}

# select <changed paths...> -> newline-separated selection on stdout.
select_for() {
  printf '%s\n' "$@" | bash "$SELECTOR" 2>/dev/null
}

contains() {
  local needle="$1"; shift
  local item
  for item in "$@"; do
    [ "$item" = "$needle" ] && return 0
  done
  return 1
}

packages=()
for dir in src/*/; do
  packages+=("$(basename "$dir")")
done
packageCount=${#packages[@]}

# ---------------------------------------------------------------------------
# 0. Denominator of this self-test itself.
#
# Every assertion below is quantified over the discovered package set. If that
# set were empty or implausibly small the suite would pass vacuously, which is
# the failure mode this wave exists to stamp out - so assert the population
# before asserting anything about it.
# ---------------------------------------------------------------------------
check
if [ "$packageCount" -lt 10 ]; then
  fail "discovered only ${packageCount} package(s) under src/ - the self-test would be near-vacuous."
else
  pass "discovered ${packageCount} packages under src/."
fi

# ---------------------------------------------------------------------------
# 1. Graph denominator, counted independently of the selector.
#
# The selector's closure is only as good as its parse: a normalization bug that
# silently dropped edges would degrade it back to the lexical selector #2330 is
# about, and every selection would still look plausible. Count the
# ProjectReference elements here by a different route and require the selector
# to have resolved exactly that many.
# ---------------------------------------------------------------------------
expectedEdges=$(grep -rhoE '<ProjectReference[[:space:]]+Include="[^"]+"' \
                  --include='*.csproj' src test | grep -c . || true)
reportedEdges=$(printf 'Orleans.Lattice.slnx\n' \
                  | bash "$SELECTOR" 2>&1 >/dev/null \
                  | sed -nE 's/^Graph: [0-9]+ projects, ([0-9]+) ProjectReference edges\.$/\1/p')

check
if [ -z "$reportedEdges" ]; then
  fail "the selector did not report its graph size."
elif [ "$reportedEdges" != "$expectedEdges" ]; then
  fail "selector resolved ${reportedEdges} ProjectReference edges but the tree contains ${expectedEdges}; some references are being dropped."
else
  pass "graph parse is complete: ${reportedEdges}/${expectedEdges} ProjectReference edges resolved."
fi

# ---------------------------------------------------------------------------
# 1b. Parse TOTALITY, counted with a deliberately different pattern.
#
# Check 1 above counts with the same `Include="..."` regex the selector uses,
# so it verifies the selector did not LOSE an element it saw - but it cannot
# see an element neither of them recognises. Both would agree, and both would
# be wrong, in the direction that narrows the selection.
#
# Count the element OPENINGS instead, which is attribute-order-, quote-style-,
# and line-break-agnostic, and require that number to equal the parsed number.
# On disagreement some csproj spells a reference in a shape the parse drops.
# ---------------------------------------------------------------------------
openings=$(grep -rhoE '<ProjectReference([[:space:]]|/>|>)' \
             --include='*.csproj' src test | grep -c . || true)

check
if [ "$openings" != "$expectedEdges" ]; then
  fail "the tree contains ${openings} <ProjectReference> element(s) but only ${expectedEdges} match the Include= parse; some reference is spelled in a shape the selector drops, which would silently narrow the selection."
else
  pass "the ProjectReference parse is total: ${openings} element(s), ${expectedEdges} parsed."
fi

# ---------------------------------------------------------------------------
# 1c. Negative control for 1b, executed rather than asserted.
#
# 1b passing means "no such reference exists today". It says nothing about
# whether the selector would NOTICE one. Plant a reference in a shape the parse
# cannot see - attributes before Include=, which is legal MSBuild and the most
# likely way this arrives - and require the selector to fail closed and name
# the file. Without this, 1b and the selector's own guard could both be dead
# code and every run would still be green.
# ---------------------------------------------------------------------------
probeDir="test/.select-test-packages-parse-probe"
probe="${probeDir}/Probe.Tests.csproj"
cleanup_probe() { rm -rf "$probeDir"; }
trap cleanup_probe EXIT

mkdir -p "$probeDir"
cat > "$probe" <<'PROBE'
<Project Sdk="Microsoft.NET.Sdk">
  <ItemGroup>
    <ProjectReference PrivateAssets="all" Include="..\..\src\lattice\Orleans.Lattice.csproj" />
  </ItemGroup>
</Project>
PROBE

probeStatus=0
probeOutput="$(printf 'Orleans.Lattice.slnx\n' | bash "$SELECTOR" 2>&1 >/dev/null)" || probeStatus=$?
cleanup_probe
trap - EXIT

check
if [ "$probeStatus" -eq 0 ]; then
  fail "the selector accepted a csproj whose ProjectReference the parse cannot see; the totality guard is not firing."
elif ! [[ $probeOutput == *"$probe"* ]]; then
  fail "the selector rejected the unparseable reference but did not name ${probe}; the diagnostic is not actionable."
else
  pass "an unparseable ProjectReference makes the selector fail closed and name the file."
fi

# ---------------------------------------------------------------------------
# 1d. The orphan report is a set, not a race.
#
# The orphan line previously tested for a test project with
# `find ... | grep -q .`. grep -q exits on its first match, find takes SIGPIPE,
# and under `set -o pipefail` the pipeline reports 141 even though grep
# succeeded - so an orphan could vanish from the report depending on whether
# find finished writing first. Require the reported set to equal the computed
# set, over repeated runs so a surviving race is not silently sampled away.
# ---------------------------------------------------------------------------
expectedOrphans=()
for dir in test/*/; do
  name="$(basename "$dir")"
  [ -d "src/${name}" ] && continue
  found="$(find "$dir" -name '*.Tests.csproj' -type f)"
  [ -n "$found" ] && expectedOrphans+=("$name")
done
expectedOrphanLine="${expectedOrphans[*]}"

orphanRuns=0
orphanMismatch=""
while [ "$orphanRuns" -lt 5 ]; do
  reportedOrphanLine="$(printf 'Orleans.Lattice.slnx\n' \
                          | bash "$SELECTOR" 2>&1 >/dev/null \
                          | sed -nE 's/^Test projects outside this job.s selection universe .*`([^`]*)`[[:space:]]*$/\1/p')"
  if [ "$reportedOrphanLine" != "$expectedOrphanLine" ]; then
    orphanMismatch="run $((orphanRuns + 1)): reported \`${reportedOrphanLine}\`, expected \`${expectedOrphanLine}\`"
    break
  fi
  orphanRuns=$((orphanRuns + 1))
done

check
if [ -z "$expectedOrphanLine" ]; then
  fail "computed no orphan test projects at all; the orphan check would be vacuous."
elif [ -n "$orphanMismatch" ]; then
  fail "the orphan report is not stable - ${orphanMismatch}."
else
  pass "the orphan report is exact and stable over ${orphanRuns} runs: \`${expectedOrphanLine}\`."
fi

# ---------------------------------------------------------------------------
# 1e. Seeding happens at NODE level, not package level.
#
# The closure walks csproj nodes, but if the seed is a package it enqueues
# every node that package owns. A change confined to `test/lattice/` would then
# also enqueue `src/lattice/Orleans.Lattice.csproj`, whose reverse edges fan the
# selection out to 45 packages - for a change no other project can observe.
#
# The expectation here is computed from the graph, NOT from the present fact
# that nothing references test/lattice. An independent reverse-closure is built
# from the changed node and compared against the selector. If some project ever
# does reference a test project, both sides widen together and this still holds,
# so nothing about test projects is assumed.
# ---------------------------------------------------------------------------
declare -A sfConsumers=()
while IFS= read -r line; do
  [ -n "$line" ] || continue
  sfProj="${line%%:*}"
  sfRef="${line#*Include=\"}"
  sfRef="${sfRef%\"}"
  sfDir="${sfProj%/*}/${sfRef//\\//}"
  sfTarget="$(cd "$(dirname "$sfDir")" 2>/dev/null && pwd)/$(basename "$sfDir")" || continue
  sfTarget="${sfTarget#"$PWD/"}"
  [ -f "$sfTarget" ] || continue
  sfConsumers["$sfTarget"]="${sfConsumers[$sfTarget]-} ${sfProj}"
done < <(grep -rhoHE '<ProjectReference[[:space:]]+Include="[^"]+"' --include='*.csproj' src test 2>/dev/null \
           || grep -roHE '<ProjectReference[[:space:]]+Include="[^"]+"' --include='*.csproj' src test)

# Reverse closure from one node -> owning package names.
closure_from_node() {
  local -A seen=(); local -a q=("$1"); local h=0 n c owner
  CLOSURE=()
  seen["$1"]=1
  while [ "$h" -lt ${#q[@]} ]; do
    n="${q[$h]}"; h=$((h + 1))
    for c in ${sfConsumers[$n]-}; do
      if [ -z "${seen[$c]-}" ]; then seen["$c"]=1; q+=("$c"); fi
    done
  done
  local -A owners=()
  for n in "${!seen[@]}"; do
    case "$n" in
      src/*/*|test/*/*)
        owner="${n#*/}"; owner="${owner%%/*}"
        [ -d "src/${owner}" ] && owners["$owner"]=1
        ;;
    esac
  done
  mapfile -t CLOSURE < <(printf '%s\n' "${!owners[@]}" | LC_ALL=C sort)
}

testNode="$(find test/lattice -maxdepth 1 -name '*.csproj' -type f | head -n 1)"
srcNode="$(find src/lattice -maxdepth 1 -name '*.csproj' -type f | head -n 1)"

check
if [ -z "$testNode" ] || [ -z "$srcNode" ]; then
  fail "could not locate the src and test csproj for package lattice; check 1e cannot run."
else
  closure_from_node "$testNode"; expectTestOnly=("${CLOSURE[@]}")
  # The always-on dashboards arm is added after the closure, so fold it in.
  contains "lattice.dashboards" "${expectTestOnly[@]}" || expectTestOnly+=("lattice.dashboards")
  mapfile -t expectTestOnly < <(printf '%s\n' "${expectTestOnly[@]}" | LC_ALL=C sort -u)

  mapfile -t actualTestOnly < <(select_for "test/lattice/Hygiene/SomeTest.cs")

  if [ "${expectTestOnly[*]}" != "${actualTestOnly[*]}" ]; then
    fail "a test/lattice-only change selected \`${actualTestOnly[*]}\` but the graph implies \`${expectTestOnly[*]}\`; the reverse walk is being seeded with nodes the change did not touch."
  else
    pass "a test/lattice-only change selects exactly the graph-implied ${#actualTestOnly[@]} package(s): \`${actualTestOnly[*]}\`."
  fi
fi

# ---------------------------------------------------------------------------
# 1f. The other direction, so 1e cannot be satisfied by under-selecting.
#
# 1e alone would still pass if the walk stopped expanding altogether. Pin the
# src side against the same independently-computed closure, and require the two
# directions to actually differ - which is the observable signature of
# node-level seeding.
# ---------------------------------------------------------------------------
check
if [ -z "$srcNode" ]; then
  fail "could not locate the src csproj for package lattice; check 1f cannot run."
else
  closure_from_node "$srcNode"; expectSrcOnly=("${CLOSURE[@]}")
  contains "lattice.dashboards" "${expectSrcOnly[@]}" || expectSrcOnly+=("lattice.dashboards")
  mapfile -t expectSrcOnly < <(printf '%s\n' "${expectSrcOnly[@]}" | LC_ALL=C sort -u)

  mapfile -t actualSrcOnly < <(select_for "src/lattice/WalMoveOptions.cs")

  if [ "${expectSrcOnly[*]}" != "${actualSrcOnly[*]}" ]; then
    fail "a src/lattice-only change selected ${#actualSrcOnly[@]} package(s) but the graph implies ${#expectSrcOnly[@]}."
  elif [ ${#actualSrcOnly[@]} -le ${#actualTestOnly[@]} ]; then
    fail "a src/lattice change selected ${#actualSrcOnly[@]} package(s), no more than the ${#actualTestOnly[@]} a test/lattice change selected; seeding is not discriminating by node."
  else
    pass "a src/lattice-only change selects the graph-implied ${#actualSrcOnly[@]} package(s), against ${#actualTestOnly[@]} for test-only."
  fi
fi

# ---------------------------------------------------------------------------
# 2. The regression in #2330/#2329: a core-library change must reach
#    lattice.api.backup, whose test project calls straight into src/lattice.
# ---------------------------------------------------------------------------
mapfile -t coreSelection < <(select_for "src/lattice/LatticeTenantResolution.cs")

check
if contains "lattice.api.backup" "${coreSelection[@]}"; then
  pass "a src/lattice change selects lattice.api.backup (the #2329 package)."
else
  fail "a src/lattice change does NOT select lattice.api.backup - the #2330 gap is open."
fi

# ---------------------------------------------------------------------------
# 3. The claim a needle cannot make: a core-library change reaches EVERY
#    package that could observe it.
#
# The check has two halves, and the second is what makes it meaningful. It is
# not enough to assert the selection is large; the packages left out have to be
# shown to be unreachable. So for each excluded package, walk the FORWARD
# ProjectReference edges from its own projects - the opposite direction to the
# selector's walk - and require that the walk never reaches a project owned by
# another package. Such a package is a dependency island: no other package's
# source can break it, so excluding it is correct rather than merely cheap.
#
# An asymmetry bug in the selector's reverse walk therefore shows up here as an
# excluded package that is demonstrably not an island.
# ---------------------------------------------------------------------------
declare -A refsOf=()

# Collapse '.' and '..' segments. Same job as the selector's normalizer, and
# deliberately not the interesting part of this check - the independence that
# matters here is the DIRECTION of the walk (forward, where the selector walks
# backward) and the independently counted edge total in check 1.
resolve_ref() {
  local raw="${1//\\//}"
  local -a out=()
  local seg oldIFS="$IFS"
  IFS=/
  set -f
  # shellcheck disable=SC2206
  local -a segs=($raw)
  set +f
  IFS="$oldIFS"
  for seg in ${segs[@]+"${segs[@]}"}; do
    case "$seg" in
      ''|.) ;;
      ..) [ ${#out[@]} -gt 0 ] && out=("${out[@]:0:${#out[@]}-1}") || true ;;
      *) out+=("$seg") ;;
    esac
  done
  local joined="" piece
  for piece in ${out[@]+"${out[@]}"}; do
    if [ -z "$joined" ]; then joined="$piece"; else joined="$joined/$piece"; fi
  done
  RESOLVED="$joined"
}

while IFS= read -r line; do
  [ -n "$line" ] || continue
  proj="${line%%:*}"
  ref="${line#*Include=\"}"
  ref="${ref%\"}"
  resolve_ref "${proj%/*}/${ref}"
  refsOf["$proj"]="${refsOf[$proj]-} ${RESOLVED}"
done < <(grep -rHoE '<ProjectReference[[:space:]]+Include="[^"]+"' \
           --include='*.csproj' src test || true)

owner_of_path() {
  local p="$1" rest candidate
  case "$p" in
    src/*/*|test/*/*)
      rest="${p#*/}"
      candidate="${rest%%/*}"
      if contains "$candidate" "${packages[@]}"; then
        printf '%s' "$candidate"
      fi
      ;;
  esac
}

# Prints the foreign packages a package's projects can reach, forwards.
foreign_dependencies_of() {
  local pkg="$1"
  local -A seen=()
  local -a queue=()
  local proj node dep owner

  while IFS= read -r proj; do
    [ -n "$proj" ] || continue
    seen["$proj"]=1
    queue+=("$proj")
  done < <(find "src/${pkg}" "test/${pkg}" -name '*.csproj' -type f 2>/dev/null | sed 's#^\./##')

  local head=0
  local -A foreign=()
  while [ "$head" -lt ${#queue[@]} ]; do
    node="${queue[$head]}"
    head=$((head + 1))
    for dep in ${refsOf[$node]-}; do
      owner="$(owner_of_path "$dep")"
      if [ -n "$owner" ] && [ "$owner" != "$pkg" ]; then
        foreign["$owner"]=1
      fi
      if [ -z "${seen[$dep]-}" ]; then
        seen["$dep"]=1
        queue+=("$dep")
      fi
    done
  done

  if [ ${#foreign[@]} -gt 0 ]; then
    printf '%s\n' "${!foreign[@]}"
  fi
}

notIsland=()
excludedCount=0
for p in "${packages[@]}"; do
  if contains "$p" "${coreSelection[@]}"; then
    continue
  fi
  excludedCount=$((excludedCount + 1))
  if [ -n "$(foreign_dependencies_of "$p")" ]; then
    notIsland+=("$p")
  fi
done

check
if [ ${#notIsland[@]} -gt 0 ]; then
  fail "a src/lattice change excludes ${notIsland[*]}, but those packages do depend on other packages' projects - the reverse walk is missing edges."
else
  pass "a src/lattice change selects ${#coreSelection[@]}/${packageCount} packages; each of the ${excludedCount} excluded is a dependency island (reaches no other package's projects)."
fi

# ---------------------------------------------------------------------------
# 4. The optimisation survives. The point of the selector is not to test
#    everything; a leaf change must stay cheap. Assert it over every package
#    rather than spot-checking one, and require the median to be small.
# ---------------------------------------------------------------------------
mapfile -t fanout < <(bash "$SELECTOR" --fanout-table)

check
if [ ${#fanout[@]} -ne "$packageCount" ]; then
  fail "--fanout-table reported ${#fanout[@]} rows for ${packageCount} packages."
else
  pass "--fanout-table covers every package."
fi

counts=()
while IFS= read -r n; do
  counts+=("$n")
done < <(printf '%s\n' "${fanout[@]}" | cut -f2 | LC_ALL=C sort -n)

median="${counts[$(( ${#counts[@]} / 2 ))]}"
belowHalf=0
for n in "${counts[@]}"; do
  if [ "$n" -le $(( packageCount / 2 )) ]; then
    belowHalf=$((belowHalf + 1))
  fi
done

check
if [ "$median" -gt $(( packageCount / 4 )) ]; then
  fail "median package fan-out is ${median} of ${packageCount} - the closure has degenerated into testing everything."
else
  pass "median package fan-out is ${median} of ${packageCount}; ${belowHalf}/${packageCount} packages fan out to at most half the repository."
fi

# ---------------------------------------------------------------------------
# 5. Behaviours that must be preserved, not merely believed to be.
# ---------------------------------------------------------------------------

# 5a. Zero-match fallback: a root file belongs to no package and fans out to all.
mapfile -t rootSelection < <(select_for "Orleans.Lattice.slnx")
check
if [ ${#rootSelection[@]} -eq "$packageCount" ]; then
  pass "a root-file change still fans out to all ${packageCount} packages."
else
  fail "a root-file change selected ${#rootSelection[@]} of ${packageCount} packages; the zero-match fallback regressed."
fi

# 5b. Markdown-only edits do not seed a package (they fall through to the
#     zero-match fallback, which is the pre-existing behaviour).
mapfile -t mdSelection < <(select_for "docs/lattice/observability.md" "src/lattice/README.md")
check
if [ ${#mdSelection[@]} -eq "$packageCount" ]; then
  pass "a markdown-only change still seeds no package."
else
  fail "a markdown-only change selected ${#mdSelection[@]} packages; the .md carve-out regressed."
fi

# 5c. lattice.dashboards is always in scope - the pre-existing always-include,
#     which the closure narrows but does not retire.
mapfile -t leafSelection < <(select_for "src/lattice.caching.azureblob/BlobCache.cs")
check
if contains "lattice.dashboards" "${leafSelection[@]}"; then
  pass "lattice.dashboards is always selected, even for an unrelated leaf change."
else
  fail "lattice.dashboards was not selected for a leaf change; the always-include regressed."
fi

# 5d. A leaf change is still cheap end to end.
check
if [ ${#leafSelection[@]} -lt "$packageCount" ]; then
  pass "a leaf change selects ${#leafSelection[@]} of ${packageCount} packages, not the whole repository."
else
  fail "a leaf change selected every package; the selector has stopped selecting."
fi

# ---------------------------------------------------------------------------
# 6. The seed match was tightened from an ERE to a literal glob. Package names
#    contain '.', which an ERE reads as "any character", so the old pattern was
#    wider than intended. Assert over every ordered pair that the tightening
#    changes nothing on this tree, rather than asserting it by eye.
# ---------------------------------------------------------------------------
overWide=()
for a in "${packages[@]}"; do
  for b in "${packages[@]}"; do
    [ "$a" = "$b" ] && continue
    # bash's own ERE engine, so the comparison forks nothing.
    if [[ "src/${b}/x.cs" =~ ^(src|test|docs)/${a}/ ]]; then
      overWide+=("${a} matched ${b}")
    fi
  done
done

check
if [ ${#overWide[@]} -gt 0 ]; then
  fail "the previous regex over-matched on this tree: ${overWide[*]}. The glob tightening is NOT behaviour-preserving here and must be reviewed."
else
  pass "the previous regex and the literal glob agree on all $(( packageCount * (packageCount - 1) )) ordered package pairs."
fi

# ---------------------------------------------------------------------------
echo
if [ "$failures" -ne 0 ]; then
  echo "select-test-packages self-test: ${failures} of ${checks} checks FAILED." >&2
  exit 1
fi
echo "select-test-packages self-test: all ${checks} checks passed."
