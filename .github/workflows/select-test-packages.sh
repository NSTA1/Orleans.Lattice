#!/usr/bin/env bash
#
# Select the packages CI must build and test for a given set of changed files.
#
# Why this exists (issue #2330): the previous inline selector matched a package
# iff a changed path started with that package's own name. That relation is
# lexical, but the relation that governs "can this change break that test?" is
# the ProjectReference graph. A change to `src/lattice/` - the core library
# nearly every other package references - therefore selected only
# `test/lattice/`, and a break it caused in a dependent package's tests was
# indistinguishable from a pass, because nothing looked. Not hypothetical: it
# hid a red `test/lattice.api.backup` assertion on `main` for an unknown number
# of merges (issue #2329).
#
# This keeps the lexical match as the *seed* and expands it along the reverse
# ProjectReference edges, so the selection is every package that could observe
# the change. The walk runs over csproj nodes rather than package names, so a
# dependency routed through an unowned project is still traversed.
#
# Usage:
#   select-test-packages.sh [--changed-file FILE] [--report-file FILE]
#   select-test-packages.sh --fanout-table
#
#   --changed-file  Newline-separated changed paths, repo-relative. Defaults to
#                   stdin.
#   --report-file   Where to append the human-readable selection report
#                   (markdown). Defaults to stderr.
#   --fanout-table  Diagnostic mode: for every package, print
#                   "<package> <TAB> <selected count>" for a hypothetical
#                   change to that package alone. Used by the self-test and to
#                   quantify the cost of the closure.
#
# stdout: the selected package names, one per line, sorted. Nothing else - the
#         caller consumes this directly.
#
# Run from the repository root.

set -euo pipefail

changedFile=""
reportFile=""
fanoutTable=false

while [ $# -gt 0 ]; do
  case "$1" in
    --changed-file) changedFile="$2"; shift 2 ;;
    --report-file)  reportFile="$2";  shift 2 ;;
    --fanout-table) fanoutTable=true; shift ;;
    *) echo "select-test-packages.sh: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

if [ ! -d src ] || [ ! -d test ]; then
  echo "select-test-packages.sh: must be run from the repository root." >&2
  exit 2
fi

# ---------------------------------------------------------------------------
# Path normalization.
#
# A ProjectReference Include is authored with Windows separators and relative
# segments (`..\..\src\lattice\Orleans.Lattice.csproj`). Collapse it to a
# repo-relative forward-slash path. Done in pure bash rather than with
# `realpath` so the result does not depend on which coreutils the host ships,
# and so a reference to a project that does not exist still normalizes and is
# then reported as unresolved rather than silently dropped.
#
# The result is returned in the global NORMALIZED rather than on stdout: the
# graph build and the closure walk call the hot helpers thousands of times, and
# a command substitution forks a subshell each time.
# ---------------------------------------------------------------------------
NORMALIZED=""
normalize_path() {
  local raw="${1//\\//}"
  local -a out=()
  local seg oldIFS="$IFS"

  IFS=/
  set -f
  # Word-splitting on '/' is the intent here.
  # shellcheck disable=SC2206
  local -a segs=($raw)
  set +f
  IFS="$oldIFS"

  for seg in ${segs[@]+"${segs[@]}"}; do
    case "$seg" in
      ''|.)
        ;;
      ..)
        if [ ${#out[@]} -gt 0 ]; then
          out=("${out[@]:0:${#out[@]}-1}")
        fi
        ;;
      *)
        out+=("$seg")
        ;;
    esac
  done

  local joined="" piece
  if [ ${#out[@]} -gt 0 ]; then
    for piece in "${out[@]}"; do
      if [ -z "$joined" ]; then joined="$piece"; else joined="$joined/$piece"; fi
    done
  fi
  NORMALIZED="$joined"
}

# ---------------------------------------------------------------------------
# 1. Discover packages. A package is a subdirectory of src/ - unchanged, and
#    the reason a new src/{name}/ needs no workflow edit.
# ---------------------------------------------------------------------------
packages=()
for dir in src/*/; do
  packages+=("$(basename "$dir")")
done

if [ ${#packages[@]} -eq 0 ]; then
  echo "select-test-packages.sh: no packages discovered under src/." >&2
  exit 2
fi

declare -A isPackage=()
for p in "${packages[@]}"; do
  isPackage["$p"]=1
done

# ownerOf[<csproj>] is the owning package, populated once below. A path under
# src/<pkg>/ or test/<pkg>/ for a discovered package is owned by that package;
# everything else (test/shared/, test/lattice.integration/) owns none, and is
# simply absent from the map.
declare -A ownerOf=()

populate_owner() {
  local p="$1" rest candidate
  case "$p" in
    src/*/*|test/*/*)
      rest="${p#*/}"
      candidate="${rest%%/*}"
      if [ -n "${isPackage[$candidate]-}" ]; then
        ownerOf["$p"]="$candidate"
      fi
      ;;
  esac
}

# ---------------------------------------------------------------------------
# 2. Build the ProjectReference graph once.
#
#    Nodes are csproj paths under src/ and test/. Edges run consumer ->
#    producer; the reverse index is what the walk uses. Unowned projects
#    (test/shared/Orleans.Lattice.Testing, test/lattice.integration) are nodes
#    too: they own no package, but a dependency routed through them must still
#    be traversed.
# ---------------------------------------------------------------------------
projects=()
while IFS= read -r proj; do
  [ -n "$proj" ] || continue
  projects+=("$proj")
  populate_owner "$proj"
done < <(find src test -name '*.csproj' -type f | sed 's#^\./##' | LC_ALL=C sort)

if [ ${#projects[@]} -eq 0 ]; then
  echo "::error::select-test-packages.sh: found no csproj under src/ or test/." >&2
  exit 1
fi

declare -A consumers=()   # producer csproj -> " consumer consumer ..."
edgeCount=0
unresolved=()

# One grep over every project rather than two processes per project: the same
# parse, two orders of magnitude fewer process spawns.
while IFS= read -r line; do
  [ -n "$line" ] || continue
  proj="${line%%:*}"
  ref="${line#*Include=\"}"
  ref="${ref%\"}"
  [ -n "$ref" ] || continue
  normalize_path "${proj%/*}/${ref}"
  target="$NORMALIZED"
  if [ ! -f "$target" ]; then
    unresolved+=("${proj} -> ${ref}")
    continue
  fi
  consumers["$target"]="${consumers[$target]-} ${proj}"
  edgeCount=$((edgeCount + 1))
done < <(grep -oHE '<ProjectReference[[:space:]]+Include="[^"]+"' "${projects[@]}" || true)

if [ ${#unresolved[@]} -gt 0 ]; then
  echo "::error::select-test-packages.sh: ${#unresolved[@]} ProjectReference(s) did not resolve to a file:" >&2
  printf '  %s\n' "${unresolved[@]}" >&2
  exit 1
fi

# An empty graph would make the closure a no-op and silently reduce this back
# to the lexical selector #2330 is about. Assert the denominator rather than
# trusting the parse.
if [ "$edgeCount" -eq 0 ]; then
  echo "::error::select-test-packages.sh: parsed 0 ProjectReference edges across ${#projects[@]} projects - the graph parse is broken." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 3. Closure. Breadth-first walk from the csprojs owned by the seeded packages,
#    following reverse edges (producer -> consumer): "who could this break?".
#    The owning package of every reached project lands in DEPENDENTS, a global
#    set, again to avoid forking a subshell per call.
# ---------------------------------------------------------------------------
declare -A DEPENDENTS=()
dependents_of() {
  local -A reached=()
  local -a queue=()
  local seedList=" $* "
  local proj owner node consumer head

  DEPENDENTS=()

  for proj in "${projects[@]}"; do
    owner="${ownerOf[$proj]-}"
    [ -n "$owner" ] || continue
    case "$seedList" in
      *" $owner "*)
        if [ -z "${reached[$proj]-}" ]; then
          reached["$proj"]=1
          queue+=("$proj")
        fi
        ;;
    esac
  done

  head=0
  while [ "$head" -lt ${#queue[@]} ]; do
    node="${queue[$head]}"
    head=$((head + 1))
    for consumer in ${consumers[$node]-}; do
      if [ -z "${reached[$consumer]-}" ]; then
        reached["$consumer"]=1
        queue+=("$consumer")
      fi
    done
  done

  for proj in "${!reached[@]}"; do
    owner="${ownerOf[$proj]-}"
    [ -n "$owner" ] || continue
    DEPENDENTS["$owner"]=1
  done
}

# --fanout-table: how far a change to each package alone reaches. This is the
# cost of the closure, computed rather than guessed.
if [ "$fanoutTable" = true ]; then
  for p in "${packages[@]}"; do
    dependents_of "$p"
    printf '%s\t%s\n' "$p" "${#DEPENDENTS[@]}"
  done
  exit 0
fi

if [ -n "$changedFile" ]; then
  changed=$(cat "$changedFile")
else
  changed=$(cat)
fi

# ---------------------------------------------------------------------------
# 4. Seed: the lexical match, with the same semantics as before - a package is
#    seeded when a non-markdown file under src/<name>/, test/<name>/ or
#    docs/<name>/ changed.
#
#    Matching is by literal bash glob rather than the previous
#    `grep -E "^(src|test|docs)/${name}/"`. Package names contain '.', which an
#    ERE reads as "any character", so the old pattern was wider than intended;
#    the glob is exactly as wide as intended. The self-test asserts over every
#    ordered package pair that no package's old regex matched another package's
#    paths, so this is a tightening with no behavioural change on this tree.
# ---------------------------------------------------------------------------
seeded=()
for name in "${packages[@]}"; do
  hit=0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in
      *.md) continue ;;
    esac
    case "$f" in
      src/"$name"/*|test/"$name"/*|docs/"$name"/*) hit=1; break ;;
    esac
  done <<< "$changed"
  if [ "$hit" -eq 1 ]; then
    seeded+=("$name")
  fi
done

declare -A selectedSet=()
declare -A reasonOf=()
fallback=false

if [ ${#seeded[@]} -eq 0 ]; then
  # A shared/root change (Orleans.Lattice.slnx, Directory.Packages.props, a
  # workflow file) belongs to no package. Unchanged behaviour: fan out to every
  # package. This fallback was never the gap #2330 describes - it already
  # worked, and is how #2329 finally surfaced.
  fallback=true
  for p in "${packages[@]}"; do
    selectedSet["$p"]=1
    reasonOf["$p"]="shared/root change - no package matched"
  done
else
  for s in "${seeded[@]}"; do
    selectedSet["$s"]=1
    reasonOf["$s"]="changed"
  done
  dependents_of "${seeded[@]}"
  for p in "${!DEPENDENTS[@]}"; do
    if [ -z "${selectedSet[$p]-}" ]; then
      selectedSet["$p"]=1
      reasonOf["$p"]="depends on a changed package"
    fi
  done
fi

# ---------------------------------------------------------------------------
# 5. Always test lattice.dashboards. Its drift guards assert over instruments
#    owned by OTHER packages' meters, and reach them by reflecting over a
#    dashboard JSON chart, not only through its own ProjectReferences. The
#    closure above already selects it for a change to src/lattice or
#    src/lattice.replication (test/lattice.dashboards project-references both),
#    so the closure narrows what this arm has left to cover - a meter added in
#    a package it does not reference - but does not retire it.
# ---------------------------------------------------------------------------
if [ -d "test/lattice.dashboards" ] && [ -z "${selectedSet[lattice.dashboards]-}" ]; then
  selectedSet["lattice.dashboards"]=1
  reasonOf["lattice.dashboards"]="always tested (cross-package meter drift guard)"
fi

selected=()
while IFS= read -r p; do
  [ -n "$p" ] || continue
  selected+=("$p")
done < <(printf '%s\n' "${!selectedSet[@]}" | LC_ALL=C sort)

# ---------------------------------------------------------------------------
# 6. Report the denominator.
#
#    A green `build-and-test` says nothing about its own scope: a run over one
#    package and a run over every package look identical in the check name.
#    Emit the count, the per-package reason, and what was NOT covered, so a
#    reader can tell them apart without reconstructing the selection by hand.
# ---------------------------------------------------------------------------
emit_report() {
  echo "### Test selection: ${#selected[@]} of ${#packages[@]} packages"
  echo
  if [ "$fallback" = true ]; then
    echo "No package matched the changed paths (shared or root change), so every package is in scope."
  else
    echo "Seeded by changed paths: ${#seeded[@]} package(s) (\`${seeded[*]}\`). Expanded along ProjectReference edges to ${#selected[@]}."
  fi
  echo
  echo "| Package | Why |"
  echo "| --- | --- |"
  local p
  for p in "${selected[@]}"; do
    echo "| \`${p}\` | ${reasonOf[$p]} |"
  done
  echo

  local notSelected=()
  for p in "${packages[@]}"; do
    if [ -z "${selectedSet[$p]-}" ]; then
      notSelected+=("$p")
    fi
  done
  if [ ${#notSelected[@]} -gt 0 ]; then
    echo "<details><summary>Not tested by this run (${#notSelected[@]} package(s))</summary>"
    echo
    for p in "${notSelected[@]}"; do
      echo "- \`${p}\`"
    done
    echo
    echo "</details>"
    echo
  fi

  # Test projects no package owns are outside this job's selection universe
  # entirely. Naming them keeps the denominator honest rather than letting them
  # read as covered.
  local orphans=() d name
  for d in test/*/; do
    name="$(basename "$d")"
    if [ -z "${isPackage[$name]-}" ] && find "$d" -name '*.Tests.csproj' -type f | grep -q .; then
      orphans+=("$name")
    fi
  done
  if [ ${#orphans[@]} -gt 0 ]; then
    echo "Test projects outside this job's selection universe (no matching \`src/<name>/\`; covered by other workflows if at all): \`${orphans[*]}\`"
    echo
  fi

  echo "Graph: ${#projects[@]} projects, ${edgeCount} ProjectReference edges."
}

if [ -n "$reportFile" ]; then
  emit_report >> "$reportFile"
else
  emit_report >&2
fi

printf '%s\n' "${selected[@]}"
