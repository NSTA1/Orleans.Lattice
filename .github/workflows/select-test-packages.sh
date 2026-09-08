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
declare -A parsedPerFile=()
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
  parsedPerFile["$proj"]=$(( ${parsedPerFile[$proj]-0} + 1 ))
  normalize_path "${proj%/*}/${ref}"
  target="$NORMALIZED"
  if [ ! -f "$target" ]; then
    unresolved+=("${proj} -> ${ref}")
    continue
  fi
  consumers["$target"]="${consumers[$target]-} ${proj}"
  edgeCount=$((edgeCount + 1))
done < <(grep -oHE '<ProjectReference[[:space:]]+Include="[^"]+"' "${projects[@]}" || true)

# The parse above is a regex, so it recognises exactly one spelling:
# `<ProjectReference` followed by whitespace and then `Include="..."`. Every
# other legal MSBuild spelling of the same element is silently invisible to it:
#
#   <ProjectReference PrivateAssets="all" Include="..." />   (Include not first)
#   <ProjectReference
#       Include="..." />                                     (Include on its own line)
#   <ProjectReference Include='...' />                       (single-quoted)
#
# A missed element is not a parse error - it is a missing edge, which narrows
# the selection, which is exactly the #2330 failure mode this script exists to
# remove. It would fail silently and in the safe-looking direction: a green run
# over too few packages.
#
# So count the `<ProjectReference` element openings independently of the
# attribute parse and require the two to agree per file. `</ProjectReference>`
# cannot be miscounted (it is `</P`, not `<P`), and the trailing character class
# stops `<ProjectReferenceSomethingElse` counting.
declare -A openPerFile=()
while IFS= read -r line; do
  [ -n "$line" ] || continue
  f="${line%%:*}"
  openPerFile["$f"]=$(( ${openPerFile[$f]-0} + 1 ))
done < <(grep -oHE '<ProjectReference([[:space:]]|/>|>)' "${projects[@]}" || true)

unparsed=()
for proj in "${projects[@]}"; do
  opened=${openPerFile[$proj]-0}
  parsed=${parsedPerFile[$proj]-0}
  if [ "$opened" -ne "$parsed" ]; then
    unparsed+=("${proj}: ${opened} <ProjectReference> element(s), ${parsed} parsed")
  fi
done

if [ ${#unparsed[@]} -gt 0 ]; then
  echo "::error::select-test-packages.sh: the ProjectReference parse is not total - ${#unparsed[@]} file(s) contain an element the Include= parse did not see. Every missed element is a missing dependency edge, so the selection would be too narrow (issue #2330). Rewrite the reference in the \`<ProjectReference Include=\"...\" />\` form, or widen the parse:" >&2
  printf '  %s\n' "${unparsed[@]}" >&2
  exit 1
fi

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
# 3. Closure. Breadth-first walk from a set of SEED CSPROJ NODES, following
#    reverse edges (producer -> consumer): "who could this break?". The owning
#    package of every reached project lands in DEPENDENTS, a global set, again
#    to avoid forking a subshell per call.
#
#    The seeds are nodes, not packages, and that distinction is load-bearing.
#    Seeding by package would enqueue every csproj the package owns, so a
#    change confined to `test/lattice/` would also enqueue
#    `src/lattice/Orleans.Lattice.csproj` - which nothing in the change touched
#    - and the src node's reverse edges would fan the selection out to 45
#    packages for a change no other package can observe. Feeding a node-level
#    walk a package-level seed is how that happens.
#
#    Seeding at node level is also strictly more general than special-casing
#    test paths: it assumes nothing about whether test projects are referenced.
#    If some project ever does reference a test project, that edge is already in
#    the graph and gets traversed, and the selection widens on its own.
# ---------------------------------------------------------------------------
declare -A DEPENDENTS=()
dependents_of() {
  local -A reached=()
  local -a queue=()
  local proj owner node consumer head

  DEPENDENTS=()

  for proj in "$@"; do
    if [ -z "${reached[$proj]-}" ]; then
      reached["$proj"]=1
      queue+=("$proj")
    fi
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
#
# Seeded with EVERY node the package owns, so each row is that package's worst
# case - the figure you get when its src project changes. A change confined to
# the package's test project reaches fewer (usually only itself), because the
# walk now starts from the changed node; see section 3. Reporting the worst
# case keeps the table an upper bound on cost rather than an average that
# flatters it.
if [ "$fanoutTable" = true ]; then
  for p in "${packages[@]}"; do
    nodes=()
    for proj in "${projects[@]}"; do
      [ "${ownerOf[$proj]-}" = "$p" ] && nodes+=("$proj")
    done
    if [ ${#nodes[@]} -eq 0 ]; then
      printf '%s\t0\n' "$p"
      continue
    fi
    dependents_of "${nodes[@]}"
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
declare -A seedNodeSet=()
declare -A seedNodelessPackage=()

# Directory -> the csproj declared directly in it, so a changed file can be
# mapped to the project that actually compiles it by walking up its parents.
declare -A projectInDir=()
for proj in "${projects[@]}"; do
  projectInDir["${proj%/*}"]="$proj"
done

# The csproj that owns a changed path: the nearest ancestor directory holding
# one. Empty when no ancestor does - a file under docs/<name>/, or a package
# with no project at all.
owning_node() {
  local p="$1" dir
  OWNING_NODE=""
  dir="${p%/*}"
  while [ -n "$dir" ] && [ "$dir" != "$p" ]; do
    if [ -n "${projectInDir[$dir]-}" ]; then
      OWNING_NODE="${projectInDir[$dir]}"
      return 0
    fi
    [[ $dir == */* ]] || break
    dir="${dir%/*}"
  done
  return 0
}

for name in "${packages[@]}"; do
  hit=0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in
      *.md) continue ;;
    esac
    case "$f" in
      src/"$name"/*|test/"$name"/*|docs/"$name"/*)
        hit=1
        owning_node "$f"
        if [ -n "$OWNING_NODE" ]; then
          seedNodeSet["$OWNING_NODE"]=1
        else
          # A changed file under the package that no csproj compiles (a
          # docs/<name>/ asset, say). It cannot be reasoned about at node
          # level, so fall back to this package's whole node set rather than
          # expanding from nothing - never narrow on the unknown case.
          seedNodelessPackage["$name"]=1
        fi
        ;;
    esac
  done <<< "$changed"
  if [ "$hit" -eq 1 ]; then
    seeded+=("$name")
  fi
done

# Expand the node-less packages to every node they own.
if [ ${#seedNodelessPackage[@]} -gt 0 ]; then
  for proj in "${projects[@]}"; do
    owner="${ownerOf[$proj]-}"
    [ -n "$owner" ] || continue
    [ -n "${seedNodelessPackage[$owner]-}" ] && seedNodeSet["$proj"]=1
  done
fi

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
  if [ ${#seedNodeSet[@]} -gt 0 ]; then
    dependents_of "${!seedNodeSet[@]}"
    for p in "${!DEPENDENTS[@]}"; do
      if [ -z "${selectedSet[$p]-}" ]; then
        selectedSet["$p"]=1
        reasonOf["$p"]="depends on a changed project"
      fi
    done
  fi
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
  #
  # `find ... | grep -q .` would be the obvious spelling and is wrong here.
  # `grep -q` exits on its first match, closing the pipe, so `find` takes
  # SIGPIPE and exits 141. Under `set -o pipefail` the pipeline then reports
  # 141 even though grep succeeded, the condition reads false, and the orphan is
  # silently dropped from the report - a race whose outcome depends on whether
  # find happens to finish writing before grep exits. Capture find's output
  # instead: a command substitution reads to EOF, so there is no pipe to break.
  local orphans=() d name found
  for d in test/*/; do
    name="$(basename "$d")"
    found="$(find "$d" -name '*.Tests.csproj' -type f)"
    if [ -z "${isPackage[$name]-}" ] && [ -n "$found" ]; then
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
