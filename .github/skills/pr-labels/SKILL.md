---
name: pr-labels
description: How to apply package labels to a pull request in Orleans.Lattice. Use when opening, triaging, or auditing a PR and you need to tag it with every package it touches. Covers the package-label naming rule and the deterministic changed-files -> package mapping.
---

# Pull-request package labels

Every pull request must carry a label for **every package it touches**, on top of
its release-category label (`enhancement`, `bug`, `documentation`, `ci`,
`dependencies`, or `breaking`). Package labels let release notes and planning
queries slice history by component.

## The package-label rule

- The repository has one GitHub label per package, named **exactly** after the
  package directory under `src/` (e.g. `lattice`, `lattice.replication`,
  `lattice.api.state`, `lattice.storage.azuretable`,
  `lattice.membership.entra.graph`).
- Enumerate the canonical package list from disk - never hard-code it, and never
  infer it from the shape of a path:

  ```powershell
  $packages = Get-ChildItem -Path src -Directory | Select-Object -ExpandProperty Name
  ```

  **This step is the mechanism, not a formality.** `src/` is the only authority for
  what a package is, so membership must be *tested* against `$packages`. Do not
  judge a path segment by its shape - "looks like a package name" is not evidence.

  To see why that matters, these second-segment directories under `test/` and
  `docs/` are not packages, and at the time of writing there are six of them:

  | Not a package | Where | What it actually is |
  | --- | --- | --- |
  | `shared` | `test/` | the shared testing library |
  | `microbench` | `test/` | microbenchmarks |
  | `azure-throughput-silo` | `test/` | the throughput rig's silo |
  | `lattice.integration` | `test/` | cross-package integration tests |
  | `lattice.explorer.uitests` | `test/` | Explorer UI tests |
  | `crdt` | `docs/` | a docs-only conceptual topic, no `src/` counterpart |

  Read that table as a demonstration that eyeballing fails, **not as a list to
  memorise** - it is a snapshot and it will drift. `shared` and `crdt` are the
  ones anybody would catch unaided; `lattice.integration` and
  `lattice.explorer.uitests` are dotted and `lattice.`-prefixed and would be
  accepted on sight by a reader applying the shape heuristic. Any rule derived
  from only the obvious cases protects against the instances that need no
  protection. The membership test against `$packages` is what covers all six, and
  the seventh that gets added after this paragraph is written.

- When a change **adds a new package** (`src/<name>/`), create the matching
  label in the same PR:

  ```powershell
  gh label create "<name>" --description "Relevant to the <name> package" --color 0e8a16
  ```

  A label audit treats a package without a same-named label as a defect.

## Which package labels a PR gets (deterministic)

Relevance is decided by the **changed files**, not by prose. A PR is relevant to
package `X` if it touches any file under `src/X/`, `test/X/`, or `docs/X/`.

**All three trees count, and this is the single most commonly broken rule here -
not a pedantic aside.** Across the 107 merged member PRs of one epic bucket, 10
(9.3%) were missing at least one package label, and the misses were concentrated
rather than scattered:

| Label missed | On | Reached almost always through |
| --- | --- | --- |
| `lattice.dashboards` | 5 of 10 | `docs/lattice.dashboards/metrics-to-panel-map.md` |
| `lattice` | 5 of 10 | `test/lattice/` (a hygiene-gate fixture) |

Both are the same shape: a PR whose product change lives in one package touches a
second package's **docs or tests only**. Deriving from `src/` alone misses those
every single time. That is not ten authors being careless, it is one rule applied
with the wrong tree set ten times - so treat "or `test/X/` or `docs/X/`" as
load-bearing, because deriving from `src/` alone is wrong about one PR in ten and
silently.

Mapping rule: split each changed path on `/`; if the first segment is `src`,
`test`, or `docs` **and the second segment is in the canonical `$packages` list
enumerated above**, that package is relevant. Because directory names are exact
(`lattice.api.state` and `lattice.api.state.grpc` are separate directories), the
match is unambiguous - no prefix guessing. Files outside those trees
(`CHANGELOG.md`, the `.slnx`, `.github/`, `samples/`, `benchmark/`) map to no
package.

### Getting the changed-file list: `gh pr view --json files` TRUNCATES AT 100

**Do not derive labels from `gh pr view <n> --json files` or
`gh pr list --json files`.** Both cap each pull request's file list at 100
entries, with no warning, no error, and no truncation flag. You get a
plausible-looking list of exactly 100 paths that reads as an answer.

Measured on this repository, PR #2482: `--json files` returned 100 where
`changedFiles` declared 436. Labels derived from the truncated list gave 4
packages where 12 were warranted, silently omitting eight - including
`lattice.auth`, `lattice.membership`, and `lattice.replication`, whose security
instructions auto-attach.

This is hard to notice because the truncating call is **correct on every pull
request anyone would spot-check**. A 4-file PR and a 9-file PR both agree with
`changedFiles` exactly. Across 107 merged member PRs of one epic bucket, the
largest was still under 100, so **not one of them could truncate**.

The hazard is therefore not merely rare, it is **correlated with exactly the pull
requests that aggregate everyone else's work**: the method gets validated a
hundred times on PRs where it is incapable of failing, then applied once to the
integration PR, where it fails - and that is the PR nobody can check by hand.

Nor is the threshold exotic. PR #2360 truncates at **102** changed files, so two
files over the line is enough and being of reviewable size is no protection.

Use the paginated REST endpoint, which walks every page:

```powershell
$files = gh api --paginate 'repos/NSTA1/Orleans.Lattice/pulls/<number>/files' --jq '.[].filename'
```

**Always cross-check the count**, because truncation is silent and this is the
only thing that makes it visible:

```powershell
$declared = gh pr view <number> --json changedFiles --jq .changedFiles
if ($files.Count -ne [int]$declared) { throw "file list is incomplete: $($files.Count) of $declared" }
```

Heuristic worth internalising: **if a file list comes back as exactly 100, treat
it as truncated until proven otherwise.**

### Auditing many PRs at once

The bulk call is still worth making - just do not trust its `files` field alone.
`gh pr list --json` also exposes `changedFiles`, so an audit can detect its own
truncation and repair only the affected rows:

```powershell
$rows = gh pr list --state all --limit 2000 --json number,changedFiles,files,labels | ConvertFrom-Json

# Rows whose file list was truncated - re-fetch just these, paginated.
$rows | Where-Object { $_.files.Count -lt $_.changedFiles } | ForEach-Object {
    gh api --paginate "repos/NSTA1/Orleans.Lattice/pulls/$($_.number)/files" --jq '.[].filename'
}
```

Do not replace the bulk call with a per-PR paginated loop. Across 107 merged
member PRs of one epic bucket, **zero** rows needed repair, so the loop would have
paid 107 round trips for nothing. Detection is also a strictly better property
than avoidance: a method that merely avoids the 100 bound becomes silently wrong
the day the bound moves, whereas comparing `files.Count` against the declared
`changedFiles` keeps working and tells you it moved.

### Before the PR exists

For pre-raise label planning there is no PR to query, so derive from the diff -
but **`git fetch` the base first**. Diffing against a local copy of a shared epic
or bucket branch attributes siblings' already-merged commits to your branch,
because a stale local ref pushes the merge base backwards:

```powershell
git fetch origin <base-branch>
git --no-pager diff --name-only FETCH_HEAD...HEAD
```

On a shared bucket the local ref goes stale within minutes of any sibling merge,
so the fetch is mandatory rather than defensive.

### The two failure directions are not symmetric

Worth holding both in mind, because they are noticed very differently:

- **Truncation under-reports.** A missing package label is indistinguishable from
  a package that genuinely was not touched. Nothing in the artefact indicates an
  absence, so nobody queries it.
- **A stale base ref over-reports.** You get extra labels naming packages your PR
  never touched, which is wrong in a way a reviewer can see.

Both fail open - neither raises an error - but only the second is self-announcing.

## Applying the labels

Add the missing ones (idempotent - re-adding an existing label is a no-op):

```powershell
gh pr edit <number> --add-label "lattice,lattice.replication"
```

The companion **issue-labels** skill covers issues, where relevance is judged
from subject matter instead of changed files.
