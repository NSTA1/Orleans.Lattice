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
- Enumerate the canonical package list from disk - never hard-code it:

  ```powershell
  Get-ChildItem -Path src -Directory | Select-Object -ExpandProperty Name
  ```

- When a change **adds a new package** (`src/<name>/`), create the matching
  label in the same PR:

  ```powershell
  gh label create "<name>" --description "Relevant to the <name> package" --color 0e8a16
  ```

  A label audit treats a package without a same-named label as a defect.

## Which package labels a PR gets (deterministic)

Relevance is decided by the **changed files**, not by prose. A PR is relevant to
package `X` if it touches any file under `src/X/`, `test/X/`, or `docs/X/`.

Mapping rule: split each changed path on `/`; if the first segment is `src`,
`test`, or `docs` and the second segment is a known package name, that package is
relevant. Because directory names are exact (`lattice.api.state` and
`lattice.api.state.grpc` are separate directories), the match is unambiguous -
no prefix guessing. Files outside those trees (`CHANGELOG.md`, the `.slnx`,
`.github/`, `samples/`, `benchmark/`) map to no package.

Fetch files and current labels in bulk, then add only the missing ones:

```powershell
# One PR
$pr = gh pr view <number> --json files,labels | ConvertFrom-Json

# All PRs (open and closed) for an audit
gh pr list --state all --limit 2000 --json number,files,labels
```

Add the missing labels (idempotent - re-adding an existing label is a no-op):

```powershell
gh pr edit <number> --add-label "lattice,lattice.replication"
```

The companion **issue-labels** skill covers issues, where relevance is judged
from subject matter instead of changed files.
