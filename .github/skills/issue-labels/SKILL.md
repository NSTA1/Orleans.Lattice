---
name: issue-labels
description: How to apply package labels to a GitHub issue in Orleans.Lattice. Use when filing, triaging, or auditing an issue and you need to tag it with every package it is relevant to. Covers the package-label rule and how to judge per-package relevance from subject matter.
---

# Issue package labels

Every issue must carry a label for **every package it is relevant to**. This is
the same package-label scheme the pull requests use; see the **pr-labels** skill
for the naming rule and for how new-package labels are created. In short: there
is one GitHub label per `src/<package>/` directory, named exactly after it.

Enumerate the canonical package list from disk (never hard-code it):

```powershell
Get-ChildItem -Path src -Directory | Select-Object -ExpandProperty Name
```

## Judging relevance (subject matter)

An issue has no changed-files signal, so relevance is judged from what the issue
is actually about. Apply a package label when the issue's **subject** concerns
that package - not merely because the package is name-dropped in passing.

Strong signals, in priority order:

1. **Closed by a PR** - if the issue was resolved by a pull request, that PR's
   package labels are ground truth. Copy them onto the issue.
2. **Fully-qualified type or namespace** in the title or body
   (`Orleans.Lattice.Api.Data`, `Orleans.Lattice.Membership.Entra.Graph`) - a
   direct, high-precision signal for that exact package.
3. **The component named in the title.** Titles are curated; a title that says
   "State API", "Explorer", "replication shipper", "dashboard", or "Azure Table
   WAL" reliably indicates the package.

Precision guidance:

- Tag the **specific sub-package** and its parents when the work spans them
  (e.g. a gRPC binding change is relevant to `lattice.api.state.grpc` and, if it
  also moves the facade, `lattice.api.state`). Do not roll every sub-package up
  to the bare `lattice` core.
- Only add the core **`lattice`** label when the core B+ tree, WAL, CRDT, shard,
  leaf, snapshot, or grain behaviour is genuinely involved.
- Treat pervasive incidental mentions as noise: the benchmark rig discusses
  "Azure Table" and "azurite" throughout, so those phrases alone do not make an
  issue a `lattice.storage.azuretable` issue - require the package/type name or a
  title-level signal.

## Applying

Fetch issues in bulk for an audit, then add only the missing labels (idempotent):

```powershell
gh issue list --state all --limit 2000 --json number,title,body,labels
gh issue edit <number> --add-label "lattice.replication,lattice.storage.azuretable"
```
