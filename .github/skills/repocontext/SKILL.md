---
name: repocontext
description: Orleans.Lattice repository-context (repocontext) MCP server usage. Use it as the primary mechanism for searching, scanning, and recalling the indexed codebase, and for capturing durable cross-session agent memory (decisions, gotchas, conventions) via remember/update/forget. Use whenever you are about to search or explore the repo, look something up you may have learned before, or record a decision or gotcha for a future session - and to know how to read search `mode`/freshness, the key/topic conventions, and when to fall back to grep.
---

# Repository-context MCP (repocontext)

All rules for using the `repocontext` MCP server live in a single master file:

> **[`.github/instructions/repocontext.instructions.md`](../../instructions/repocontext.instructions.md)**

**Tool names.** For brevity this skill and that file name the retrieval and
capture tools by their bare verb - `search`, `scan`, `recall`, `remember`,
`update`, `forget`, `add_repo`, `remove_repo` - but the real tool ids carry the
`repocontext_` prefix (`repocontext_search`, `repocontext_remember`, and so on),
as do the health / status / list tools (`repocontext_health`,
`repocontext_index_status`, `repocontext_list_repos`).

That file is the authority for everything this skill covers - do not restate its
rules here or elsewhere; link to it so there is one place to change and nothing
to drift. It covers:

- **Primary recall and search** - when the surface is available, lead with
  `repocontext` before `grep` / `glob` (and if the `repocontext_*` tools are not
  present, silently fall back); plus the two guardrails that make leading with it
  safe (locate with `repocontext` but read the real file with `view`; fall back
  when the index is degraded or stale).
- **Retrieval** - `search` (relevance, how to read the `mode` field, and the
  per-hit `reasons` explaining why a hit ranked), `scan` (ordered completeness over
  Files / Packages / Symbols / Memory), `recall` (one record by key, and memory
  link-staleness `stale` / `staleLinks`), `neighbors` (walk the knowledge-linking
  edges out of a memory entry), the graph-navigation tools `outline` / `related` /
  `changed` (a file's declared symbols, structural neighbours, and workspace drift
  without full-file reads), and `context` (a ranked, explained source bundle packed
  under a hard token budget in one call, with reuse economics) plus `stats` (aggregate
  token-savings accounting) - plus the `repo/{repoId}/...` key shapes.
- **Capture** - durable agent memory via `remember` / `update` / `forget`, the
  small stable topic vocabulary, knowledge-linking edges between entries
  (`addLinks` / `removeLinks` with a small `broader` / `narrower` / `related` /
  `partOf` relation vocabulary), TTL and CRDT-merge semantics, and what is and
  is not worth capturing.
- **Safety and health** - write tools are destructive and fail-closed;
  `repocontext_health` and `repocontext_index_status`; and what a `keyword` /
  `Failed` degraded state means.

Open that file and follow it directly.

## At a glance (the master file is authoritative)

Enough to know *whether to reach for it* without opening the master file first;
open it for the full rules before you rely on any of this. If these ever
disagree with the master file, the master file wins.

- **Reach for it first.** When the `repocontext_*` tools are present, lead with
  `search` / `scan` / `recall` *before* `grep` / `glob` - both for finding code
  and for recalling what past sessions captured. If the tools are absent (or
  `repocontext_health` is not reachable), silently fall back to
  `grep` / `glob` / `view`.
- **Guardrail 1 - locate here, read with `view`.** The index reflects the last
  ingest, not your uncommitted edits; treat every hit as a pointer and `view` the
  real file before quoting, relying on, or editing it.
- **Guardrail 2 - fall back when weak.** `mode: keyword` (a capable BM25 literal
  scan, but tokens not meaning), `status: Failed`, or a stale / mid-ingest
  (partially embedded) index can be a worse locator than `grep` - prefer
  distinctive terms, and do not force it when your terms are too generic.
- **Writes are destructive and fail-closed.** Never call `remember` / `update` /
  `forget` / `add_repo` / `remove_repo` speculatively, and never `remove_repo`
  the `lattice` repo. `remove_repo` in particular requires **explicit user
  consent** - it drops a repository's entire indexed context, so only run it when
  the user has explicitly asked for that repository to be removed; otherwise ask
  first.
