---
applyTo: "**"
---

# Repository-context MCP (repocontext)

This file is the **single master** for how to use the `repocontext` MCP server in
Orleans.Lattice. The `repocontext` skill (`.github/skills/repocontext/SKILL.md`)
points here rather than restating these rules, so there is one place to change
and nothing to drift.

`repocontext` is a repository-context store that indexes a repository into queryable records - structural nodes
(files / packages / symbols) with content digests and vector embeddings - plus a
durable **agent-memory** layer (notes, decisions, gotchas) keyed by topic. Read
tools are safe and read-only; write tools are **destructive and fail-closed**.

**Tool names.** For brevity this guide names the retrieval and capture tools by
their bare verb (`search`, `recall`, `remember`, and so on); the real tool ids
carry the `repocontext_` prefix, as do the health / status / list tools
(`repocontext_health`, `repocontext_index_status`), so any bare `index_status`
in the prose means the same tool.

## Use it as the primary recall and search mechanism

`repocontext` is the **default first move** for finding things in this repo and
for recalling what past sessions learned. For any discovery task - locating
code, docs, symbols, or a prior decision - reach for `search` / `scan` / `recall`
*before* `grep` / `glob`, even when you think you know the path. Do not default
to `grep` / `glob` for repo-wide discovery.

(Availability: if the `repocontext_*` tools are not in your toolset - or
`repocontext_health` does not report the surface reachable - then it is simply
not wired up in this environment; fall back to `grep` / `glob` / `view` and do
not mention or block on it. Everything below applies when the surface is
present.)

The two jobs it is your first move for:

- **Discovery across the whole repo** - lead with a natural-language `search`, or
  a `scan` when you want completeness (every file under a path, every memory
  entry). Use it even when you have a rough idea of the filename; it is faster
  than guessing paths.
- **Durable cross-session memory** - check what past sessions captured
  (decisions, gotchas, conventions, glossary) before rediscovering it. Note that
  `search` ranks across code *and* memory together and has no memory-only
  filter, so for a memory-only sweep use `scan` with scope `Memory` (or
  `MemoryTopic` for a single topic).

A typical retrieval loop: `search` for the question, take the best hit's `path`,
`view` that file to read the real content, and (optionally) `remember` any
durable gotcha you uncovered. When you need the actual source to *do* a task (not
just find a file), reach for [`context`](#context---budgeted-context-bundle-in-one-call)
instead - it packs ranked, explained source under a hard token budget in one call.
To navigate the code graph (a file's callers, dependents, tests, or declared
symbols) without full-file reads, use the
[graph-navigation tools](#graph-navigation---outline--related--changed)
`outline` / `related` / `changed`.

Two guardrails make it safe to lean on as primary - follow both every time:

1. **Locate with `repocontext`, read the real file with `view`.** The index
   reflects the **last ingest, not your uncommitted edits**. Treat every hit as a
   pointer: once `repocontext` tells you *where*, `view` (or `grep`) the actual
   file before you quote, rely on, or edit its content. Never edit from an index
   digest.
2. **Fall back only after a probe shows it is degraded - never sight-unseen.**
   Every trigger below is a signal you can read **only from a `repocontext`
   call**, so a discovery task must *open* with a `repocontext` probe
   (a `repocontext_search`, or a quick `repocontext_health` / `index_status`
   check) and may drop to `grep` / `glob` only once that probe shows one of
   these. Opening with `grep` without probing `repocontext` first is **not** a
   valid fallback - it is skipping the primary tool, and is the one thing this
   guardrail exists to stop. Drop to `grep` / `glob` for a query when any of
   these hold:
   - `search` returns `mode: keyword` (semantic index unhealthy);
   - `index_status` reports `status: Failed`;
   - the index is **mid-ingest** (`status: Running` with
     `filesEmbedded < filesScanned`): `mode: semantic` still answers, but it only
     covers the already-embedded slice, so a *missing* hit does **not** mean the
     code is absent (see "Health and degraded mode");
   - the newest hit's `lastIngested` (or `index_status`'s `updatedAt`) predates
     committed work you are relying on. Note the index never reflects
     *uncommitted* edits - that is guardrail 1's job to handle, not a reason to
     fall back.

   A stale, keyword-only, or partially-embedded index is a worse locator than a
   direct search, so do not force it - but confirm that with a probe first
   rather than assuming it.

Only skip `repocontext` outright for a lookup you can nail in one or two direct
calls on a file you already know by path - there the hop is pure overhead.

**First-use smoke check.** The first time you reach for it in a session, spend
one round to learn which mode you are in: `repocontext_health`, then
`repocontext_index_status {repoId}`, then one representative `search`. That tells
you up front whether you have full `semantic` coverage, a still-`Vectorising`
index that is only partially embedded, or a `keyword` / `Failed` degraded state -
so you calibrate how hard to lean on it, instead of discovering a weak index
mid-task.

## The repo id

- **Derivation.** A repository's `repoId` defaults to the **final path segment of
  the indexed path** - the repo's own folder name - unless an explicit `repoId`
  was passed to `repocontext_add_repo`. Do not assume it; confirm with
  `repocontext_list_repos`. Examples in this file write it as `{repoId}` -
  substitute the value you derived.
- **Disambiguation.** If `repocontext_list_repos` returns more than one repo,
  select the one whose id matches the workspace you are working in and ignore
  unrelated indexes (for example throwaway test fixtures).
- **Visibility during a first ingest.** `list_repos` enumerates **committed,
  materialised structural records**, which is a different source from the live
  progress counters `index_status` reads. A repository still in its **first**
  ingest can therefore be **absent from `list_repos` entirely** - and not yet
  answer `scan` or `search` - while `index_status` already reports it `Running`
  with advancing counters, because its structural writes are durable in the WAL
  but have not yet materialised into the readable projection. So do **not** infer
  "not indexed" from an empty `list_repos`: `index_status {repoId}` is the
  authority for an onboarding still in progress, and a repo surfaces in
  `list_repos` only once its structural records materialise.
- **Fields.** `repocontext_list_repos` reports one row per repo, but the fields
  it returns depend on ingest state: expect at least `repoId` and
  `embeddedVectorCount` (how many **sources** - files and captured symbols - have
  a landed embedding, so it can exceed the file count once symbols are embedded),
  and treat a per-repo `lastIngested` / `fileCount` as **best-effort - they can be
  absent while an ingest is still running**. Do not rely on `list_repos` alone to
  judge staleness. The dependable freshness signals are the per-hit `lastIngested`
  on `search` results and `index_status`'s `updatedAt` (see "Health and degraded
  mode").

## Retrieval

### search - relevance-ranked, natural language

- Pass a natural-language `query` and a small `k` (1-100, default 10); start
  small and widen only if needed.
- **Always read the `mode` field on the result:**
  - `semantic` - vector nearest-neighbour search (the good path).
  - `keyword` - deterministic BM25 keyword/structural scan, used because the
    vector index is unavailable or stale. Ranking is corpus-relative BM25 over
    file content and names (a distinctive term outweighs a ubiquitous one), so it
    is a capable literal-term search - but it matches tokens, not meaning, so a
    purely conceptual query with no shared vocabulary can still rank poorly. When
    you see `keyword`, prefer distinctive identifier-like terms, and fall back to
    `grep` only if the terms you have are too generic.
  - `empty` - no matches.
- Hits carry `key`, `path`, `fields` (`digest`, `language`, `sizeBytes`,
  `lastIngested`), `tags`, `links` (structural cross-references between
  records - informational; there is no client tool to set them), and `reasons`
  (see next bullet). **`search` does not return file contents** - `view` the file
  (or `recall` the record) for the body.
- **Every hit carries a `reasons` list explaining why it ranked** - server-derived,
  deterministic, ordinal-ordered, bounded, and never null. A `semantic` hit lists
  `semantic`, the matched chunk kind (`chunk:symbol` or `chunk:file`), and
  `symbol:<fqName>` for a symbol vector; a `keyword` hit lists whichever projected
  fields the query terms hit (`path-name-match`, `symbol:<fqName>`, `tag:<tag>`,
  `topic-match`, `content-match`, `key-match`). Use `reasons` to judge whether a hit
  is relevant for the right reason (a name/path coincidence versus a genuine content
  or semantic match) before you act on it.
- Results are verbose - each hit carries full metadata, and the server returns
  every payload as both structured and text content - so keep `k` small and
  prefer a targeted `scan` (with `pathPrefix`) when you want breadth without the
  per-hit weight.

### scan - ordered, complete enumeration

- Deterministic paged walk of a `scope`: `Files`, `Packages`, `Symbols`,
  `Memory`, or `MemoryTopic` (with `topic`).
- Restrict a `Files` scan to a subtree with `pathPrefix`, e.g.
  `src/lattice/Primitives/`.
- Page with the returned `continuationToken` while `hasMore` is true;
  `pageSize` is 1-500 (default 100).
- Reach for `scan` when you want **completeness** (audit every file under a path,
  list every memory entry in a topic) rather than relevance.
- **Expiry and link staleness are not evaluated by a bulk read.** A `scan` (and a
  degraded `keyword`-mode `search`) enumerates key+value only, so its expiry
  fields (`expires`, `hasExpired`, `expiresAtUtc`, `remainingSeconds`) and its
  memory link-staleness fields (`stale`, `staleLinks`) come back `null`
  ("not evaluated") - this is by design, not a durable claim. A scan still yields
  only live (non-expired, non-tombstoned) entries. To read an entry's authoritative
  TTL or link staleness, `recall` it (or, for TTL, use a `semantic` `search`,
  whose hits are hydrated per-key).

### recall - one record by key

- Fetch a single record by its full key. Key shapes:
  - File: `repo/{repoId}/file/{path}` - e.g.
    `repo/{repoId}/file/src/lattice/Primitives/GCounter.cs`
  - Memory: `repo/{repoId}/mem/{topic}/{id}`
- `recall` returns the stored record - its flattened `fields`, `tags`, `links`,
  and remaining TTL - not live file content; per guardrail 1, `view` the file for
  the current body.
- **`recall` evaluates memory link staleness.** For a memory entry, `recall`
  compares each structural link (to a file or symbol) against the target's
  current content digest, captured when the link was made, and reports drift
  through `stale` (any linked target changed or was deleted) and `staleLinks`
  (the specific target keys). A `stale` link is a cue to re-read the target and
  refresh or retire the note. `neighbors` evaluates the same per walked entry.
  Bulk reads do not (see below), so `stale`/`staleLinks` come back `null` there.
- A missing or expired key returns `exists: false`, so you can tell an absent
  entry from an empty one.

### neighbors - walk knowledge-linking edges

- `neighbors` walks the typed knowledge-linking edges out of a memory entry (see
  [Knowledge linking](#knowledge-linking---typed-edges-between-memory-entries))
  and returns the adjacent entries, hydrated from the store, as a bounded
  breadth-first traversal. Use it to explore the curated concept graph past
  sessions captured - "what does this concept relate to?" - rather than to search
  free text.
- Bound the walk: `relation` restricts it to one edge type (e.g. `broader`);
  `depth` is clamped to `[1, 3]` (default 1, immediate neighbors); `maxNodes` is
  clamped to `[1, 100]` (default 50). The result's `truncated` flag reports when
  the node cap stopped the walk.
- A seed key with no live entry returns `exists: false`; a dangling edge whose
  target has no live value is still returned as a neighbor with its own
  `exists: false`, so you can see broken links.
- Each walked neighbor that is a memory entry is returned with its link staleness
  evaluated (`stale` / `staleLinks`), exactly as `recall` does, so a graph walk
  surfaces which linked concepts point at drifted code.

### Graph navigation - outline / related / changed

Three read-only tools navigate the structural graph the reconcilers maintain (file,
symbol, content, and reverse cross-reference records) so you can understand code
without spending tokens on full-file reads. All are pure reads over stored records;
`related` and `outline` never touch disk, and `changed` reaches the workspace only
through the fail-closed boundary.

- **`outline`** - the structural skeleton of one indexed file *without its body*:
  each declared symbol (kind, signature, 1-based start/end line span), ordered by
  position, plus the token cost of reading the whole file. It is the cheapest way to
  grasp a file's shape and decide whether a full `view` is worth the tokens. The
  token count is null only when the file was never content-processed; a path with no
  stored file node returns `exists=false`.
- **`related`** - the structural neighbourhood of one file: the type-names it
  references (outbound imports), the indexed symbols that reference *its*
  declarations (inbound dependents, resolved to their declaring files), and the test
  types that cover it (from the `{Name}Tests` / `{Name}Test` convention). Use it to
  navigate the code graph (callers, dependents, tests) rather than guessing.
  **Caveat:** edges are keyed by *simple* (unqualified) type-name, a syntactic
  approximation - two distinct types sharing a simple name are not disambiguated, so
  treat a dependent set as a lead to confirm by reading, not a proof.
- **`changed`** - how the current workspace has drifted from the index (files added,
  updated, removed), by digest comparison and **without invoking git**, so it works
  in any checkout. It also lists the indexed files that depend on the changed ones
  (the reverse-reference impact set / blast radius). Use it to scope a review to what
  actually moved, or to see what an index needs to catch up on. A supplied path
  resolving outside the mounted workspace is refused.

### context - budgeted context bundle in one call

`repocontext_context` is the highest-leverage retrieval tool: it returns a ranked,
explained bundle of source for a natural-language `task`, packed under a **hard token
ceiling**, collapsing the `search -> recall -> view` loop into one round trip that
can never overrun your context budget. Prefer it over hand-running that loop when you
need the actual source to *do* a task (not just locate a file).

- It searches for the `task` (semantic when available, else a keyword bundle),
  resolves the top hits to unique files, and packs each at a `detail` level:
  `paths` (path only), `outline` (declared-symbol skeleton), or `slices` (bounded
  body text). `auto` (default) packs the richest level that yields a non-empty bundle
  and reports the level it settled on in `detail`.
- Budgeting: `responseBudgetTokens` is the hard ceiling (the reported `totalTokens`
  is the exact BPE sum and never exceeds it); `top` bounds how many files are
  considered. Both are clamped, never trusted to drive unbounded work. `truncated`
  flags that lower-ranked candidates were dropped. It **fails closed** when even the
  cheapest entry does not fit: `entries` is empty and `retryBudgetTokens` reports a
  budget guaranteed to admit at least one entry (null when the search matched
  nothing).
- Each entry carries its match `reasons`, its exact BPE `tokenCount`, the whole-file
  `fullReadTokenCount`, and a per-version `contentHash`. Per guardrail 1, the packed
  `slices`/`outline` content still reflects the last ingest - `view` the file before
  editing.
- **Reuse economics - never pay twice.** Each delivered unit carries a stable opaque
  `receipt`. Hand receipts back in `seen` to suppress exactly those units (the rest
  of the file still arrives), or assert whole-file possession in `known` as
  `path@hash`. Pass a stable `session` id to persist this bookkeeping across calls:
  the session auto-suppresses units it already delivered and validates `known`
  claims. A whole-file claim is honoured **only** for a version actually delivered as
  a complete body, so partial evidence is never promoted to whole-file possession.
  Suppressed content is acknowledged in `reused` and never charged against `top` or
  the budget. Reuse the same `session` id across a multi-step task to keep each
  follow-up bundle cheap.

### stats - usage accounting

`repocontext_stats` reports an aggregate summary of the surface's own usage over a
bounded recent window (no arguments), so you can see whether it is actually reducing
context cost. It returns only summed token figures - `calls`, `responseTokens`,
`readsReplacedTokens` (whole-file reads conservatively replaced, credited only for
delivered whole-file-equivalent content), `netSavedTokens`, and `windowSeconds` - and
carries no body, query, path, or repository identity. Read-only.

`netSavedTokens` (= `readsReplacedTokens - responseTokens`) is **signed**, and a
negative value is expected, not a defect: read-replacement credit is awarded only for
whole-file-equivalent (`slices`) delivery, so a discovery-heavy window - cheap
`paths` / `outline` calls, or `context` used only to locate - spends response tokens
while earning little credit and nets negative. Net trends positive as a task moves to
reading real bodies (`slices`) and reuses a stable `session` so repeated context is
suppressed and never re-charged. Treat it as a deliberately conservative floor on the
true saving - it never credits the `search -> recall -> view` round trips it also
removed - not a live figure you must keep above zero.

## Capture - durable agent memory

### remember

- Creates or updates a memory entry under a `topic` (both `repoId` and `topic`
  required). Omit `id` to create a new entry with a generated id; pass an
  existing `id` to **CRDT-merge in place** rather than blind-overwrite.
- Useful fields: `title`, `body`, `kind` (`Decision` / `Note` / `Memory`),
  `tags`, `author`, `provenance`, and `ttlSeconds`. To relate one entry to
  another, pass `addLinks` / `removeLinks` (see
  [Knowledge linking](#knowledge-linking---typed-edges-between-memory-entries)).
- **TTL - when to set it.** Default to **no `ttlSeconds`** (durable, or the repo
  default): decisions, gotchas, conventions, and glossary are meant to outlive
  the session, and when one goes wrong you `update` or `forget` it rather than
  let it lapse. Set a TTL only when the entry has a **known natural expiry and
  its silent disappearance is acceptable** - for example a time-boxed fact ("CI
  flaky on X until #123 lands"), short-lived coordination for sibling sessions
  working the same task right now, or provisional state you expect to supersede
  shortly. Never TTL something whose loss would be a problem: expiry is silent
  and unlogged, so correctness-critical memory must be retired deliberately with
  `forget`, not left to time out.

### Keep the topic vocabulary small and stable

Prefer a fixed set so related notes stay groupable instead of fragmenting into
synonyms (`decision` vs `decisions`):

- `decisions` - a design choice **with its rationale** (use `kind: Decision`).
- `gotchas` - a non-obvious pitfall a future agent would trip on.
- `conventions` - a project norm not obvious from a single file.
- `glossary` - a domain term.
- `todo` - a cross-session follow-up.
- or a stable component/feature name (e.g. `wal`, `replication`).

### What to capture (and what not)

- **Do** capture non-obvious decisions with rationale, hard-won gotchas,
  cross-session TODOs, and domain glossary - things that outlive this session.
  Keep each entry short and self-contained: enough context to act on without the
  originating conversation.
- **Do not** capture secrets or credentials, anything already committed to
  `docs/` or `AGENTS.md` (link to it instead), transient within-turn state, or
  large blobs. Memory is not a scratchpad for the current turn.
- **A user telling you to remember counts as a capture request.** When the user
  says *remember* / *note* / *keep in mind* / *don't forget* a standing fact,
  decision, or convention, that is a `repocontext_remember` request - not a cue
  to merely hold it in the current chat. Persist it under the right topic and
  confirm briefly, rather than only acknowledging it. Skip persistence only when
  the instruction is plainly task-scoped (keep it in-conversation, or give it a
  short TTL).

**Example - good vs. weak.** A good entry is short, self-contained, correctly
topiced, and carries the rationale:

- Good: `remember(topic: "gotchas", kind: Note, title: "WAL shard read path returns ValueTask", body: "IWalShardGrain.ReadAsync / ReadShippingAsync / GetNextSequenceAsync return ValueTask (not Task) for the same-silo fast path the shipper polls; add .AsTask() only at Task.WhenAll fan-out sites. Reverting to Task reintroduces a real per-poll allocation.")` - actionable months later without this conversation.
- Weak: `remember(topic: "notes", body: "looked at the WAL, seems fine")` - vague, no rationale, transient, and filed under a catch-all topic instead of a stable one.

### update / forget

- `update` - CRDT-patches scalar `fields` and `tags` on an **existing** record
  (preserves any remaining TTL; fails if the key does not exist). Use it to
  correct or re-tag, not to recreate. On a memory record it also patches
  knowledge-linking edges via `addLinks` / `removeLinks`.
- `forget` - removes an entry. Default is an immediate hard delete; pass
  `lapse: true` (optionally with `lapseSeconds`) to re-write it with a short TTL
  so concurrent readers drain gracefully. Prefer `lapse` when another session
  may be reading.

### Knowledge linking - typed edges between memory entries

A memory entry can carry typed, directional **links** to other repository-context
keys: a small knowledge graph over your captured concepts, layered on top of the
same CRDT-merged store. Use it to connect a glossary/concept entry to the ones it
generalises, specialises, or relates to, so a later session can *walk* the graph
with `neighbors` instead of guessing search terms.

- **Write edges** with `remember` or `update` using `addLinks` / `removeLinks`:
  a map from a **relation name** to a list of **target keys**. Links are a
  memory-record feature only; supplying them for a structural (file/symbol)
  record is rejected. Every target must be a well-formed key
  (`repo/{repoId}/...`) or the whole write is rejected before any change is made;
  the target need not exist yet (dangling edges are allowed and surface as
  `exists: false` when walked).
- **Read edges** with `recall` (the entry's `links` map) or walk them with
  `neighbors`.
- **A link to code is tracked for staleness.** When you link a memory entry to a
  structural target (a file or symbol), the store captures that target's content
  digest at link time. A later `recall` (or `neighbors` walk) flags the entry
  `stale` when the target has drifted or been deleted, so a note that anchors a
  decision to a specific file tells a future session when the file has moved on.
  Prefer linking a durable note to the code it depends on for exactly this signal;
  a memory-to-memory edge carries no digest and is never stale.
- **Relation vocabulary - keep it small and stable**, the same discipline as
  topics. Prefer this fixed set (a lightweight SKOS-style derivation), authored
  from the **more general** entry outward:
  - `broader` - the target is a more general concept (this is a kind of target).
  - `narrower` - the target is a more specific concept (inverse of `broader`).
  - `related` - a non-hierarchical association.
  - `partOf` - the target is a whole this entry is a component of.
  Author one direction and let the reader infer the inverse; do not write both
  `broader` and `narrower` for the same pair.
- **What to link.** Connect durable concept/glossary entries into a navigable
  graph (e.g. `Orleans.Lattice` --`narrower`--> `WAL`, `CRDT`, `Shard`). Do not
  link transient notes or use links as a second tag system - a link asserts a
  relationship between two concepts, not a label on one.

**Example.** Capture two concepts and relate them:

- `remember(topic: "glossary", id: "wal", title: "Write-ahead log", body: "...")`
- `remember(topic: "glossary", id: "tree", title: "B+ tree", body: "...", addLinks: { "related": ["repo/{repoId}/mem/glossary/wal"] })`
- later: `neighbors(key: "repo/{repoId}/mem/glossary/tree", relation: "related")` returns the WAL entry.

## Write-tool safety

- Every write tool (`add_repo`, `remove_repo`, `remember`, `update`, `forget`)
  is **destructive and fail-closed** - offered only when the host opted writes
  in. Never call one speculatively.
- Do not write memory without a clear durable reason, and never `remove_repo`
  the repo you are working in.
- **`remove_repo` requires explicit user consent.** It drops a repository's
  entire indexed context (structural nodes, memory, and vectors), so never call
  it on your own initiative, as a cleanup step, or to "reset" an index. Invoke it
  only when the user has explicitly asked for that specific repository to be
  removed; if a task seems to need it but the user has not asked, stop and ask
  first rather than assuming consent.

## Freshness and re-ingest

Freshness is mostly automatic. Once a repository is onboarded, its
per-repository self-index grain keeps the index converged with no client call: a
continuous background reconcile walks the tree, diffs it against the stored
records, and applies exactly the delta, so files added, edited, and deleted on
disk are picked up on their own. Directory-modification-time pruning keeps that
cheap, and a pure in-place content edit (which does not bump its directory's
mtime) is caught by a periodic full sweep - so the index trails on-disk state
only by a bounded reconcile / full-walk interval, not until some manual
re-ingest. That bounded lag is exactly why guardrail 1 still holds: locate with
the index, then read the live file with `view`.

You therefore rarely need to re-ingest by hand. `repocontext_add_repo` is
idempotent and resumable and re-ingests only what changed (pruning deleted
files); its one edge over the background reconcile is that it forces an
*immediate* full, exact walk, so reach for it only when you cannot wait out the
reconcile bound after a large change. A re-add (like the initial onboard) starts
an async job - poll `index_status` and read its two progress fractions
separately: `chunksCommitted` / `chunksTotal` is the structural **apply** phase,
while `filesEmbedded` / `filesScanned` is the slower **embedding** phase that
follows it, so chunks reaching completion while `filesEmbedded` still climbs is
normal, not a contradiction (embeddings also require a healthy vector projection
- see "Health and degraded mode"). A still-`Running` job whose `filesEmbedded` or
`updatedAt` keeps advancing is healthy; only a stalled `updatedAt` or
`status: Failed` warrants giving up on it.

## Health and degraded mode

- `repocontext_health` - is the surface registered and reachable.
- `repocontext_index_status {repoId}` - `status` / `phase` / counters
  (`filesScanned`, `filesEmbedded`, `chunksCommitted`, `updatedAt`). Read it two
  ways:
  - **Degraded:** `status: Failed`, or `filesEmbedded: 0` alongside a
    stale-projection error, means semantic search has degraded to `keyword` -
    retrieval still works but ranking is literal. Report it and fall back to
    `grep`; do not try to repair projection state from here - it needs an
    operator-driven rebuild.
  - **Partially embedded (mid-ingest):** `status: Running` with
    `0 < filesEmbedded < filesScanned` means vectorising is still in flight.
    `search` returns `mode: semantic`, but only the already-embedded files are
    reachable by vector, so a confident-looking result can silently omit
    not-yet-embedded files. For **completeness** while an ingest runs, do not
    trust a single `search` - also `scan` the relevant `pathPrefix` (or `grep`),
    and re-run the `search` once `filesEmbedded` reaches `filesScanned`.
  - **Stale content projection (body-text ranking only):** the per-file content
    projection is a separate, rebuildable tree from the vector index. If it is
    terminally stale (its leaf checkpoint fell off the write-ahead log awaiting an
    operator rebuild), the store degrades gracefully rather than failing: ingest
    still completes (structural, symbol, and vector passes are independent and the
    content back-fill retries the skipped files once the tree is healed), and
    keyword `search` still ranks over filenames, identifiers, and memory - it just
    loses file-body matches until the rebuild lands. So a `keyword` result that
    misses a term you know is inside a file body (but not its path or symbols) can
    mean a stale content tree, not an absent match; confirm with `grep` before
    concluding the code is gone.

## Regions

- Every tool accepts an optional `region`; omit it to target the current region.
  Set it only when you are deliberately working against a named peer region
  (see `lattice_list_regions`).
