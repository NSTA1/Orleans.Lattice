# Retrieval and token economics

The repository-context surface is only useful to an agent if the context it
returns is worth the tokens it costs. This topic covers the retrieval and
token-economics capabilities layered on top of the [record model](record-model.md):
explainable search, the graph-navigation tools, the budgeted context bundle, its
reuse economics, usage accounting, and the shared token counter they all budget
in. Every tool here is read-only and clears the same fail-closed authorization
gate as the rest of the surface.

## The shared token counter

A single byte-pair-encoding (BPE) token counter underpins every figure on this
page, so a token cost reported by one tool means the same thing everywhere. It is
constructed once from a tokenizer **profile** and reused by the reconcile path
(the per-file `TokenCount` on each file node) and the retrieval surface (bundle
budgets and outline costs).

The profile is selected by the `LATTICE_REPOCONTEXT_TOKENIZER` environment
variable: `o200k` (the default, matching current-generation models) or `cl100k`.
Because the same counter is used to compute the stored per-file counts and to pack
a bundle, a bundle's `totalTokens` is an exact sum over the same encoding the
consuming model uses, not an estimate. The bundle's `responseTokens` builds on that
exact content sum but is itself a deliberately conservative **estimate**, because it
also accounts for JSON envelope and the SDK's dual emission (see
[The budgeted context bundle](#the-budgeted-context-bundle)).

## Explainable search

`repocontext_search` ranks records against a natural-language query and returns
each hit hydrated from the store. Beyond the `mode` field (`semantic`, `keyword`,
or `empty`) that reports which path answered, every hit carries a machine-readable
**`reasons`** list: server-derived, deterministic, ordinal-ordered, bounded, and
never null.

- A **semantic** hit lists `semantic`, the matched chunk kind (`chunk:symbol` or
  `chunk:file`), and `symbol:<fqName>` when the match was a symbol vector.
- A **keyword** hit lists whichever projected fields the query terms actually hit,
  in a fixed high-signal-first order: `path-name-match`, `symbol:<fqName>`,
  `tag:<tag>`, `topic-match`, `content-match`, and `key-match`.

The reasons let an agent (or a human reviewing a trace) understand *why* a result
ranked where it did, rather than treating the ranking as opaque - and they let a
caller decide which hits are worth a full read before spending the tokens.

## Graph navigation

Three read-only tools let an agent navigate the code graph without reading whole
files, each a bounded read over stored records that never touches the workspace on
disk (except `repocontext_changed`, which walks the workspace only through the
fail-closed boundary):

- **`repocontext_outline`** returns a file's declared-symbol skeleton - each
  symbol's kind, signature, and 1-based line span, ordered by position - plus the
  token cost of reading the whole file. It is the cheapest way to grasp a file's
  shape and decide whether a full read is worth the tokens.
- **`repocontext_related`** resolves a file's structural neighbourhood: the
  type-names it references (outbound imports), the indexed symbols that reference
  its declarations (inbound dependents, resolved to their declaring files), and
  the test types that cover it. Dependents and tests come from the reverse
  cross-reference projection, so the lookup is bounded rather than a
  whole-repository scan.
- **`repocontext_changed`** reports how the current workspace has drifted from the
  index - files added, updated, and removed - by comparing content digests without
  invoking git, and lists the indexed files that depend on the changed ones (the
  reverse-reference impact set), so an agent sees the blast radius of a set of
  edits before re-indexing. The walk is rooted at the repository's *indexed* root
  and reuses the filters it was ingested with, so the report always compares the
  same path space the index was built in; the supplied path is a scope, so a
  directory inside the repository restricts the report to that subtree, and a path
  outside the indexed root is refused rather than compared. Unchanged files are
  settled by a stat against the stored size and ingest anchor instead of being
  re-read, the same fast path the periodic reconcile uses, so a whole-repository
  drift report stays cheap on a large tree.

## The budgeted context bundle

`repocontext_context` is the headline capability: it collapses the
search -> recall -> read loop into a single round trip that can never overrun the
context budget. Given a natural-language task and a token budget, it searches the
store, resolves the top hits to unique files, and packs each file at a **detail
level** under a hard token ceiling:

- `paths` - the path only.
- `outline` - the declared-symbol skeleton, reusing the outline projection.
- `slices` - bounded body text.
- `auto` (the default) - the richest level that still yields a non-empty bundle,
  with the concrete level reported back in `detail`.

Every entry carries its match `reasons`, its exact BPE `tokenCount`, and the
whole-file `fullReadTokenCount`. The bundle reports **two** figures, and it is the
first that the ceiling bounds:

- `responseTokens` - the estimated cost of the response **as the caller receives
  it**: the delivered content plus each entry's JSON envelope (path, reasons,
  content hash, per-unit receipts), multiplied by the MCP SDK's dual-emission
  factor, because every tool result is serialized twice - once as structured
  content and once as text. This **never exceeds** `budgetTokens`.
- `totalTokens` - the narrower exact BPE sum of the packed source text alone.
  Useful as "how much source did I get", but it is not what the budget bounds:
  charging content alone once let a bundle reporting a few thousand tokens land
  as a response many times that size (issue #1811).

The estimate is deliberately conservative, so a bundle may come in slightly under
the ceiling but never over it. When even the cheapest entry does not fit,
the tool **fails closed**: `entries` is empty and `retryBudgetTokens` reports a
budget guaranteed to admit at least one entry on a retry (null when the search
matched nothing, so no larger budget would help). A `truncated` flag marks a
bundle that had to drop lower-ranked candidates. The `top`, `responseBudgetTokens`,
and `detail` arguments are validated and clamped, never trusted to drive unbounded
work.

## Reuse economics

The bundle never makes an agent pay twice for context it already holds. Each
delivered **unit** - a path pointer, a body span, or an outline symbol - carries a
stable opaque `receipt`, and each entry carries a per-version `contentHash`. A
unit is a **descriptor, not a copy of the text**: the delivered text lives once,
on its entry's `content`, and the units correspond one-to-one, in order, to that
content's newline-separated segments. (Carrying the text on the units too would
put every byte of source on the wire twice within a single payload, and four
times across the emitted pair - see issue #1811.) A caller feeds prior knowledge
back in three ways:

- Hand receipts back in **`seen`** to suppress exactly those units; the rest of
  the file still arrives.
- Assert whole-file possession in **`known`** as `path@hash`.
- Pass a **`session`** id to persist this bookkeeping across calls: the session
  auto-suppresses units it already delivered and validates `known` claims, so a
  multi-call conversation converges on delivering each unit once.

The load-bearing guard is that a whole-file claim is honoured **only** for a
version that was actually delivered as a complete body. The session store records
possession only for `slices` (whole-body) deliveries; a `known` claim is validated
only against recorded possession. So partial evidence (an outline or a path) can
never be promoted to whole-file possession, and without a session a `known` claim
can never validate (fail closed). Suppressed content is acknowledged in `reused`
and is **never** charged against `top` or the token budget - a fully-reused file
does not consume a result slot, so the freed budget backfills lower-ranked
candidates.

The per-session bookkeeping lives on the `repo-context-session` tree as a
grow-only CRDT with a finite time-to-live; see
[record-model.md](record-model.md#session-reuse-bookkeeping) for the storage
model.

## Usage accounting

`repocontext_stats` reports whether the surface actually reduces context cost. Over
a bounded recent window it returns only summed token figures:

- `calls` - how many context calls were answered.
- `responseTokens` - the exact response tokens they spent.
- `readsReplacedTokens` - the whole-file read tokens they conservatively replaced,
  credited only for delivered whole-file-equivalent content (`slices` detail),
  never for discovery, partial detail, or content the caller already held.
- `netSavedTokens` - the net tokens saved, `readsReplacedTokens - responseTokens` (a
  signed figure; see below).
- `windowSeconds` - the length of the reporting window.

Crediting is deliberately conservative so the figure is never inflated: reused or
suppressed content is structurally excluded, and only `slices` deliveries earn
read-replacement credit. Because crediting is this conservative, `netSavedTokens` is
**signed** and routinely negative for discovery-heavy or reuse-light usage - that is
correct, not a defect. It turns positive as a task delivers real bodies (`slices`) and
reuses a `session` so repeated context is suppressed and never re-charged, and it is
deliberately not clamped at zero so the surface can honestly report when it is not yet
paying for itself. The figures are recorded per answered context call on a
bounded in-memory window and are also emitted as
`System.Diagnostics.Metrics` counters carrying a single low-cardinality `command`
tag, so a host already scraping OpenTelemetry sees them flow through the existing
[telemetry](../lattice.api.mcp.telemetry/README.md) surface with no bespoke plumbing.
The tool carries no body, query, path, or repository identity - aggregate figures
only.

### Emitted instruments

Every instrument this package publishes is listed below, all on the
`Orleans.Lattice.Api.Mcp.RepoContext` meter, so one scraper subscription covers the
whole surface. Each carries only low-cardinality tags - never a repository id, path,
query, or any body text.

| Instrument | Kind | Unit | Tags | What it records |
|---|---|---|---|---|
| `repocontext.calls` | `Counter<long>` | `{call}` | `command` | Answered repocontext calls, by tool. |
| `repocontext.response_tokens` | `Counter<long>` | `{token}` | `command` | The exact response tokens those calls spent. |
| `repocontext.reads_replaced_tokens` | `Counter<long>` | `{token}` | `command` | The whole-file read tokens they conservatively replaced. Credited only for delivered whole-file-equivalent content, so it is a floor rather than an estimate. |
| `repocontext.retrieval.ready_seconds` | `Histogram<double>` | `s` | `phase` | Seconds from host start to the retrieval plane first reporting ready, tagged by the phase it reached. Recorded once per process, so it is the cold-start time-to-retrieval-ready figure. |
| `repocontext.retrieval.unavailable` | `Counter<long>` | `{event}` | `cause` | Observed vector-plane fault episodes that made semantic retrieval unavailable, tagged by cause. A non-zero rate is what distinguishes a keyword answer caused by a real capability loss from an intended keyword-only deployment. |
| `repocontext.retrieval.ann.search` | `Counter<long>` | `{query}` | `state` | Semantic searches partitioned by the approximate-plane state that answered them: `bootstrapping` (the plane could not answer, so the fallback ladder ran), `exhaustive` (answered by scanning the vectors it holds), or `approximate` (answered from its trained partitioning). Because **every** answered query is counted, the total is a denominator: `approximate` pinned at zero beside a rising total is a measured absence of trained serving, not an absent measurement. |
| `repocontext.ann.sweep` | `Counter<long>` | `{sweep}` | `outcome` | Approximate-index build sweeps partitioned by outcome: `armed` (armed at least one build coordinator), `empty` (completed without arming anything, either because it observed no repository in the store listing or because every repository it observed declined arming - this arm reports what the sweep observed, never that the store is empty), or `faulted` (threw, so nothing is scheduled until a sweep gets through). Denominate by the **total across all three arms**, never by the configured sweep interval: a completing sweep advances at that interval, but `faulted` advances on the far faster retry backoff (250 ms doubling to a 30-second ceiling). All three reading zero means the sweep loop never ran at all, which the service's startup log line distinguishes. |
| `repocontext.vectorplane.rederive` | `Counter<long>` | `{event}` | `tree`, `outcome` | Rebuildable vector-plane tree fall-off observations and re-derivations: `observed` (an allowlisted fall-off was seen and a reset triggered), `completed`, `failed` (the fault stands for the next pass to retry), or `refused` (the tree is not a rebuildable derived tree, so re-derivation is declined fail-closed and the fault propagates). The `tree` tag is one of the fixed vector-tree names, never a repository id. |
| `repocontext.bootstrap.pass_arm_faults` | `Counter<long>` | `{fault}` | `arm`, `kind` | Indexing-pass arms that faulted, by the arm that faulted (`retire`, `ingest-files`, `ingest-symbols`, `ingest-memory`) and the fault kind (`scan-page-stalled`, otherwise the exception type name). Read it as a diagnosis of *which stage* aborted a pass, which previously required correlating log lines by timestamp. It is deliberately **not** denominated by passes started, because a single pass can fault on more than one arm. A rising `arm="retire"` is the one value that does not mean lost work: a retirement fault defers every removal to the next pass and the additions and updates already computed are still committed, so it reports deferral rather than an aborted pass. Carries no repository id - the accompanying warning does, at a cardinality logs can afford. |

Subtracting `repocontext.response_tokens` from `repocontext.reads_replaced_tokens`
gives the same signed net saving `repocontext_stats` reports, so the dashboard and
the tool cannot disagree.

## Enabling the surface

All of these tools are read-only and are contributed to any caller whose data read-or-write permission unlocks the repository-context group; none requires `enableWrites`. Register the module as
a companion to `AddLatticeMcp`, exactly as for the rest of the surface:

```csharp verify
using Orleans.Lattice.Api.Mcp.RepoContext;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddLatticeMcp(o => o.RequireAuthorization = true);
services.AddRepoContextTools();
```

Bind an `IEmbeddingProvider` for semantic search and bundles; without one, search
and the bundle still answer by keyword. For a ready-to-run local deployment see the
[container quickstart](container.md); the
[RepoContext MCP container sample](../../samples/RepoContextContainer/README.md)
runs the box end to end and walks the explainable-search, budgeted-bundle, reuse,
and stats tools against it.
