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
whole surface. Each carries only low-cardinality tags - never a path, query, or any
body text. One instrument, `repocontext.ann.build.slice`, does carry a repository
id, and that is the deliberate exception recorded in its own row below: the
approximate-index build plane is one coordinator per onboarded repository and
embedding space, an operator-chosen set of single or low double digits, and issue
#2855 established that without it fifteen failing repositories and one succeeding
repository were the same series.

| Instrument | Kind | Unit | Tags | What it records |
|---|---|---|---|---|
| `repocontext.calls` | `Counter<long>` | `{call}` | `command` | Answered repocontext calls, by tool. |
| `repocontext.response_tokens` | `Counter<long>` | `{token}` | `command` | The exact response tokens those calls spent. |
| `repocontext.reads_replaced_tokens` | `Counter<long>` | `{token}` | `command` | The whole-file read tokens they conservatively replaced. Credited only for delivered whole-file-equivalent content, so it is a floor rather than an estimate. |
| `repocontext.retrieval.ready_seconds` | `Histogram<double>` | `s` | `phase` | Seconds from host start to the retrieval plane first reporting ready, tagged by the phase it reached. Recorded once per process, so it is the cold-start time-to-retrieval-ready figure. |
| `repocontext.retrieval.unavailable` | `Counter<long>` | `{event}` | `cause` | Observed vector-plane fault episodes that made semantic retrieval unavailable, tagged by cause. A non-zero rate is what distinguishes a keyword answer caused by a real capability loss from an intended keyword-only deployment. |
| `repocontext.retrieval.ann.search` | `Counter<long>` | `{query}` | `state` | Semantic searches partitioned by the approximate-plane state that answered them: `bootstrapping` (the plane could not answer, so the fallback ladder ran), `exhaustive` (answered by scanning the vectors it holds), or `approximate` (answered from its trained partitioning). Because **every** answered query is counted, the total is a denominator: `approximate` pinned at zero beside a rising total is a measured absence of trained serving, not an absent measurement. All three arms are pre-minted at zero when the reporter is constructed, so each exists from process start; if an arm is *absent* rather than zero, read `lattice_metrics_series` against the collector ceiling and `lattice_metrics_dropped_measurements_by_family_total` before concluding anything, because a series whose first occurrence falls after a ceiling is reached is refused at creation. |
| `repocontext.ann.sweep` | `Counter<long>` | `{sweep}` | `outcome` | Approximate-index build sweeps partitioned by outcome: `armed` (armed at least one build coordinator), `empty` (completed without arming anything, either because it observed no repository in the store listing or because every repository it observed declined arming - this arm reports what the sweep observed, never that the store is empty), or `faulted` (threw, so nothing is scheduled until a sweep gets through). Denominate by the **total across all three arms**, never by the configured sweep interval: a completing sweep advances at that interval, but `faulted` advances on the far faster retry backoff (250 ms doubling to a 30-second ceiling). All three arms are pre-minted at zero when the reporter is constructed, so all three reading zero means no sweep has completed yet. That does not localise the fault and in particular does not establish that the sweep loop is not running, which the service's startup log line reports directly; an arm that is *absent* rather than zero points at collector saturation, read from `lattice_metrics_series` and `lattice_metrics_dropped_measurements_by_family_total`. |
| `repocontext.ann.sweep.arming` | `Counter<long>` | `{repository}` | `result` | Arming calls the sweep made, partitioned by what the coordinator answered: `armed` (the coordinator accepted, so a build is scheduled), `deferred` (the call timed out because the coordinator is non-reentrant and already inside a long build turn, which is the expected answer from a healthy coordinator mid-build and deliberately **not** a fault), or `faulted` (the call threw something other than a timeout). Its denominator is **repository visits**, where `repocontext.ann.sweep` counts **sweeps**, and one sweep visits every repository in the listing - so the two are different populations, neither decomposes the other, and a ratio between them means nothing. This series exists because the sweep outcome could not carry the deferral at all: a sweep arming one repository and deferring nine reported `armed` exactly as one that armed ten, and a sweep on which **every** coordinator deferred fell through to `empty`, indistinguishable from a sweep over a store holding no repositories - two states at opposite extremes, one observation. A rising `deferred` beside `armed` at zero is a wholly wedged build plane; a `result` partition totalling zero beside a completed sweep is a genuinely empty listing. All three arms are pre-minted at zero when the reporter is constructed, so an arm reading zero and an arm being absent are different observations; an absent arm is a collector fault, read from `lattice_metrics_series` and `lattice_metrics_dropped_measurements_by_family_total`. |
| `repocontext.vectorplane.rederive` | `Counter<long>` | `{event}` | `tree`, `outcome` | Rebuildable vector-plane tree fall-off observations and re-derivations: `observed` (an allowlisted fall-off was seen and a reset triggered), `completed`, `failed` (a transient fault stands and a later pass retries it), `denied` (the access gate refused the reset, which is deterministic and will not clear on retry), `suppressed` (a fall-off was seen inside the post-failure backoff window, so no reset was attempted), or `refused` (the tree is not a rebuildable derived tree, so re-derivation is declined fail-closed and the fault propagates). The `tree` tag is one of the fixed vector-tree names, never a repository id. |
| `repocontext.ann.build.corpus` | `Counter<long>` | `{build}` | `coverage` | Approximate-index builds that reached `Ready`, partitioned by how much of the repository's vector prefix the read-path access gate admitted: `nonempty` (the build holds vectors, so the corpus read plainly succeeded and no probe was taken), `unrestricted` (it holds nothing and the whole prefix is admitted, so the repository genuinely has no vectors), `filtered` (it holds nothing and the gate narrowed the prefix, so an unknown subset was withheld), `denied` (it holds nothing because the gate refused the prefix outright, so the read never happened and the index state is **unknown** rather than empty), or `unknown` (it holds nothing and the coverage probe could not answer). A denied *range* read returns a clean, successful, empty result rather than throwing, so without this partition an authorization failure and an empty repository are the same observation. Because **every** completed build is counted, the total is a denominator: `denied` pinned at zero beside a rising total is a measured absence of denial, not an absent measurement. All five series are pre-minted, so `denied` is present and reads `0` on a healthy host rather than being missing. |
| `repocontext.ann.build.denial_terminal` | `Counter<long>` | `{coordinator}` | none | Build coordinators that observed enough consecutive uninterpretable corpus reads to conclude the host is refusing them, and have parked on the capped retry interval (one attempt roughly every five minutes) instead of retrying on the two-second phase cadence. Counted once per denial episode, so it separates "a denial happened and is being retried" from "this deployment is permanently refused and the approximate plane will never build" - which a monotonically rising `repocontext.ann.build.corpus{coverage="denied"}` cannot do on its own. Any non-zero value warrants an operator: an index that should exist does not, and it will not appear by itself. |
| `repocontext.ann.build.slice` | `Counter<long>` | `{step}` | `repository`, `space`, `phase`, `progress`, `cause` | Build steps taken by an approximate-index coordinator, partitioned by what the step achieved, compared against the same coordinator's previous reading: `advanced` (it banked vectors or resolved partitions, so the build got closer to serving), `churned` (it moved the build phase while banking no vector and resolving no partition, so the step did something and got the build nowhere), `starved` (its slice hit the ingest deadline having consumed nothing, so the source could not be read fast enough to bank a single vector), `idle` (the step completed and changed nothing measurable, which is what a coordinator stepping over a corpus it cannot consume looks like), or `faulted` (the step threw rather than completing, so the store-of-record read could not be served at all - a different condition from a read that was served and returned nothing, and one whose remedy is the projection or the store behind it rather than the access gate or the embedding throughput). This is the only series on the approximate-build plane that fires **before** a build reaches `Ready`. Every other one is terminal - `repocontext.ann.build.corpus` at `Ready`, `repocontext.ann.build.denial_terminal` at a terminal denial, `repocontext.ann.partitioning` on a plane that has finished building, `repocontext.ann.sweep` at arming and never again - so without it the whole interval between `sweep{outcome="armed"}` and `Ready` emits nothing, and a coordinator grinding through slices that bank nothing is byte-identical in telemetry to one that never took a step. Both present as every arm of every counter at its primed zero. Read it beside `repocontext.ann.sweep{outcome="armed"}`: a total pinned at zero beside a non-zero `armed` means the coordinator is **not stepping**, which is a scheduling fault; a rising `starved` means a source read that cannot complete inside the slice; a `churned` arm rising without bound beside a flat `advanced` means it is stepping and moving between phases while banking nothing, which is the #2791 `Training -> Persisting -> Training` livelock and which read as steadily rising `advanced` until #2818 split the arm (a healthy build churns at most once per phase transition, so its churn arm is bounded by the phase count); a rising `idle` means it is stepping over a corpus it cannot consume; and a rising `faulted` means it is stepping and throwing, which is the reading that would otherwise be indistinguishable from not stepping at all, because a step that throws never reaches the record taken after it completes. The `faulted` arm alone carries a second tag, `cause`, naming the class of fault the tick raised: `scan-page-stalled`, `projection-stale`, `dependency-unavailable`, `plane-rejected`, or `unexpected`. It exists because the run-12 build faulted on every tick and localising it needed an 8 MB container log read by hand, which established that 39 of 39 faults were a single condition - a grain-call timeout reading the vector-index tree - that the counter alone could not name. Causes are classified across the **whole tick**, not only the build step: five further call sites on a tick can throw, three of them before the step is counted at all, and a fault at any of those previously left the tick silent in this series entirely. The twenty-two `(phase, progress)` arms of a plane are pre-minted at zero the first time that plane is armed, so an arm reading zero and an arm being absent are different observations; an absent arm is a collector fault, read from `lattice_metrics_series` and `lattice_metrics_dropped_measurements_by_family_total`. The `cause` values are deliberately **not** pre-minted: priming them would mint five faulted-arm series on a host that has never faulted, so anything counting series rather than values would read a healthy host as a faulting one. The consequence is that a cause reading zero is **uninterpretable rather than innocent** - it says only that no fault of that class has been recorded since process start, which is equally what a healthy host and a mis-wired classifier look like. Read a cause only once `faulted` itself is non-zero, and denominate the causes against it: they sum to it exactly. Three further dimensions - `repository`, `space`, and `phase` - name **where** a step was taken, and were added by issue #2855 because without them the series could not answer the question the epic's acceptance run turns on. `phase` is the load-bearing one: it names the stage of the build the step was executing when it was counted, and on the `faulted` arm it is read at the fault site from the index's own progress rather than snapshotted on entry, so it is exact rather than approximate. That precision is not incidental. A build step entered in `training` runs `Train()` and then `PersistTrainedAsync` **within the same step**, so an entry snapshot would file a persist fault under `training` and destroy the one distinction the tag exists to draw: a fault reading `ingesting` is a corpus that could not be read, which is an independent defect, while a fault reading `persisting` is a trained index that could not be **written into the vector-index tree** - and if that tree is itself the subject of an open fault, the persist fault is a downstream symptom of it rather than a second defect, so scoring the two separately would count one defect twice. The values are `coordinating`, `opening`, `ingesting`, `training`, `persisting`, and `reconciling`; the first two precede any build step and are therefore reachable on the `faulted` arm alone, which is why a plane primes twenty-two arms rather than thirty - the eight combinations a completing step can never reach are deliberately not minted. `repository` and `space` bound the plane the step belongs to. Their cardinality is the product of onboarded repositories and embedding spaces, which is exactly the number of durable indexes and build coordinators the host already runs - one apiece - so it is operator-chosen and small, and is **not** the per-grain cardinality class of issue #2518. `space` reads as `{model-id}/{dimension}`, or `unspecified` on a plane whose space is not yet resolved. A plane re-derived onto a new embedding model is a different build over a different corpus, so merging the two would hide a migration mid-flight. Priming is per plane and happens when a plane is first armed rather than when the process starts, because a process cannot know which planes exist and a primed series for a plane nobody armed claims a build nobody asked for; the consequence is that a host with no armed plane emits no series for this instrument at all, which is the correct reading of a host that has never built an index. |
| `repocontext.ann.partitioning` | `Counter<long>` | `{observation}` | `state` | Approximate-index planes observed at each maintenance turn, partitioned by whether the plane holds a partitioning and, when it does not, by why: `partitioned` (it answers from a trained partitioning), `unpartitioned-small` (it holds none and its corpus is below `MinimumTrainingCount`, which is the correct state at that size), or `unpartitioned-large` (it holds none although its corpus is at or above the minimum). The third arm is the entire reason the partition exists: before issue #2706 those two cases were the same observation, so a plane holding 7.5x the threshold across zero partitions was indistinguishable from one that was simply too small to train, and every semantic query was answered by brute-force scan with nothing reporting it. A sustained non-zero `unpartitioned-large` means exactly that, and warrants an operator. All three arms are pre-minted at zero when the reporter is constructed, so an arm reading zero and an arm being absent are different observations; an absent arm is a collector fault, read from `lattice_metrics_series` and `lattice_metrics_dropped_measurements_by_family_total`, not a statement about the plane.
| `repocontext.ann.repartition` | `Counter<long>` | `{training}` | `outcome` | Training passes taken because a plane's corpus crossed `MinimumTrainingCount` after an earlier training had declined to partition it, by outcome: `partitioned` (the pass produced a partitioning, so the plane now serves approximate) or `declined` (the corpus met the minimum yet still resolved to fewer than two partitions, so the plane stays exhaustive and exact and the next attempt waits until the corpus has doubled). This series reports the *repair*, where `repocontext.ann.partitioning` reports the *state*, so it is expected to read zero forever on a deployment that partitioned on its first build; a single `partitioned` is one latched plane healing itself. A rising `declined` is not a fault in the plane but a configuration one: a `MinimumTrainingCount` has been set below the corpus size at which a partitioning can actually be resolved. Both arms are pre-minted at zero. |
| `repocontext.ann.index.load` | `Counter<long>` | `{attempt}` | `outcome` | Attempts to load the durable approximate index into memory, partitioned by how the attempt ended: `fresh` (it started from nothing and completed, which is the first attempt on a healthy plane and also a deliberate reload), `resumed` (it continued progress banked by an earlier attempt that faulted, and completed), or `faulted` (it threw partway, banking its progress for the next attempt). Opening the index walks the whole identifier key map, which is `O(corpus)` and is the most timeout-prone read on the plane; before issue #2953 a walk that faulted discarded the partially built mapping with the instance holding it, so the next phase tick reissued the entire walk and every attempt regenerated the identical demand. **A resumed load and a restarted one are indistinguishable in their result** - same mapping, same phase, same vector count - so no other series anywhere separates them and a regression would be completely silent. That is the whole reason this instrument exists. It is a **two-sided discriminator** and both sides are needed: counting only `resumed` would leave a zero ambiguous between "nothing ever faulted, so nothing needed resuming" (health) and "everything faulted and none of it resumed" (the defect restored), whereas faults with no resumptions beside them is conclusively the defect and no faults at all is conclusively health. All three arms are pre-minted at zero when the reporter is constructed, so an arm reading zero and an arm being absent are different observations; an absent arm means the build did not ship, which is a different fact from "it shipped and never resumed". Deliberately a counter and not a histogram: the quantity of interest is how many attempts resumed rather than how long one took, and priming a histogram would fabricate a zero-valued sample that reads as a real measurement of an instantaneous operation. |
| `repocontext.retrieval.duration` | `Histogram<double>` | `s` | `tool`, `path` | End-to-end seconds for one retrieval tool call, tagged by the tool (`search`, `context`, `outline`, `related`) and by the retrieval path that answered it (`semantic.exact`, `semantic.approximate`, one of the `keyword.*` causes, `not_applicable` for a graph read that consults no vector plane, or `unresolved` for a call that ended - cancelled or faulted - before a path was settled). The path tag is what makes the figure interpretable rather than merely present: a fast keyword answer and a fast approximate answer mean opposite things about the health of the box. Recorded from a `finally`, **once per call, on every termination including cancellation and failure**, so the count is a true call total and is the denominator for the stage series below; the only measurement it can lose is one whose process died mid-call. A `context` call records exactly one row under `tool="context"` and none under `tool="search"` even though it runs a search internally, so the two tools' latencies never contaminate each other. |
| `repocontext.retrieval.stage.duration` | `Histogram<double>` | `s` | `stage`, `path` | Seconds spent inside one stage of a retrieval, tagged by stage - `embed` (the network hop to the embedding service, covering both its availability probe and the query embed), `vector_search` (the index scan), `hydrate` (reading each matched identity back from the store of record, which the index never returns a second copy of), `keyword_scan` (the BM25 fallback) - and by the same `path` value the enclosing call resolved to. Separating them is the whole point: an end-to-end figure alone cannot distinguish a slow embedder from a slow index from a slow store, and those are three different owners with three different fixes. **A stage is recorded if and only if it ran**, including when it ran and then failed, so this series is deliberately sparse and its zero does not describe itself. Denominate it with `repocontext.retrieval.duration` above: no `embed` beside a rising call total is a *measured* absence of the semantic path - an intended keyword-only host - whereas both reading zero means no retrieval ran at all. |
| `repocontext.retrieval.exact_gather.faults` | `Counter<long>` | `{fault}` | `fault` | Exact k-nearest-neighbour gathers that faulted, partitioned by the class of fault, which is the distinction the whole fallback ladder turns on: `stalled` (the tree abandoned its own page fill), `timed_out` (a call the gather issued never answered), `exhausted` (the gather could not allocate), `abandoned` (a deadline this process owns cancelled it - **not** a caller walking away, which is deliberately excluded so one client cannot arm a backoff shared by every other), or `propagated` (the fault said something about the index's contents rather than about capacity). A sixth arm, `deterministic`, is not a cause but a **verdict on the episode**: a capacity-shaped fault that has recurred consecutively with no intervening success has stopped behaving like capacity, and is reported rather than absorbed. The first four are absorbed into the exact-scan breaker's backoff and answered with keyword recall classified `keyword.exact_fallback_suppressed`; `propagated` and `deterministic` are **not** absorbed and surface as `keyword.index_degraded`. **Do not read a flat `propagated` count beside a climbing absorbed one as load.** That is the reading that ran issue #2948's six-hour total retrieval outage as capacity pressure: all 25 of its gather faults were individually textbook timeouts, so the per-event classification was right about each one and the aggregate was still wrong. A deterministic defect and sustained load produce the same per-event classification; what separates them is the fault **rate**, which load cannot hold at one hundred percent. That is what `deterministic` measures, and the first faults of an episode stay on their own cause arm, so a real episode reads as a short run of (say) `timed_out` followed by a long run of `deterministic` - which names both what faulted and that it stopped being transient. This series exists because issue #2749 had to be diagnosed by counting exception type names in a container's log - the ladder absorbed `ScanPageStalledException` only, which is a *subclass* of the `TimeoutException` the deployment actually raised, so the absorbed set matched nothing that was happening and the breaker's backoff never escalated past its initial delay. All six arms are pre-minted at zero when the reporter is constructed, so an arm reading zero and an arm being absent are different observations; an absent arm is a collector fault, read from `lattice_metrics_series` and `lattice_metrics_dropped_measurements_by_family_total`. |
| `repocontext.bootstrap.pass_arm_faults` | `Counter<long>` | `{fault}` | `arm`, `kind` | Indexing-pass arms that faulted, by the arm that faulted (`retire`, `ingest-files`, `ingest-symbols`, `ingest-memory`) and the fault kind (`scan-page-stalled`, otherwise the exception type name). Read it as a diagnosis of *which stage* aborted a pass, which previously required correlating log lines by timestamp. It is deliberately **not** denominated by passes started, because a single pass can fault on more than one arm. A rising `arm="retire"` is the one value that does not mean lost work: a retirement fault defers every removal to the next pass and the additions and updates already computed are still committed, so it reports deferral rather than an aborted pass. Carries no repository id - the accompanying warning does, at a cardinality logs can afford. |
| `repocontext.bootstrap.phase_cancelled` | `Counter<long>` | `{cancellation}` | `phase` | Indexing runs cancelled part-way through a phase, tagged by the phase that was executing (`Walking`, `Reconciling`, `Applying`, `Vectorising`). Durable structural writes already committed survive a cancellation, but everything the cancelled phase had accumulated and not yet banked is lost and a re-run pays for it again, so this counts **discarded work**, not merely a stopped run. All four phase series are **zero-primed when the service is constructed**, so a flat zero is a reading - this deployment has discarded nothing - rather than the absence a counter reports before its first `Add`. A zero does **not** mean indexing is converging: a run that completes having made no progress is not a cancellation and is invisible here. Carries no repository id - the accompanying log line does, at a cardinality logs can afford. |
| `repocontext.bootstrap.phase_cancelled.discarded_time` | `Counter<long>` | `ms` | `phase` | Running total of run time thrown away by the cancellations above, tagged by the same phase. A counter rather than a histogram deliberately: the question this answers is "how much work has this deployment discarded", which is a total, and a counter is the only one of the two shapes that can be zero-primed without fabricating a sample that never happened. Zero-primed for the same four phases. Read it beside `repocontext.bootstrap.phase_cancelled` to get mean discarded time per cancellation; a rising total against a flat count is one long-running phase being abandoned repeatedly, which is the shape that starves a repository of an index indefinitely. |
| `repocontext.bootstrap.memory_marker_scan` | `Counter<long>` | `{walk}` | `outcome` | Walks of the embedded-memory-key marker range, partitioned by how the walk ended: `complete` (it exhausted the range in a single pass, having consumed no banked progress), `resumed` (it exhausted the range after consuming progress banked by an earlier pass that faulted), or `banked` (a page faulted, so it banked the pages already read and resumes from them on the next pass). The marker scan walks its range in small resumable pages so a walk that cannot finish inside one page-fill ceiling still converges within a bounded number of passes, instead of restarting from the beginning and never finishing at all (issue #2071). Whether it actually converges was previously observable only as the presence or absence of a warning in the host log, and the call site itself records why that is not enough: "the warning stopped" is a much weaker signal than "the range was exhausted", because the warning also stops when the scan is never reached at all. These arms make that three-state question readable from the scrape - all three at their primed zero means the scan was never reached, which no log grep can distinguish from a scan that ran and completed. `resumed` is split out of `complete` deliberately: folding them together would conflate "never needed to bank" with "banked and recovered", which are opposite answers about whether the resumable cursor is live, so the mechanism would be unobservable exactly when it is working. A rising `banked` with both completion arms flat is the thrash the cursor exists to prevent. All three arms are pre-minted at zero when the reporter is constructed, so an arm reading zero and an arm being absent are different observations. Named `banked` rather than `faulted` (which is what the structurally similar arm on `repocontext.ann.index.load` is called) because the fault here is swallowed and the pass continues successfully with a usable skip signal, so `faulted` would make the scrape assert something false. Deliberately a counter and not a histogram: the quantity of interest is how many walks ended each way, and priming a histogram would fabricate a zero-valued sample that reads as a real measurement. A `complete` reading is **not** evidence that bootstrap ingestion as a whole is healthy - the coverage-probe stand-down paths beside it are silent by design and are tracked separately (issue #2964). |
| `repocontext.bootstrap.coverage_probe` | `Counter<long>` | `{probe}` | `arm`, `outcome` | Bootstrap embedding-coverage resolutions, partitioned by the ingestion arm that resolved coverage (`file`, `symbol`, `sweep`) and by how the resolution ended (`conclusive`, `gate_pruned`, `probe_failed`). Each arm decides, before it does its work, whether it can trust an absence of embedding-membership keys as evidence that embeddings are genuinely missing. When the store''s read-path access gate prunes that membership probe the arm cannot trust the absence, so it stands down: it skips the back-fill sweep and proceeds with reduced work. That stand-down was previously invisible on the scrape, and invisible in a way worse than an ordinary missing signal, because it is produced by a correctly functioning safety gate - the probe answered, the gate did its job, nothing on the path looks wrong at the point the signal is lost, so there is no error, no fault, and no warning an operator has any reason to expect (issue #2964). On the scrape, "did less work because there was less to do" and "did less work because it was not allowed to look" render identically. These arms separate them. Read the instrument four ways: **all nine zero** means no bootstrap coverage resolution was reached at all; **`conclusive` only** is the healthy steady state; **`gate_pruned` > 0** is a standing misconfiguration that is actionable and never clears by waiting, because the ingestor cannot read its own membership keys; and **`probe_failed` > 0 with `gate_pruned` == 0 in the same arm** means that zero proves nothing, since the gate check sits structurally below the probe-failure branch at every site, so a failing probe masks whatever the gate would have done. Four limits on the reading, each load-bearing. First, the `arm` dimension localises which consumer stood down, not which grant is missing: all three probes funnel through one `ProbeMembershipAsync`, so there is exactly one grant to repair regardless of how many arms report. Second, five further gate-pruning decision sites exist that are deliberately not instrumented here because they do not stand an arm down, and their absence from this instrument is not evidence the gate is not pruning there. Third, `conclusive` means "this arm resolved coverage it can trust", not "a network probe succeeded" - the file arm may be served from the coverage digest without probing at all. Fourth, the file arm has two guard returns above this seam (no embedding provider bound, and nothing to embed), so all-nine-zero is also the normal steady state of a keyword-only deployment with no embedder bound; that benign cause and a genuinely unreached seam are **not separable by this instrument alone**, and a reader must check whether an embedding provider is registered to tell them apart. All nine pairs are pre-minted at zero when the reporter is constructed, on the same path the arms are charged from, so an arm reading zero and an arm being absent are different observations. Deliberately a counter and not a histogram: the quantity of interest is how many resolutions ended each way, and priming a histogram would fabricate a zero-valued sample that reads as a real measurement. |
| `repocontext.bootstrap.symbol_walk` | `Counter<long>` | `{walk}` | `outcome` | Passes of the symbol arm's whole-symbol-space range walk, partitioned by how the pass ended: `complete` (it closed a circuit in a single pass, having consumed no banked progress), `resumed` (it closed a circuit after consuming progress banked by an earlier pass that faulted), or `banked` (a page read faulted, so it banked the continuation token it had reached and resumes from there on the next pass). Before issue #2953 a faulted page discarded every page the pass had already read and the next pass restarted at the head, re-issuing the identical leaf reads. That re-drive is not a bystander to the stall that caused it: scan-page issued leaf reads measure at roughly 98% of cold WAL replay permit demand on a deployed container, against 1.6% for tombstone compaction and 0.16% for the WAL-GC blocked-leaf sweep, so a restarting walk regenerates exactly the load that made it fault. The cycle has no exit - the walk never closes a circuit, so the arm never banks a snapshot, so the tree's WAL cursor floor never advances and nothing is reclaimed. `resumed` is split out of `complete` for the same reason as on `memory_marker_scan`: folding them together would conflate "never needed to bank" with "banked and recovered", which are opposite answers about whether the cursor is live, and at the tree the two passes are byte-identical - so without this split the fix would be unfalsifiable in the deployment it exists to fix. A rising `banked` with both completion arms flat is the thrash the cursor exists to prevent, and is the reading that falsifies the fix. All three arms are pre-minted at zero when the reporter is constructed, so an arm reading zero and an arm being absent are different observations. Named `banked` rather than `faulted` because the fault is rethrown to the arm's own fault accounting after the pass has landed its partial work, so the bank is a durable side effect of the pass rather than its outcome. The memory arm has no counterpart arm here and is deliberately still re-driving: its walk builds the live-key set that drives the orphan sweep, so a partial walk would present as a partial live-key set and delete live vectors. |

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
