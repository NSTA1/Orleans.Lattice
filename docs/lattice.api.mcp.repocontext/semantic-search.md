# Semantic search

`repocontext_search` answers a natural-language query with the records most relevant to it, ranked best-first and hydrated from the store of record. It has two paths and always returns the best available answer rather than failing.

## The two paths

- **Semantic.** When an embedding provider is bound and vectors exist for the repository, the query is embedded and matched against the stored vectors with an exact nearest-neighbour (kNN) search. The result's `mode` is `semantic`.
- **Keyword.** When no embedding provider is bound, the provider is unavailable, or the query fails to embed, search degrades to a deterministic keyword/structural scan over the store. The scan ranks over each record's key, path, topic, tags, and - via the per-file **content projection** - the file's body text, so a keyword query matches file **content**, not just filenames and identifiers. It walks the structural, memory, and content trees; it does not scan the symbol tree, so a symbol's fully-qualified name is not part of the keyword haystack. Ranking is Okapi BM25 (see below), so a distinctive term outweighs a ubiquitous one and no single flooded field can dominate. The result's `mode` is `keyword`.

If nothing matches at all, `mode` is `empty`. The path that answered is always reported, so a caller can tell meaning-based retrieval from a fallback scan.

## Keyword search over file content

The keyword path is not limited to filenames and symbol names. During the structural reconcile, every text file's bounded body text is written to the dedicated [content projection tree](record-model.md#content-projection) at `repo/{repoId}/content/{path}`. The keyword scanner folds that body text into each candidate's searchable haystack, so a query token present only inside a file (not in its path or any declared identifier) still matches. This is deliberately **decoupled from the embedding provider**: the content projection is populated by the indexing walk regardless of whether an embedder is bound, precisely so the no-embedder path is more than filename matching. A repository indexed before the content projection existed is healed by an idempotent content back-fill (see [record-model.md](record-model.md#content-projection)). The scan keeps its existing bounded-candidate safety limit, so folding in content does not change its cost profile.

The keyword scan reads three trees - the structural, memory, and content projections - and each is scanned **in isolation**. The underlying range scan already recovers transparently from a transient enumerator abort (silo failover, cold start, idle expiry, scale-down) via its retry budget. A **terminal** fault materialising one tree - for example a stale leaf projection whose durable checkpoint has fallen off the write-ahead log and awaits an operator-driven rebuild, which the retry budget rightly does not swallow - is caught per tree and logged, and the scan ranks over the remaining healthy trees rather than collapsing to `empty`. Because the content projection is a rebuildable derived index, a keyword query stays useful (filename and identifier matches, memory) even while that projection is being rebuilt.

## BM25 ranking

The keyword path ranks with Okapi **BM25** computed over the bounded candidate set the scan already gathered, not a flat count of matched tokens. This gives ranking three properties a token-overlap count cannot express:

- **Inverse document frequency.** A term that occurs in few candidates contributes more than one that occurs in nearly all of them, so a distinctive identifier outranks a ubiquitous keyword.
- **Term-frequency saturation.** A term's contribution rises with its frequency in a record but saturates, so a field flooded with the query term cannot run away with the ranking.
- **Length normalisation.** A record's length is measured against the candidate-set average, so a short, on-topic record is not buried under a long one that merely mentions the term in passing.

Fields are weighted so a name-like match (title, path, fully-qualified name, tags) outranks an incidental body mention, and pure-noise fields (content digest, byte size, line numbers, timestamps) are excluded from ranking entirely. Text is tokenised **identifier-aware**: a token is split on non-alphanumeric characters and on identifier boundaries - a `camelCase` hump or a letter/digit transition - and lower-cased, so a query term matches a sub-token of a compound identifier (`order` matches `OrderService`). The ranker holds no state and touches no store, and the scan keeps its existing bounded-candidate safety limit, so BM25 does not change the cost profile. Ties break on ordinal key order, so ranking is deterministic.

## Warm vector cache behind the exact-kNN index

The semantic path range-scans all vector metadata and decodes every vector payload for a repository on each query. A warm in-memory cache sits behind the exact-kNN index and holds the decoded candidate set per `(repoId, embedding space)`, so repeated queries between writes skip the re-scan and re-decode. The cache is transparent: a hit is filtered by the query's embedding space exactly as the uncached scan is, so it produces byte-identical ranking and recall. It is kept correct two ways - a local write to a repository's vectors invalidates its cached sets immediately and precisely, and a bounded time-to-live (default 30s, configurable via `LATTICE_VECTOR_CACHE_TTL_SECONDS`) backstops any change that bypasses the local writer, such as a vector landing through cross-cluster replication. Setting the TTL to zero disables the cache, reproducing the original scan-every-query behaviour.

## The embedding seam

Embedding is provided by an `IEmbeddingProvider` the host binds (for example the Onyx embedding companion). The provider is fail-closed by contract: it never throws, and reports its own availability, so a missing or unhealthy embedder degrades search to keyword recall instead of erroring. The bundled container points its default embedding provider at a separate embedding companion container, keeping the MCP host a single-listener surface.

### Binding and configuring the bundled provider

`AddOnyxEmbeddingProvider` binds the client for the companion model-server container as the singleton `IEmbeddingProvider`. The registration is `TryAdd`, so a host that has already bound its own provider (OpenAI, Azure OpenAI, a self-hosted endpoint) keeps it, and a host that wants a different provider simply does not call this method.

```csharp verify
using Orleans.Lattice.Api.Mcp.RepoContext;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddOnyxEmbeddingProvider(o =>
{
    o.BaseAddress = new Uri("http://embedding:9000");
    o.ModelName = OnyxEmbeddingOptions.DefaultModelName;
    o.Dimension = OnyxEmbeddingOptions.DefaultDimension;
});
```

The optional callback populates `OnyxEmbeddingOptions`. Every default matches the model and endpoint baked into the shipped `apps/embedding` image, so an unconfigured host targets the companion container as-is:

| Option | Type | Default | Meaning |
|---|---|---|---|
| `BaseAddress` | `Uri` | `http://localhost:9000` | Base address of the companion model-server container. The health probe is issued against `api/health` and embeds against `encoder/bi-encoder-embed`, both relative to this address. Point it at the managed or external endpoint in a cloud deployment. |
| `ModelName` | `string` | `nomic-ai/nomic-embed-text-v1` | The HuggingFace model id the server embeds with. Must match a model baked into (or mounted into) the container. |
| `Dimension` | `int` | 768 | The vector dimension `ModelName` produces. Used to build the provider's embedding space and to fail-closed-reject any response whose vectors are a different length. |
| `MaxContextLength` | `int` | 512 | The maximum context length, in tokens, sent with each embed request. Longer inputs are truncated by the model server. |
| `NormalizeEmbeddings` | `bool` | `true` | Whether the server L2-normalizes the returned vectors. Reflected in the provider's embedding space. |
| `RequestTimeout` | `TimeSpan?` | (unset) | Optional per-request timeout on the underlying HTTP client. When unset the ambient `HttpClient` default applies. A timeout elapsing is a fail-closed failure, not an exception surfaced to the caller. |

Changing `ModelName`, `Dimension`, or `NormalizeEmbeddings` selects a **new embedding space** and must be paired with the matching model in the container - see [Embedding-space safety](#embedding-space-safety) below.

## Where vectors come from

Vectors are produced by the indexing path. When the host has enabled writes, `AddRepoContextTools` wires the embed-and-store ingestor in place of the deferred no-op, so an onboarding run embeds the files it added or updated and lands their vectors on the reserved vector trees. A later search then finds them by meaning. If no embedder is bound at onboarding time, no vectors are written and search stays on the keyword path until vectors exist.

## What gets embedded: windowed passages, symbols, and memory

Indexing embeds a repository at three granularities so broad-file, pinpoint, and remembered-knowledge queries all land.

- **Windowed file passages.** Rather than embedding only a file's leading window, indexing reads the leading ~64K characters of the file, chunks that span into up to 32 overlapping character windows, and stores one vector per passage. Content deeper in a large file is now reachable up to that bound, and the overlap keeps a match that straddles a window boundary from being lost; content beyond the leading ~64K characters (or past the 32-window cap) is not embedded and stays reachable only through keyword search over the content projection. Every passage vector links back to the same file source, so the file is still recalled as one record.
- **Symbol passages.** Each structural symbol record (namespace, type, interface, enum, method, property, field, or function) is embedded as its own passage built from its kind, fully-qualified name, and signature. This gives function- and type-level recall: a query about a specific operation can match the symbol directly instead of only the file that contains it. Symbol embeddings are driven from the [structural reconcile](record-model.md) - a changed symbol is re-embedded and a pruned symbol's vector is retired - and any symbol lacking a live embedding is back-filled on the next onboarding pass.

- **Memory passages.** Each durable agent-memory entry (a decision, gotcha, convention, or glossary term) is embedded as its own passage built from its kind, topic and id, title, tags, and body, and is chunked exactly as a file is so a long entry's tail stays reachable. Before this, only files and symbols were embedded, so a healthy semantic index could not return a captured entry at all and memory was reachable only through the degraded keyword path - which meant a session that searched, found nothing, and concluded the entry had never been written would simply write it again. Memory embeddings are driven from the same reconcile as symbols: an entry is re-embedded when it is written, its vector is retired when it is forgotten, any entry lacking a live embedding is back-filled, and a sweep retires the vector of an entry that expired by its own time-to-live, which nothing else observes.
Because a single file now contributes several passage vectors, `repocontext_search` over-fetches an enlarged candidate pool from the nearest-neighbour search and **deduplicates hits by source**, so a file matched by several of its passages hydrates once as a single result. The best-ranked passage decides the source's position. Symbol hits hydrate from their own canonical symbol record.

## Embedding presence is tracked and self-healed

Whether a source has a live embedding is tracked independently of its content, as add-wins **membership** of stable source identifiers - one presence flag per source, not the embeddings themselves - on the vector-membership tree. Membership covers all three embedded sources: files, symbols, and memory entries. This matters because content-digest change detection and embedding presence answer different questions: a file whose digest is unchanged is structurally skipped on re-index, but digest equality says nothing about whether its vector was ever written. A vector can be missing for reasons unrelated to content - the embedder was unavailable at first onboarding, an earlier run failed part-way, or the model space changed - and the presence set is what catches exactly those gaps.

The embedding pass is therefore an idempotent **back-fill**: it embeds every source the membership reports as missing and skips the rest, so re-running it converges to zero new embeds once every source is present. The per-repository [self-index grain](tools.md#staying-fully-indexed-the-self-index-grain) drives the file back-fill continuously - a cheap keys-only structural scan probes membership for the first unembedded file and re-drives the index to close the gap - so embeddings heal on their own once an unavailable embedder returns, without a client call and without re-hashing unchanged content.

A file that is genuinely empty - zero bytes, whitespace only, or content that chunks to no passage - would otherwise never gain a membership flag, because there is nothing to embed. That would leave it permanently "missing" to the back-fill, so the gap scan would re-select and re-read it on every reconcile and never converge. To close that, a considered-but-contentless file is recorded with a distinct **contentless marker** in the same membership tree (a reserved-prefix flag that carries no vector). A file is treated as covered when it has either a real embedding or a contentless marker, so an empty file is considered exactly once and then left alone. The marker is deliberately excluded from `embeddedVectorCount`, which stays an honest tally of sources that carry a real vector; it is cleared automatically when the file later gains embeddable content (its real embedding takes over) or when the file is deleted.

## Embedding-space safety

Every stored vector carries its embedding-space identity (model, dimension, normalisation). A query embedding is compared only against vectors in the same space; a wrong-space vector is never compared or stored. This means switching the embedding model does not silently mix incompatible vectors - re-embed to populate the new space.

## Projections are rebuildable

The vector trees hold discardable, regenerable projections of the store of record, not primary data. Payloads are content-addressed and write-once; membership is an add-wins presence flag per stable source id; metadata carries the space tag and source linkage. A re-embed deletes the stale presence keys (leaving tree tombstones the compactor reclaims) and writes fresh ones, so live membership stays bounded and there is one payload per key. Losing or rebuilding the vector trees never loses context - only the semantic index, which a re-bootstrap regenerates.

## A vector tree that falls off the write-ahead log is auto re-derived

Because the vector-metadata and vector-membership trees are rebuildable derived projections, the repository-context layer heals the one terminal fault the retry budget deliberately does not swallow: a leaf whose durable projection checkpoint has been trimmed past the write-ahead log with no covering snapshot. Such a leaf can never activate again - it surfaces `LeafProjectionStaleException` on every activation - so, left alone, every ingest write and gap-scan probe against that tree would spin in a permanent failing state.

The heal is a **cure, not a mask**. Every vector-plane write and coverage probe runs through a guard at the single seam where the target tree is a known local constant. When that guard observes the terminal fall-off, it always logs the originating exception with its full stack trace and increments a dedicated `repocontext.vectorplane.rederive` counter (tagged with the tree and the outcome) **before** any remediation, then triggers a bounded, single-flight, idempotent re-derivation of that one tree: the terminal state is reset so the tree activates clean, and the always-on gap scanner and back-fill re-embed every uncovered source from the store-of-record structural, symbol, and memory trees plus the working files. A re-derivation already in flight for a tree is joined rather than restarted, and the originating fault still propagates so the current pass fails loudly and the next always-on pass converges once the reset has landed.

Re-derivation is **fail-closed**. It applies only to the two rebuildable vector projections (`repo-context-vector-metadata` and `repo-context-vector-membership`). The write-once, content-addressed payload tree is excluded (it has no in-place deletes and cannot be re-derived by a drop-and-re-embed), and every store-of-record tree - structural, symbol, agent memory - is refused outright: resetting one of those would be real data loss. A refused fall-off is still surfaced (logged and metered) but never auto-reset.


## Scheduling the approximate index build

The approximate index is what makes a semantic query sub-linear in the corpus, and building it means streaming that corpus once. **The build is scheduled, not triggered by traffic.** It used to be armed by a declining query through a fire-and-forget background task, which put the work that accelerates queries behind a query: the task died with the process with nothing to resume it, the first query after a restart both paid the un-indexed cost and was the trigger, and a repository nobody queried never indexed itself at all.

Scheduling now sits on the same reminder-anchored coordinator pattern the tree coordinators use. One internal coordinator grain exists per `(repository, embedding space)` pair, keyed by the repository and the space's fingerprint:

- Its **phase timer** advances the build by exactly one bounded slice per tick, so a query arriving mid-build is answered by the exact scan immediately rather than queueing behind the build.
- Its **keep-alive reminder** reactivates it after a silo restart while work remains, and its activation hook re-arms the pump - so a build interrupted by a process death resumes on the restart itself. The reminder is the retry, and a durable one, which is why there is no in-process retry loop.
- Orleans' **single-threaded activation** is what keeps two builds off one index, in place of an in-process flag that a process death forgets.
- On reaching `Ready` it unregisters its reminder and deactivates, so a converged repository costs nothing.

A **startup sweep** arms a coordinator for every registered repository and re-sweeps periodically, so a restored volume with no client traffic at all converges to a serving index, and a repository onboarded later is picked up. The self-index grain also arms it directly when it drives an indexing pass, so a freshly onboarded repository converges on that pass rather than on the following sweep.

Arming is idempotent. A coordinator that finds its index already built still performs one step, which **reloads the persisted index into the process** rather than leaving that for the next query - the reload is roughly twenty times cheaper than a rebuild, and paying it off the request path is the point.

**Kill switch.** `LATTICE_REPOCONTEXT_ANN_INDEX_SCHEDULING=false` turns scheduling off. The off state is honest rather than a fall-back to the old behaviour: nothing is scheduled, an index that is not already built is never built, and every semantic query is answered by the exact scan with complete recall. Setting `LATTICE_REPOCONTEXT_SEMANTIC_RETRIEVAL=exact` implies the same thing, because such a host maintains no index at all.

**Multi-silo note.** A coordinator is a single cluster-wide activation, and the in-memory index a query is served from is per silo. On a multi-silo host the coordinator's silo is warmed with no query; another silo opens its own handle on its first query, which is a **reload** of the already-built index rather than a rebuild. The expensive half - streaming the corpus - is paid once, off the request path, whichever silo hosts the coordinator.

## How a superseded index is retired

There are three distinct transitions, and all three are now handled.

1. **No index to index (adoption).** An existing deployment starts with no index. Until one is built, queries are answered by the exact scan with complete recall, and `retrievalPath` reports which path answered. No operator action, no migration step.
2. **Generation to generation, within one embedding space (retrain or rebuild).** A generation covers a whole partitioning, so a retrain writes a **fresh** generation and flips the manifest to it rather than editing the live one. The manifest is written last by every flush, so it is the atomic swap point: the previous generation stays queryable until the flip, and a crash mid-write leaves it live rather than a half-built one. The superseded generation is then retired by prefix delete.
3. **Space to space, that is a new model or dimension.** The index prefix is keyed by `(repository, embedding space)`, so a model change produces a wholly separate index. That separation is deliberate - retirement works by prefix delete, so two spaces sharing a prefix would delete each other's generations - but it left the abandoned space resident forever: invisible to queries, harmless to correctness, and hundreds of megabytes per abandoned space.

The build coordinator closes the third case, because it is the only component that knows which space is *current* for a repository. On reaching `Ready` it retires the sibling prefixes of its own repository whose space fingerprint is not the live one.

Three properties make that safe:

- **Strictly after `Ready`.** Until the replacement can answer, the space it replaces is the only thing a failed re-embed could fall back to.
- **Repository-scoped and bounded.** Every space a repository has been indexed under is a sibling in one contiguous ordinal range, because the repository sorts *before* the space fingerprint in the key. The walk therefore reads one key per space and skips a whole space in a single hop, rather than enumerating hundreds of thousands of records - and it is keys-only, so it never reads the very data it exists to remove. Another repository's index is never in reach.
- **Abort-resilient.** The walk goes through the resilient scan wrapper, because a reclaimed remote enumerator yields a short result, and a short result here reads as "no more spaces" - silently leaving a superseded index behind.

**Kill switch.** `LATTICE_REPOCONTEXT_ANN_INDEX_RECLAMATION=false` keeps superseded spaces, which is the setting to use when an operator wants a previous space retained for a deliberate roll-back.
