# Semantic search

`repocontext_search` answers a natural-language query with the records most relevant to it, ranked best-first and hydrated from the store of record. It has two paths and always returns the best available answer rather than failing.

## The two paths

- **Semantic.** When an embedding provider is bound and vectors exist for the repository, the query is embedded and matched against the stored vectors with an exact nearest-neighbour (kNN) search. The result's `mode` is `semantic`.
- **Keyword.** When no embedding provider is bound, the provider is unavailable, or the query fails to embed, search degrades to a deterministic keyword/structural scan over the store. The result's `mode` is `keyword`.

If nothing matches at all, `mode` is `empty`. The path that answered is always reported, so a caller can tell meaning-based retrieval from a fallback scan.

## The embedding seam

Embedding is provided by an `IEmbeddingProvider` the host binds (for example the Onyx embedding companion). The provider is fail-closed by contract: it never throws, and reports its own availability, so a missing or unhealthy embedder degrades search to keyword recall instead of erroring. The bundled container points its default embedding provider at a separate Onyx model-server container, keeping the MCP host a single-listener surface.

## Where vectors come from

Vectors are produced by the indexing path. When the host has enabled writes, `AddRepoContextTools` wires the embed-and-store ingestor in place of the deferred no-op, so an onboarding run embeds the files it added or updated and lands their vectors on the reserved vector trees. A later search then finds them by meaning. If no embedder is bound at onboarding time, no vectors are written and search stays on the keyword path until vectors exist.

## What gets embedded: windowed passages and symbols

Indexing embeds a repository at two granularities so both broad-file and pinpoint queries land.

- **Windowed file passages.** Rather than embedding only a file's leading window, indexing slides an overlapping character window across the whole file and stores one vector per passage. Content deep in a large file is now reachable, and the overlap keeps a match that straddles a window boundary from being lost. Every passage vector links back to the same file source, so the file is still recalled as one record.
- **Symbol passages.** Each structural symbol record (namespace, type, interface, enum, method, property, field, or function) is embedded as its own passage built from its kind, fully-qualified name, and signature. This gives function- and type-level recall: a query about a specific operation can match the symbol directly instead of only the file that contains it. Symbol embeddings are driven from the [structural reconcile](record-model.md) - a changed symbol is re-embedded and a pruned symbol's vector is retired - and any symbol lacking a live embedding is back-filled on the next onboarding pass.

Because a single file now contributes several passage vectors, `repocontext_search` over-fetches an enlarged candidate pool from the nearest-neighbour search and **deduplicates hits by source**, so a file matched by several of its passages hydrates once as a single result. The best-ranked passage decides the source's position. Symbol hits hydrate from their own canonical symbol record.

## Embedding presence is tracked and self-healed

Whether a source has a live embedding is tracked independently of its content, as add-wins **membership** of stable source identifiers - one presence flag per source, not the embeddings themselves - on the vector-membership tree. Membership covers both embedded sources: files and symbols. This matters because content-digest change detection and embedding presence answer different questions: a file whose digest is unchanged is structurally skipped on re-index, but digest equality says nothing about whether its vector was ever written. A vector can be missing for reasons unrelated to content - the embedder was unavailable at first onboarding, an earlier run failed part-way, or the model space changed - and the presence set is what catches exactly those gaps.

The embedding pass is therefore an idempotent **back-fill**: it embeds every source the membership reports as missing and skips the rest, so re-running it converges to zero new embeds once every source is present. The per-repository [self-index grain](tools.md#staying-fully-indexed-the-self-index-grain) drives the file back-fill continuously - a cheap keys-only structural scan probes membership for the first unembedded file and re-drives the index to close the gap - so embeddings heal on their own once an unavailable embedder returns, without a client call and without re-hashing unchanged content.

## Embedding-space safety

Every stored vector carries its embedding-space identity (model, dimension, normalisation). A query embedding is compared only against vectors in the same space; a wrong-space vector is never compared or stored. This means switching the embedding model does not silently mix incompatible vectors - re-embed to populate the new space.

## Projections are rebuildable

The vector trees hold discardable, regenerable projections of the store of record, not primary data. Payloads are content-addressed and write-once; membership is an add-wins presence flag per stable source id; metadata carries the space tag and source linkage. A re-embed deletes the stale presence keys (leaving tree tombstones the compactor reclaims) and writes fresh ones, so live membership stays bounded and there is one payload per key. Losing or rebuilding the vector trees never loses context - only the semantic index, which a re-bootstrap regenerates.
