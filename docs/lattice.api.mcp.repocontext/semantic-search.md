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

## Embedding presence is tracked and self-healed

Whether a file has a live embedding is tracked independently of its content, as an add-wins **membership set** of stable source identifiers - not the embeddings themselves - on the vector-membership tree. This matters because content-digest change detection and embedding presence answer different questions: a file whose digest is unchanged is structurally skipped on re-index, but digest equality says nothing about whether its vector was ever written. A vector can be missing for reasons unrelated to content - the embedder was unavailable at first onboarding, an earlier run failed part-way, or the model space changed - and the presence set is what catches exactly those gaps.

The embedding pass is therefore an idempotent **back-fill**: it embeds every file the membership set reports as missing and skips the rest, so re-running it converges to zero new embeds once every file is present. The per-repository [self-index grain](tools.md#staying-fully-indexed-the-self-index-grain) drives this continuously - a cheap keys-only scan probes the membership set for the first unembedded file and re-drives the index to close the gap - so embeddings heal on their own once an unavailable embedder returns, without a client call and without re-hashing unchanged content.

## Embedding-space safety

Every stored vector carries its embedding-space identity (model, dimension, normalisation). A query embedding is compared only against vectors in the same space; a wrong-space vector is never compared or stored. This means switching the embedding model does not silently mix incompatible vectors - re-embed to populate the new space.

## Projections are rebuildable

The vector trees hold discardable, regenerable projections of the store of record, not primary data. Payloads are content-addressed and write-once; membership is an add-wins set of stable source ids; metadata carries the space tag and source linkage. A re-embed deletes the stale presence keys (leaving tree tombstones the compactor reclaims) and writes fresh ones, so live membership stays bounded and there is one payload per key. Losing or rebuilding the vector trees never loses context - only the semantic index, which a re-bootstrap regenerates.
